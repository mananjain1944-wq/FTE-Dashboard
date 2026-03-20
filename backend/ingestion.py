import pandas as pd
import numpy as np
from datetime import date, datetime
import time
from database import get_conn
from intelligence import (
    normalize_text, normalize_region, normalize_category,
    compute_audit_alert, compute_risk_scores
)

def safe_date(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        if isinstance(val, (datetime, date)):
            return val if isinstance(val, date) else val.date()
        d = pd.to_datetime(val, errors='coerce')
        if pd.isna(d):
            return None
        return d.date()
    except:
        return None

def safe_float(val):
    if val is None:
        return None
    try:
        v = float(val)
        if np.isnan(v):
            return None
        return v
    except:
        return None

def safe_str(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ['nan', 'none', ''] else None

def convert_score(val):
    """All scores are percentages stored as decimals — multiply by 100"""
    v = safe_float(val)
    if v is None:
        return None
    # If already looks like a percentage (> 1.5), keep as is
    if v > 1.5:
        return round(v, 2)
    # Convert decimal to percentage
    return round(v * 100, 2)

def ingest_excel(filepath, filename):
    start = time.time()
    conn = get_conn()
    cur = conn.cursor()

    result = {
        "filename": filename,
        "total_rows": 0,
        "new_vendors": 0,
        "new_audits": 0,
        "updated_audits": 0,
        "skipped": 0,
        "errors": 0,
        "error_details": [],
        "status": "SUCCESS"
    }

    try:
        df = pd.read_excel(filepath, engine='openpyxl', header=0)
        df.columns = [str(c).strip() for c in df.columns]
        result["total_rows"] = len(df)

        for idx, row in df.iterrows():
            try:
                # Extract and clean fields
                vendor_name = safe_str(row.get('Vendor'))
                if not vendor_name:
                    result["skipped"] += 1
                    continue

                vendor_code = safe_str(row.get('Vendor Code'))
                factory_address = safe_str(row.get('Factory Address'))
                factory_location = safe_str(row.get('Factory Location'))
                region = normalize_region(row.get('Sourcing Region'))
                product_category = normalize_category(row.get('Product Category'))
                sub_category = safe_str(row.get('Sub Category'))
                vendor_name_norm = normalize_text(vendor_name)

                # Scores — convert all from decimal to percentage
                self_score = convert_score(row.get('Vendor Self assessment  Score'))
                audit_1_score = convert_score(row.get('1st Audit Assessment  score'))
                audit_2_score = convert_score(row.get('2nd Audit Assessment score'))
                score_diff = safe_float(row.get('Difference'))
                if score_diff is not None:
                    score_diff = round(score_diff * 100, 2)

                # Dates
                sourcing_req = safe_date(row.get('Sourcing  Request Date'))
                fte_sent = safe_date(row.get('FTE Sent'))
                fte_received = safe_date(row.get('FTE received'))
                planned_audit = safe_date(row.get('Planned Audit window'))
                audit_1_date = safe_date(row.get('1st Audit Annual'))
                reaudit_1 = safe_date(row.get('1st Reaudit'))
                audit_2_due = safe_date(row.get('2nd Audit Due Date'))
                audit_2_date = safe_date(row.get('2nd Audit Annual'))

                # Other fields
                days_to_audit = safe_float(row.get('No. of Days'))
                audit_month = safe_float(row.get('Month'))
                vendor_status = safe_str(row.get('Status'))
                audit_alert_excel = safe_str(row.get('Audit Alert'))
                followup_status = safe_str(row.get('Column1'))
                action_note = safe_str(row.get('Column2'))
                remarks = safe_str(row.get('Remarks'))
                fscp = safe_str(row.get('FSCP'))
                factory_status = safe_str(row.get('Factory running status'))

                # Compute our own audit alert
                audit_alert_system = compute_audit_alert(audit_1_date, audit_2_due)

                # Check mismatch
                mismatch = False
                if audit_alert_excel and audit_alert_system:
                    if audit_alert_excel.strip().lower() != audit_alert_system.strip().lower():
                        mismatch = True

                # Use Excel value but flag mismatch
                audit_alert_final = audit_alert_excel or audit_alert_system

                # ── VENDOR UPSERT ──
                vendor_id = None
                if vendor_code:
                    cur.execute("SELECT id FROM vendors WHERE vendor_code=%s AND factory_address=%s", (vendor_code, factory_address))
                else:
                    cur.execute("SELECT id FROM vendors WHERE vendor_name_normalized=%s AND factory_address=%s", (vendor_name_norm, factory_address))

                existing = cur.fetchone()

                if existing:
                    vendor_id = existing[0]
                    cur.execute("""
                        UPDATE vendors SET
                            vendor_code=COALESCE(%s, vendor_code),
                            region=%s, product_category=%s, sub_category=%s,
                            factory_location=%s, factory_status=%s,
                            is_fscp=%s, updated_at=NOW()
                        WHERE id=%s
                    """, (vendor_code, region, product_category, sub_category,
                          factory_location, factory_status,
                          fscp == 'Yes', vendor_id))
                else:
                    cur.execute("""
                        INSERT INTO vendors (
                            vendor_code, vendor_name, vendor_name_normalized,
                            factory_address, factory_location, region,
                            product_category, sub_category, factory_status, is_fscp
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                    """, (vendor_code, vendor_name, vendor_name_norm,
                          factory_address, factory_location, region,
                          product_category, sub_category, factory_status,
                          fscp == 'Yes'))
                    vendor_id = cur.fetchone()[0]
                    result["new_vendors"] += 1

                # ── AUDIT UPSERT ──
                # Check if audit record exists for this vendor + audit_1_date
                if audit_1_date:
                    cur.execute("""
                        SELECT id, audit_1_score FROM audits
                        WHERE vendor_id=%s AND audit_1_date=%s
                    """, (vendor_id, audit_1_date))
                    existing_audit = cur.fetchone()
                else:
                    # No audit date — check if any record exists
                    cur.execute("SELECT id FROM audits WHERE vendor_id=%s AND audit_1_date IS NULL LIMIT 1", (vendor_id,))
                    existing_audit = cur.fetchone()

                if existing_audit:
                    # Update if score changed
                    if existing_audit[1] != audit_1_score:
                        cur.execute("""
                            UPDATE audits SET
                                audit_1_score=%s, self_assessment_score=%s,
                                audit_2_score=%s, score_difference=%s,
                                vendor_status=%s, audit_alert=%s,
                                audit_alert_excel=%s, audit_alert_mismatch=%s,
                                followup_status=%s, action_note=%s,
                                remarks=%s, updated_at=NOW()
                            WHERE id=%s
                        """, (audit_1_score, self_score, audit_2_score,
                              score_diff, vendor_status, audit_alert_final,
                              audit_alert_excel, mismatch, followup_status,
                              action_note, remarks, existing_audit[0]))
                        result["updated_audits"] += 1
                    else:
                        result["skipped"] += 1
                else:
                    cur.execute("""
                        INSERT INTO audits (
                            vendor_id, sourcing_request_date, fte_sent_date,
                            fte_received_date, planned_audit_date, audit_1_date,
                            reaudit_1_date, audit_2_due_date, self_assessment_score,
                            audit_1_score, audit_2_date, audit_2_score,
                            score_difference, days_to_audit, audit_month,
                            vendor_status, audit_alert, audit_alert_excel,
                            audit_alert_mismatch, followup_status, action_note,
                            remarks, fscp, factory_running_status
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        vendor_id, sourcing_req, fte_sent, fte_received,
                        planned_audit, audit_1_date, reaudit_1, audit_2_due,
                        self_score, audit_1_score, audit_2_date, audit_2_score,
                        score_diff, days_to_audit, audit_month, vendor_status,
                        audit_alert_final, audit_alert_excel, mismatch,
                        followup_status, action_note, remarks, fscp, factory_status
                    ))
                    result["new_audits"] += 1

                # Flag mismatch alert
                if mismatch:
                    cur.execute("""
                        INSERT INTO alerts (vendor_id, alert_type, message, severity)
                        VALUES (%s, 'Audit Alert Mismatch',
                            %s, 'LOW')
                    """, (vendor_id,
                          f"Excel says '{audit_alert_excel}' but system computes '{audit_alert_system}'"))

            except Exception as e:
                result["errors"] += 1
                result["error_details"].append(f"Row {idx}: {str(e)}")

        conn.commit()

        # Log batch
        duration = round(time.time() - start, 2)
        cur.execute("""
            INSERT INTO ingestion_log (
                filename, total_rows, new_vendors, new_audits,
                updated_audits, skipped, errors, error_details,
                status, duration_seconds
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            filename, result["total_rows"], result["new_vendors"],
            result["new_audits"], result["updated_audits"],
            result["skipped"], result["errors"],
            str(result["error_details"][:10]),
            result["status"], duration
        ))
        conn.commit()

        # Trigger intelligence engine
        compute_risk_scores()

    except Exception as e:
        result["status"] = "FAILED"
        result["error_details"].append(str(e))
        conn.rollback()
    finally:
        cur.close()
        conn.close()

    return result
