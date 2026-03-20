import os
import json
from datetime import date, timedelta
from database import get_conn
import google.generativeai as genai

genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

def normalize_text(s):
    if not s:
        return ""
    return str(s).strip().lower()

def normalize_region(s):
    if not s:
        return ""
    return str(s).strip().title()

def normalize_category(s):
    if not s:
        return ""
    s = str(s).strip().title()
    if "Apparel" in s:
        return "Apparel"
    if "Footwear" in s or "Foot" in s:
        return "Footwear"
    if "Access" in s:
        return "Accessories"
    return s

def compute_audit_alert(audit_1_date, audit_2_due_date):
    today = date.today()
    if not audit_1_date:
        return "Yet to Audit"
    if audit_2_due_date and today > audit_2_due_date:
        return "Pending / Due"
    return "OK"

def compute_risk_tier(score):
    if score is None:
        return "UNSCORED"
    if score >= 80:
        return "EXCELLENT"
    if score >= 65:
        return "STABLE"
    if score >= 50:
        return "AT-RISK"
    return "CRITICAL"

def compute_composite_score(latest_score, prev_score, is_overdue, days_overdue, vendor_status, self_score):
    if latest_score is None:
        return None

    score = latest_score * 0.50

    # Trend component
    if prev_score is not None:
        trend = (latest_score - prev_score)
        score += min(max(trend * 0.20, -20), 20)

    # Overdue penalty
    if is_overdue and days_overdue:
        penalty = min(days_overdue / 7, 15)
        score -= penalty * 0.15

    # Self assessment gap
    if self_score is not None:
        gap = latest_score - self_score
        if gap < -15:
            score -= 5

    # Status modifier
    status_map = {
        "approved": 3, "not approved need to re-audit": -10,
        "not approved": -10, "rejected": -15,
        "business terminated": -20, "inactive": -5
    }
    s = normalize_text(vendor_status)
    for k, v in status_map.items():
        if k in s:
            score += v
            break

    return max(0, min(100, score))

def ai_predict_next_audit(vendor_name, latest_score, trend, tier, cap_open, consecutive_failures, last_audit_date):
    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        prompt = f"""You are a QA audit scheduling expert for a fashion sourcing company.

Vendor: {vendor_name}
Latest Audit Score: {latest_score}%
Score Trend: {trend}
Risk Tier: {tier}
CAP Open: {cap_open}
Consecutive Failures: {consecutive_failures}
Last Audit Date: {last_audit_date}
Today: {date.today()}

Standard audit cycle is 11 months. Based on the vendor's performance, recommend:
1. Next audit date (YYYY-MM-DD format)
2. One sentence reason

Respond ONLY in this JSON format with no other text:
{{"next_audit_date": "YYYY-MM-DD", "reason": "one sentence"}}"""

        response = model.generate_content(prompt)
        text = response.text.strip()
        if "```" in text:
            text = text.split("```")[1].replace("json","").strip()
        result = json.loads(text)
        return result.get("next_audit_date"), result.get("reason")
    except Exception as e:
        # Fallback: rule-based
        if not last_audit_date:
            return None, None
        if tier == "CRITICAL":
            delta = 90
            reason = "Critical tier — audit scheduled in 3 months"
        elif tier == "AT-RISK" or cap_open:
            delta = 180
            reason = "AT-RISK or CAP open — audit scheduled in 6 months"
        elif tier == "EXCELLENT":
            delta = 365
            reason = "Excellent performance — standard annual cycle extended"
        else:
            delta = 330
            reason = "Stable performance — standard 11-month cycle"
        next_date = last_audit_date + timedelta(days=delta)
        return str(next_date), reason

def compute_risk_scores():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM vendors WHERE is_active = TRUE")
    vendor_ids = [r[0] for r in cur.fetchall()]

    for vid in vendor_ids:
        # Get all audits for vendor ordered by date
        cur.execute("""
            SELECT audit_1_score, self_assessment_score, audit_1_date,
                   audit_2_due_date, vendor_status, followup_status,
                   audit_alert, audit_2_score
            FROM audits WHERE vendor_id = %s
            ORDER BY audit_1_date DESC NULLS LAST
        """, (vid,))
        audits = cur.fetchall()

        if not audits:
            continue

        latest = audits[0]
        latest_score = latest[0]
        self_score = latest[1]
        last_audit_date = latest[2]
        due_date = latest[3]
        vendor_status = latest[4]
        followup = latest[5]
        audit_alert = latest[6]

        # Also check audit_2_score
        if latest[7] and latest_score:
            latest_score = max(latest_score, latest[7])

        prev_score = audits[1][0] if len(audits) > 1 else None

        # Overdue
        today = date.today()
        is_overdue = False
        days_overdue = 0
        if due_date and today > due_date:
            is_overdue = True
            days_overdue = (today - due_date).days

        # Trend
        trend_direction = "STABLE"
        trend_delta = 0
        if latest_score is not None and prev_score is not None:
            trend_delta = latest_score - prev_score
            if trend_delta > 3:
                trend_direction = "IMPROVING"
            elif trend_delta < -3:
                trend_direction = "DECLINING"

        # Score gap
        score_gap = None
        if latest_score is not None and self_score is not None:
            score_gap = latest_score - self_score

        # Composite
        composite = compute_composite_score(
            latest_score, prev_score, is_overdue,
            days_overdue, vendor_status, self_score
        )

        tier = compute_risk_tier(composite or latest_score)

        # Days since last audit
        days_since = None
        if last_audit_date:
            days_since = (today - last_audit_date).days

        # CAP open
        cap_open = followup and "cap" in str(followup).lower()

        # Consecutive failures
        failures = sum(1 for a in audits if a[0] and a[0] < 50)

        # AI predicted next audit
        next_audit, reason = ai_predict_next_audit(
            f"Vendor ID {vid}", latest_score, trend_direction,
            tier, cap_open, failures, last_audit_date
        )

        # Upsert risk score
        cur.execute("""
            INSERT INTO risk_scores (
                vendor_id, composite_score, risk_tier, latest_audit_score,
                self_assessment_score, score_gap, trend_direction, trend_delta,
                days_since_last_audit, is_overdue, days_overdue,
                audit_alert, vendor_status, ai_recommended_next_audit,
                ai_audit_reason, last_computed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (vendor_id) DO UPDATE SET
                composite_score=EXCLUDED.composite_score,
                risk_tier=EXCLUDED.risk_tier,
                latest_audit_score=EXCLUDED.latest_audit_score,
                self_assessment_score=EXCLUDED.self_assessment_score,
                score_gap=EXCLUDED.score_gap,
                trend_direction=EXCLUDED.trend_direction,
                trend_delta=EXCLUDED.trend_delta,
                days_since_last_audit=EXCLUDED.days_since_last_audit,
                is_overdue=EXCLUDED.is_overdue,
                days_overdue=EXCLUDED.days_overdue,
                audit_alert=EXCLUDED.audit_alert,
                vendor_status=EXCLUDED.vendor_status,
                ai_recommended_next_audit=EXCLUDED.ai_recommended_next_audit,
                ai_audit_reason=EXCLUDED.ai_audit_reason,
                last_computed_at=NOW()
        """, (
            vid, composite, tier, latest_score, self_score, score_gap,
            trend_direction, trend_delta, days_since, is_overdue, days_overdue,
            audit_alert, vendor_status, next_audit, reason
        ))

        # Generate alerts
        generate_alerts(cur, vid, composite, tier, is_overdue, days_overdue,
                       trend_delta, score_gap, cap_open, vendor_status)

    conn.commit()
    cur.close()
    conn.close()

def generate_alerts(cur, vendor_id, score, tier, is_overdue, days_overdue, trend_delta, score_gap, cap_open, vendor_status):
    alerts = []

    if score and score < 50:
        alerts.append(("Score Below Threshold", f"Score {score:.1f}% is below 50% threshold", "CRITICAL"))

    if tier == "CRITICAL":
        alerts.append(("Critical Tier", f"Vendor has entered CRITICAL risk tier", "CRITICAL"))

    if is_overdue and days_overdue > 14:
        alerts.append(("Overdue Audit", f"Audit overdue by {days_overdue} days", "HIGH"))
    elif is_overdue:
        alerts.append(("Overdue Audit", f"Audit overdue by {days_overdue} days", "MEDIUM"))

    if trend_delta and trend_delta < -10:
        alerts.append(("Score Drop", f"Score dropped {abs(trend_delta):.1f} points vs previous audit", "HIGH"))

    if score_gap and score_gap < -20:
        alerts.append(("Self-Assessment Gap", f"Vendor overrated itself by {abs(score_gap):.1f} points", "MEDIUM"))

    if cap_open:
        alerts.append(("CAP Required", "Corrective Action Plan is open", "MEDIUM"))

    if vendor_status and "rejected" in str(vendor_status).lower():
        alerts.append(("Vendor Rejected", "Vendor status is Rejected", "HIGH"))

    for alert_type, message, severity in alerts:
        cur.execute("""
            INSERT INTO alerts (vendor_id, alert_type, message, severity)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM alerts
                WHERE vendor_id=%s AND alert_type=%s
                AND is_acknowledged=FALSE
                AND created_at > NOW() - INTERVAL '24 hours'
            )
        """, (vendor_id, alert_type, message, severity,
              vendor_id, alert_type))
