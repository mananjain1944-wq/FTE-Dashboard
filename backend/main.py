import os
import json
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from database import get_conn, init_db
from ingestion import ingest_excel
from intelligence import compute_risk_scores
import google.generativeai as genai
from datetime import date

app = FastAPI(title="FTE QA Intelligence Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))

@app.on_event("startup")
def startup():
    init_db()

# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "ok", "message": "FTE QA Dashboard API running"}

# ── UPLOAD ────────────────────────────────────────────────────────────────────
@app.post("/api/upload")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(400, "Only Excel files accepted")
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name
    result = ingest_excel(tmp_path, file.filename)
    os.unlink(tmp_path)
    return result

# ── OVERVIEW ──────────────────────────────────────────────────────────────────
@app.get("/api/overview")
def get_overview():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM vendors WHERE is_active=TRUE")
    total_vendors = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM vendors WHERE is_active=TRUE AND vendor_code IS NOT NULL")
    vendors_with_code = cur.fetchone()[0]

    cur.execute("SELECT AVG(latest_audit_score) FROM risk_scores WHERE latest_audit_score IS NOT NULL")
    avg_score = cur.fetchone()[0]

    cur.execute("SELECT risk_tier, COUNT(*) FROM risk_scores GROUP BY risk_tier")
    tiers = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT COUNT(*) FROM risk_scores WHERE is_overdue=TRUE")
    overdue = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM audits a
        JOIN vendors v ON a.vendor_id=v.id
        WHERE LOWER(a.vendor_status) LIKE '%approved%'
        AND LOWER(a.vendor_status) NOT LIKE '%not%'
        AND LOWER(a.vendor_status) NOT LIKE '%inactive%'
    """)
    approved = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM audits
        WHERE LOWER(vendor_status) LIKE '%reject%'
    """)
    rejected = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM alerts WHERE is_acknowledged=FALSE")
    unacked_alerts = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM audits
        WHERE audit_alert='Yet to Audit' OR audit_1_date IS NULL
    """)
    yet_to_audit = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        "total_vendors": total_vendors,
        "vendors_with_code": vendors_with_code,
        "avg_score": round(float(avg_score), 1) if avg_score else 0,
        "tiers": tiers,
        "overdue": overdue,
        "approved": approved,
        "rejected": rejected,
        "unacked_alerts": unacked_alerts,
        "yet_to_audit": yet_to_audit
    }

# ── VENDORS ───────────────────────────────────────────────────────────────────
@app.get("/api/vendors")
def get_vendors(
    region: str = None,
    category: str = None,
    tier: str = None,
    status: str = None,
    search: str = None,
    limit: int = 100,
    offset: int = 0
):
    conn = get_conn()
    cur = conn.cursor()

    where = ["v.is_active=TRUE"]
    params = []

    if region:
        where.append("v.region ILIKE %s")
        params.append(f"%{region}%")
    if category:
        where.append("v.product_category ILIKE %s")
        params.append(f"%{category}%")
    if tier:
        where.append("r.risk_tier=%s")
        params.append(tier.upper())
    if status:
        where.append("a.vendor_status ILIKE %s")
        params.append(f"%{status}%")
    if search:
        where.append("(v.vendor_name ILIKE %s OR v.factory_address ILIKE %s OR v.vendor_code::text ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    where_str = " AND ".join(where)

    cur.execute(f"""
        SELECT
            v.id, v.vendor_code, v.vendor_name, v.factory_address,
            v.factory_location, v.region, v.product_category, v.sub_category,
            v.factory_status,
            r.composite_score, r.risk_tier, r.latest_audit_score,
            r.self_assessment_score, r.score_gap, r.trend_direction,
            r.trend_delta, r.is_overdue, r.days_overdue,
            r.audit_alert, r.vendor_status,
            r.ai_recommended_next_audit, r.ai_audit_reason,
            r.days_since_last_audit,
            a.audit_1_date, a.audit_2_due_date, a.followup_status,
            a.action_note, a.remarks
        FROM vendors v
        LEFT JOIN risk_scores r ON r.vendor_id=v.id
        LEFT JOIN LATERAL (
            SELECT * FROM audits WHERE vendor_id=v.id
            ORDER BY audit_1_date DESC NULLS LAST LIMIT 1
        ) a ON TRUE
        WHERE {where_str}
        ORDER BY r.composite_score ASC NULLS LAST
        LIMIT %s OFFSET %s
    """, params + [limit, offset])

    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # Convert dates to strings
    for row in rows:
        for k, v in row.items():
            if isinstance(v, date):
                row[k] = str(v)

    cur.execute(f"""
        SELECT COUNT(*) FROM vendors v
        LEFT JOIN risk_scores r ON r.vendor_id=v.id
        LEFT JOIN LATERAL (
            SELECT * FROM audits WHERE vendor_id=v.id
            ORDER BY audit_1_date DESC NULLS LAST LIMIT 1
        ) a ON TRUE
        WHERE {where_str}
    """, params)
    total = cur.fetchone()[0]

    cur.close()
    conn.close()
    return {"vendors": rows, "total": total}

# ── VENDOR DETAIL ─────────────────────────────────────────────────────────────
@app.get("/api/vendors/{vendor_id}")
def get_vendor_detail(vendor_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT v.*, r.* FROM vendors v
        LEFT JOIN risk_scores r ON r.vendor_id=v.id
        WHERE v.id=%s
    """, (vendor_id,))
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Vendor not found")
    vendor = dict(zip(cols, row))

    cur.execute("""
        SELECT * FROM audits WHERE vendor_id=%s
        ORDER BY audit_1_date DESC NULLS LAST
    """, (vendor_id,))
    cols2 = [d[0] for d in cur.description]
    audits = [dict(zip(cols2, r)) for r in cur.fetchall()]

    for a in audits:
        for k, v in a.items():
            if isinstance(v, date):
                a[k] = str(v)

    for k, v in vendor.items():
        if isinstance(v, date):
            vendor[k] = str(v)

    cur.close()
    conn.close()
    return {"vendor": vendor, "audits": audits}

# ── ANALYTICS ────────────────────────────────────────────────────────────────
@app.get("/api/analytics/regions")
def get_region_analytics():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT v.region,
            COUNT(DISTINCT v.id) as vendor_count,
            AVG(r.latest_audit_score) as avg_score,
            COUNT(DISTINCT CASE WHEN r.risk_tier='CRITICAL' THEN v.id END) as critical,
            COUNT(DISTINCT CASE WHEN r.risk_tier='AT-RISK' THEN v.id END) as at_risk,
            COUNT(DISTINCT CASE WHEN r.risk_tier='STABLE' THEN v.id END) as stable,
            COUNT(DISTINCT CASE WHEN r.risk_tier='EXCELLENT' THEN v.id END) as excellent,
            COUNT(DISTINCT CASE WHEN LOWER(a.vendor_status) LIKE '%approved%'
                AND LOWER(a.vendor_status) NOT LIKE '%not%' THEN v.id END) as approved,
            COUNT(DISTINCT CASE WHEN LOWER(a.vendor_status) LIKE '%reject%' THEN v.id END) as rejected
        FROM vendors v
        LEFT JOIN risk_scores r ON r.vendor_id=v.id
        LEFT JOIN LATERAL (
            SELECT * FROM audits WHERE vendor_id=v.id
            ORDER BY audit_1_date DESC NULLS LAST LIMIT 1
        ) a ON TRUE
        WHERE v.is_active=TRUE AND v.region IS NOT NULL AND v.region != ''
        GROUP BY v.region
        ORDER BY avg_score DESC NULLS LAST
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    for r in rows:
        if r.get('avg_score'):
            r['avg_score'] = round(float(r['avg_score']), 1)

    cur.close()
    conn.close()
    return rows

@app.get("/api/analytics/scores")
def get_score_distribution():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(CASE WHEN latest_audit_score < 40 THEN 1 END) as "0-40",
            COUNT(CASE WHEN latest_audit_score >= 40 AND latest_audit_score < 50 THEN 1 END) as "40-50",
            COUNT(CASE WHEN latest_audit_score >= 50 AND latest_audit_score < 60 THEN 1 END) as "50-60",
            COUNT(CASE WHEN latest_audit_score >= 60 AND latest_audit_score < 70 THEN 1 END) as "60-70",
            COUNT(CASE WHEN latest_audit_score >= 70 AND latest_audit_score < 80 THEN 1 END) as "70-80",
            COUNT(CASE WHEN latest_audit_score >= 80 AND latest_audit_score < 90 THEN 1 END) as "80-90",
            COUNT(CASE WHEN latest_audit_score >= 90 THEN 1 END) as "90+"
        FROM risk_scores WHERE latest_audit_score IS NOT NULL
    """)
    row = cur.fetchone()
    labels = ["0-40%","40-50%","50-60%","60-70%","70-80%","80-90%","90%+"]
    cur.close()
    conn.close()
    return [{"range": labels[i], "count": row[i]} for i in range(7)]

@app.get("/api/analytics/categories")
def get_category_analytics():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT v.product_category,
            COUNT(DISTINCT v.id) as count,
            AVG(r.latest_audit_score) as avg_score
        FROM vendors v
        LEFT JOIN risk_scores r ON r.vendor_id=v.id
        WHERE v.is_active=TRUE AND v.product_category IS NOT NULL
        GROUP BY v.product_category
        ORDER BY avg_score DESC NULLS LAST
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    for r in rows:
        if r.get('avg_score'):
            r['avg_score'] = round(float(r['avg_score']), 1)
    cur.close()
    conn.close()
    return rows

@app.get("/api/analytics/alerts-status")
def get_alert_status():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT audit_alert, COUNT(*) FROM audits
        WHERE audit_alert IS NOT NULL
        GROUP BY audit_alert
        ORDER BY COUNT(*) DESC
    """)
    rows = [{"status": r[0], "count": r[1]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows

@app.get("/api/analytics/trend")
def get_score_trend():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            EXTRACT(YEAR FROM audit_1_date) as year,
            AVG(audit_1_score) as avg_score,
            COUNT(*) as count
        FROM audits
        WHERE audit_1_date IS NOT NULL AND audit_1_score IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM audit_1_date)
        ORDER BY year
    """)
    rows = [{"year": int(r[0]), "avg_score": round(float(r[1]),1), "count": r[2]} for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows

# ── ALERTS ────────────────────────────────────────────────────────────────────
@app.get("/api/alerts")
def get_alerts(acknowledged: bool = False):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT a.id, a.alert_type, a.message, a.severity,
               a.is_acknowledged, a.created_at,
               v.vendor_name, v.region, v.id as vendor_id
        FROM alerts a
        JOIN vendors v ON v.id=a.vendor_id
        WHERE a.is_acknowledged=%s
        ORDER BY
            CASE a.severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
            a.created_at DESC
        LIMIT 100
    """, (acknowledged,))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    for r in rows:
        for k, v in r.items():
            if isinstance(v, date):
                r[k] = str(v)
    cur.close()
    conn.close()
    return rows

@app.post("/api/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE alerts SET is_acknowledged=TRUE, acknowledged_at=NOW() WHERE id=%s", (alert_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"success": True}

@app.post("/api/alerts/acknowledge-all")
def acknowledge_all():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE alerts SET is_acknowledged=TRUE, acknowledged_at=NOW() WHERE is_acknowledged=FALSE")
    conn.commit()
    cur.close()
    conn.close()
    return {"success": True}

# ── INGESTION LOG ────────────────────────────────────────────────────────────
@app.get("/api/ingestion-log")
def get_ingestion_log():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM ingestion_log ORDER BY created_at DESC LIMIT 20")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    for r in rows:
        for k, v in r.items():
            if isinstance(v, date):
                r[k] = str(v)
    cur.close()
    conn.close()
    return rows

# ── AI CHAT ───────────────────────────────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str
    history: list = []

@app.post("/api/chat")
def chat(req: ChatRequest):
    conn = get_conn()
    cur = conn.cursor()

    # Get live stats for context
    cur.execute("SELECT COUNT(*) FROM vendors WHERE is_active=TRUE")
    total_vendors = cur.fetchone()[0]

    cur.execute("SELECT AVG(latest_audit_score) FROM risk_scores WHERE latest_audit_score IS NOT NULL")
    avg_score = cur.fetchone()[0]

    cur.execute("SELECT region, AVG(latest_audit_score), COUNT(*) FROM vendors v JOIN risk_scores r ON r.vendor_id=v.id WHERE v.region IS NOT NULL GROUP BY region ORDER BY AVG(latest_audit_score) DESC")
    regions = cur.fetchall()

    cur.execute("SELECT risk_tier, COUNT(*) FROM risk_scores GROUP BY risk_tier")
    tiers = cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM risk_scores WHERE is_overdue=TRUE")
    overdue = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM alerts WHERE is_acknowledged=FALSE")
    active_alerts = cur.fetchone()[0]

    cur.execute("""
        SELECT v.vendor_name, v.region, r.latest_audit_score, r.risk_tier, r.audit_alert
        FROM vendors v JOIN risk_scores r ON r.vendor_id=v.id
        WHERE r.risk_tier IN ('CRITICAL','AT-RISK')
        ORDER BY r.composite_score ASC NULLS LAST LIMIT 10
    """)
    at_risk_vendors = cur.fetchall()

    cur.execute("""
        SELECT v.vendor_name, v.region, a.followup_status
        FROM vendors v JOIN audits a ON a.vendor_id=v.id
        WHERE LOWER(a.followup_status) LIKE '%cap%'
        LIMIT 10
    """)
    cap_vendors = cur.fetchall()

    cur.execute("""
        SELECT v.vendor_name, v.factory_address, v.region, r.latest_audit_score, r.risk_tier,
               a.vendor_status, a.followup_status, a.audit_alert, r.ai_recommended_next_audit, r.ai_audit_reason
        FROM vendors v
        JOIN risk_scores r ON r.vendor_id=v.id
        LEFT JOIN LATERAL (SELECT * FROM audits WHERE vendor_id=v.id ORDER BY audit_1_date DESC LIMIT 1) a ON TRUE
        ORDER BY v.vendor_name
        LIMIT 50
    """)
    all_vendors_data = cur.fetchall()

    cur.close()
    conn.close()

    region_summary = "\n".join([f"- {r[0]}: avg {round(float(r[1]),1) if r[1] else 'N/A'}%, {r[2]} vendors" for r in regions])
    tier_summary = "\n".join([f"- {t[0]}: {t[1]} vendors" for t in tiers])
    risk_list = "\n".join([f"- {v[0]} ({v[1]}): {v[2]}%, {v[3]}, Alert: {v[4]}" for v in at_risk_vendors])
    cap_list = "\n".join([f"- {v[0]} ({v[1]}): {v[2]}" for v in cap_vendors])
    vendor_details = "\n".join([
        f"- {v[0]} | Address: {v[1]} | Region: {v[2]} | Score: {v[3]}% | Tier: {v[4]} | Status: {v[5]} | Followup: {v[6]} | Alert: {v[7]} | AI Next Audit: {v[8]} | Reason: {v[9]}"
        for v in all_vendors_data
    ])

    system_prompt = f"""You are an intelligent QA audit assistant for a fashion sourcing company.
You have access to real live data from the FTE QA Intelligence Dashboard.

LIVE DATABASE SUMMARY:
- Total vendors: {total_vendors}
- Average audit score: {round(float(avg_score),1) if avg_score else 'N/A'}%
- Overdue audits: {overdue}
- Active alerts: {active_alerts}

REGION SCORES:
{region_summary}

RISK TIERS:
{tier_summary}

AT-RISK / CRITICAL VENDORS:
{risk_list}

CAP OPEN VENDORS:
{cap_list}

ALL VENDOR DETAILS (for specific queries):
{vendor_details}

Answer questions about vendors, scores, regions, audit dates, risk tiers, CAP status.
You can search by vendor name, factory address, region, score, or any other field.
Be specific with numbers. If asked about a specific vendor or factory unit, search the vendor details above.
Keep answers concise and actionable."""

    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        history = []
        for h in req.history[-6:]:
            history.append({"role": h["role"], "parts": [h["content"]]})

        chat_session = model.start_chat(history=history)
        full_prompt = system_prompt + "\n\nUser question: " + req.message
        response = chat_session.send_message(full_prompt)
        return {"response": response.text}
    except Exception as e:
        return {"response": f"Error connecting to AI: {str(e)}"}

# ── RECOMPUTE ────────────────────────────────────────────────────────────────
@app.post("/api/recompute")
def recompute():
    compute_risk_scores()
    return {"success": True, "message": "Risk scores recomputed"}
