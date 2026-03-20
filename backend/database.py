import psycopg2
import os

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
            id SERIAL PRIMARY KEY,
            vendor_code VARCHAR(50),
            vendor_name VARCHAR(300) NOT NULL,
            vendor_name_normalized VARCHAR(300),
            factory_address TEXT,
            factory_location VARCHAR(100),
            region VARCHAR(50),
            product_category VARCHAR(50),
            sub_category VARCHAR(100),
            is_fscp BOOLEAN DEFAULT FALSE,
            factory_status VARCHAR(50),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(vendor_code, factory_address),
            UNIQUE(vendor_name_normalized, factory_address)
        );

        CREATE TABLE IF NOT EXISTS audits (
            id SERIAL PRIMARY KEY,
            vendor_id INTEGER REFERENCES vendors(id),
            sourcing_request_date DATE,
            fte_sent_date DATE,
            fte_received_date DATE,
            planned_audit_date DATE,
            audit_1_date DATE,
            reaudit_1_date DATE,
            audit_2_due_date DATE,
            self_assessment_score DECIMAL(6,2),
            audit_1_score DECIMAL(6,2),
            audit_2_date DATE,
            audit_2_score DECIMAL(6,2),
            score_difference DECIMAL(6,2),
            days_to_audit INTEGER,
            audit_month INTEGER,
            vendor_status VARCHAR(100),
            audit_alert VARCHAR(100),
            audit_alert_excel VARCHAR(100),
            audit_alert_mismatch BOOLEAN DEFAULT FALSE,
            followup_status VARCHAR(200),
            action_note VARCHAR(200),
            remarks TEXT,
            fscp VARCHAR(10),
            factory_running_status VARCHAR(50),
            batch_id INTEGER,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS risk_scores (
            id SERIAL PRIMARY KEY,
            vendor_id INTEGER REFERENCES vendors(id) UNIQUE,
            composite_score DECIMAL(6,2),
            risk_tier VARCHAR(20),
            latest_audit_score DECIMAL(6,2),
            self_assessment_score DECIMAL(6,2),
            score_gap DECIMAL(6,2),
            trend_direction VARCHAR(20),
            trend_delta DECIMAL(6,2),
            days_since_last_audit INTEGER,
            is_overdue BOOLEAN DEFAULT FALSE,
            days_overdue INTEGER DEFAULT 0,
            audit_alert VARCHAR(100),
            vendor_status VARCHAR(100),
            ai_recommended_next_audit DATE,
            ai_audit_reason TEXT,
            last_computed_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS ingestion_log (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(300),
            total_rows INTEGER,
            new_vendors INTEGER DEFAULT 0,
            new_audits INTEGER DEFAULT 0,
            updated_audits INTEGER DEFAULT 0,
            skipped INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            error_details TEXT,
            status VARCHAR(20),
            duration_seconds DECIMAL(6,2),
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id SERIAL PRIMARY KEY,
            vendor_id INTEGER REFERENCES vendors(id),
            alert_type VARCHAR(100),
            message TEXT,
            severity VARCHAR(20),
            is_acknowledged BOOLEAN DEFAULT FALSE,
            acknowledged_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_audits_vendor ON audits(vendor_id);
        CREATE INDEX IF NOT EXISTS idx_audits_date ON audits(audit_1_date);
        CREATE INDEX IF NOT EXISTS idx_risk_vendor ON risk_scores(vendor_id);
        CREATE INDEX IF NOT EXISTS idx_alerts_vendor ON alerts(vendor_id);
        CREATE INDEX IF NOT EXISTS idx_vendors_code ON vendors(vendor_code);
        CREATE INDEX IF NOT EXISTS idx_vendors_region ON vendors(region);
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized successfully.")
