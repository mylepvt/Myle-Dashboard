import sqlite3
import os
from werkzeug.security import generate_password_hash, check_password_hash

# On Render: set DATABASE_PATH=/var/data/leads.db (persistent disk)
# Locally: falls back to leads.db in project folder
DATABASE = os.environ.get(
    'DATABASE_PATH',
    os.path.join(os.path.dirname(__file__), 'leads.db')
)


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA cache_size=-4000")   # 4 MB page cache
    return conn


def init_db():
    conn = get_db()
    cursor = conn.cursor()

    # Leads table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT    NOT NULL,
            phone          TEXT    NOT NULL,
            email          TEXT,
            referred_by    TEXT,
            assigned_to    TEXT    NOT NULL DEFAULT '',
            source         TEXT    NOT NULL DEFAULT '',
            status         TEXT    NOT NULL DEFAULT 'New',
            payment_done   INTEGER NOT NULL DEFAULT 0,
            payment_amount REAL    NOT NULL DEFAULT 0.0,
            revenue        REAL    NOT NULL DEFAULT 0.0,
            day1_done      INTEGER NOT NULL DEFAULT 0,
            day2_done      INTEGER NOT NULL DEFAULT 0,
            interview_done INTEGER NOT NULL DEFAULT 0,
            follow_up_date TEXT    NOT NULL DEFAULT '',
            call_result    TEXT    NOT NULL DEFAULT '',
            notes          TEXT,
            city           TEXT    NOT NULL DEFAULT '',
            deleted_at     TEXT    NOT NULL DEFAULT '',
            in_pool        INTEGER NOT NULL DEFAULT 0,
            pool_price     REAL    NOT NULL DEFAULT 0.0,
            claimed_at     TEXT    NOT NULL DEFAULT '',
            created_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
            updated_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Daily Reports table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_reports (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            username         TEXT    NOT NULL,
            upline_name      TEXT    NOT NULL DEFAULT '',
            report_date      TEXT    NOT NULL,
            total_calling    INTEGER NOT NULL DEFAULT 0,
            pdf_covered      INTEGER NOT NULL DEFAULT 0,
            calls_picked     INTEGER NOT NULL DEFAULT 0,
            wrong_numbers    INTEGER NOT NULL DEFAULT 0,
            enrollments_done INTEGER NOT NULL DEFAULT 0,
            pending_enroll   INTEGER NOT NULL DEFAULT 0,
            underage         INTEGER NOT NULL DEFAULT 0,
            leads_educated   TEXT    NOT NULL DEFAULT '',
            plan_2cc         INTEGER NOT NULL DEFAULT 0,
            seat_holdings    INTEGER NOT NULL DEFAULT 0,
            remarks          TEXT,
            submitted_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
            UNIQUE(username, report_date)
        )
    """)

    # Team members table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS team_members (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL UNIQUE,
            phone       TEXT,
            joined_at   TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Users table (authentication)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            username              TEXT    NOT NULL UNIQUE,
            password              TEXT    NOT NULL,
            role                  TEXT    NOT NULL DEFAULT 'team',
            fbo_id                TEXT    NOT NULL DEFAULT '',
            upline_name           TEXT    NOT NULL DEFAULT '',
            phone                 TEXT    NOT NULL DEFAULT '',
            email                 TEXT    NOT NULL DEFAULT '',
            status                TEXT    NOT NULL DEFAULT 'pending',
            display_picture       TEXT    NOT NULL DEFAULT '',
            calling_reminder_time TEXT    NOT NULL DEFAULT '',
            created_at            TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # App settings (key-value store)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT ''
        )
    """)

    # Wallet recharge requests
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wallet_recharges (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            username     TEXT    NOT NULL,
            amount       REAL    NOT NULL,
            utr_number   TEXT    NOT NULL DEFAULT '',
            status       TEXT    NOT NULL DEFAULT 'pending',
            requested_at TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
            processed_at TEXT    NOT NULL DEFAULT '',
            admin_note   TEXT    NOT NULL DEFAULT ''
        )
    """)

    # Admin announcements (notice board)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS announcements (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            message    TEXT    NOT NULL,
            created_by TEXT    NOT NULL DEFAULT 'admin',
            pin        INTEGER NOT NULL DEFAULT 0,
            created_at TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Lead notes / timeline
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lead_notes (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id    INTEGER NOT NULL,
            username   TEXT    NOT NULL,
            note       TEXT    NOT NULL,
            created_at TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Web Push subscriptions (VAPID)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username   TEXT    NOT NULL,
            endpoint   TEXT    NOT NULL UNIQUE,
            auth       TEXT    NOT NULL DEFAULT '',
            p256dh     TEXT    NOT NULL DEFAULT '',
            created_at TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Password reset tokens
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username   TEXT    NOT NULL,
            token      TEXT    NOT NULL UNIQUE,
            expires_at TEXT    NOT NULL,
            used       INTEGER NOT NULL DEFAULT 0,
            created_at TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Activity / Punch Log
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS activity_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username   TEXT    NOT NULL,
            event_type TEXT    NOT NULL,
            details    TEXT    NOT NULL DEFAULT '',
            ip_address TEXT    NOT NULL DEFAULT '',
            created_at TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    conn.commit()
    conn.close()


def migrate_db():
    """Safely add new columns to an existing database without data loss."""
    conn = get_db()
    cursor = conn.cursor()

    # --- leads table columns ---
    for col, definition in [
        ("assigned_to",    "TEXT NOT NULL DEFAULT ''"),
        ("source",         "TEXT NOT NULL DEFAULT ''"),
        ("revenue",        "REAL NOT NULL DEFAULT 0.0"),
        ("follow_up_date", "TEXT NOT NULL DEFAULT ''"),
        ("call_result",    "TEXT NOT NULL DEFAULT ''"),
        ("in_pool",        "INTEGER NOT NULL DEFAULT 0"),
        ("pool_price",     "REAL NOT NULL DEFAULT 0.0"),
        ("claimed_at",     "TEXT NOT NULL DEFAULT ''"),
        ("city",           "TEXT NOT NULL DEFAULT ''"),
        ("deleted_at",     "TEXT NOT NULL DEFAULT ''"),
        # Extended funnel fields
        ("track_selected",   "TEXT NOT NULL DEFAULT ''"),
        ("track_price",      "REAL NOT NULL DEFAULT 0.0"),
        ("seat_hold_amount", "REAL NOT NULL DEFAULT 0.0"),
        ("pending_amount",   "REAL NOT NULL DEFAULT 0.0"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE leads ADD COLUMN {col} {definition}")
        except Exception:
            pass  # column already exists

    # --- users table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                username     TEXT    NOT NULL UNIQUE,
                password     TEXT    NOT NULL,
                role         TEXT    NOT NULL DEFAULT 'team',
                fbo_id       TEXT    NOT NULL DEFAULT '',
                upline_name  TEXT    NOT NULL DEFAULT '',
                phone        TEXT    NOT NULL DEFAULT '',
                status       TEXT    NOT NULL DEFAULT 'pending',
                created_at   TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    for col, definition in [
        ("fbo_id",           "TEXT NOT NULL DEFAULT ''"),
        ("upline_name",      "TEXT NOT NULL DEFAULT ''"),
        ("phone",            "TEXT NOT NULL DEFAULT ''"),
        ("email",            "TEXT NOT NULL DEFAULT ''"),
        ("status",           "TEXT NOT NULL DEFAULT 'pending'"),
        ("display_picture",      "TEXT NOT NULL DEFAULT ''"),
        ("calling_reminder_time", "TEXT NOT NULL DEFAULT ''"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE users ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # --- daily_reports table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_reports (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                username         TEXT    NOT NULL,
                upline_name      TEXT    NOT NULL DEFAULT '',
                report_date      TEXT    NOT NULL,
                total_calling    INTEGER NOT NULL DEFAULT 0,
                pdf_covered      INTEGER NOT NULL DEFAULT 0,
                calls_picked     INTEGER NOT NULL DEFAULT 0,
                wrong_numbers    INTEGER NOT NULL DEFAULT 0,
                enrollments_done INTEGER NOT NULL DEFAULT 0,
                pending_enroll   INTEGER NOT NULL DEFAULT 0,
                underage         INTEGER NOT NULL DEFAULT 0,
                leads_educated   TEXT    NOT NULL DEFAULT '',
                plan_2cc         INTEGER NOT NULL DEFAULT 0,
                seat_holdings    INTEGER NOT NULL DEFAULT 0,
                remarks          TEXT,
                submitted_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
                UNIQUE(username, report_date)
            )
        """)
    except Exception:
        pass

    # --- app_settings table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
        """)
    except Exception:
        pass

    # --- wallet_recharges table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wallet_recharges (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                username     TEXT    NOT NULL,
                amount       REAL    NOT NULL,
                utr_number   TEXT    NOT NULL DEFAULT '',
                status       TEXT    NOT NULL DEFAULT 'pending',
                requested_at TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
                processed_at TEXT    NOT NULL DEFAULT '',
                admin_note   TEXT    NOT NULL DEFAULT ''
            )
        """)
    except Exception:
        pass

    # --- new tables (safe if already exist) ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS announcements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT NOT NULL,
                created_by TEXT NOT NULL DEFAULT 'admin',
                pin INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lead_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                note TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                username   TEXT    NOT NULL,
                endpoint   TEXT    NOT NULL UNIQUE,
                auth       TEXT    NOT NULL DEFAULT '',
                p256dh     TEXT    NOT NULL DEFAULT '',
                created_at TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                username   TEXT    NOT NULL,
                token      TEXT    NOT NULL UNIQUE,
                expires_at TEXT    NOT NULL,
                used       INTEGER NOT NULL DEFAULT 0,
                created_at TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # --- activity_log table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                username   TEXT    NOT NULL,
                event_type TEXT    NOT NULL,
                details    TEXT    NOT NULL DEFAULT '',
                ip_address TEXT    NOT NULL DEFAULT '',
                created_at TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # --- Performance indexes ---
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_leads_pool_assigned  ON leads(in_pool, assigned_to)",
        "CREATE INDEX IF NOT EXISTS idx_leads_pool_status    ON leads(in_pool, status)",
        "CREATE INDEX IF NOT EXISTS idx_leads_payment        ON leads(payment_done, in_pool)",
        "CREATE INDEX IF NOT EXISTS idx_leads_updated        ON leads(updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_leads_phone          ON leads(phone)",
        "CREATE INDEX IF NOT EXISTS idx_wallet_user_status   ON wallet_recharges(username, status)",
        "CREATE INDEX IF NOT EXISTS idx_reports_user_date    ON daily_reports(username, report_date)",
        "CREATE INDEX IF NOT EXISTS idx_leads_call_result ON leads(call_result, in_pool, deleted_at)",
        "CREATE INDEX IF NOT EXISTS idx_activity_user_time ON activity_log(username, created_at)",
    ]
    for idx in indexes:
        try:
            cursor.execute(idx)
        except Exception:
            pass

    conn.commit()
    conn.close()


def seed_users():
    """Create a default admin account if no users exist yet.
       Also auto-upgrades any legacy plain-text passwords to hashed."""
    conn = get_db()
    cursor = conn.cursor()
    count = cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        cursor.execute(
            "INSERT INTO users (username, password, role, status) VALUES (?, ?, ?, ?)",
            ('admin', generate_password_hash('admin123', method='pbkdf2:sha256'), 'admin', 'approved')
        )
        conn.commit()
    else:
        cursor.execute("UPDATE users SET status='approved' WHERE role='admin'")

        users = cursor.execute("SELECT id, password FROM users").fetchall()
        for u in users:
            pwd = u[1]
            if not pwd.startswith(('pbkdf2:', 'scrypt:', 'argon2:')):
                cursor.execute("UPDATE users SET password=? WHERE id=?",
                               (generate_password_hash(pwd, method='pbkdf2:sha256'), u[0]))

        conn.commit()
    conn.close()
