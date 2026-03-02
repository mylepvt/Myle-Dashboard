import sqlite3
import os

DATABASE = os.path.join(os.path.dirname(__file__), 'leads.db')


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
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
            notes          TEXT,
            created_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
            updated_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
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
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            username     TEXT    NOT NULL UNIQUE,
            password     TEXT    NOT NULL,
            role         TEXT    NOT NULL DEFAULT 'team',
            fbo_id       TEXT    NOT NULL DEFAULT '',
            upline_name  TEXT    NOT NULL DEFAULT '',
            phone        TEXT    NOT NULL DEFAULT '',
            email        TEXT    NOT NULL DEFAULT '',
            status       TEXT    NOT NULL DEFAULT 'pending',
            created_at   TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    conn.commit()
    conn.close()


def migrate_db():
    """Safely add new columns to an existing database without data loss."""
    new_columns = [
        ("assigned_to",    "TEXT NOT NULL DEFAULT ''"),
        ("source",         "TEXT NOT NULL DEFAULT ''"),
        ("revenue",        "REAL NOT NULL DEFAULT 0.0"),
        ("follow_up_date", "TEXT NOT NULL DEFAULT ''"),
    ]
    conn = get_db()
    cursor = conn.cursor()
    for col, definition in new_columns:
        try:
            cursor.execute(f"ALTER TABLE leads ADD COLUMN {col} {definition}")
        except Exception:
            pass  # column already exists — safe to skip

    # Ensure users table exists (for databases created before this migration)
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

    # Add new user columns to existing databases
    for col, definition in [
        ("fbo_id",      "TEXT NOT NULL DEFAULT ''"),
        ("upline_name", "TEXT NOT NULL DEFAULT ''"),
        ("phone",       "TEXT NOT NULL DEFAULT ''"),
        ("email",       "TEXT NOT NULL DEFAULT ''"),
        ("status",      "TEXT NOT NULL DEFAULT 'pending'"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE users ADD COLUMN {col} {definition}")
        except Exception:
            pass  # column already exists

    conn.commit()
    conn.close()


def seed_users():
    """Create a default admin account if no users exist yet."""
    conn = get_db()
    cursor = conn.cursor()
    count = cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        cursor.execute(
            "INSERT INTO users (username, password, role, status) VALUES (?, ?, ?, ?)",
            ('admin', 'admin123', 'admin', 'approved')
        )
        conn.commit()
    else:
        # Ensure existing admin accounts are always approved (migration safety)
        cursor.execute(
            "UPDATE users SET status='approved' WHERE role='admin'"
        )
        conn.commit()
    conn.close()
