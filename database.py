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
    try:
        conn = sqlite3.connect(DATABASE, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA cache_size=-4000")   # 4 MB page cache
        return conn
    except sqlite3.Error as e:
        import logging
        logging.getLogger('database').error(f"DB connection failed: {e}")
        raise


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
            last_contacted TEXT    NOT NULL DEFAULT '',
            contact_count  INTEGER NOT NULL DEFAULT 0,
            created_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
            updated_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
            d1_morning     INTEGER NOT NULL DEFAULT 0,
            d1_afternoon   INTEGER NOT NULL DEFAULT 0,
            d1_evening     INTEGER NOT NULL DEFAULT 0,
            d2_morning     INTEGER NOT NULL DEFAULT 0,
            d2_afternoon   INTEGER NOT NULL DEFAULT 0,
            d2_evening     INTEGER NOT NULL DEFAULT 0,
            working_date   TEXT    NOT NULL DEFAULT '',
            daily_score    INTEGER NOT NULL DEFAULT 0
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
            training_required     INTEGER NOT NULL DEFAULT 0,
            training_status       TEXT    NOT NULL DEFAULT 'not_required',
            joining_date          TEXT    NOT NULL DEFAULT '',
            certificate_path      TEXT    NOT NULL DEFAULT '',
            badges_json           TEXT    NOT NULL DEFAULT '[]',
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

    # Training videos (one per day, 7 days)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS training_videos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            day_number  INTEGER NOT NULL UNIQUE,
            title       TEXT    NOT NULL DEFAULT '',
            youtube_url TEXT    NOT NULL DEFAULT '',
            podcast_url TEXT    NOT NULL DEFAULT '',
            pdf_url     TEXT    NOT NULL DEFAULT '',
            description TEXT    NOT NULL DEFAULT '',
            created_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Training progress per user per day
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS training_progress (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            username     TEXT    NOT NULL,
            day_number   INTEGER NOT NULL,
            completed    INTEGER NOT NULL DEFAULT 0,
            completed_at TEXT    NOT NULL DEFAULT '',
            UNIQUE(username, day_number)
        )
    """)

    # Monthly targets per member
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS targets (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            username     TEXT NOT NULL,
            metric       TEXT NOT NULL,
            target_value REAL NOT NULL DEFAULT 0,
            month        TEXT NOT NULL,
            created_by   TEXT NOT NULL DEFAULT 'admin',
            created_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            UNIQUE(username, metric, month)
        )
    """)

    # User achievement badges
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_badges (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT NOT NULL,
            badge_key   TEXT NOT NULL,
            unlocked_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            UNIQUE(username, badge_key)
        )
    """)

    # Training test questions (MCQ)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS training_questions (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            question       TEXT    NOT NULL,
            option_a       TEXT    NOT NULL DEFAULT '',
            option_b       TEXT    NOT NULL DEFAULT '',
            option_c       TEXT    NOT NULL DEFAULT '',
            option_d       TEXT    NOT NULL DEFAULT '',
            correct_answer TEXT    NOT NULL DEFAULT 'a',
            sort_order     INTEGER NOT NULL DEFAULT 0,
            created_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Training test attempt history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS training_test_attempts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            username        TEXT    NOT NULL,
            score           INTEGER NOT NULL DEFAULT 0,
            total_questions INTEGER NOT NULL DEFAULT 0,
            passed          INTEGER NOT NULL DEFAULT 0,
            attempted_at    TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
        )
    """)

    # Bonus/additional videos
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bonus_videos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT    NOT NULL DEFAULT '',
            youtube_url TEXT    NOT NULL DEFAULT '',
            description TEXT    NOT NULL DEFAULT '',
            sort_order  INTEGER NOT NULL DEFAULT 0,
            created_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
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
        # Core fields (may be missing from very old DBs)
        ("email",          "TEXT NOT NULL DEFAULT ''"),
        ("referred_by",    "TEXT NOT NULL DEFAULT ''"),
        ("assigned_to",    "TEXT NOT NULL DEFAULT ''"),
        ("source",         "TEXT NOT NULL DEFAULT ''"),
        ("payment_done",   "INTEGER NOT NULL DEFAULT 0"),
        ("payment_amount", "REAL NOT NULL DEFAULT 0.0"),
        ("revenue",        "REAL NOT NULL DEFAULT 0.0"),
        ("notes",          "TEXT NOT NULL DEFAULT ''"),
        ("updated_at",     "TEXT NOT NULL DEFAULT (datetime('now','localtime'))"),
        # 3-day funnel flags
        ("day1_done",      "INTEGER NOT NULL DEFAULT 0"),
        ("day2_done",      "INTEGER NOT NULL DEFAULT 0"),
        ("interview_done", "INTEGER NOT NULL DEFAULT 0"),
        # Filtering / routing
        ("follow_up_date", "TEXT NOT NULL DEFAULT ''"),
        ("call_result",    "TEXT NOT NULL DEFAULT ''"),
        ("city",           "TEXT NOT NULL DEFAULT ''"),
        ("deleted_at",     "TEXT NOT NULL DEFAULT ''"),
        # Pool system
        ("in_pool",        "INTEGER NOT NULL DEFAULT 0"),
        ("pool_price",     "REAL NOT NULL DEFAULT 0.0"),
        ("claimed_at",     "TEXT NOT NULL DEFAULT ''"),
        # Extended funnel fields
        ("track_selected",   "TEXT NOT NULL DEFAULT ''"),
        ("track_price",      "REAL NOT NULL DEFAULT 0.0"),
        ("seat_hold_amount", "REAL NOT NULL DEFAULT 0.0"),
        ("pending_amount",   "REAL NOT NULL DEFAULT 0.0"),
        # Contact tracking
        ("last_contacted",   "TEXT NOT NULL DEFAULT ''"),
        ("contact_count",    "INTEGER NOT NULL DEFAULT 0"),
        ("follow_up_time",   "TEXT NOT NULL DEFAULT ''"),
        # Batch tracking (which batch within each day)
        ("day1_batch",       "TEXT NOT NULL DEFAULT ''"),
        ("day2_batch",       "TEXT NOT NULL DEFAULT ''"),
        ("day3_batch",       "TEXT NOT NULL DEFAULT ''"),
        # 3-Day process batch checkboxes
        ("d1_morning",       "INTEGER NOT NULL DEFAULT 0"),
        ("d1_afternoon",     "INTEGER NOT NULL DEFAULT 0"),
        ("d1_evening",       "INTEGER NOT NULL DEFAULT 0"),
        ("d2_morning",       "INTEGER NOT NULL DEFAULT 0"),
        ("d2_afternoon",     "INTEGER NOT NULL DEFAULT 0"),
        ("d2_evening",       "INTEGER NOT NULL DEFAULT 0"),
        # Working section metadata
        ("working_date",     "TEXT NOT NULL DEFAULT ''"),
        ("daily_score",      "INTEGER NOT NULL DEFAULT 0"),
        # Pipeline system (Part 2)
        ("pipeline_stage",   "TEXT NOT NULL DEFAULT 'enrollment'"),
        ("current_owner",    "TEXT NOT NULL DEFAULT ''"),
        ("call_status",      "TEXT NOT NULL DEFAULT 'Not Called Yet'"),
        ("priority_score",   "INTEGER NOT NULL DEFAULT 0"),
        ("seat_hold_expiry", "TEXT NOT NULL DEFAULT ''"),
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
        ("fbo_id",                "TEXT NOT NULL DEFAULT ''"),
        ("upline_name",           "TEXT NOT NULL DEFAULT ''"),
        ("phone",                 "TEXT NOT NULL DEFAULT ''"),
        ("email",                 "TEXT NOT NULL DEFAULT ''"),
        ("status",                "TEXT NOT NULL DEFAULT 'pending'"),
        ("display_picture",       "TEXT NOT NULL DEFAULT ''"),
        ("calling_reminder_time", "TEXT NOT NULL DEFAULT ''"),
        # Training system
        ("training_required",     "INTEGER NOT NULL DEFAULT 0"),
        ("training_status",       "TEXT NOT NULL DEFAULT 'not_required'"),
        ("joining_date",          "TEXT NOT NULL DEFAULT ''"),
        ("certificate_path",      "TEXT NOT NULL DEFAULT ''"),
        # Badges
        ("badges_json",           "TEXT NOT NULL DEFAULT '[]'"),
        ("test_score",            "INTEGER NOT NULL DEFAULT -1"),
        ("test_attempts",         "INTEGER NOT NULL DEFAULT 0"),
        # Pipeline role system (Part 2)
        ("upline_username",       "TEXT NOT NULL DEFAULT ''"),
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

    # --- training_videos table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS training_videos (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                day_number  INTEGER NOT NULL UNIQUE,
                title       TEXT    NOT NULL DEFAULT '',
                youtube_url TEXT    NOT NULL DEFAULT '',
                podcast_url TEXT    NOT NULL DEFAULT '',
                pdf_url     TEXT    NOT NULL DEFAULT '',
                description TEXT    NOT NULL DEFAULT '',
                created_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # Add podcast_url / pdf_url to existing training_videos rows
    for col, definition in [
        ("podcast_url", "TEXT NOT NULL DEFAULT ''"),
        ("pdf_url",     "TEXT NOT NULL DEFAULT ''"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE training_videos ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # Clear placeholder seed podcast/pdf URLs (local paths to files that never existed)
    cursor.execute("UPDATE training_videos SET podcast_url='' WHERE podcast_url LIKE 'audio/day%_podcast.%'")
    cursor.execute("UPDATE training_videos SET pdf_url='' WHERE pdf_url LIKE 'pdf/day%_resource.pdf'")

    # --- training_progress table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS training_progress (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                username     TEXT    NOT NULL,
                day_number   INTEGER NOT NULL,
                completed    INTEGER NOT NULL DEFAULT 0,
                completed_at TEXT    NOT NULL DEFAULT '',
                UNIQUE(username, day_number)
            )
        """)
    except Exception:
        pass

    # --- targets table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS targets (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                username     TEXT NOT NULL,
                metric       TEXT NOT NULL,
                target_value REAL NOT NULL DEFAULT 0,
                month        TEXT NOT NULL,
                created_by   TEXT NOT NULL DEFAULT 'admin',
                created_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                UNIQUE(username, metric, month)
            )
        """)
    except Exception:
        pass

    # --- user_badges table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_badges (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                username    TEXT NOT NULL,
                badge_key   TEXT NOT NULL,
                unlocked_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                UNIQUE(username, badge_key)
            )
        """)
    except Exception:
        pass

    # --- training_questions table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS training_questions (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                question       TEXT    NOT NULL,
                option_a       TEXT    NOT NULL DEFAULT '',
                option_b       TEXT    NOT NULL DEFAULT '',
                option_c       TEXT    NOT NULL DEFAULT '',
                option_d       TEXT    NOT NULL DEFAULT '',
                correct_answer TEXT    NOT NULL DEFAULT 'a',
                sort_order     INTEGER NOT NULL DEFAULT 0,
                created_at     TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # --- training_test_attempts table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS training_test_attempts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                username        TEXT    NOT NULL,
                score           INTEGER NOT NULL DEFAULT 0,
                total_questions INTEGER NOT NULL DEFAULT 0,
                passed          INTEGER NOT NULL DEFAULT 0,
                attempted_at    TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # --- daily_scores table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_scores (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                username           TEXT    NOT NULL,
                score_date         TEXT    NOT NULL,
                calls_made         INTEGER NOT NULL DEFAULT 0,
                videos_sent        INTEGER NOT NULL DEFAULT 0,
                batches_marked     INTEGER NOT NULL DEFAULT 0,
                payments_collected INTEGER NOT NULL DEFAULT 0,
                total_points       INTEGER NOT NULL DEFAULT 0,
                streak_days        INTEGER NOT NULL DEFAULT 1,
                created_at         TEXT    NOT NULL DEFAULT (datetime('now','localtime')),
                UNIQUE(username, score_date)
            )
        """)
    except Exception:
        pass

    # Add index for daily_scores lookups
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_scores_user_date ON daily_scores(username, score_date)")
    except Exception:
        pass

    # Pipeline sync: new columns on daily_scores
    for col, definition in [
        ("enroll_links_sent", "INTEGER NOT NULL DEFAULT 0"),
        ("prospect_views", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE daily_scores ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # Pipeline sync: new columns on daily_reports (actual system counts + verified flag)
    for col, definition in [
        ("videos_sent_actual", "INTEGER NOT NULL DEFAULT -1"),
        ("calls_made_actual", "INTEGER NOT NULL DEFAULT -1"),
        ("payments_actual", "INTEGER NOT NULL DEFAULT -1"),
        ("system_verified", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE daily_reports ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # --- enroll_content table (for Enroll To share — video titles) ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS enroll_content (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                curiosity_title TEXT NOT NULL DEFAULT '',
                title          TEXT NOT NULL DEFAULT '',
                created_at     TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # Add columns required by leader Working / Enroll To (is_active, day_number, sort_order)
    for col, definition in [
        ("is_active", "INTEGER NOT NULL DEFAULT 1"),
        ("day_number", "INTEGER NOT NULL DEFAULT 1"),
        ("sort_order", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE enroll_content ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # --- enroll_pdfs table (Enroll To — PDFs for leaders) ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS enroll_pdfs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT NOT NULL DEFAULT '',
                url         TEXT NOT NULL DEFAULT '',
                is_active   INTEGER NOT NULL DEFAULT 1,
                sort_order  INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # --- enroll_share_links table (Enroll To share link → lead sync) ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS enroll_share_links (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                token      TEXT NOT NULL UNIQUE,
                lead_id    INTEGER,
                content_id INTEGER,
                shared_by  TEXT NOT NULL DEFAULT '',
                view_count INTEGER NOT NULL DEFAULT 0,
                lead_status_before TEXT NOT NULL DEFAULT '',
                synced_to_lead INTEGER NOT NULL DEFAULT 0,
                watch_synced INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    for col, definition in [
        ("lead_status_before", "TEXT NOT NULL DEFAULT ''"),
        ("synced_to_lead", "INTEGER NOT NULL DEFAULT 0"),
        ("watch_synced", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            cursor.execute(f"ALTER TABLE enroll_share_links ADD COLUMN {col} {definition}")
        except Exception:
            pass

    # --- batch_share_links: token per (lead_id, slot) so prospect open = auto-mark batch ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS batch_share_links (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                token      TEXT NOT NULL UNIQUE,
                lead_id    INTEGER NOT NULL,
                slot       TEXT NOT NULL,
                used       INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # --- bonus_videos table ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bonus_videos (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT    NOT NULL DEFAULT '',
                youtube_url TEXT    NOT NULL DEFAULT '',
                description TEXT    NOT NULL DEFAULT '',
                sort_order  INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # --- lead_stage_history table (pipeline transitions log) ---
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lead_stage_history (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id      INTEGER NOT NULL,
                stage        TEXT    NOT NULL,
                owner        TEXT    NOT NULL DEFAULT '',
                triggered_by TEXT    NOT NULL DEFAULT '',
                created_at   TEXT    NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
    except Exception:
        pass

    # --- Performance indexes ---
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_stage_history_lead ON lead_stage_history(lead_id)",
        "CREATE INDEX IF NOT EXISTS idx_leads_pipeline ON leads(pipeline_stage, current_owner)",
        "CREATE INDEX IF NOT EXISTS idx_leads_pool_assigned  ON leads(in_pool, assigned_to)",
        "CREATE INDEX IF NOT EXISTS idx_leads_pool_status    ON leads(in_pool, status)",
        "CREATE INDEX IF NOT EXISTS idx_leads_payment        ON leads(payment_done, in_pool)",
        "CREATE INDEX IF NOT EXISTS idx_leads_updated        ON leads(updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_leads_phone          ON leads(phone)",
        "CREATE INDEX IF NOT EXISTS idx_wallet_user_status   ON wallet_recharges(username, status)",
        "CREATE INDEX IF NOT EXISTS idx_reports_user_date    ON daily_reports(username, report_date)",
        "CREATE INDEX IF NOT EXISTS idx_leads_call_result ON leads(call_result, in_pool, deleted_at)",
        "CREATE INDEX IF NOT EXISTS idx_activity_user_time ON activity_log(username, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_targets_user_month ON targets(username, month)",
        "CREATE INDEX IF NOT EXISTS idx_lead_notes_lead ON lead_notes(lead_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_leads_followup ON leads(follow_up_date, assigned_to)",
        "CREATE INDEX IF NOT EXISTS idx_leads_contacted ON leads(last_contacted, assigned_to)",
        "CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_leads_deleted_pool ON leads(in_pool, deleted_at, assigned_to, created_at)",
    ]
    for idx in indexes:
        try:
            cursor.execute(idx)
        except Exception:
            pass

    # ── FBO ID fix: 910900367506 is Karanveer Singh (admin) ──────────────────
    # Step 1: Set admin's FBO ID to 910900367506
    cursor.execute("""
        UPDATE users SET fbo_id = '910900367506'
        WHERE role = 'admin' AND (fbo_id IS NULL OR fbo_id = '' OR fbo_id = '910900367506')
    """)
    # Step 2: Any non-admin user who has 910900367506 as their own FBO ID
    # (wrong entry) — clear their FBO and set admin as their upline.
    cursor.execute("""
        UPDATE users
        SET    fbo_id          = '',
               upline_name     = COALESCE((SELECT username FROM users WHERE role='admin' LIMIT 1), 'admin'),
               upline_username = COALESCE((SELECT username FROM users WHERE role='admin' LIMIT 1), 'admin')
        WHERE  fbo_id = '910900367506'
          AND  role   != 'admin'
    """)
    # Step 3: Fix existing rows where upline_name was stored as a display name
    # like 'Karanveer Singh' instead of the actual username — update to admin username.
    cursor.execute("""
        UPDATE users
        SET    upline_name     = COALESCE((SELECT username FROM users WHERE role='admin' LIMIT 1), 'admin'),
               upline_username = COALESCE((SELECT username FROM users WHERE role='admin' LIMIT 1), 'admin')
        WHERE  LOWER(upline_name) LIKE '%karanveer%'
          AND  role != 'admin'
    """)

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


def seed_training_questions():
    """Insert the 20 MCQ training test questions if none exist yet."""
    conn = get_db()
    cursor = conn.cursor()
    count = cursor.execute("SELECT COUNT(*) FROM training_questions").fetchone()[0]
    if count > 0:
        conn.close()
        return

    questions = [
        {
            "q": "Tumhara WHY genuinely powerful hai ya sirf sunne mein achha lagta hai — kaise identify karoge?",
            "a": "Jab WHY ek weapon ki tarah kaam kare — mushkil waqt mein bhi tumhe uthake khada kar de (yeh powerful WHY ki pehchaan hai)",
            "b": "Jab WHY sunne mein inspiring lage aur logo ko motivate kare",
            "c": "Jab WHY clearly aur logically express ho sake",
            "d": "Jab WHY realistic aur achievable goals pe based ho",
            "ans": "a",
        },
        {
            "q": "Invitation call mein 'Please dekh lena' bolne se kya exactly galat hota hai — psychology explain karo",
            "a": "Tone weak lagti hai aur professional nahi lagta",
            "b": "Prospect ko lagta hai tum desperate ho aur business mein vishwaas nahi",
            "c": "Yeh Beggar mindset hai — value maangna nahi chahiye, value dena chahiye. Selector ban ke chalo — serious logon ko hi opportunity do",
            "d": "Isse prospect pe zyada pressure padta hai",
            "ans": "c",
        },
        {
            "q": "Prospect ne 'Thoda baad mein batata hoon' bola — training ke hisaab se step by step exactly kya karna hai?",
            "a": "Sirf ek baar 24 ghante baad remind karo phir chhod do",
            "b": "2 follow-ups karo — pehle agle din, doosra 2-3 din baad — agar phir bhi na, toh move on",
            "c": "Roz follow-up karo jab tak jawab na aaye",
            "d": "Seedha poochho 'Haan ya Na?' — ek baar mein decision lo",
            "ans": "b",
        },
        {
            "q": "Law of Average ke hisaab se 10 invite ke baad sirf 1 payment aaya — kya yeh failure hai? Numbers se justify karo.",
            "a": "Haan — improve karna chahiye, average 3-4 mein se 1 hona chahiye",
            "b": "Depends — quality of invitation zyada important hai quantity se",
            "c": "Nahi — starting average 10 mein se 1 hi hota hai. Yeh normal hai, Law of Average kaam kar raha hai",
            "d": "Nahi, lekin improve karna chahiye — 5 mein se 1 target banana chahiye",
            "ans": "c",
        },
        {
            "q": "10 invitations bheje, 9 'Na' bola, partner bolta hai 'Aaj waste gaya.' Colonel Sanders example se connect karke kya bologe?",
            "a": "Colonel Sanders ne bhi bahut struggle kiya — hum bhi try karte rahenge, result zaroor aayega",
            "b": "Ek positive lead kaafi hai — Colonel Sanders ne bhi ek chance se company shuru ki",
            "c": "Colonel Sanders ka example alag situation tha — hum smarter approach lenge",
            "d": "Colonel Sanders ne 1009 baar rejection li. 9 Na = failure nahi — rejection statistical hai, personal nahi. 9 Na = 9 steps complete, average improve ho raha hai",
            "ans": "d",
        },
        {
            "q": "Rs.196 payment ke baad maximum kitne ghante mein call karni chahiye — aur rule toota toh kya galat hoga?",
            "a": "24 ghante ke andar — prospect ko settle hone ka time chahiye",
            "b": "12 ghante ke andar — same day mein karo",
            "c": "2 ghante ke andar — payment ke turant baad excitement peak hota hai, zyada der = excitement khatam",
            "d": "Koi specific time limit nahi — jab convenient ho tab karo",
            "ans": "c",
        },
        {
            "q": "Day 1 call mein 'Teen Verbal YES' lene ka exactly kya psychological reason hai? Sirf ek baar 'haan' kaafi nahi?",
            "a": "Formal agreement feel hoti hai teen baar agree karne se",
            "b": "Teen baar 'haan' kehne se prospect ka psychological ownership ban jaata hai — commitment ek baar se zyada deeper hoti hai",
            "c": "Senior ko confirm karna hota hai ki prospect genuinely ready hai",
            "d": "Day 2 ke liye formality complete karna zaroori hai",
            "ans": "b",
        },
        {
            "q": "Prospect ne Day 1 ke baad 'Day 1 Ready' message nahi bheja — exactly kya karoge? Ek reminder ke baad bhi response nahi aaya toh?",
            "a": "Ek reminder bhejo. Agar phir bhi response nahi — prospect unserious hai, track karo aur aage badho",
            "b": "Seedha call karo aur poochho kyon message nahi aaya",
            "c": "2-3 baar remind karo — prospect busy ho sakta hai",
            "d": "Day 2 pe directly senior ke saath connect karo, message ki zaroorat nahi",
            "ans": "a",
        },
        {
            "q": "Day 2 mein senior poori handling karti hain — phir bhi tumhara ek specific critical kaam kya hai?",
            "a": "Prospect ko motivate karte rehna aur positive rakhna",
            "b": "Brief banana — exactly 3 points mein: tumhara journey, results, aur belief statement. Galat ya lamba brief = Day 3 pe weak closing",
            "c": "Notes lena aur Day 3 ke liye questions prepare karna",
            "d": "Payment confirmation aur Day 3 timing confirm karna",
            "ans": "b",
        },
        {
            "q": "Day 3 interview mein senior teesra sawaal poochti hain — 'Pehle 30 dinon mein kya ek result chahiye?' — yeh kyun?",
            "a": "Prospect ka short-term goal samajhne ke liye taaki training customize ho sake",
            "b": "Prospect ki seriousness aur readiness test karne ke liye",
            "c": "Training plan ke hisaab se targets set karne ke liye",
            "d": "Specific target + senior ka confirmation — closing mein senior usi specific target se connect karti hain, commitment concrete hoti hai",
            "ans": "d",
        },
        {
            "q": "Seat Holding ke time prospect hesitate karta hai — senior exactly kya poochhegi aur woh Day 2 se kaise connect hoga?",
            "a": "'Kya paisa nahi hai?' — directly financial barrier puchha jaata hai",
            "b": "'Tum serious ho ya nahi?' — commitment check kiya jaata hai",
            "c": "Day 2 mein prospect ne jo dream/problem share ki thi, usi se connect karegi — 'Tune bataya tha [X] chahiye — kya woh abhi bhi important hai?'",
            "d": "'Kyun hesitate kar rahe ho?' — objection explore ki jaati hai",
            "ans": "c",
        },
        {
            "q": "'Jo pehle bolta hai woh haarta hai' — yeh rule exactly kab apply hota hai? Prospect 3 minute chup rahe toh kya karna chahiye?",
            "a": "Hamesha apply hota hai — har conversation mein chup rehna powerful hota hai",
            "b": "Seat Hold amount maangne ke baad silence mein apply hota hai — prospect 3 minute bhi chup rahe, BILKUL mat bolo. Silence unhe uncomfortable lagti hai, tumhe nahi",
            "c": "Sirf objection handling mein apply hota hai",
            "d": "Jab prospect confused lag raha ho tab chup rehna best hai",
            "ans": "b",
        },
        {
            "q": "Prospect genuinely 'Na' bolta hai Day 3 par — exactly kya bolna hai aur door kyun nahi bandhni chahiye?",
            "a": "Ek baar aur convince karne ki koshish karo — last attempt zaroori hai",
            "b": "Seedha poochho kya problem hai aur real objection solve karo",
            "c": "'Bilkul theek hai — koi baat nahi. Door hamesha open hai.' Best members woh hote hain jo pehle Na bolte hain — door band karna permanently lose karna hai",
            "d": "Senior ko handle karne do — unhe zyada experience hai",
            "ans": "c",
        },
        {
            "q": "Prospect bolta hai 'Paisa nahi hai.' Training ka exact sawaal-jawab technique use karke poori conversation kaise hogi?",
            "a": "Sequence: 'Income kahan se aati hai?' → 'Kitna kaafi feel hota hai?' → 'Pehle 30 dinon mein ek plan kya hai?' — prospect khud apna solution nikalta hai",
            "b": "EMI ya installment ka option de do — financial barrier remove karo",
            "c": "Sympathy dikhao aur kaho 'Theek hai, jab ready ho tab baat karte hain'",
            "d": "'Agar paisa hota toh kya karte?' — aspiration unlock karo",
            "ans": "a",
        },
        {
            "q": "'Ghar wale nahi maanenge' — ghar waalon ko villain banana kyun galat hai? Exact alternative framing kya hai?",
            "a": "Isse family relationship kharab hoti hai — prospect defensive ho jaata hai",
            "b": "Villain banana galat hai — ghar waale Motivator ban sakte hain. Framing: 'Ghar waale chahte hain tumhara acha ho — agar yeh unke liye bhi benefit hai, toh woh support zaroor karenge'",
            "c": "Prospect ka confidence aur zyada girta hai jab family ko involve karo",
            "d": "Isse conversation negative ho jaati hai — topic change karna better hai",
            "ans": "b",
        },
        {
            "q": "Objection handling mein sabse powerful rule ek hi hai — woh kya hai? Sirf objection handling mein ya aur bhi kaam aata hai?",
            "a": "Prospect ko pehle acknowledge karo phir counter karo — empathy first",
            "b": "'Jo pehle bolta hai woh haarta hai' — yeh Seat Hold silence mein bhi apply hota hai aur Day 3 closing mein bhi",
            "c": "3 objections ke baad chhod do — zyada push karna backfire karta hai",
            "d": "Pehle agree karo phir gently reframe karo",
            "ans": "b",
        },
        {
            "q": "Social media pe teen types ki content ka exactly kya psychological purpose hai prospect ke mind mein?",
            "a": "Zyada variety se zyada reach aur engagement milti hai",
            "b": "Algorithm ke liye variety important hai — ek type ki content pe reach drop hoti hai",
            "c": "Journey = prospect relatable feel karta hai; Value = trust banta hai; Social Proof = FOMO create hoti hai — teeno milke prospect ke mind mein belief system banta hai",
            "d": "Alag alag logon ko alag content appeal karti hai — har type ek alag prospect target karta hai",
            "ans": "c",
        },
        {
            "q": "Day 12, sirf 1 join, 30-day target 10 — 300 invitations formula se check karo kya galat ho raha hai?",
            "a": "Follow-up weak hai — zyada consistent follow-up karo",
            "b": "Day 12 tak 120 invites hone chahiye the, 36 watches, 12 serious. Agar nahi hua — invitation volume bahut kam hai. Roz 10 invitations pakka karo, numbers khud theek ho jaayenge",
            "c": "Quality of invitation improve karo — random logon ko nahi, targeted logon ko invite karo",
            "d": "Target unrealistic hai — 10 joins in 30 days adjust karo",
            "ans": "b",
        },
        {
            "q": "'Aaj mood nahi, kal karunga' — training mein is exact situation ke liye kya solution diya gaya hai aur kaam kyun karta hai?",
            "a": "Rest karo — forced kaam ka result achha nahi hota, mood mein kaam better hota hai",
            "b": "Motivation video dekho phir karo — energy aayegi",
            "c": "Partner ya upline se baat karo — accountability se mood aata hai",
            "d": "Consistency ka matlab perfection nahi — sirf 1 chota action aaj karo. Ek action chain nahi todta, aur karne ke baad mood khud aata hai",
            "ans": "d",
        },
        {
            "q": "Is poori training ki chain kya hai — ek link bhi toota toh system fail. Woh complete chain kya hai?",
            "a": "Invitation → Law of Average → 3-Day Process (Day 1, Day 2 senior, Day 3 closing) → Objection Handling → Social Media → Tracker — har link connected hai, ek toota toh poora result nahi aata",
            "b": "Product Knowledge → Confidence → Invitation → Follow-up → Objection Handling → Closing",
            "c": "Mindset → WHY → Invitation → Payment → Training → Certificate",
            "d": "WHY → Goal Setting → Daily Action → Law of Average → Result → Duplication",
            "ans": "a",
        },
    ]

    for i, q in enumerate(questions, start=1):
        cursor.execute(
            """INSERT INTO training_questions
               (question, option_a, option_b, option_c, option_d, correct_answer, sort_order)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (q["q"], q["a"], q["b"], q["c"], q["d"], q["ans"], i)
        )
    conn.commit()
    conn.close()
