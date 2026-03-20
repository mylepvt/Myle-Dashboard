#!/usr/bin/env python3
"""Verify app routes and pipeline: no errors, enrollment watch, leader working."""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

TEST_DB = os.path.join(os.path.dirname(__file__), '..', 'leads_verify_flow.db')
os.environ['DATABASE_PATH'] = os.path.abspath(TEST_DB)

def main():
    errors = []
    from database import init_db, migrate_db, seed_users, get_db

    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    init_db()
    migrate_db()
    seed_users()

    # Set enrollment video so /watch/enrollment has content
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO app_settings (key, value) VALUES ('enrollment_video_url', 'https://www.youtube.com/watch?v=dQw4w9WgXcQ')"
    )
    conn.execute(
        "INSERT OR REPLACE INTO app_settings (key, value) VALUES ('enrollment_video_title', 'Enrollment Video')"
    )
    conn.commit()
    conn.execute(
        "INSERT OR IGNORE INTO users (username, password, role, status, upline_name, upline_username) VALUES (?,?,?,?,?,?)",
        ('leadertest', 'pbkdf2:sha256:x', 'leader', 'approved', 'admin', 'admin')
    )
    conn.commit()
    conn.close()

    from app import app
    with app.test_client() as c:
        # 1) Public /watch/enrollment — 200 and embed
        r = c.get('/watch/enrollment')
        if r.status_code != 200:
            errors.append(f'/watch/enrollment returned {r.status_code}')
        elif b'embed' not in r.data and b'iframe' not in r.data:
            errors.append('/watch/enrollment: no embed/iframe in response')
        # 2) Leader Working — 200 and leader UI
        with c.session_transaction() as sess:
            sess['username'] = 'leadertest'
            sess['role'] = 'leader'
        r = c.get('/working')
        if r.status_code != 200:
            errors.append(f'Leader /working returned {r.status_code}')
        elif b'My Work' not in r.data and b'Team Work' not in r.data:
            errors.append('Leader /working: My Work or Team Work not in response')
        # 3) /watch/enrollment without video — 404
        conn = get_db()
        conn.execute("DELETE FROM app_settings WHERE key='enrollment_video_url'")
        conn.commit()
        conn.close()
        r = c.get('/watch/enrollment')
        if r.status_code != 404:
            errors.append(f'/watch/enrollment (no video) expected 404 got {r.status_code}')
    if errors:
        for e in errors:
            print('FAIL:', e, file=sys.stderr)
        sys.exit(1)
    print('OK: enrollment watch, leader working, 404 when no video.')

if __name__ == '__main__':
    main()
