"""
Pytest fixtures for smoke tests.

IMPORTANT: `DATABASE_PATH` and related env vars must be set before `app` is
imported (app runs init_db/migrate at import time).
"""
from __future__ import annotations

import os
import tempfile

import pytest

# ── Isolated SQLite DB + predictable admin seed (before app import) ─────────
_fd, _TEST_DB_PATH = tempfile.mkstemp(suffix='.db')
os.close(_fd)
os.environ['DATABASE_PATH'] = _TEST_DB_PATH
os.environ['BOOTSTRAP_ADMIN_PASSWORD'] = 'SmokeTest_Admin_99!'
os.environ['SECRET_KEY'] = 'pytest-secret-key-not-for-production'
os.environ['GUNICORN_MULTI_WORKER'] = '1'  # skip APScheduler on import

import app as _app_module  # noqa: E402 — after env

app = _app_module.app

ADMIN_USER = 'admin'
ADMIN_PASSWORD = os.environ['BOOTSTRAP_ADMIN_PASSWORD']


def _seed_team_user() -> None:
    from werkzeug.security import generate_password_hash

    from database import get_db

    db = get_db()
    row = db.execute("SELECT id FROM users WHERE username='smoke_team'").fetchone()
    if not row:
        db.execute(
            """INSERT INTO users (username, password, role, status)
               VALUES (?, ?, 'team', 'approved')""",
            ('smoke_team', generate_password_hash('SmokeTeam_99!', method='pbkdf2:sha256')),
        )
        db.commit()
    db.close()


_seed_team_user()


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as c:
        yield c


def login(client, username: str, password: str, follow_redirects: bool = True):
    """Establish session via POST /login with CSRF token."""
    client.get('/login')
    with client.session_transaction() as sess:
        token = sess.get('_csrf_token', '')
    return client.post(
        '/login',
        data={'username': username, 'password': password, 'csrf_token': token},
        follow_redirects=follow_redirects,
    )


@pytest.fixture
def admin_client(client):
    login(client, ADMIN_USER, ADMIN_PASSWORD)
    return client


@pytest.fixture
def team_client(client):
    login(client, 'smoke_team', 'SmokeTeam_99!')
    return client
