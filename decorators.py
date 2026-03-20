"""
Flask route decorators (extracted from app for maintainability).
Uses current_app for logging to avoid circular imports with app.py.
"""
from __future__ import annotations

from functools import wraps

from flask import (
    current_app,
    flash,
    redirect,
    request,
    session,
    url_for,
)

from database import get_db


def _check_session_valid():
    """Return True if the current session user is still active/approved in the DB."""
    username = session.get('username')
    if not username:
        return False
    try:
        db = get_db()
        row = db.execute(
            "SELECT status FROM users WHERE username=?", (username,)
        ).fetchone()
        db.close()
        if not row or row['status'] != 'approved':
            session.clear()
            return False
        return True
    except Exception as e:
        current_app.logger.error("_check_session_valid() DB error: %s", e)
        return True  # fail-open: let the route handle the DB error


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'username' not in session:
            flash('Please log in to continue.', 'warning')
            return redirect(url_for('login'))
        if not _check_session_valid():
            flash('Your account is no longer active. Please contact an admin.', 'danger')
            return redirect(url_for('login'))
        return f(*args, **kwargs)

    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'username' not in session:
            flash('Please log in to continue.', 'warning')
            return redirect(url_for('login'))
        if not _check_session_valid():
            flash('Your account is no longer active. Please contact an admin.', 'danger')
            return redirect(url_for('login'))
        if session.get('role') != 'admin':
            flash('Admin access required.', 'danger')
            return redirect(url_for('team_dashboard'))
        return f(*args, **kwargs)

    return decorated


def safe_route(f):
    """Catch unhandled exceptions in routes, log them, and show a friendly error."""

    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            import traceback as _tb

            current_app.logger.error(
                "Route %s crashed: %s\n%s", request.path, e, _tb.format_exc()
            )
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
                return {'ok': False, 'error': 'Something went wrong, please try again'}, 500
            flash('Kuch gadbad ho gayi. Please dubara try karein.', 'danger')
            if session.get('role') == 'admin':
                return redirect(url_for('admin_dashboard'))
            if 'username' in session:
                return redirect(url_for('team_dashboard'))
            return redirect(url_for('login'))

    return decorated
