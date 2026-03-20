"""
Miscellaneous routes: PWA support, health check, push notifications.

Registered via register_misc_routes(app) so endpoint names stay unchanged
(no Blueprint prefix).
"""
from __future__ import annotations

from flask import request, session

from database import get_db
from decorators import login_required


def register_misc_routes(app):
    """Attach misc URL rules to the Flask app (preserves endpoint names)."""
    from app import _get_or_create_vapid_keys  # noqa: PLC0415 — late import

    # ─────────────────────────────────────────────
    #  PWA support routes
    # ─────────────────────────────────────────────

    @app.route('/sw.js')
    def service_worker():
        """Serve service worker from root scope (required for full PWA control)."""
        return app.send_static_file('sw.js'), 200, {
            'Content-Type': 'application/javascript',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Service-Worker-Allowed': '/'
        }

    @app.route('/manifest.json')
    def pwa_manifest():
        """Serve PWA manifest from root."""
        return app.send_static_file('manifest.json'), 200, {
            'Content-Type': 'application/manifest+json'
        }

    # ─────────────────────────────────────────────
    #  Health check
    # ─────────────────────────────────────────────

    @app.route('/health')
    def health():
        return {'status': 'ok'}, 200

    # ─────────────────────────────────────────────
    #  Push notification routes
    # ─────────────────────────────────────────────

    @app.route('/push/vapid-key')
    @login_required
    def push_vapid_key():
        """Return VAPID public key for browser subscription."""
        db = get_db()
        _, public_key = _get_or_create_vapid_keys(db)
        db.close()
        return {'public_key': public_key or ''}

    @app.route('/push/subscribe', methods=['POST'])
    @login_required
    def push_subscribe():
        """Save a browser push subscription for the logged-in user."""
        data = request.get_json(silent=True)
        if not data or not data.get('endpoint'):
            return {'ok': False, 'error': 'Missing endpoint'}, 400

        endpoint = data.get('endpoint', '')
        auth     = data.get('keys', {}).get('auth', '')
        p256dh   = data.get('keys', {}).get('p256dh', '')
        username = session['username']

        db = get_db()
        db.execute("""
            INSERT INTO push_subscriptions (username, endpoint, auth, p256dh)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(endpoint) DO UPDATE SET
                username=excluded.username,
                auth=excluded.auth,
                p256dh=excluded.p256dh
        """, (username, endpoint, auth, p256dh))
        db.commit()
        db.close()
        return {'ok': True}
