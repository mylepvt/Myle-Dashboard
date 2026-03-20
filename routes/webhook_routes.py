"""
Meta webhook routes (verify + receive).

Registered via register_webhook_routes(app) so endpoint names stay unchanged
(no Blueprint prefix).
"""
from __future__ import annotations

import hashlib
import hmac

from flask import request

from database import get_db


def register_webhook_routes(app):
    """Attach webhook-related URL rules to the Flask app (preserves endpoint names)."""
    from app import _get_setting  # noqa: PLC0415 — late import

    @app.route('/meta/webhook', methods=['GET'])
    def meta_webhook_verify():
        """Meta webhook verification (hub.challenge handshake)."""
        db           = get_db()
        verify_token = _get_setting(db, 'meta_webhook_token', '')
        db.close()

        mode      = request.args.get('hub.mode')
        token     = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')

        if mode == 'subscribe' and token == verify_token and verify_token:
            return challenge, 200
        return 'Forbidden', 403

    @app.route('/meta/webhook', methods=['POST'])
    def meta_webhook_receive():
        """Receive Meta Lead Ads leads via webhook."""
        db = get_db()

        app_secret = _get_setting(db, 'meta_app_secret', '')
        if app_secret:
            sig_header = request.headers.get('X-Hub-Signature-256', '')
            if not sig_header.startswith('sha256='):
                db.close()
                return 'Forbidden', 403
            expected = 'sha256=' + hmac.new(
                app_secret.encode(), request.data, hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(sig_header, expected):
                db.close()
                return 'Forbidden', 403

        data = request.get_json(silent=True)
        if not data:
            db.close()
            return 'OK', 200

        default_price = float(_get_setting(db, 'default_lead_price', '50') or 50)

        imported = 0
        for entry in data.get('entry', []):
            for change in entry.get('changes', []):
                try:
                    if change.get('field') != 'leadgen':
                        continue
                    value      = change.get('value', {})
                    field_data = value.get('field_data', [])
                    lead_fields = {
                        f['name']: (f['values'][0] if f.get('values') else '')
                        for f in field_data
                    }

                    name  = (lead_fields.get('full_name') or lead_fields.get('name') or
                             lead_fields.get('full name') or '').strip()
                    phone = lead_fields.get('phone_number', lead_fields.get('phone', '')).strip()
                    email = lead_fields.get('email', '').strip()

                    if not phone:
                        phone = str(value.get('leadgen_id', 'N/A'))
                    if not name:
                        name = phone

                    leadgen_id = str(value.get('leadgen_id', ''))
                    if leadgen_id:
                        existing = db.execute(
                            "SELECT id FROM leads WHERE notes LIKE ?",
                            (f'%meta_id:{leadgen_id}%',)
                        ).fetchone()
                        if existing:
                            continue

                    db.execute("""
                        INSERT INTO leads
                            (name, phone, email, assigned_to, source, status,
                             in_pool, pool_price, claimed_at, notes)
                        VALUES (?, ?, ?, '', 'Meta', 'New', 1, ?, '', ?)
                    """, (name, phone, email, default_price,
                          f'meta_id:{leadgen_id}' if leadgen_id else ''))
                    imported += 1
                except Exception:
                    continue

        db.commit()
        db.close()
        return 'OK', 200
