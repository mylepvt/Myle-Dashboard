"""
Wallet, Lead Pool, and Calling Reminder routes.

Registered via register_wallet_routes(app) at the end of app.py load so helpers
on the app module are available without circular import at import time.
"""
from __future__ import annotations

import re

from flask import flash, redirect, render_template, request, session, url_for

from database import get_db
from decorators import login_required, safe_route


def register_wallet_routes(app):
    """Attach wallet / lead-pool / calling-reminder URL rules to the Flask app."""
    from app import (  # noqa: PLC0415 — late import after app module is populated
        _generate_upi_qr_base64,
        _get_setting,
        _get_wallet,
        _log_activity,
        _now_ist,
    )

    # ─────────────────────────────────────────────────
    #  Team – Wallet
    # ─────────────────────────────────────────────────

    @app.route('/wallet')
    @login_required
    @safe_route
    def wallet():
        username = session['username']
        db       = get_db()

        wallet_stats = _get_wallet(db, username)

        recharges = db.execute(
            "SELECT * FROM wallet_recharges WHERE username=? ORDER BY requested_at DESC LIMIT 20",
            (username,)
        ).fetchall()

        claimed_leads = db.execute(
            "SELECT name, phone, source, pool_price, claimed_at "
            "FROM leads WHERE assigned_to=? AND claimed_at!='' "
            "ORDER BY claimed_at DESC LIMIT 20",
            (username,)
        ).fetchall()

        upi_id     = _get_setting(db, 'upi_id')
        upi_qr_b64 = _generate_upi_qr_base64(upi_id) if upi_id else None

        pending_mine = db.execute(
            "SELECT COUNT(*) FROM wallet_recharges WHERE username=? AND status='pending'",
            (username,)
        ).fetchone()[0]

        db.close()
        return render_template('wallet.html',
                               wallet=wallet_stats,
                               recharges=recharges,
                               claimed_leads=claimed_leads,
                               upi_id=upi_id,
                               upi_qr_b64=upi_qr_b64,
                               pending_mine=pending_mine)


    @app.route('/wallet/request-recharge', methods=['POST'])
    @login_required
    @safe_route
    def request_recharge():
        username = session['username']
        db       = get_db()

        try:
            amount = float(request.form.get('amount') or 0)
        except ValueError:
            amount = 0

        utr = (request.form.get('utr_number') or '').strip()

        if amount <= 0:
            flash('Please enter a valid amount greater than 0.', 'danger')
            db.close()
            return redirect(url_for('wallet'))

        if not utr:
            flash('UTR / Transaction number is required.', 'danger')
            db.close()
            return redirect(url_for('wallet'))

        existing = db.execute(
            "SELECT id FROM wallet_recharges WHERE utr_number=?", (utr,)
        ).fetchone()
        if existing:
            flash('This UTR number has already been submitted. Contact admin if this is an error.', 'danger')
            db.close()
            return redirect(url_for('wallet'))

        try:
            db.execute(
                "INSERT INTO wallet_recharges (username, amount, utr_number, status) "
                "VALUES (?, ?, ?, 'pending')",
                (username, amount, utr)
            )
            db.commit()
        except Exception as _e:
            app.logger.error(f"wallet recharge INSERT failed for {username}: {_e}")
            try: db.execute("ROLLBACK")
            except Exception: pass
            db.close()
            flash('Could not save your request. Please try again or contact admin.', 'danger')
            return redirect(url_for('wallet'))

        db.close()
        flash(f'Recharge request of \u20b9{amount:.0f} submitted! UTR: {utr}. '
              f'Admin will credit your wallet within 24 hours.', 'success')
        return redirect(url_for('wallet'))


    # ─────────────────────────────────────────────────
    #  Team – Lead Pool (Claim Leads)
    # ─────────────────────────────────────────────────

    @app.route('/lead-pool')
    @login_required
    def lead_pool():
        username = session['username']
        db       = get_db()

        wallet_stats = _get_wallet(db, username)

        pool_count = db.execute(
            "SELECT COUNT(*) FROM leads WHERE in_pool=1"
        ).fetchone()[0]

        price_info = db.execute(
            "SELECT MIN(pool_price) as min_p, MAX(pool_price) as max_p, "
            "AVG(pool_price) as avg_p FROM leads WHERE in_pool=1"
        ).fetchone()

        avg_price  = price_info['avg_p'] or 0
        if pool_count == 0:
            can_claim = 0
        elif avg_price > 0:
            can_claim = min(int(wallet_stats['balance'] // avg_price), pool_count)
        else:
            can_claim = pool_count

        my_claims = db.execute(
            "SELECT COUNT(*) FROM leads WHERE assigned_to=? AND claimed_at!=''",
            (username,)
        ).fetchone()[0]

        upi_id = _get_setting(db, 'upi_id', '')

        db.close()
        return render_template('lead_pool.html',
                               wallet=wallet_stats,
                               pool_count=pool_count,
                               price_info=price_info,
                               can_claim=can_claim,
                               my_claims=my_claims,
                               upi_id=upi_id)


    @app.route('/calling-reminder/set', methods=['POST'])
    @login_required
    def set_calling_reminder():
        """Team member sets their personal calling reminder time (HH:MM or blank to clear)."""
        time_val = request.form.get('reminder_time', '').strip()
        if time_val and not re.match(r'^\d{2}:\d{2}$', time_val):
            flash('Invalid time format.', 'danger')
            return redirect(url_for('team_dashboard'))
        db = get_db()
        db.execute(
            "UPDATE users SET calling_reminder_time=? WHERE username=?",
            (time_val, session['username'])
        )
        db.commit()
        db.close()
        if time_val:
            flash(f'Calling reminder set for {time_val} every day.', 'success')
        else:
            flash('Calling reminder cleared.', 'success')
        return redirect(url_for('team_dashboard'))


    @app.route('/lead-pool/claim', methods=['POST'])
    @login_required
    def claim_leads():
        username = session['username']
        db       = get_db()

        try:
            count = int(request.form.get('count') or 1)
            count = max(1, min(count, 50))
        except ValueError:
            count = 1

        try:
            db.execute("BEGIN IMMEDIATE")

            wallet_stats = _get_wallet(db, username)

            available = db.execute(
                "SELECT id, pool_price FROM leads WHERE in_pool=1 ORDER BY created_at ASC LIMIT ?",
                (count,)
            ).fetchall()

            if not available:
                db.execute("ROLLBACK")
                db.close()
                flash('No leads available in pool right now. Check back later.', 'warning')
                return redirect(url_for('lead_pool'))

            total_cost = sum(r['pool_price'] for r in available)

            if total_cost > wallet_stats['balance']:
                db.execute("ROLLBACK")
                db.close()
                flash(f'Insufficient balance! Need \u20b9{total_cost:.0f} but you have \u20b9{wallet_stats["balance"]:.0f}. '
                      f'Please recharge your wallet.', 'danger')
                return redirect(url_for('lead_pool'))

            now = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
            for row in available:
                db.execute(
                    "UPDATE leads SET assigned_to=?, in_pool=0, claimed_at=?, "
                    "pipeline_stage=CASE WHEN COALESCE(pipeline_stage,'')='' THEN 'prospecting' ELSE pipeline_stage END, "
                    "status=CASE WHEN COALESCE(status,'')='' THEN 'New Lead' ELSE status END, "
                    "updated_at=? WHERE id=?",
                    (username, now, now, row['id'])
                )

            db.commit()
            _log_activity(db, username, 'lead_claim', f"Claimed {len(available)} leads")
            db.close()
            flash(f'Successfully claimed {len(available)} leads for \u20b9{total_cost:.0f}! '
                  f'Check "My Leads" to view them.', 'success')
            return redirect(url_for('leads'))

        except Exception as e:
            try:
                db.execute("ROLLBACK")
            except Exception:
                pass
            db.close()
            flash('Something went wrong while claiming leads. Please try again.', 'danger')
            return redirect(url_for('lead_pool'))
