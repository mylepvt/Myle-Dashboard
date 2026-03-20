"""
Profile, password, help, earnings, and activity-feed routes.

Registered via register_profile_routes(app) at the end of app.py load so helpers
on the app module are available without circular import at import time.
"""
from __future__ import annotations

import base64

from flask import (
    abort,
    flash,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

from database import get_db


def register_profile_routes(app):
    """Attach profile-related URL rules to the Flask app (preserves endpoint names)."""
    from app import _push_all_team  # noqa: PLC0415 — late import
    from decorators import admin_required, login_required
    from helpers import (
        BADGE_DEFS,
        _get_downline_usernames,
        _get_setting,
        _today_ist,
    )

    @app.route('/change-password', methods=['GET', 'POST'])
    @login_required
    def change_password():
        if request.method == 'POST':
            current_pw  = request.form.get('current_password', '').strip()
            new_pw      = request.form.get('new_password', '').strip()
            confirm_pw  = request.form.get('confirm_password', '').strip()

            if not current_pw or not new_pw or not confirm_pw:
                flash('All fields are required.', 'danger')
                return render_template('change_password.html')

            if new_pw != confirm_pw:
                flash('New password and confirmation do not match.', 'danger')
                return render_template('change_password.html')

            if len(new_pw) < 6:
                flash('New password must be at least 6 characters.', 'danger')
                return render_template('change_password.html')

            db   = get_db()
            user = db.execute(
                "SELECT id, password FROM users WHERE username=?",
                (session['username'],)
            ).fetchone()

            if not user or not check_password_hash(user['password'], current_pw):
                db.close()
                flash('Current password is incorrect.', 'danger')
                return render_template('change_password.html')

            db.execute(
                "UPDATE users SET password=? WHERE id=?",
                (generate_password_hash(new_pw, method='pbkdf2:sha256'), user['id'])
            )
            db.commit()
            db.close()
            flash('Password changed successfully!', 'success')
            if session.get('role') == 'admin':
                return redirect(url_for('admin_dashboard'))
            return redirect(url_for('team_dashboard'))

        return render_template('change_password.html')

    @app.route('/help')
    @login_required
    def help_page():
        return render_template('help.html')

    @app.route('/profile', methods=['GET', 'POST'])
    @login_required
    def profile():
        username = session['username']
        db       = get_db()

        if request.method == 'POST':
            action = request.form.get('action', 'update_info')

            if action == 'upload_dp':
                f = request.files.get('dp_file')
                if f and f.filename:
                    allowed = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
                    ext = f.filename.rsplit('.', 1)[-1].lower() if '.' in f.filename else ''
                    if ext not in allowed:
                        flash('Only PNG, JPG, GIF, WEBP images allowed.', 'danger')
                    else:
                        img_data = f.read()
                        if len(img_data) > 2 * 1024 * 1024:
                            flash('Image too large. Max 2 MB.', 'danger')
                        else:
                            try:
                                from PIL import Image
                                import io as _io
                                img = Image.open(_io.BytesIO(img_data))
                                img = img.convert('RGB')
                                img.thumbnail((100, 100))
                                buf = _io.BytesIO()
                                img.save(buf, format='JPEG', quality=80)
                                img_data = buf.getvalue()
                            except Exception:
                                pass
                            dp_b64 = 'data:image/jpeg;base64,' + base64.b64encode(img_data).decode()
                            db.execute("UPDATE users SET display_picture=? WHERE username=?",
                                       (dp_b64, username))
                            db.commit()
                            session['has_dp'] = True    # flag only — image served via /profile/dp
                            session.pop('dp', None)     # clear any legacy base64 from old sessions
                            flash('Profile picture updated!', 'success')
                else:
                    flash('No file selected.', 'danger')

            elif action == 'remove_dp':
                db.execute("UPDATE users SET display_picture='' WHERE username=?", (username,))
                db.commit()
                session['has_dp'] = False
                session.pop('dp', None)   # clear any legacy base64
                flash('Profile picture removed.', 'info')

            else:  # update_info
                phone     = request.form.get('phone', '').strip()
                email     = request.form.get('email', '').strip()
                db.execute(
                    "UPDATE users SET phone=?, email=? WHERE username=?",
                    (phone, email, username)
                )
                db.commit()
                flash('Profile updated!', 'success')

            db.close()
            return redirect(url_for('profile'))

        user = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        db.close()
        return render_template('profile.html', user=user)

    @app.route('/profile/dp')
    @login_required
    def profile_dp():
        """Serve the current user's display picture from the DB.
        Using a route instead of storing base64 in the session cookie keeps
        cookie size well under the 4 KB browser limit.
        """
        db  = get_db()
        row = db.execute(
            "SELECT display_picture FROM users WHERE username=?", (session['username'],)
        ).fetchone()
        db.close()
        dp = row['display_picture'] if row else ''
        if not dp:
            abort(404)
        # dp is stored as 'data:image/jpeg;base64,<b64>'
        if ',' in dp:
            header, b64data = dp.split(',', 1)
            mime = header.replace('data:', '').replace(';base64', '') or 'image/jpeg'
        else:
            b64data = dp
            mime = 'image/jpeg'
        try:
            img_bytes = base64.b64decode(b64data)
        except Exception:
            abort(404)
        resp = make_response(img_bytes)
        resp.headers['Content-Type'] = mime
        resp.headers['Cache-Control'] = 'private, max-age=300'
        return resp

    @app.route('/profile/badges')
    @login_required
    def profile_badges():
        """Return earned badges for the current user (JSON)."""
        db = get_db()
        rows = db.execute(
            "SELECT badge_key, unlocked_at FROM user_badges WHERE username=? ORDER BY unlocked_at",
            (session['username'],)
        ).fetchall()
        db.close()
        result = []
        for r in rows:
            d = BADGE_DEFS.get(r['badge_key'])
            if d:
                result.append({**d, 'key': r['badge_key'], 'unlocked_at': r['unlocked_at']})
        return {'badges': result}

    @app.route('/api/activity-feed')
    @login_required
    def api_activity_feed():
        """Return recent activity from the user's network for the live feed."""
        db       = get_db()
        username = session['username']
        since    = request.args.get('since', '')

        network = _get_downline_usernames(db, username)
        if not network:
            db.close()
            return {'events': [], 'latest': ''}

        placeholders = ','.join('?' * len(network))
        params = list(network)

        if since:
            rows = db.execute(
                f"SELECT username, event_type, details, created_at FROM activity_log "
                f"WHERE username IN ({placeholders}) AND created_at > ? "
                f"ORDER BY created_at DESC LIMIT 20",
                params + [since]
            ).fetchall()
        else:
            rows = db.execute(
                f"SELECT username, event_type, details, created_at FROM activity_log "
                f"WHERE username IN ({placeholders}) "
                f"ORDER BY created_at DESC LIMIT 20",
                params
            ).fetchall()

        db.close()
        events = [dict(r) for r in rows]
        latest = events[0]['created_at'] if events else ''
        return {'events': events, 'latest': latest}

    @app.route('/earnings')
    @login_required
    def earnings():
        db       = get_db()
        username = session['username']

        # Commission rates from settings (admin configurable)
        gen1_rate = float(_get_setting(db, 'commission_gen1', '10')) / 100
        gen2_rate = float(_get_setting(db, 'commission_gen2', '5'))  / 100
        gen3_rate = float(_get_setting(db, 'commission_gen3', '2'))  / 100

        # My own payments
        my_paid = db.execute(
            "SELECT COALESCE(SUM(payment_amount),0) as total FROM leads "
            "WHERE assigned_to=? AND payment_done=1 AND in_pool=0 AND deleted_at=''",
            (username,)
        ).fetchone()['total']

        # Gen 1 downline usernames
        gen1_users = [r['username'] for r in db.execute(
            "SELECT username FROM users WHERE upline_name=? AND role='team' AND status='approved'",
            (username,)
        ).fetchall()]

        gen2_users = []
        for u in gen1_users:
            gen2_users += [r['username'] for r in db.execute(
                "SELECT username FROM users WHERE upline_name=? AND role='team' AND status='approved'",
                (u,)
            ).fetchall()]

        gen3_users = []
        for u in gen2_users:
            gen3_users += [r['username'] for r in db.execute(
                "SELECT username FROM users WHERE upline_name=? AND role='team' AND status='approved'",
                (u,)
            ).fetchall()]

        def _sum_payments(users):
            if not users:
                return 0.0
            ph = ','.join('?' * len(users))
            return db.execute(
                f"SELECT COALESCE(SUM(payment_amount),0) as t FROM leads "
                f"WHERE assigned_to IN ({ph}) AND payment_done=1 AND in_pool=0 AND deleted_at=''",
                users
            ).fetchone()['t']

        gen1_paid = _sum_payments(gen1_users)
        gen2_paid = _sum_payments(gen2_users)
        gen3_paid = _sum_payments(gen3_users)

        my_earn   = my_paid  * gen1_rate
        gen1_earn = gen1_paid * gen2_rate
        gen2_earn = gen2_paid * gen3_rate
        gen3_earn = gen3_paid * (gen3_rate / 2)   # half rate for gen 3+
        total_earn = my_earn + gen1_earn + gen2_earn + gen3_earn

        # Monthly breakdown (my own payments by month)
        monthly = db.execute("""
            SELECT strftime('%Y-%m', updated_at) as month,
                   COUNT(*) as count,
                   SUM(payment_amount) as amount
            FROM leads
            WHERE assigned_to=? AND payment_done=1 AND in_pool=0
              AND deleted_at=''
            GROUP BY month ORDER BY month DESC LIMIT 12
        """, (username,)).fetchall()

        db.close()
        return render_template('earnings.html',
                               my_paid=my_paid,   my_earn=my_earn,
                               gen1_paid=gen1_paid, gen1_earn=gen1_earn, gen1_count=len(gen1_users),
                               gen2_paid=gen2_paid, gen2_earn=gen2_earn, gen2_count=len(gen2_users),
                               gen3_paid=gen3_paid, gen3_earn=gen3_earn, gen3_count=len(gen3_users),
                               total_earn=total_earn,
                               gen1_rate=gen1_rate, gen2_rate=gen2_rate, gen3_rate=gen3_rate,
                               monthly=monthly)

    @app.route('/profile/change-username', methods=['POST'])
    @login_required
    def change_username():
        if session.get('role') != 'admin':
            flash('Only admin can change username.', 'danger')
            return redirect(url_for('profile'))

        old_username = session['username']
        new_username = request.form.get('new_username', '').strip()

        if not new_username:
            flash('New username cannot be empty.', 'danger')
            return redirect(url_for('profile'))
        if new_username == old_username:
            flash('New username is the same as current.', 'warning')
            return redirect(url_for('profile'))
        if len(new_username) < 3:
            flash('Username must be at least 3 characters.', 'danger')
            return redirect(url_for('profile'))

        db = get_db()
        existing = db.execute("SELECT id FROM users WHERE username=?", (new_username,)).fetchone()
        if existing:
            db.close()
            flash(f'Username "{new_username}" is already taken.', 'danger')
            return redirect(url_for('profile'))

        try:
            # Cascade update all tables in one transaction
            db.execute("UPDATE users        SET username=?    WHERE username=?", (new_username, old_username))
            db.execute("UPDATE leads        SET assigned_to=? WHERE assigned_to=?", (new_username, old_username))
            db.execute("UPDATE leads        SET referred_by=? WHERE referred_by=?", (new_username, old_username))
            db.execute("UPDATE daily_reports SET username=?   WHERE username=?", (new_username, old_username))
            db.execute("UPDATE wallet_recharges SET username=? WHERE username=?", (new_username, old_username))
            db.execute("UPDATE announcements SET created_by=? WHERE created_by=?", (new_username, old_username))
            db.execute("UPDATE push_subscriptions SET username=? WHERE username=?", (new_username, old_username))
            db.execute("UPDATE users        SET upline_name=? WHERE upline_name=?", (new_username, old_username))
            db.execute("UPDATE activity_log SET username=?    WHERE username=?", (new_username, old_username))
            db.commit()
            session['username'] = new_username
            flash(f'Username changed to "{new_username}" successfully.', 'success')
        except Exception as e:
            db.rollback()
            flash(f'Error changing username: {str(e)}', 'danger')
        finally:
            db.close()

        return redirect(url_for('profile'))
