"""
Authentication routes (register, login, logout, password reset).

Registered via register_auth_routes(app) at the end of app.py load so helpers
on the app module are available without circular import at import time.
"""
from __future__ import annotations

import datetime
import secrets

from flask import flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from database import get_db


def register_auth_routes(app):
    """Attach auth-related URL rules to the Flask app (preserves endpoint names)."""
    from app import (  # noqa: PLC0415 — late import after app module is populated
        _log_activity,
        _now_ist,
        _send_password_reset_email,
        _today_ist,
    )

    @app.route('/register', methods=['GET', 'POST'])
    def register():
        if 'username' in session:
            return redirect(url_for('index'))

        if request.method == 'POST':
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '').strip()
            email = request.form.get('email', '').strip()
            fbo_id = request.form.get('fbo_id', '').strip()
            upline_fbo_id = request.form.get('upline_fbo_id', '').strip()
            phone = request.form.get('phone', '').strip()

            if not username or not password or not email or not fbo_id or not upline_fbo_id:
                flash('Username, Password, Email, FBO ID, and Upline FBO ID are required.', 'danger')
                return render_template('register.html')

            db = get_db()

            if db.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone():
                db.close()
                flash('That username is already taken. Please choose another.', 'danger')
                return render_template('register.html')

            if db.execute("SELECT id FROM users WHERE fbo_id=? AND fbo_id!=''", (fbo_id,)).fetchone():
                db.close()
                flash('That FBO ID is already registered. Each FBO ID must be unique.', 'danger')
                return render_template('register.html')

            if phone and db.execute("SELECT id FROM users WHERE phone=? AND phone!=''", (phone,)).fetchone():
                db.close()
                flash('That mobile number is already registered. Please use a different number.', 'danger')
                return render_template('register.html')

            upline_user = db.execute(
                "SELECT username FROM users WHERE fbo_id=?", (upline_fbo_id,)
            ).fetchone()
            if not upline_user:
                db.close()
                flash(
                    f'Upline FBO ID "{upline_fbo_id}" not found. Please ask your upline for their correct FBO ID.',
                    'danger',
                )
                return render_template('register.html')
            upline_name = upline_user['username']

            is_new = 1 if request.form.get('is_new_joining') else 0
            joining_dt = request.form.get('joining_date', '').strip()
            t_status = 'pending' if is_new else 'not_required'

            db.execute(
                "INSERT INTO users (username, password, role, fbo_id, upline_name, upline_username, phone, email, status, "
                "training_required, training_status, joining_date) "
                "VALUES (?, ?, 'team', ?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
                (
                    username,
                    generate_password_hash(password, method='pbkdf2:sha256'),
                    fbo_id,
                    upline_name,
                    upline_name,
                    phone,
                    email,
                    is_new,
                    t_status,
                    joining_dt,
                ),
            )
            db.commit()
            db.close()
            flash('Registration submitted! Your account is pending admin approval.', 'success')
            return redirect(url_for('login'))

        today = _today_ist().isoformat()
        return render_template('register.html', today=today)

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if 'username' in session:
            return redirect(url_for('index'))

        if request.method == 'POST':
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '').strip()

            if not username or not password:
                flash('Username and password are required.', 'danger')
                return render_template('login.html')

            db = get_db()
            user = db.execute(
                "SELECT * FROM users WHERE username=?", (username,)
            ).fetchone()

            password_ok = False
            if user:
                stored = user['password']
                if stored.startswith(('pbkdf2:', 'scrypt:', 'argon2:')):
                    password_ok = check_password_hash(stored, password)
                else:
                    password_ok = stored == password
                    if password_ok:
                        db.execute(
                            "UPDATE users SET password=? WHERE id=?",
                            (generate_password_hash(password, method='pbkdf2:sha256'), user['id']),
                        )
                        db.commit()

            db.close()

            if user and password_ok:
                if user['status'] == 'pending':
                    flash('Your account is pending admin approval. Please check back soon.', 'warning')
                    return render_template('login.html')
                if user['status'] == 'rejected':
                    flash('Your registration request was rejected. Contact the admin for help.', 'danger')
                    return render_template('login.html')

                session.permanent = True
                session['username'] = user['username']
                session['role'] = user['role']
                session['has_dp'] = bool(user['display_picture'])
                keys = user.keys() if hasattr(user, 'keys') else []
                session['training_status'] = user['training_status'] if 'training_status' in keys else 'not_required'
                db = get_db()
                _log_activity(db, user['username'], 'login', f"Role: {user['role']}")
                db.close()
                flash(f'Welcome back, {user["username"]}!', 'success')
                if user['role'] == 'admin':
                    return redirect(url_for('admin_dashboard'))
                return redirect(url_for('team_dashboard'))
            flash('Invalid username or password.', 'danger')

        return render_template('login.html')

    @app.route('/forgot-password', methods=['GET', 'POST'])
    def forgot_password():
        if 'username' in session:
            return redirect(url_for('index'))

        email_sent = False
        if request.method == 'POST':
            email = request.form.get('email', '').strip().lower()
            if email:
                db = get_db()
                user = db.execute(
                    "SELECT username, email FROM users WHERE LOWER(email)=? AND status='approved'",
                    (email,),
                ).fetchone()
                if user:
                    token = secrets.token_urlsafe(32)
                    expires_at = (_now_ist() + datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
                    db.execute(
                        "INSERT INTO password_reset_tokens (username, token, expires_at) VALUES (?,?,?)",
                        (user['username'], token, expires_at),
                    )
                    db.commit()
                    reset_url = url_for('reset_password', token=token, _external=True)
                    sent = _send_password_reset_email(user['email'], user['username'], reset_url)
                    if not sent:
                        flash(f'SMTP not configured. Reset link (share manually): {reset_url}', 'warning')
                db.close()
            email_sent = True

        return render_template('forgot_password.html', email_sent=email_sent)

    @app.route('/reset-password/<token>', methods=['GET', 'POST'])
    def reset_password(token):
        if 'username' in session:
            return redirect(url_for('index'))

        db = get_db()
        row = db.execute(
            "SELECT * FROM password_reset_tokens WHERE token=? AND used=0",
            (token,),
        ).fetchone()

        if not row:
            db.close()
            flash('This password reset link is invalid or has already been used.', 'danger')
            return redirect(url_for('login'))

        expires_at = datetime.datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S')
        if _now_ist() > expires_at:
            db.close()
            flash('This password reset link has expired. Please request a new one.', 'danger')
            return redirect(url_for('forgot_password'))

        if request.method == 'POST':
            new_password = request.form.get('password', '').strip()
            confirm = request.form.get('confirm_password', '').strip()
            if not new_password or len(new_password) < 6:
                flash('Password must be at least 6 characters.', 'danger')
                db.close()
                return render_template('reset_password.html', token=token)
            if new_password != confirm:
                flash('Passwords do not match.', 'danger')
                db.close()
                return render_template('reset_password.html', token=token)

            db.execute(
                "UPDATE users SET password=? WHERE username=?",
                (generate_password_hash(new_password, method='pbkdf2:sha256'), row['username']),
            )
            db.execute("UPDATE password_reset_tokens SET used=1 WHERE id=?", (row['id'],))
            db.commit()
            db.close()
            flash('Password updated successfully! Please sign in.', 'success')
            return redirect(url_for('login'))

        db.close()
        return render_template('reset_password.html', token=token)

    @app.route('/logout')
    def logout():
        if 'username' in session:
            db = get_db()
            _log_activity(db, session['username'], 'logout', '')
            db.close()
        session.clear()
        flash('You have been logged out.', 'info')
        return redirect(url_for('login'))
