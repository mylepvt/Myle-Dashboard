import os
import io
import csv
import base64
import hashlib
import hmac
import json
import datetime
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from flask import (Flask, render_template, request, redirect, url_for,
                   flash, session, Response)
from werkzeug.security import generate_password_hash, check_password_hash
from database import get_db, init_db, migrate_db, seed_users

# Optional QR code support
try:
    import qrcode
    QR_AVAILABLE = True
except ImportError:
    QR_AVAILABLE = False

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'myle_community_secret_2024_local')
app.permanent_session_lifetime = datetime.timedelta(days=3650)  # ~10 years = effectively forever

STATUSES = ['New', 'Contacted', 'Day 1', 'Day 2', 'Interview', 'Converted', 'Lost']
SOURCES  = ['WhatsApp', 'Facebook', 'Instagram', 'LinkedIn',
            'Walk-in', 'Referral', 'YouTube', 'Cold Call', 'Meta', 'Other']
PAYMENT_AMOUNT = 196.0


# ─────────────────────────────────────────────
#  Auth Decorators
# ─────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'username' not in session:
            flash('Please log in to continue.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'username' not in session:
            flash('Please log in to continue.', 'warning')
            return redirect(url_for('login'))
        if session.get('role') != 'admin':
            flash('Admin access required.', 'danger')
            return redirect(url_for('team_dashboard'))
        return f(*args, **kwargs)
    return decorated


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

def _get_metrics(db, username=None):
    """All dashboard KPIs. Excludes pool leads (in_pool=1)."""
    if username:
        where_clause = "WHERE assigned_to = ? AND in_pool = 0"
        params = (username,)
    else:
        where_clause = "WHERE in_pool = 0"
        params = ()

    row = db.execute(f"""
        SELECT
            COUNT(*)                                                      AS total,
            SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END)          AS converted,
            SUM(CASE WHEN payment_done=1     THEN 1 ELSE 0 END)          AS paid,
            SUM(COALESCE(payment_amount,0) + COALESCE(revenue,0))        AS revenue,
            SUM(CASE WHEN day1_done=1        THEN 1 ELSE 0 END)          AS day1,
            SUM(CASE WHEN day2_done=1        THEN 1 ELSE 0 END)          AS day2,
            SUM(CASE WHEN interview_done=1   THEN 1 ELSE 0 END)          AS interviews,
            ROUND(
                CAST(SUM(CASE WHEN payment_done=1 THEN 1 ELSE 0 END) AS REAL)
                / NULLIF(COUNT(*), 0) * 100
            , 1)                                                          AS paid196_pct,
            ROUND(
                CAST(SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END) AS REAL)
                / NULLIF(COUNT(*), 0) * 100
            , 1)                                                          AS close_pct,
            ROUND(
                SUM(COALESCE(payment_amount,0) + COALESCE(revenue,0))
                / NULLIF(COUNT(*), 0)
            , 2)                                                          AS rev_per_lead
        FROM leads {where_clause}
    """, params).fetchone()

    return dict(
        total        = row['total']        or 0,
        converted    = row['converted']    or 0,
        paid         = row['paid']         or 0,
        revenue      = row['revenue']      or 0.0,
        day1         = row['day1']         or 0,
        day2         = row['day2']         or 0,
        interviews   = row['interviews']   or 0,
        paid196_pct  = row['paid196_pct']  or 0.0,
        close_pct    = row['close_pct']    or 0.0,
        rev_per_lead = row['rev_per_lead'] or 0.0,
        conv_rate    = row['close_pct']    or 0.0,
    )


def _get_setting(db, key, default=''):
    """Get an app setting value."""
    row = db.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    return row['value'] if row else default


def _set_setting(db, key, value):
    """Upsert an app setting."""
    db.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )


def _get_wallet(db, username):
    """Compute wallet stats for a team member."""
    recharged = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM wallet_recharges "
        "WHERE username=? AND status='approved'",
        (username,)
    ).fetchone()[0] or 0.0

    spent = db.execute(
        "SELECT COALESCE(SUM(pool_price), 0) FROM leads "
        "WHERE assigned_to=? AND in_pool=0 AND claimed_at!=''",
        (username,)
    ).fetchone()[0] or 0.0

    return {
        'recharged': recharged,
        'spent':     spent,
        'balance':   recharged - spent,
    }


def _generate_upi_qr_bytes(upi_id):
    """Generate UPI QR code PNG bytes. Returns None if qrcode not available."""
    if not QR_AVAILABLE or not upi_id:
        return None
    upi_string = f"upi://pay?pa={upi_id}&pn=Myle+Community&cu=INR"
    qr = qrcode.QRCode(version=1, box_size=8, border=4)
    qr.add_data(upi_string)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    return buf.getvalue()


def _generate_upi_qr_base64(upi_id):
    """Generate UPI QR as base64 string."""
    data = _generate_upi_qr_bytes(upi_id)
    return base64.b64encode(data).decode('utf-8') if data else None


def _send_welcome_email(user_email, username, login_url):
    """Send welcome email when a team member is approved. Silently skips if SMTP not configured."""
    db = get_db()
    smtp_host     = _get_setting(db, 'smtp_host', '')
    smtp_port     = int(_get_setting(db, 'smtp_port', '587') or 587)
    smtp_user     = _get_setting(db, 'smtp_user', '')
    smtp_password = _get_setting(db, 'smtp_password', '')
    from_name     = _get_setting(db, 'smtp_from_name', 'Myle Community')
    db.close()

    if not smtp_host or not smtp_user or not smtp_password or not user_email:
        return  # SMTP not configured, skip silently

    subject = 'Welcome to Myle Community – Account Approved!'

    html_body = f"""
    <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;border:1px solid #e0e0e0;">
      <div style="background:linear-gradient(135deg,#1a1a2e,#0f3460);padding:32px;text-align:center;">
        <h2 style="color:#fff;margin:0;font-size:22px;">Myle Community</h2>
        <p style="color:rgba(255,255,255,0.7);margin:8px 0 0;font-size:14px;">Team Dashboard</p>
      </div>
      <div style="padding:32px;">
        <h3 style="color:#1a1a2e;margin-top:0;">Hi {username}, your account is approved! 🎉</h3>
        <p style="color:#555;line-height:1.6;">
          Great news! Your registration request for <strong>Myle Community</strong> has been approved by the admin.
          You can now log in and access your dashboard.
        </p>
        <div style="background:#f0f4ff;border-radius:8px;padding:16px;margin:20px 0;border-left:4px solid #6366f1;">
          <p style="margin:0;color:#333;font-size:14px;"><strong>Username:</strong> {username}</p>
        </div>
        <div style="text-align:center;margin:28px 0;">
          <a href="{login_url}"
             style="background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:600;font-size:15px;display:inline-block;">
            Login to Dashboard &rarr;
          </a>
        </div>
        <p style="color:#888;font-size:13px;line-height:1.6;">
          From your dashboard you can:<br>
          &bull; View and manage your leads<br>
          &bull; Submit daily reports<br>
          &bull; Recharge wallet &amp; claim leads from pool
        </p>
      </div>
      <div style="background:#f8f9fa;padding:16px;text-align:center;border-top:1px solid #e0e0e0;">
        <p style="color:#aaa;font-size:12px;margin:0;">Myle Community &mdash; Internal Team Portal</p>
      </div>
    </div>
    """

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = f'{from_name} <{smtp_user}>'
    msg['To']      = user_email
    msg.attach(MIMEText(html_body, 'html'))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, user_email, msg.as_string())
    except Exception:
        pass  # Don't break approval flow if email fails


# ─────────────────────────────────────────────
#  Context processor – inject counts for nav badges
# ─────────────────────────────────────────────

@app.context_processor
def inject_pending_count():
    if session.get('role') == 'admin':
        db             = get_db()
        pending_users  = db.execute(
            "SELECT COUNT(*) FROM users WHERE status='pending'"
        ).fetchone()[0]
        wallet_pending = db.execute(
            "SELECT COUNT(*) FROM wallet_recharges WHERE status='pending'"
        ).fetchone()[0]
        db.close()
        return {'pending_count': pending_users, 'wallet_pending': wallet_pending}
    return {'pending_count': 0, 'wallet_pending': 0}


# ─────────────────────────────────────────────
#  Register
# ─────────────────────────────────────────────

@app.route('/register', methods=['GET', 'POST'])
def register():
    if 'username' in session:
        return redirect(url_for('index'))

    if request.method == 'POST':
        username    = request.form.get('username', '').strip()
        password    = request.form.get('password', '').strip()
        email       = request.form.get('email', '').strip()
        fbo_id      = request.form.get('fbo_id', '').strip()
        upline_name = request.form.get('upline_name', '').strip()
        phone       = request.form.get('phone', '').strip()

        if not username or not password or not email or not fbo_id or not upline_name:
            flash('Username, Password, Email, FBO ID, and Upline Name are required.', 'danger')
            return render_template('register.html')

        db = get_db()
        existing = db.execute(
            "SELECT id FROM users WHERE username=?", (username,)
        ).fetchone()

        if existing:
            db.close()
            flash('That username is already taken. Please choose another.', 'danger')
            return render_template('register.html')

        db.execute(
            "INSERT INTO users (username, password, role, fbo_id, upline_name, phone, email, status) "
            "VALUES (?, ?, 'team', ?, ?, ?, ?, 'pending')",
            (username, generate_password_hash(password, method='pbkdf2:sha256'),
             fbo_id, upline_name, phone, email)
        )
        db.commit()
        db.close()
        flash('Registration submitted! Your account is pending admin approval.', 'success')
        return redirect(url_for('login'))

    return render_template('register.html')


# ─────────────────────────────────────────────
#  Login / Logout
# ─────────────────────────────────────────────

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

        db   = get_db()
        user = db.execute(
            "SELECT * FROM users WHERE username=?", (username,)
        ).fetchone()

        password_ok = False
        if user:
            stored = user['password']
            if stored.startswith(('pbkdf2:', 'scrypt:', 'argon2:')):
                password_ok = check_password_hash(stored, password)
            else:
                password_ok = (stored == password)
                if password_ok:
                    db.execute("UPDATE users SET password=? WHERE id=?",
                               (generate_password_hash(password, method='pbkdf2:sha256'), user['id']))
                    db.commit()

        db.close()

        if user and password_ok:
            if user['status'] == 'pending':
                flash('Your account is pending admin approval. Please check back soon.', 'warning')
                return render_template('login.html')
            if user['status'] == 'rejected':
                flash('Your registration request was rejected. Contact the admin for help.', 'danger')
                return render_template('login.html')

            # Remember Me – keep session alive for 30 days
            if request.form.get('remember_me'):
                session.permanent = True

            session['username'] = user['username']
            session['role']     = user['role']
            flash(f'Welcome back, {user["username"]}!', 'success')
            if user['role'] == 'admin':
                return redirect(url_for('admin_dashboard'))
            return redirect(url_for('team_dashboard'))
        else:
            flash('Invalid username or password.', 'danger')

    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


# ─────────────────────────────────────────────
#  Admin – Approvals
# ─────────────────────────────────────────────

@app.route('/admin/approvals')
@admin_required
def admin_approvals():
    filter_by = request.args.get('filter', 'all')
    db = get_db()

    query  = "SELECT * FROM users WHERE role != 'admin'"
    params = []
    if filter_by in ('pending', 'approved', 'rejected'):
        query += " AND status=?"
        params.append(filter_by)
    query += " ORDER BY created_at DESC"

    users = db.execute(query, params).fetchall()
    db.close()
    return render_template('admin_approvals.html', users=users, filter_by=filter_by)


@app.route('/admin/approvals/<int:user_id>/approve', methods=['POST'])
@admin_required
def approve_user(user_id):
    db   = get_db()
    user = db.execute("SELECT username, email FROM users WHERE id=?", (user_id,)).fetchone()
    if user:
        db.execute("UPDATE users SET status='approved' WHERE id=?", (user_id,))
        db.commit()
        flash(f'"{user["username"]}" has been approved and can now log in.', 'success')
        # Send welcome email (non-blocking, fails silently if SMTP not configured)
        login_url = request.host_url.rstrip('/') + url_for('login')
        _send_welcome_email(user['email'], user['username'], login_url)
    db.close()
    return redirect(url_for('admin_approvals', filter=request.form.get('current_filter', 'all')))


@app.route('/admin/approvals/<int:user_id>/reject', methods=['POST'])
@admin_required
def reject_user(user_id):
    db   = get_db()
    user = db.execute("SELECT username FROM users WHERE id=?", (user_id,)).fetchone()
    if user:
        db.execute("UPDATE users SET status='rejected' WHERE id=?", (user_id,))
        db.commit()
        flash(f'"{user["username"]}" registration has been rejected.', 'warning')
    db.close()
    return redirect(url_for('admin_approvals', filter=request.form.get('current_filter', 'all')))


# ─────────────────────────────────────────────
#  Root redirect
# ─────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    if session.get('role') == 'admin':
        return redirect(url_for('admin_dashboard'))
    return redirect(url_for('team_dashboard'))


# ─────────────────────────────────────────────
#  Admin Dashboard
# ─────────────────────────────────────────────

@app.route('/admin')
@admin_required
def admin_dashboard():
    db      = get_db()
    metrics = _get_metrics(db)

    recent = db.execute(
        "SELECT * FROM leads WHERE in_pool=0 ORDER BY created_at DESC LIMIT 5"
    ).fetchall()

    status_data = {}
    for s in STATUSES:
        count = db.execute(
            "SELECT COUNT(*) as c FROM leads WHERE status=? AND in_pool=0", (s,)
        ).fetchone()['c']
        status_data[s] = count

    monthly = db.execute("""
        SELECT strftime('%Y-%m', created_at) as month,
               SUM(payment_amount) as total
        FROM leads
        WHERE payment_done=1 AND in_pool=0
        GROUP BY month
        ORDER BY month DESC
        LIMIT 6
    """).fetchall()

    members = db.execute("SELECT * FROM team_members ORDER BY name").fetchall()
    team_stats = []
    for m in members:
        row = db.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END) as converted,
                SUM(CASE WHEN payment_done=1     THEN 1 ELSE 0 END) as paid,
                SUM(COALESCE(payment_amount,0) + COALESCE(revenue,0)) as revenue
            FROM leads WHERE assigned_to=? AND in_pool=0
        """, (m['name'],)).fetchone()
        team_stats.append({'member': m, 'stats': row})

    pending_users = db.execute(
        "SELECT * FROM users WHERE status='pending' ORDER BY created_at DESC"
    ).fetchall()

    today = datetime.date.today().isoformat()
    today_reports = db.execute(
        "SELECT * FROM daily_reports WHERE report_date=? ORDER BY submitted_at DESC",
        (today,)
    ).fetchall()
    approved_team = db.execute(
        "SELECT username FROM users WHERE role='team' AND status='approved'"
    ).fetchall()
    missing_reports = [u['username'] for u in approved_team
                       if u['username'] not in [r['username'] for r in today_reports]]

    report_verification = {}
    for r in today_reports:
        actual_payments = db.execute("""
            SELECT COUNT(*) as cnt FROM leads
            WHERE assigned_to=? AND payment_done=1
              AND date(updated_at) = ? AND in_pool=0
        """, (r['username'], today)).fetchone()['cnt']
        report_verification[r['username']] = actual_payments

    # Wallet pending count (also in context processor but useful for template)
    wallet_pending_count = db.execute(
        "SELECT COUNT(*) FROM wallet_recharges WHERE status='pending'"
    ).fetchone()[0]

    # Pool summary for admin dashboard
    pool_count = db.execute("SELECT COUNT(*) FROM leads WHERE in_pool=1").fetchone()[0]

    db.close()
    return render_template('admin.html',
                           metrics=metrics,
                           recent=recent,
                           status_data=status_data,
                           monthly=monthly,
                           team_stats=team_stats,
                           pending_users=pending_users,
                           payment_amount=PAYMENT_AMOUNT,
                           today_reports=today_reports,
                           missing_reports=missing_reports,
                           report_verification=report_verification,
                           today=today,
                           wallet_pending_count=wallet_pending_count,
                           pool_count=pool_count)


# ─────────────────────────────────────────────
#  Team Dashboard  (scoped to logged-in user)
# ─────────────────────────────────────────────

@app.route('/dashboard')
@login_required
def team_dashboard():
    username = session['username']
    db       = get_db()
    metrics  = _get_metrics(db, username=username)
    wallet   = _get_wallet(db, username)

    recent = db.execute(
        "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 ORDER BY created_at DESC LIMIT 5",
        (username,)
    ).fetchall()

    status_data = {}
    for s in STATUSES:
        count = db.execute(
            "SELECT COUNT(*) as c FROM leads WHERE status=? AND assigned_to=? AND in_pool=0",
            (s, username)
        ).fetchone()['c']
        status_data[s] = count

    monthly = db.execute("""
        SELECT strftime('%Y-%m', created_at) as month,
               SUM(payment_amount) as total
        FROM leads
        WHERE payment_done=1 AND assigned_to=? AND in_pool=0
        GROUP BY month
        ORDER BY month DESC
        LIMIT 6
    """, (username,)).fetchall()

    today = datetime.date.today().isoformat()
    today_report = db.execute(
        "SELECT * FROM daily_reports WHERE username=? AND report_date=?",
        (username, today)
    ).fetchone()

    pool_count = db.execute("SELECT COUNT(*) FROM leads WHERE in_pool=1").fetchone()[0]

    db.close()
    return render_template('dashboard.html',
                           metrics=metrics,
                           wallet=wallet,
                           recent=recent,
                           status_data=status_data,
                           monthly=monthly,
                           payment_amount=PAYMENT_AMOUNT,
                           today_report=today_report,
                           today=today,
                           pool_count=pool_count)


# ─────────────────────────────────────────────
#  Leads – List
# ─────────────────────────────────────────────

@app.route('/leads')
@login_required
def leads():
    db     = get_db()
    status = request.args.get('status', '')
    search = request.args.get('q', '').strip()

    query  = "SELECT * FROM leads WHERE in_pool=0"
    params = []

    if session.get('role') != 'admin':
        query += " AND assigned_to=?"
        params.append(session['username'])

    if status:
        query += " AND status=?"
        params.append(status)
    if search:
        query += " AND (name LIKE ? OR phone LIKE ? OR email LIKE ?)"
        params += [f'%{search}%', f'%{search}%', f'%{search}%']

    query += " ORDER BY created_at DESC"
    all_leads = db.execute(query, params).fetchall()
    db.close()
    return render_template('leads.html',
                           leads=all_leads,
                           statuses=STATUSES,
                           selected_status=status,
                           search=search)


# ─────────────────────────────────────────────
#  Leads – Add
# ─────────────────────────────────────────────

@app.route('/leads/add', methods=['GET', 'POST'])
@login_required
def add_lead():
    db   = get_db()
    team = db.execute("SELECT name FROM team_members ORDER BY name").fetchall()

    if request.method == 'POST':
        name           = request.form.get('name', '').strip()
        phone          = request.form.get('phone', '').strip()
        email          = request.form.get('email', '').strip()
        referred_by    = request.form.get('referred_by', '').strip()
        source         = request.form.get('source', '').strip()
        status         = request.form.get('status', 'New')
        payment_done   = 1 if request.form.get('payment_done') else 0
        payment_amount = PAYMENT_AMOUNT if payment_done else 0.0
        try:
            revenue = float(request.form.get('revenue') or 0)
        except ValueError:
            revenue = 0.0
        follow_up_date = request.form.get('follow_up_date', '').strip()
        notes          = request.form.get('notes', '').strip()

        if session.get('role') == 'admin':
            assigned_to = request.form.get('assigned_to', '').strip()
        else:
            assigned_to = session['username']

        if not name or not phone:
            flash('Name and Phone are required.', 'danger')
            db.close()
            return render_template('add_lead.html',
                                   statuses=STATUSES, sources=SOURCES, team=team)

        if status not in STATUSES:
            status = 'New'

        db.execute("""
            INSERT INTO leads
                (name, phone, email, referred_by, assigned_to, source,
                 status, payment_done, payment_amount, revenue,
                 follow_up_date, notes, in_pool, pool_price, claimed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '')
        """, (name, phone, email, referred_by, assigned_to, source,
              status, payment_done, payment_amount, revenue,
              follow_up_date, notes))
        db.commit()
        db.close()
        flash(f'Lead "{name}" added successfully.', 'success')
        return redirect(url_for('leads'))

    db.close()
    return render_template('add_lead.html',
                           statuses=STATUSES, sources=SOURCES, team=team)


# ─────────────────────────────────────────────
#  Leads – Edit / Update
# ─────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_lead(lead_id):
    db   = get_db()
    team = db.execute("SELECT name FROM team_members ORDER BY name").fetchall()

    if session.get('role') == 'admin':
        lead = db.execute(
            "SELECT * FROM leads WHERE id=? AND in_pool=0", (lead_id,)
        ).fetchone()
    else:
        lead = db.execute(
            "SELECT * FROM leads WHERE id=? AND assigned_to=? AND in_pool=0",
            (lead_id, session['username'])
        ).fetchone()

    if not lead:
        flash('Lead not found or access denied.', 'danger')
        db.close()
        return redirect(url_for('leads'))

    if request.method == 'POST':
        name           = request.form.get('name', '').strip()
        phone          = request.form.get('phone', '').strip()
        email          = request.form.get('email', '').strip()
        referred_by    = request.form.get('referred_by', '').strip()
        status         = request.form.get('status', lead['status'])
        payment_done   = 1 if request.form.get('payment_done') else 0
        payment_amount = PAYMENT_AMOUNT if payment_done else 0.0
        day1_done      = 1 if request.form.get('day1_done') else 0
        day2_done      = 1 if request.form.get('day2_done') else 0
        interview_done = 1 if request.form.get('interview_done') else 0
        notes          = request.form.get('notes', '').strip()

        if not name or not phone:
            flash('Name and Phone are required.', 'danger')
            db.close()
            return render_template('edit_lead.html',
                                   lead=lead, statuses=STATUSES,
                                   team=team, payment_amount=PAYMENT_AMOUNT)

        if status not in STATUSES:
            status = lead['status']

        if session.get('role') == 'admin':
            assigned_to = request.form.get('assigned_to', lead['assigned_to']).strip()
        else:
            assigned_to = lead['assigned_to']

        db.execute("""
            UPDATE leads
            SET name=?, phone=?, email=?, referred_by=?, assigned_to=?, status=?,
                payment_done=?, payment_amount=?,
                day1_done=?, day2_done=?, interview_done=?,
                notes=?, updated_at=datetime('now','localtime')
            WHERE id=?
        """, (name, phone, email, referred_by, assigned_to, status,
              payment_done, payment_amount,
              day1_done, day2_done, interview_done,
              notes, lead_id))
        db.commit()
        db.close()
        flash(f'Lead "{name}" updated.', 'success')
        return redirect(url_for('leads'))

    db.close()
    return render_template('edit_lead.html',
                           lead=lead,
                           statuses=STATUSES,
                           team=team,
                           payment_amount=PAYMENT_AMOUNT)


# ─────────────────────────────────────────────
#  Leads – Quick status toggle
# ─────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/status', methods=['POST'])
@login_required
def update_status(lead_id):
    status = request.form.get('status')
    if status not in STATUSES:
        flash('Invalid status.', 'danger')
        return redirect(url_for('leads'))

    db = get_db()

    if session.get('role') != 'admin':
        lead = db.execute(
            "SELECT id FROM leads WHERE id=? AND assigned_to=? AND in_pool=0",
            (lead_id, session['username'])
        ).fetchone()
        if not lead:
            flash('Access denied.', 'danger')
            db.close()
            return redirect(url_for('leads'))

    db.execute(
        "UPDATE leads SET status=?, updated_at=datetime('now','localtime') WHERE id=? AND in_pool=0",
        (status, lead_id)
    )
    db.commit()
    db.close()
    flash('Status updated.', 'success')
    return redirect(request.referrer or url_for('leads'))


# ─────────────────────────────────────────────
#  Leads – Delete
# ─────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/delete', methods=['POST'])
@login_required
def delete_lead(lead_id):
    db = get_db()

    if session.get('role') == 'admin':
        lead = db.execute(
            "SELECT name FROM leads WHERE id=? AND in_pool=0", (lead_id,)
        ).fetchone()
    else:
        lead = db.execute(
            "SELECT name FROM leads WHERE id=? AND assigned_to=? AND in_pool=0",
            (lead_id, session['username'])
        ).fetchone()

    if lead:
        db.execute("DELETE FROM leads WHERE id=?", (lead_id,))
        db.commit()
        flash(f'Lead "{lead["name"]}" deleted.', 'warning')
    else:
        flash('Lead not found or access denied.', 'danger')
    db.close()
    return redirect(url_for('leads'))


# ─────────────────────────────────────────────
#  Team  (Admin only)
# ─────────────────────────────────────────────

@app.route('/team')
@admin_required
def team():
    db      = get_db()
    members = db.execute("SELECT * FROM team_members ORDER BY name").fetchall()

    stats = []
    for m in members:
        row = db.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END) as converted,
                SUM(CASE WHEN payment_done=1    THEN 1 ELSE 0 END) as paid,
                SUM(payment_amount)                                  as revenue,
                SUM(CASE WHEN day1_done=1       THEN 1 ELSE 0 END) as day1,
                SUM(CASE WHEN day2_done=1       THEN 1 ELSE 0 END) as day2,
                SUM(CASE WHEN interview_done=1  THEN 1 ELSE 0 END) as interviews
            FROM leads WHERE referred_by=? AND in_pool=0
        """, (m['name'],)).fetchone()
        stats.append({'member': m, 'stats': row})

    db.close()
    return render_template('team.html', stats=stats)


@app.route('/team/add', methods=['POST'])
@admin_required
def add_team_member():
    name  = request.form.get('name', '').strip()
    phone = request.form.get('phone', '').strip()
    if not name:
        flash('Member name is required.', 'danger')
        return redirect(url_for('team'))
    db = get_db()
    try:
        db.execute("INSERT INTO team_members (name, phone) VALUES (?, ?)", (name, phone))
        db.commit()
        flash(f'Team member "{name}" added.', 'success')
    except Exception:
        flash(f'Member "{name}" already exists.', 'warning')
    db.close()
    return redirect(url_for('team'))


@app.route('/team/<int:member_id>/delete', methods=['POST'])
@admin_required
def delete_team_member(member_id):
    db = get_db()
    member = db.execute("SELECT name FROM team_members WHERE id=?", (member_id,)).fetchone()
    if member:
        db.execute("DELETE FROM team_members WHERE id=?", (member_id,))
        db.commit()
        flash(f'Member "{member["name"]}" removed.', 'warning')
    db.close()
    return redirect(url_for('team'))


# ─────────────────────────────────────────────
#  Daily Reports – Submit (team member)
# ─────────────────────────────────────────────

@app.route('/reports/submit', methods=['GET', 'POST'])
@login_required
def report_submit():
    username = session['username']
    today    = datetime.date.today().isoformat()
    db       = get_db()

    existing = db.execute(
        "SELECT * FROM daily_reports WHERE username=? AND report_date=?",
        (username, today)
    ).fetchone()

    if request.method == 'POST':
        report_date      = request.form.get('report_date', today)
        upline_name      = request.form.get('upline_name', '').strip()
        try:
            total_calling    = int(request.form.get('total_calling') or 0)
            pdf_covered      = int(request.form.get('pdf_covered') or 0)
            calls_picked     = int(request.form.get('calls_picked') or 0)
            wrong_numbers    = int(request.form.get('wrong_numbers') or 0)
            enrollments_done = int(request.form.get('enrollments_done') or 0)
            pending_enroll   = int(request.form.get('pending_enroll') or 0)
            underage         = int(request.form.get('underage') or 0)
            plan_2cc         = int(request.form.get('plan_2cc') or 0)
            seat_holdings    = int(request.form.get('seat_holdings') or 0)
        except ValueError:
            flash('Please enter valid numbers.', 'danger')
            db.close()
            return render_template('report_form.html', existing=existing, today=today)

        leads_educated = request.form.get('leads_educated', '')
        remarks        = request.form.get('remarks', '').strip()

        db.execute("""
            INSERT INTO daily_reports
                (username, upline_name, report_date, total_calling, pdf_covered,
                 calls_picked, wrong_numbers, enrollments_done, pending_enroll,
                 underage, leads_educated, plan_2cc, seat_holdings, remarks,
                 submitted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(username, report_date) DO UPDATE SET
                upline_name=excluded.upline_name,
                total_calling=excluded.total_calling,
                pdf_covered=excluded.pdf_covered,
                calls_picked=excluded.calls_picked,
                wrong_numbers=excluded.wrong_numbers,
                enrollments_done=excluded.enrollments_done,
                pending_enroll=excluded.pending_enroll,
                underage=excluded.underage,
                leads_educated=excluded.leads_educated,
                plan_2cc=excluded.plan_2cc,
                seat_holdings=excluded.seat_holdings,
                remarks=excluded.remarks,
                submitted_at=datetime('now','localtime')
        """, (username, upline_name, report_date, total_calling, pdf_covered,
              calls_picked, wrong_numbers, enrollments_done, pending_enroll,
              underage, leads_educated, plan_2cc, seat_holdings, remarks))
        db.commit()
        db.close()
        flash('Daily report submitted successfully!', 'success')
        return redirect(url_for('team_dashboard'))

    db.close()
    return render_template('report_form.html', existing=existing, today=today,
                           username=username)


# ─────────────────────────────────────────────
#  Daily Reports – Admin View
# ─────────────────────────────────────────────

@app.route('/reports')
@admin_required
def reports_admin():
    db          = get_db()
    filter_date = request.args.get('date', '')
    filter_user = request.args.get('user', '')

    query  = "SELECT * FROM daily_reports WHERE 1=1"
    params = []
    if filter_date:
        query += " AND report_date=?"
        params.append(filter_date)
    if filter_user:
        query += " AND username=?"
        params.append(filter_user)
    query += " ORDER BY report_date DESC, submitted_at DESC"

    reports = db.execute(query, params).fetchall()

    totals = db.execute(f"""
        SELECT
            COUNT(DISTINCT username || report_date) AS total_reports,
            SUM(total_calling)    AS total_calling,
            SUM(pdf_covered)      AS pdf_covered,
            SUM(calls_picked)     AS calls_picked,
            SUM(enrollments_done) AS enrollments_done,
            SUM(plan_2cc)         AS plan_2cc
        FROM daily_reports WHERE 1=1
        {'AND report_date=?' if filter_date else ''}
        {'AND username=?' if filter_user else ''}
    """, params).fetchone()

    members = db.execute(
        "SELECT DISTINCT username FROM daily_reports ORDER BY username"
    ).fetchall()

    today = datetime.date.today().isoformat()
    submitted_today = [r['username'] for r in db.execute(
        "SELECT username FROM daily_reports WHERE report_date=?", (today,)
    ).fetchall()]
    approved_team = [u['username'] for u in db.execute(
        "SELECT username FROM users WHERE role='team' AND status='approved'"
    ).fetchall()]
    missing_today = [u for u in approved_team if u not in submitted_today]

    trend = db.execute("""
        SELECT report_date,
               COUNT(DISTINCT username)  AS reporters,
               SUM(total_calling)        AS calling,
               SUM(enrollments_done)     AS enrolments
        FROM daily_reports
        WHERE report_date >= date('now', '-13 days')
        GROUP BY report_date
        ORDER BY report_date ASC
    """).fetchall()

    db.close()
    return render_template('reports_admin.html',
                           reports=reports,
                           totals=totals,
                           members=members,
                           submitted_today=submitted_today,
                           missing_today=missing_today,
                           trend=trend,
                           filter_date=filter_date,
                           filter_user=filter_user,
                           today=today)


# ─────────────────────────────────────────────
#  Admin – Settings
# ─────────────────────────────────────────────

@app.route('/admin/settings', methods=['GET', 'POST'])
@admin_required
def admin_settings():
    db = get_db()
    if request.method == 'POST':
        upi_id          = request.form.get('upi_id', '').strip()
        lead_price      = request.form.get('lead_price', '50').strip()
        webhook_token   = request.form.get('webhook_token', '').strip()
        meta_page_token = request.form.get('meta_page_token', '').strip()
        smtp_host       = request.form.get('smtp_host', '').strip()
        smtp_port       = request.form.get('smtp_port', '587').strip()
        smtp_user       = request.form.get('smtp_user', '').strip()
        smtp_from_name  = request.form.get('smtp_from_name', 'Myle Community').strip()
        # Only update password if provided (don't overwrite with blank)
        smtp_password   = request.form.get('smtp_password', '').strip()

        _set_setting(db, 'upi_id', upi_id)
        _set_setting(db, 'default_lead_price', lead_price)
        _set_setting(db, 'meta_webhook_token', webhook_token)
        _set_setting(db, 'meta_page_token', meta_page_token)
        _set_setting(db, 'smtp_host', smtp_host)
        _set_setting(db, 'smtp_port', smtp_port)
        _set_setting(db, 'smtp_user', smtp_user)
        _set_setting(db, 'smtp_from_name', smtp_from_name)
        if smtp_password:
            _set_setting(db, 'smtp_password', smtp_password)
        db.commit()
        db.close()
        flash('Settings saved successfully.', 'success')
        return redirect(url_for('admin_settings'))

    settings = {
        'upi_id':             _get_setting(db, 'upi_id'),
        'default_lead_price': _get_setting(db, 'default_lead_price', '50'),
        'meta_webhook_token': _get_setting(db, 'meta_webhook_token'),
        'meta_page_token':    _get_setting(db, 'meta_page_token'),
        'smtp_host':          _get_setting(db, 'smtp_host', 'smtp.gmail.com'),
        'smtp_port':          _get_setting(db, 'smtp_port', '587'),
        'smtp_user':          _get_setting(db, 'smtp_user'),
        'smtp_from_name':     _get_setting(db, 'smtp_from_name', 'Myle Community'),
        'smtp_password_set':  bool(_get_setting(db, 'smtp_password')),
    }
    db.close()
    return render_template('admin_settings.html', settings=settings)


@app.route('/admin/upi-qr-preview')
@admin_required
def admin_upi_qr_preview():
    """Serve UPI QR code PNG for admin settings preview."""
    db     = get_db()
    upi_id = _get_setting(db, 'upi_id', '')
    db.close()
    img_bytes = _generate_upi_qr_bytes(upi_id)
    if not img_bytes:
        return 'QR not available', 404
    return Response(img_bytes, mimetype='image/png')


# ─────────────────────────────────────────────
#  Admin – Lead Pool Management
# ─────────────────────────────────────────────

@app.route('/admin/lead-pool')
@admin_required
def admin_lead_pool():
    db = get_db()
    page     = request.args.get('page', 1, type=int)
    per_page = 50
    offset   = (page - 1) * per_page

    total_in_pool = db.execute(
        "SELECT COUNT(*) FROM leads WHERE in_pool=1"
    ).fetchone()[0]
    total_claimed = db.execute(
        "SELECT COUNT(*) FROM leads WHERE in_pool=0 AND claimed_at!=''"
    ).fetchone()[0]

    pool_leads = db.execute(
        "SELECT * FROM leads WHERE in_pool=1 ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (per_page, offset)
    ).fetchall()

    default_price = _get_setting(db, 'default_lead_price', '50')
    db.close()
    return render_template('lead_pool_admin.html',
                           pool_leads=pool_leads,
                           total_in_pool=total_in_pool,
                           total_claimed=total_claimed,
                           default_price=default_price,
                           page=page,
                           per_page=per_page)


@app.route('/admin/lead-pool/import-csv', methods=['POST'])
@admin_required
def import_lead_pool_csv():
    """Import Meta Lead Ads CSV into the lead pool."""
    db             = get_db()
    price_per_lead = float(request.form.get('price_per_lead') or 50)
    source_tag     = request.form.get('source_tag', 'Meta').strip() or 'Meta'

    if 'csv_file' not in request.files:
        flash('No file uploaded.', 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    f = request.files['csv_file']
    if not f.filename.lower().endswith('.csv'):
        flash('Please upload a .csv file.', 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    content = f.read().decode('utf-8-sig', errors='replace')  # utf-8-sig handles BOM
    reader  = csv.DictReader(io.StringIO(content))

    imported = 0
    skipped  = 0

    for row in reader:
        # Support common Meta Lead Ads export column names
        name  = (row.get('full_name') or row.get('name') or
                 row.get('Name') or row.get('Full Name') or '').strip()
        phone = (row.get('phone_number') or row.get('phone') or
                 row.get('Phone') or row.get('Phone Number') or '').strip()
        email = (row.get('email') or row.get('Email') or
                 row.get('email_address') or '').strip()

        if not name and not phone:
            skipped += 1
            continue

        if not name:
            name = phone
        if not phone:
            phone = 'N/A'

        # Skip duplicates already in pool
        existing = db.execute(
            "SELECT id FROM leads WHERE phone=? AND in_pool=1", (phone,)
        ).fetchone()
        if existing:
            skipped += 1
            continue

        db.execute("""
            INSERT INTO leads
                (name, phone, email, assigned_to, source, status,
                 in_pool, pool_price, claimed_at)
            VALUES (?, ?, ?, '', ?, 'New', 1, ?, '')
        """, (name, phone, email, source_tag, price_per_lead))
        imported += 1

    db.commit()
    db.close()
    flash(f'Imported {imported} leads into pool. Skipped {skipped} (duplicates/empty).', 'success')
    return redirect(url_for('admin_lead_pool'))


@app.route('/admin/lead-pool/add-single', methods=['POST'])
@admin_required
def add_to_pool():
    """Admin manually adds a single lead to the pool."""
    db     = get_db()
    name   = request.form.get('name', '').strip()
    phone  = request.form.get('phone', '').strip()
    email  = request.form.get('email', '').strip()
    price  = float(request.form.get('price') or 50)
    source = request.form.get('source', 'Other').strip()

    if not name or not phone:
        flash('Name and phone are required.', 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    db.execute("""
        INSERT INTO leads
            (name, phone, email, assigned_to, source, status,
             in_pool, pool_price, claimed_at)
        VALUES (?, ?, ?, '', ?, 'New', 1, ?, '')
    """, (name, phone, email, source, price))
    db.commit()
    db.close()
    flash(f'Lead "{name}" added to pool.', 'success')
    return redirect(url_for('admin_lead_pool'))


@app.route('/admin/lead-pool/<int:lead_id>/remove', methods=['POST'])
@admin_required
def remove_from_pool(lead_id):
    db = get_db()
    db.execute("DELETE FROM leads WHERE id=? AND in_pool=1", (lead_id,))
    db.commit()
    db.close()
    flash('Lead removed from pool.', 'warning')
    return redirect(url_for('admin_lead_pool'))


# ─────────────────────────────────────────────
#  Admin – Wallet Recharge Requests
# ─────────────────────────────────────────────

@app.route('/admin/wallet-requests')
@admin_required
def admin_wallet_requests():
    db            = get_db()
    filter_status = request.args.get('status', 'pending')

    query  = ("SELECT wr.*, u.phone as user_phone "
              "FROM wallet_recharges wr "
              "LEFT JOIN users u ON wr.username=u.username "
              "WHERE 1=1")
    params = []
    if filter_status in ('pending', 'approved', 'rejected'):
        query += " AND wr.status=?"
        params.append(filter_status)
    query += " ORDER BY wr.requested_at DESC"

    requests_list = db.execute(query, params).fetchall()

    pending_count = db.execute(
        "SELECT COUNT(*) FROM wallet_recharges WHERE status='pending'"
    ).fetchone()[0]

    db.close()
    return render_template('wallet_requests_admin.html',
                           requests=requests_list,
                           filter_status=filter_status,
                           pending_count=pending_count)


@app.route('/admin/wallet-requests/<int:req_id>/approve', methods=['POST'])
@admin_required
def approve_recharge(req_id):
    db      = get_db()
    recharge = db.execute(
        "SELECT * FROM wallet_recharges WHERE id=?", (req_id,)
    ).fetchone()
    if recharge:
        db.execute(
            "UPDATE wallet_recharges SET status='approved', "
            "processed_at=datetime('now','localtime') WHERE id=?",
            (req_id,)
        )
        db.commit()
        flash(f'Recharge of ₹{recharge["amount"]:.0f} for @{recharge["username"]} approved!', 'success')
    db.close()
    return redirect(url_for('admin_wallet_requests', status='pending'))


@app.route('/admin/wallet-requests/<int:req_id>/reject', methods=['POST'])
@admin_required
def reject_recharge(req_id):
    admin_note = request.form.get('admin_note', '').strip()
    db         = get_db()
    recharge   = db.execute(
        "SELECT * FROM wallet_recharges WHERE id=?", (req_id,)
    ).fetchone()
    if recharge:
        db.execute(
            "UPDATE wallet_recharges SET status='rejected', "
            "processed_at=datetime('now','localtime'), admin_note=? WHERE id=?",
            (admin_note, req_id)
        )
        db.commit()
        flash(f'Recharge request from @{recharge["username"]} rejected.', 'warning')
    db.close()
    return redirect(url_for('admin_wallet_requests', status='pending'))


# ─────────────────────────────────────────────
#  Team – Wallet
# ─────────────────────────────────────────────

@app.route('/wallet')
@login_required
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
def request_recharge():
    username = session['username']
    db       = get_db()

    try:
        amount = float(request.form.get('amount') or 0)
    except ValueError:
        amount = 0

    utr = request.form.get('utr_number', '').strip()

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

    db.execute(
        "INSERT INTO wallet_recharges (username, amount, utr_number, status) "
        "VALUES (?, ?, ?, 'pending')",
        (username, amount, utr)
    )
    db.commit()
    db.close()
    flash(f'Recharge request of ₹{amount:.0f} submitted! UTR: {utr}. '
          f'Admin will credit your wallet within 24 hours.', 'success')
    return redirect(url_for('wallet'))


# ─────────────────────────────────────────────
#  Team – Lead Pool (Claim Leads)
# ─────────────────────────────────────────────

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

    avg_price  = price_info['avg_p'] or 50
    can_claim  = int(wallet_stats['balance'] / avg_price) if avg_price > 0 else 0
    can_claim  = min(can_claim, pool_count)

    my_claims = db.execute(
        "SELECT COUNT(*) FROM leads WHERE assigned_to=? AND claimed_at!=''",
        (username,)
    ).fetchone()[0]

    db.close()
    return render_template('lead_pool.html',
                           wallet=wallet_stats,
                           pool_count=pool_count,
                           price_info=price_info,
                           can_claim=can_claim,
                           my_claims=my_claims)


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

    wallet_stats = _get_wallet(db, username)

    available = db.execute(
        "SELECT id, pool_price FROM leads WHERE in_pool=1 ORDER BY created_at ASC LIMIT ?",
        (count,)
    ).fetchall()

    if not available:
        flash('No leads available in pool right now. Check back later.', 'warning')
        db.close()
        return redirect(url_for('lead_pool'))

    total_cost = sum(r['pool_price'] for r in available)

    if total_cost > wallet_stats['balance']:
        flash(f'Insufficient balance! Need ₹{total_cost:.0f} but you have ₹{wallet_stats["balance"]:.0f}. '
              f'Please recharge your wallet.', 'danger')
        db.close()
        return redirect(url_for('lead_pool'))

    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for row in available:
        db.execute(
            "UPDATE leads SET assigned_to=?, in_pool=0, claimed_at=?, "
            "updated_at=? WHERE id=?",
            (username, now, now, row['id'])
        )

    db.commit()
    db.close()
    flash(f'Successfully claimed {len(available)} leads for ₹{total_cost:.0f}! '
          f'Check "My Leads" to view them.', 'success')
    return redirect(url_for('leads'))


# ─────────────────────────────────────────────
#  Meta Webhook
# ─────────────────────────────────────────────

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

    data = request.get_json(silent=True)
    if not data:
        db.close()
        return 'OK', 200

    default_price = float(_get_setting(db, 'default_lead_price', '50') or 50)

    imported = 0
    for entry in data.get('entry', []):
        for change in entry.get('changes', []):
            if change.get('field') != 'leadgen':
                continue
            value      = change.get('value', {})
            field_data = value.get('field_data', [])
            lead_fields = {
                f['name']: (f['values'][0] if f.get('values') else '')
                for f in field_data
            }

            name  = lead_fields.get('full_name', lead_fields.get('name', 'Meta Lead')).strip()
            phone = lead_fields.get('phone_number', lead_fields.get('phone', '')).strip()
            email = lead_fields.get('email', '').strip()

            if not phone:
                phone = str(value.get('leadgen_id', 'N/A'))

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

    db.commit()
    db.close()
    return 'OK', 200


# ─────────────────────────────────────────────
#  Boot – runs on every startup
# ─────────────────────────────────────────────

init_db()
migrate_db()
seed_users()

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5001)
