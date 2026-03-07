import os
import io
import csv
import base64
import hashlib
import hmac
import json
import secrets
import datetime
import smtplib
import ssl
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from flask import (Flask, render_template, request, redirect, url_for,
                   flash, session, Response, make_response)
from werkzeug.security import generate_password_hash, check_password_hash
from database import get_db, init_db, migrate_db, seed_users

# Optional QR code support
try:
    import qrcode
    QR_AVAILABLE = True
except ImportError:
    QR_AVAILABLE = False

# Optional PDF support
try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# Optional Web Push support (pywebpush + cryptography)
try:
    from pywebpush import webpush, WebPushException
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import serialization as _crypto_serial
    PUSH_AVAILABLE = True
except ImportError:
    PUSH_AVAILABLE = False

# Optional APScheduler for daily reminder push notifications
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    import atexit
    import pytz
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'myle_community_secret_2024_local')
app.permanent_session_lifetime = datetime.timedelta(days=3650)  # ~10 years = effectively forever
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True

STATUSES = ['New Lead', 'New', 'Contacted', 'Invited', 'Video Sent', 'Video Watched',
            'Paid ₹196', 'Day 1', 'Day 2', 'Interview', 'Converted', 'Lost', 'Retarget']

CALL_RESULT_TAGS = [
    '',
    # No response
    'Call Not Picked',
    'Phone Switched Off',
    'Not Reachable',
    'Wrong Number',
    # Follow-up needed
    'Follow Up Later',
    'Callback Requested',
    # Positive
    'Interested',
    # Negative / disqualified
    'Not Interested',
    'Already Forever Living Distributor',
    'Already in Another Network',
    'Underage',
    'Language Barrier',
]

RETARGET_TAGS = {
    'Call Not Picked', 'Phone Switched Off', 'Not Reachable',
    'Follow Up Later', 'Callback Requested'
}
SOURCES  = ['WhatsApp', 'Facebook', 'Instagram', 'LinkedIn',
            'Walk-in', 'Referral', 'YouTube', 'Cold Call', 'Meta', 'Other']
PAYMENT_AMOUNT = 196.0


# ─────────────────────────────────────────────
#  Auth Decorators
# ─────────────────────────────────────────────

def _check_session_valid():
    """Return True if the current session user is still active/approved in the DB."""
    username = session.get('username')
    if not username:
        return False
    db = get_db()
    row = db.execute(
        "SELECT status FROM users WHERE username=?", (username,)
    ).fetchone()
    db.close()
    if not row or row['status'] != 'approved':
        session.clear()
        return False
    return True


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


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

def _get_downline_usernames(db, username):
    """Return [username] + all recursive downline usernames via upline_name chain."""
    rows = db.execute("""
        WITH RECURSIVE downline(uname) AS (
            SELECT ?
            UNION ALL
            SELECT u.username FROM users u JOIN downline d ON u.upline_name = d.uname
        )
        SELECT uname FROM downline
    """, (username,)).fetchall()
    return [r['uname'] for r in rows]


# Drill-down metric config
DRILL_LEAD_METRICS = {
    'total':     ('Total Leads',    'bi bi-people-fill',              'primary', None),
    'converted': ('Converted',      'bi bi-check-circle-fill',        'success', "status='Converted'"),
    'paid':      ('Payments ₹196',  'bi bi-credit-card-2-front-fill', 'info',    'payment_done=1'),
    'day1':      ('Day 1 Done',     'bi bi-1-circle-fill',            'info',    'day1_done=1'),
    'day2':      ('Day 2 Done',     'bi bi-2-circle-fill',            'warning', 'day2_done=1'),
    'interview': ('Interview Done', 'bi bi-mic-fill',                 'danger',  'interview_done=1'),
    'revenue':   ('Total Revenue',  'bi bi-currency-rupee',           'warning', 'payment_done=1'),
}

DRILL_REPORT_METRICS = {
    'total_calling':    ('Total Calls',   'bi bi-telephone-fill',         'primary'),
    'pdf_covered':      ('PDF Covered',   'bi bi-file-earmark-pdf-fill',  'danger'),
    'calls_picked':     ('Calls Picked',  'bi bi-telephone-inbound-fill', 'success'),
    'enrollments_done': ('Enrollments',   'bi bi-person-check-fill',      'success'),
    'plan_2cc':         ('2CC Plan',      'bi bi-star-fill',              'warning'),
}


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


_qr_cache: dict = {}   # {upi_id: (bytes, b64_str)}

def _generate_upi_qr_bytes(upi_id):
    """Generate UPI QR code PNG bytes. Returns None if qrcode not available."""
    if not QR_AVAILABLE or not upi_id:
        return None
    if upi_id in _qr_cache:
        return _qr_cache[upi_id][0]
    upi_string = f"upi://pay?pa={upi_id}&pn=Myle+Community&cu=INR"
    qr = qrcode.QRCode(version=1, box_size=8, border=4)
    qr.add_data(upi_string)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    data = buf.getvalue()
    b64 = base64.b64encode(data).decode('utf-8')
    _qr_cache[upi_id] = (data, b64)
    return data


def _generate_upi_qr_base64(upi_id):
    """Generate UPI QR as base64 string (cached)."""
    if not QR_AVAILABLE or not upi_id:
        return None
    if upi_id in _qr_cache:
        return _qr_cache[upi_id][1]
    _generate_upi_qr_bytes(upi_id)   # populates cache
    return _qr_cache.get(upi_id, (None, None))[1]


import re as _re
_PHONE_RE = _re.compile(r'(?:(?:\+|0{0,2})91[-\s]?)?([6-9]\d{9})\b')


def _extract_leads_from_pdf(file_stream):
    """
    Extract (name, phone, email) rows from a PDF file stream.
    Returns (list_of_dicts, error_string).  error_string is None on success.
    """
    if not PDF_AVAILABLE:
        return None, "PDF parsing library not installed. Run: pip install pdfplumber"

    leads = []
    try:
        with pdfplumber.open(file_stream) as pdf:
            for page in pdf.pages:
                # ── Try table extraction first ───────────────────────────
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        if not table:
                            continue
                        header_row = [str(c or '').lower().strip() for c in table[0]]
                        name_col  = next((i for i, h in enumerate(header_row)
                                          if 'name' in h), None)
                        phone_col = next((i for i, h in enumerate(header_row)
                                          if any(k in h for k in ('phone', 'mobile', 'contact', 'number'))), None)
                        email_col = next((i for i, h in enumerate(header_row)
                                          if 'email' in h or 'mail' in h), None)
                        city_col  = next((i for i, h in enumerate(header_row)
                                          if 'city' in h or 'location' in h), None)
                        # skip header row if we detected column labels
                        start = 1 if (name_col is not None or phone_col is not None) else 0
                        for row in table[start:]:
                            if not row:
                                continue
                            cells = [str(c or '').strip() for c in row]
                            safe  = lambda i: cells[i] if i is not None and i < len(cells) else ''
                            name  = safe(name_col)
                            phone = safe(phone_col)
                            email = safe(email_col)
                            city  = safe(city_col)
                            # normalize phone
                            m = _PHONE_RE.search(phone)
                            if m:
                                phone = m.group(1)
                            if name or phone:
                                leads.append({'name': name, 'phone': phone,
                                              'email': email, 'city': city})
                else:
                    # ── Fall back to line-by-line text scan ──────────────
                    text = page.extract_text() or ''
                    for line in text.split('\n'):
                        m = _PHONE_RE.search(line)
                        if not m:
                            continue
                        phone = m.group(1)
                        # strip phone (and +91 prefix) from line → remaining text = name
                        name = _PHONE_RE.sub('', line).strip(' -|,;:\t')
                        leads.append({'name': name, 'phone': phone,
                                      'email': '', 'city': ''})
    except Exception as exc:
        return None, f"Could not parse PDF: {exc}"

    return leads, None


def _get_or_create_vapid_keys(db):
    """Return (private_pem_str, public_b64url). Generates & stores on first call."""
    if not PUSH_AVAILABLE:
        return None, None

    private_pem = _get_setting(db, 'vapid_private_pem', '')
    public_b64  = _get_setting(db, 'vapid_public_key',  '')

    if private_pem and public_b64:
        return private_pem, public_b64

    # Generate new P-256 key pair
    private_key = ec.generate_private_key(ec.SECP256R1())
    private_pem = private_key.private_bytes(
        _crypto_serial.Encoding.PEM,
        _crypto_serial.PrivateFormat.PKCS8,
        _crypto_serial.NoEncryption()
    ).decode()

    pub_raw = private_key.public_key().public_bytes(
        _crypto_serial.Encoding.X962,
        _crypto_serial.PublicFormat.UncompressedPoint
    )
    public_b64 = base64.urlsafe_b64encode(pub_raw).rstrip(b'=').decode()

    _set_setting(db, 'vapid_private_pem', private_pem)
    _set_setting(db, 'vapid_public_key',  public_b64)
    db.commit()
    return private_pem, public_b64


def _push_to_users(db, usernames, title, body, url='/'):
    """
    Send a Web Push notification to all subscriptions of the given usernames.
    Automatically removes dead (410/404) subscriptions.
    Fails silently — never breaks the calling route.
    """
    if not PUSH_AVAILABLE:
        return

    private_pem, _ = _get_or_create_vapid_keys(db)
    if not private_pem:
        return

    if isinstance(usernames, str):
        usernames = [usernames]

    payload   = json.dumps({'title': title, 'body': body, 'url': url})
    dead_ids  = []

    for username in usernames:
        subs = db.execute(
            "SELECT id, endpoint, auth, p256dh FROM push_subscriptions WHERE username=?",
            (username,)
        ).fetchall()
        for sub in subs:
            sub_info = {
                'endpoint': sub['endpoint'],
                'keys': {'auth': sub['auth'], 'p256dh': sub['p256dh']}
            }
            try:
                webpush(
                    subscription_info=sub_info,
                    data=payload,
                    vapid_private_key=private_pem,
                    vapid_claims={'sub': 'mailto:admin@mylecommunity.com'}
                )
            except Exception as exc:
                # 410 Gone / 404 Not Found → subscription expired, clean up
                resp = getattr(exc, 'response', None)
                if resp is not None and getattr(resp, 'status_code', 0) in (404, 410):
                    dead_ids.append(sub['id'])

    if dead_ids:
        ph = ','.join('?' for _ in dead_ids)
        db.execute(f"DELETE FROM push_subscriptions WHERE id IN ({ph})", dead_ids)
        db.commit()


def _push_all_team(db, title, body, url='/'):
    """Push to every approved team member."""
    rows = db.execute(
        "SELECT username FROM users WHERE role='team' AND status='approved'"
    ).fetchall()
    _push_to_users(db, [r['username'] for r in rows], title, body, url)


def _get_network_usernames(db, username):
    """
    Return the list of usernames visible to `username`.

    Includes the user themselves + ALL recursive downlines (BFS tree walk).
    A user is a downline of X if their `upline_name` field matches X's username
    (direct or via chain).

    Returns None for admins → caller should show everything.

    Example chain:  A → B → C → D
      B's network = {B, C, D}  (not A — uplines are excluded)
      A's network = {A, B, C, D}
    """
    visible = {username}
    queue   = [username]
    while queue:
        current  = queue.pop(0)
        downlines = db.execute(
            "SELECT username FROM users WHERE upline_name=? AND status='approved'",
            (current,)
        ).fetchall()
        for row in downlines:
            u = row['username']
            if u not in visible:
                visible.add(u)
                queue.append(u)
    return list(visible)


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


def _send_password_reset_email(user_email, username, reset_url):
    """Send password reset link email. Silently skips if SMTP not configured."""
    db = get_db()
    smtp_host     = _get_setting(db, 'smtp_host', '')
    smtp_port     = int(_get_setting(db, 'smtp_port', '587') or 587)
    smtp_user     = _get_setting(db, 'smtp_user', '')
    smtp_password = _get_setting(db, 'smtp_password', '')
    from_name     = _get_setting(db, 'smtp_from_name', 'Myle Community')
    db.close()

    if not smtp_host or not smtp_user or not smtp_password or not user_email:
        return False  # SMTP not configured

    subject   = 'Myle Community – Password Reset Request'
    html_body = f"""
    <div style="font-family:Arial,sans-serif;max-width:520px;margin:auto;padding:24px;background:#f8f9fa;border-radius:12px;">
      <div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:8px;padding:20px;text-align:center;margin-bottom:24px;">
        <h2 style="color:#fff;margin:0;">🔐 Password Reset</h2>
      </div>
      <p style="color:#333;">Hello <strong>{username}</strong>,</p>
      <p style="color:#555;">We received a request to reset your Myle Community dashboard password.
      Click the button below to set a new password. This link expires in <strong>1 hour</strong>.</p>
      <div style="text-align:center;margin:28px 0;">
        <a href="{reset_url}" style="background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;
           padding:12px 32px;border-radius:8px;text-decoration:none;font-weight:600;font-size:15px;">
          Reset My Password
        </a>
      </div>
      <p style="color:#999;font-size:0.8rem;">If you did not request this, you can safely ignore this email.
      Your password will not change.</p>
      <p style="color:#999;font-size:0.8rem;">Or copy this link: {reset_url}</p>
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
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────
#  Template Filters
# ─────────────────────────────────────────────

@app.template_filter('wa_phone')
def wa_phone_filter(phone):
    """Clean phone number for WhatsApp wa.me link."""
    import re
    digits = re.sub(r'[^\d]', '', str(phone))
    if len(digits) == 10 and digits[0] in '6789':
        digits = '91' + digits          # Indian mobile – prepend country code
    elif digits.startswith('0') and len(digits) == 11:
        digits = '91' + digits[1:]      # 0XXXXXXXXXX → 91XXXXXXXXXX
    return digits


# ─────────────────────────────────────────────
#  Context processor – inject counts for nav badges
# ─────────────────────────────────────────────

@app.context_processor
def inject_pending_count():
    if session.get('role') == 'admin':
        db  = get_db()
        row = db.execute("""
            SELECT
              (SELECT COUNT(*) FROM users           WHERE status='pending') as pu,
              (SELECT COUNT(*) FROM wallet_recharges WHERE status='pending') as wp
        """).fetchone()
        db.close()
        return {'pending_count': row['pu'], 'wallet_pending': row['wp']}
    return {'pending_count': 0, 'wallet_pending': 0}


# ─────────────────────────────────────────────
#  Register
# ─────────────────────────────────────────────

@app.route('/register', methods=['GET', 'POST'])
def register():
    if 'username' in session:
        return redirect(url_for('index'))

    if request.method == 'POST':
        username       = request.form.get('username', '').strip()
        password       = request.form.get('password', '').strip()
        email          = request.form.get('email', '').strip()
        fbo_id         = request.form.get('fbo_id', '').strip()
        upline_fbo_id  = request.form.get('upline_fbo_id', '').strip()
        phone          = request.form.get('phone', '').strip()

        if not username or not password or not email or not fbo_id or not upline_fbo_id:
            flash('Username, Password, Email, FBO ID, and Upline FBO ID are required.', 'danger')
            return render_template('register.html')

        db = get_db()

        # Unique username check
        if db.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone():
            db.close()
            flash('That username is already taken. Please choose another.', 'danger')
            return render_template('register.html')

        # Unique FBO ID check
        if db.execute("SELECT id FROM users WHERE fbo_id=? AND fbo_id!=''", (fbo_id,)).fetchone():
            db.close()
            flash('That FBO ID is already registered. Each FBO ID must be unique.', 'danger')
            return render_template('register.html')

        # Unique phone check
        if phone and db.execute("SELECT id FROM users WHERE phone=? AND phone!=''", (phone,)).fetchone():
            db.close()
            flash('That mobile number is already registered. Please use a different number.', 'danger')
            return render_template('register.html')

        # Upline FBO ID lookup — find the user with that FBO ID
        upline_user = db.execute(
            "SELECT username FROM users WHERE fbo_id=?", (upline_fbo_id,)
        ).fetchone()
        if not upline_user:
            db.close()
            flash(f'Upline FBO ID "{upline_fbo_id}" not found. Please ask your upline for their correct FBO ID.', 'danger')
            return render_template('register.html')
        upline_name = upline_user['username']  # store username so network traversal works

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

            session.permanent = True

            session['username'] = user['username']
            session['role']     = user['role']
            session['dp']       = user['display_picture'] if user['display_picture'] else ''
            flash(f'Welcome back, {user["username"]}!', 'success')
            if user['role'] == 'admin':
                return redirect(url_for('admin_dashboard'))
            return redirect(url_for('team_dashboard'))
        else:
            flash('Invalid username or password.', 'danger')

    return render_template('login.html')


# ─────────────────────────────────────────────
#  Forgot / Reset Password
# ─────────────────────────────────────────────

@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if 'username' in session:
        return redirect(url_for('index'))

    email_sent = False
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        if email:
            db   = get_db()
            user = db.execute(
                "SELECT username, email FROM users WHERE LOWER(email)=? AND status='approved'",
                (email,)
            ).fetchone()
            if user:
                token      = secrets.token_urlsafe(32)
                expires_at = (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
                db.execute(
                    "INSERT INTO password_reset_tokens (username, token, expires_at) VALUES (?,?,?)",
                    (user['username'], token, expires_at)
                )
                db.commit()
                reset_url = url_for('reset_password', token=token, _external=True)
                sent = _send_password_reset_email(user['email'], user['username'], reset_url)
                if not sent:
                    # SMTP not configured — show link directly (admin use)
                    flash(f'SMTP not configured. Reset link (share manually): {reset_url}', 'warning')
            db.close()
        # Always show success to avoid email enumeration
        email_sent = True

    return render_template('forgot_password.html', email_sent=email_sent)


@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    if 'username' in session:
        return redirect(url_for('index'))

    db  = get_db()
    row = db.execute(
        "SELECT * FROM password_reset_tokens WHERE token=? AND used=0",
        (token,)
    ).fetchone()

    if not row:
        db.close()
        flash('This password reset link is invalid or has already been used.', 'danger')
        return redirect(url_for('login'))

    # Check expiry
    expires_at = datetime.datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S')
    if datetime.datetime.now() > expires_at:
        db.close()
        flash('This password reset link has expired. Please request a new one.', 'danger')
        return redirect(url_for('forgot_password'))

    if request.method == 'POST':
        new_password = request.form.get('password', '').strip()
        confirm      = request.form.get('confirm_password', '').strip()
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
            (generate_password_hash(new_password, method='pbkdf2:sha256'), row['username'])
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
        # Send welcome email in background thread (SMTP can take 3-10s, don't block worker)
        login_url = request.host_url.rstrip('/') + url_for('login')
        threading.Thread(target=_send_welcome_email,
                         args=(user['email'], user['username'], login_url),
                         daemon=True).start()
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

    # Single query instead of 7 (one per status)
    _sc = db.execute(
        "SELECT status, COUNT(*) as c FROM leads WHERE in_pool=0 AND deleted_at='' GROUP BY status"
    ).fetchall()
    status_data = {s: 0 for s in STATUSES}
    for row in _sc:
        if row['status'] in status_data:
            status_data[row['status']] = row['c']

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
    # Single GROUP BY query instead of N per-member queries
    _stats_rows = db.execute("""
        SELECT assigned_to,
            COUNT(*) as total,
            SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN payment_done=1     THEN 1 ELSE 0 END) as paid,
            SUM(COALESCE(payment_amount,0) + COALESCE(revenue,0)) as revenue
        FROM leads WHERE in_pool=0
        GROUP BY assigned_to
    """).fetchall()
    _stats_map = {r['assigned_to']: r for r in _stats_rows}
    _empty = {'total': 0, 'converted': 0, 'paid': 0, 'revenue': 0}
    team_stats = [{'member': m, 'stats': _stats_map.get(m['name'], _empty)}
                  for m in members]

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
    submitted_set   = {r['username'] for r in today_reports}
    missing_reports = [u['username'] for u in approved_team
                       if u['username'] not in submitted_set]

    # Single query instead of one per reporter
    _verif_rows = db.execute("""
        SELECT assigned_to, COUNT(*) as cnt FROM leads
        WHERE payment_done=1 AND date(updated_at)=? AND in_pool=0
        GROUP BY assigned_to
    """, (today,)).fetchall()
    report_verification = {r['assigned_to']: r['cnt'] for r in _verif_rows}

    # Wallet pending count (also in context processor but useful for template)
    wallet_pending_count = db.execute(
        "SELECT COUNT(*) FROM wallet_recharges WHERE status='pending'"
    ).fetchone()[0]

    # Pool summary for admin dashboard
    pool_count = db.execute("SELECT COUNT(*) FROM leads WHERE in_pool=1").fetchone()[0]

    db.close()
    resp = make_response(render_template('admin.html',
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
                           pool_count=pool_count))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


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

    # Single query instead of 7
    _sc = db.execute(
        "SELECT status, COUNT(*) as c FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' GROUP BY status",
        (username,)
    ).fetchall()
    status_data = {s: 0 for s in STATUSES}
    for row in _sc:
        if row['status'] in status_data:
            status_data[row['status']] = row['c']

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

    _rt_ph = ','.join('?' * len(RETARGET_TAGS))
    retarget_count = db.execute(
        f"SELECT COUNT(*) FROM leads WHERE in_pool=0 AND deleted_at='' "
        f"AND assigned_to=? AND (call_result IN ({_rt_ph}) OR status='Retarget')",
        (username, *RETARGET_TAGS)
    ).fetchone()[0]

    zoom_link  = _get_setting(db, 'zoom_link', '')
    zoom_title = _get_setting(db, 'zoom_title', "Today's Live Session")
    zoom_time  = _get_setting(db, 'zoom_time', '2:00 PM')

    # Daily earnings (₹196 payments done today)
    today_paid = db.execute("""
        SELECT COUNT(*) FROM leads
        WHERE assigned_to=? AND payment_done=1 AND in_pool=0
          AND date(updated_at)=?
    """, (username, today)).fetchone()[0] or 0
    today_earnings = today_paid * PAYMENT_AMOUNT

    # Follow-up reminders (due today or overdue)
    followups = db.execute("""
        SELECT id, name, phone, follow_up_date FROM leads
        WHERE assigned_to=? AND in_pool=0
          AND follow_up_date != ''
          AND follow_up_date <= ?
          AND status NOT IN ('Converted','Lost')
        ORDER BY follow_up_date ASC LIMIT 10
    """, (username, today)).fetchall()

    # Announcements (pinned first, latest 5)
    notices = db.execute(
        "SELECT * FROM announcements ORDER BY pin DESC, created_at DESC LIMIT 5"
    ).fetchall()

    _cr_row = db.execute(
        "SELECT calling_reminder_time FROM users WHERE username=?", (username,)
    ).fetchone()
    calling_reminder_time = _cr_row['calling_reminder_time'] if _cr_row else ''

    db.close()
    resp = make_response(render_template('dashboard.html',
                           metrics=metrics,
                           wallet=wallet,
                           recent=recent,
                           status_data=status_data,
                           monthly=monthly,
                           payment_amount=PAYMENT_AMOUNT,
                           today_report=today_report,
                           today=today,
                           pool_count=pool_count,
                           today_paid=today_paid,
                           today_earnings=today_earnings,
                           followups=followups,
                           notices=notices,
                           calling_reminder_time=calling_reminder_time,
                           retarget_count=retarget_count,
                           zoom_link=zoom_link,
                           zoom_title=zoom_title,
                           zoom_time=zoom_time))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


# ─────────────────────────────────────────────
#  Leads – List
# ─────────────────────────────────────────────

@app.route('/leads')
@login_required
def leads():
    db     = get_db()
    status = request.args.get('status', '')
    search = request.args.get('q', '').strip()

    query  = "SELECT * FROM leads WHERE in_pool=0 AND deleted_at=''"
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
    team      = db.execute("SELECT name FROM team_members ORDER BY name").fetchall()
    db.close()
    return render_template('leads.html',
                           leads=all_leads,
                           statuses=STATUSES,
                           call_result_tags=CALL_RESULT_TAGS,
                           sources=SOURCES,
                           selected_status=status,
                           search=search,
                           team=team)


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
        call_result    = request.form.get('call_result', '').strip()
        notes          = request.form.get('notes', '').strip()
        city           = request.form.get('city', '').strip()

        if session.get('role') == 'admin':
            assigned_to = request.form.get('assigned_to', '').strip()
        else:
            assigned_to = session['username']

        if not name or not phone:
            flash('Name and Phone are required.', 'danger')
            db.close()
            return render_template('add_lead.html',
                                   statuses=STATUSES, sources=SOURCES, team=team,
                                   call_result_tags=CALL_RESULT_TAGS)

        # Req 8: Phone duplicate check
        dup = db.execute(
            "SELECT name FROM leads WHERE phone=? AND in_pool=0 AND deleted_at=''", (phone,)
        ).fetchone()
        if dup:
            flash(f'A lead with phone {phone} already exists ({dup["name"]}). Duplicate entries are not allowed.', 'danger')
            db.close()
            return render_template('add_lead.html',
                                   statuses=STATUSES, sources=SOURCES, team=team,
                                   call_result_tags=CALL_RESULT_TAGS)

        if status not in STATUSES:
            status = 'New'

        db.execute("""
            INSERT INTO leads
                (name, phone, email, referred_by, assigned_to, source,
                 status, payment_done, payment_amount, revenue,
                 follow_up_date, call_result, notes, city, in_pool, pool_price, claimed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '')
        """, (name, phone, email, referred_by, assigned_to, source,
              status, payment_done, payment_amount, revenue,
              follow_up_date, call_result, notes, city))
        db.commit()
        db.close()
        flash(f'Lead "{name}" added successfully.', 'success')
        return redirect(url_for('leads'))

    db.close()
    return render_template('add_lead.html',
                           statuses=STATUSES, sources=SOURCES, team=team,
                           call_result_tags=CALL_RESULT_TAGS)


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
            "SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)
        ).fetchone()
    else:
        lead = db.execute(
            "SELECT * FROM leads WHERE id=? AND assigned_to=? AND in_pool=0 AND deleted_at=''",
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
        follow_up_date = request.form.get('follow_up_date', '').strip()
        call_result    = request.form.get('call_result', lead['call_result'] if 'call_result' in lead.keys() else '').strip()
        city           = request.form.get('city', '').strip()

        if not name or not phone:
            flash('Name and Phone are required.', 'danger')
            lead_notes_rows = db.execute(
                "SELECT * FROM lead_notes WHERE lead_id=? ORDER BY created_at ASC",
                (lead_id,)
            ).fetchall()
            db.close()
            return render_template('edit_lead.html',
                                   lead=lead, statuses=STATUSES,
                                   team=team, payment_amount=PAYMENT_AMOUNT,
                                   lead_notes=lead_notes_rows,
                                   call_result_tags=CALL_RESULT_TAGS)

        # Req 8: Phone duplicate check (exclude current lead)
        dup = db.execute(
            "SELECT name FROM leads WHERE phone=? AND id!=? AND in_pool=0 AND deleted_at=''",
            (phone, lead_id)
        ).fetchone()
        if dup:
            flash(f'Another lead with phone {phone} already exists ({dup["name"]}). Duplicate entries are not allowed.', 'danger')
            lead_notes_rows = db.execute(
                "SELECT * FROM lead_notes WHERE lead_id=? ORDER BY created_at ASC",
                (lead_id,)
            ).fetchall()
            db.close()
            return render_template('edit_lead.html',
                                   lead=lead, statuses=STATUSES,
                                   team=team, payment_amount=PAYMENT_AMOUNT,
                                   lead_notes=lead_notes_rows,
                                   call_result_tags=CALL_RESULT_TAGS)

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
                follow_up_date=?, call_result=?, notes=?, city=?,
                updated_at=datetime('now','localtime')
            WHERE id=?
        """, (name, phone, email, referred_by, assigned_to, status,
              payment_done, payment_amount,
              day1_done, day2_done, interview_done,
              follow_up_date, call_result, notes, city, lead_id))
        db.commit()
        db.close()
        flash(f'Lead "{name}" updated.', 'success')
        return redirect(url_for('leads'))

    lead_notes_rows = db.execute(
        "SELECT * FROM lead_notes WHERE lead_id=? ORDER BY created_at ASC",
        (lead_id,)
    ).fetchall()
    db.close()
    return render_template('edit_lead.html',
                           lead=lead,
                           statuses=STATUSES,
                           team=team,
                           payment_amount=PAYMENT_AMOUNT,
                           lead_notes=lead_notes_rows,
                           call_result_tags=CALL_RESULT_TAGS)


# ─────────────────────────────────────────────
#  Retarget List
# ─────────────────────────────────────────────

@app.route('/retarget')
@login_required
def retarget():
    db = get_db()
    rt_placeholders = ','.join('?' * len(RETARGET_TAGS))
    query  = f"""SELECT * FROM leads
                WHERE in_pool=0 AND deleted_at=''
                AND (call_result IN ({rt_placeholders}) OR status='Retarget')"""
    params = list(RETARGET_TAGS)
    if session.get('role') != 'admin':
        query += " AND assigned_to=?"
        params.append(session['username'])
    query += " ORDER BY updated_at DESC"
    leads_list = db.execute(query, params).fetchall()
    db.close()
    return render_template('retarget.html',
                           leads=leads_list,
                           call_result_tags=CALL_RESULT_TAGS,
                           statuses=STATUSES)


# ─────────────────────────────────────────────
#  Leads – Quick status toggle
# ─────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/status', methods=['POST'])
@login_required
def update_status(lead_id):
    status = request.form.get('status')
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'

    if status not in STATUSES:
        if is_ajax:
            return {'ok': False, 'error': 'Invalid status'}, 400
        flash('Invalid status.', 'danger')
        return redirect(url_for('leads'))

    db = get_db()

    if session.get('role') != 'admin':
        lead = db.execute(
            "SELECT id FROM leads WHERE id=? AND assigned_to=? AND in_pool=0",
            (lead_id, session['username'])
        ).fetchone()
        if not lead:
            db.close()
            if is_ajax:
                return {'ok': False, 'error': 'Access denied'}, 403
            flash('Access denied.', 'danger')
            return redirect(url_for('leads'))

    db.execute(
        "UPDATE leads SET status=?, updated_at=datetime('now','localtime') WHERE id=? AND in_pool=0",
        (status, lead_id)
    )
    db.commit()
    db.close()

    if is_ajax:
        return {'ok': True, 'status': status}

    flash('Status updated.', 'success')
    return redirect(request.referrer or url_for('leads'))


# ─────────────────────────────────────────────
#  Leads – Quick call-result update (AJAX)
# ─────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/call-result', methods=['POST'])
@login_required
def update_call_result(lead_id):
    tag = request.form.get('call_result', '').strip()
    if tag not in CALL_RESULT_TAGS:
        return {'ok': False, 'error': 'Invalid tag'}, 400
    db = get_db()
    if session.get('role') != 'admin':
        lead = db.execute(
            "SELECT id FROM leads WHERE id=? AND assigned_to=? AND in_pool=0",
            (lead_id, session['username'])
        ).fetchone()
        if not lead:
            db.close()
            return {'ok': False, 'error': 'Access denied'}, 403
    db.execute(
        "UPDATE leads SET call_result=?, updated_at=datetime('now','localtime') WHERE id=? AND in_pool=0",
        (tag, lead_id)
    )
    db.commit()
    db.close()
    return {'ok': True, 'call_result': tag}


# ─────────────────────────────────────────────
#  Leads – Delete
# ─────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/delete', methods=['POST'])
@login_required
def delete_lead(lead_id):
    """Soft-delete: move to recycle bin."""
    db = get_db()

    if session.get('role') == 'admin':
        lead = db.execute(
            "SELECT name FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)
        ).fetchone()
    else:
        lead = db.execute(
            "SELECT name FROM leads WHERE id=? AND assigned_to=? AND in_pool=0 AND deleted_at=''",
            (lead_id, session['username'])
        ).fetchone()

    if lead:
        db.execute(
            "UPDATE leads SET deleted_at=datetime('now','localtime') WHERE id=?", (lead_id,)
        )
        db.commit()
        flash(f'Lead "{lead["name"]}" moved to Recycle Bin.', 'warning')
    else:
        flash('Lead not found or access denied.', 'danger')
    db.close()
    return redirect(url_for('leads'))


@app.route('/leads/recycle-bin')
@login_required
def recycle_bin():
    db = get_db()
    if session.get('role') == 'admin':
        deleted_leads = db.execute(
            "SELECT * FROM leads WHERE in_pool=0 AND deleted_at!='' ORDER BY deleted_at DESC"
        ).fetchall()
    else:
        deleted_leads = db.execute(
            "SELECT * FROM leads WHERE in_pool=0 AND deleted_at!='' AND assigned_to=? ORDER BY deleted_at DESC",
            (session['username'],)
        ).fetchall()
    db.close()
    return render_template('recycle_bin.html', leads=deleted_leads)


@app.route('/leads/<int:lead_id>/restore', methods=['POST'])
@login_required
def restore_lead(lead_id):
    db = get_db()
    if session.get('role') == 'admin':
        lead = db.execute(
            "SELECT name FROM leads WHERE id=? AND deleted_at!=''", (lead_id,)
        ).fetchone()
    else:
        lead = db.execute(
            "SELECT name FROM leads WHERE id=? AND assigned_to=? AND deleted_at!=''",
            (lead_id, session['username'])
        ).fetchone()

    if lead:
        db.execute("UPDATE leads SET deleted_at='' WHERE id=?", (lead_id,))
        db.commit()
        flash(f'Lead "{lead["name"]}" restored successfully.', 'success')
    else:
        flash('Lead not found or access denied.', 'danger')
    db.close()
    return redirect(url_for('recycle_bin'))


@app.route('/leads/<int:lead_id>/permanent-delete', methods=['POST'])
@admin_required
def permanent_delete_lead(lead_id):
    db   = get_db()
    lead = db.execute(
        "SELECT name FROM leads WHERE id=? AND deleted_at!=''", (lead_id,)
    ).fetchone()
    if lead:
        db.execute("DELETE FROM lead_notes WHERE lead_id=?", (lead_id,))
        db.execute("DELETE FROM leads WHERE id=?", (lead_id,))
        db.commit()
        flash(f'Lead "{lead["name"]}" permanently deleted.', 'danger')
    else:
        flash('Lead not found in recycle bin.', 'danger')
    db.close()
    return redirect(url_for('recycle_bin'))


# ─────────────────────────────────────────────
#  Team  (Admin only)
# ─────────────────────────────────────────────

@app.route('/team')
@admin_required
def team():
    db      = get_db()
    members = db.execute("SELECT * FROM team_members ORDER BY name").fetchall()

    _rows = db.execute("""
        SELECT
            referred_by,
            COUNT(*) as total,
            SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN payment_done=1    THEN 1 ELSE 0 END) as paid,
            SUM(payment_amount)                                  as revenue,
            SUM(CASE WHEN day1_done=1       THEN 1 ELSE 0 END) as day1,
            SUM(CASE WHEN day2_done=1       THEN 1 ELSE 0 END) as day2,
            SUM(CASE WHEN interview_done=1  THEN 1 ELSE 0 END) as interviews
        FROM leads WHERE in_pool=0 GROUP BY referred_by
    """).fetchall()
    _stats_map = {r['referred_by']: r for r in _rows}
    stats = [{'member': m, 'stats': _stats_map.get(m['name'])} for m in members]

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
            return render_template('report_form.html', existing=existing, today=today,
                                   username=username)

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

        _qr_cache.clear()   # invalidate QR cache when UPI ID may change
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

    try:
        content = f.read().decode('utf-8-sig', errors='replace')
        reader  = csv.DictReader(io.StringIO(content))
        rows_list = list(reader)  # read all at once; raises if malformed
    except Exception as e:
        flash(f'Could not parse CSV: {e}', 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    # Pre-fetch all existing pool phones → O(1) duplicate lookup per row
    existing_phones = {
        r[0] for r in db.execute(
            "SELECT phone FROM leads WHERE in_pool=1"
        ).fetchall()
    }

    imported = 0
    skipped  = 0

    for row in rows_list:
        # Support common Meta Lead Ads export column names
        # Also handles user's custom Meta form: Submit Time, Full Name, Age, Gender,
        # Phone Number (Calling Number), Your City Name, Ad Name
        name  = (row.get('Full Name') or row.get('full_name') or
                 row.get('name') or row.get('Name') or '').strip()
        phone = (row.get('Phone Number (Calling Number)') or
                 row.get('phone_number') or row.get('phone') or
                 row.get('Phone') or row.get('Phone Number') or '').strip()
        email = (row.get('email') or row.get('Email') or
                 row.get('email_address') or '').strip()

        # Extra Meta fields stored in notes
        age         = (row.get('Age') or row.get('age') or '').strip()
        gender      = (row.get('Gender') or row.get('gender') or '').strip()
        city        = (row.get('Your City Name') or row.get('city') or
                       row.get('City') or '').strip()
        ad_name     = (row.get('Ad Name') or row.get('ad_name') or '').strip()
        submit_time = (row.get('Submit Time') or row.get('submit_time') or '').strip()

        # Build source from Ad Name if available
        lead_source = ad_name if ad_name else source_tag

        # Build notes string from extra fields (city now in its own column)
        extra_parts = []
        if age:         extra_parts.append(f'Age: {age}')
        if gender:      extra_parts.append(f'Gender: {gender}')
        if submit_time: extra_parts.append(f'Submit Time: {submit_time}')
        notes_str = ' | '.join(extra_parts) if extra_parts else ''

        if not name and not phone:
            skipped += 1
            continue

        if not name:
            name = phone
        if not phone:
            phone = 'N/A'

        # Skip duplicates already in pool (set lookup, no extra query)
        if phone in existing_phones:
            skipped += 1
            continue
        existing_phones.add(phone)  # prevent intra-file duplicates too

        db.execute("""
            INSERT INTO leads
                (name, phone, email, assigned_to, source, status,
                 in_pool, pool_price, claimed_at, city, notes)
            VALUES (?, ?, ?, '', ?, 'New', 1, ?, '', ?, ?)
        """, (name, phone, email, lead_source, price_per_lead, city, notes_str))
        imported += 1

    db.commit()
    db.close()
    flash(f'Imported {imported} leads into pool. Skipped {skipped} (duplicates/empty).', 'success')
    return redirect(url_for('admin_lead_pool'))


@app.route('/admin/lead-pool/import-pdf', methods=['POST'])
@admin_required
def import_lead_pool_pdf():
    """Import leads from PDF into the lead pool."""
    db             = get_db()
    price_per_lead = float(request.form.get('price_per_lead') or 50)
    source_tag     = request.form.get('source_tag', 'PDF').strip() or 'PDF'

    if 'pdf_file' not in request.files:
        flash('No file uploaded.', 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    f = request.files['pdf_file']
    if not f.filename.lower().endswith('.pdf'):
        flash('Please upload a .pdf file.', 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    rows_list, err = _extract_leads_from_pdf(f.stream)
    if err:
        flash(err, 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    existing_phones = {
        r[0] for r in db.execute("SELECT phone FROM leads WHERE in_pool=1").fetchall()
    }

    imported = skipped = 0
    for row in rows_list:
        name  = row.get('name', '').strip()
        phone = row.get('phone', '').strip()
        email = row.get('email', '').strip()
        city  = row.get('city', '').strip()

        if not name and not phone:
            skipped += 1
            continue
        if not name:
            name = phone
        if not phone:
            phone = 'N/A'

        if phone in existing_phones:
            skipped += 1
            continue
        existing_phones.add(phone)

        db.execute("""
            INSERT INTO leads
                (name, phone, email, assigned_to, source, status,
                 in_pool, pool_price, claimed_at, city, notes)
            VALUES (?, ?, ?, '', ?, 'New', 1, ?, '', ?, '')
        """, (name, phone, email, source_tag, price_per_lead, city))
        imported += 1

    db.commit()
    db.close()
    flash(f'PDF import: {imported} leads added to pool. Skipped {skipped} (duplicates/empty).', 'success')
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
        # Push in background (external HTTP call, don't block the worker)
        _username = recharge['username']
        _amount   = recharge['amount']
        def _bg_push_recharge(u, amt):
            _db = get_db()
            try:
                _push_to_users(_db, u, '✅ Wallet Recharged!',
                               f'₹{amt:.0f} has been added to your wallet.',
                               '/wallet')
            finally:
                _db.close()
        threading.Thread(target=_bg_push_recharge, args=(_username, _amount), daemon=True).start()
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

    avg_price  = price_info['avg_p'] or 0
    if pool_count == 0:
        can_claim = 0
    elif avg_price > 0:
        can_claim = min(int(wallet_stats['balance'] / avg_price), pool_count)
    else:
        can_claim = pool_count  # free leads — can claim all available

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
    # Validate HH:MM or empty
    import re
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
        # BEGIN IMMEDIATE locks the DB so no other writer can proceed concurrently,
        # preventing two users from claiming the same leads or over-spending wallet.
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
            flash(f'Insufficient balance! Need ₹{total_cost:.0f} but you have ₹{wallet_stats["balance"]:.0f}. '
                  f'Please recharge your wallet.', 'danger')
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

    except Exception as e:
        try:
            db.execute("ROLLBACK")
        except Exception:
            pass
        db.close()
        flash('Something went wrong while claiming leads. Please try again.', 'danger')
        return redirect(url_for('lead_pool'))


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

    # Verify X-Hub-Signature-256 if app_secret is configured
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
            except Exception:
                continue  # skip malformed entries; never crash the webhook

    db.commit()
    db.close()
    return 'OK', 200


# ─────────────────────────────────────────────
#  Change Password
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
#  Profile (with display picture)
# ─────────────────────────────────────────────

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
                        # Resize to 200×200 using Pillow if available
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
                        session['dp'] = dp_b64
                        flash('Profile picture updated!', 'success')
            else:
                flash('No file selected.', 'danger')

        elif action == 'remove_dp':
            db.execute("UPDATE users SET display_picture='' WHERE username=?", (username,))
            db.commit()
            session['dp'] = ''
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


# ─────────────────────────────────────────────
#  CSV Export
# ─────────────────────────────────────────────

@app.route('/leads/export')
@login_required
def export_leads():
    """Download leads as CSV."""
    import io as _io
    db     = get_db()
    username = session['username']

    if session.get('role') == 'admin':
        rows = db.execute(
            "SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' ORDER BY created_at DESC"
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' ORDER BY created_at DESC",
            (username,)
        ).fetchall()
    db.close()

    buf = _io.StringIO()
    cols = ['id','name','phone','email','city','referred_by','assigned_to','source',
            'status','payment_done','payment_amount','revenue','day1_done','day2_done',
            'interview_done','follow_up_date','notes','created_at','updated_at']
    writer = csv.writer(buf)
    writer.writerow(cols)
    for r in rows:
        writer.writerow([r[c] for c in cols])

    buf.seek(0)
    fname = f"leads_{datetime.date.today().isoformat()}.csv"
    return Response(buf.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename={fname}'})


# ─────────────────────────────────────────────
#  Leads – Bulk Import (CSV / PDF)
# ─────────────────────────────────────────────

@app.route('/leads/import', methods=['POST'])
@login_required
def import_leads():
    """
    Bulk import leads from CSV or PDF into the current user's leads.
    Admins can optionally assign to another user via form field 'assigned_to'.
    """
    db          = get_db()
    username    = session['username']
    is_admin    = session.get('role') == 'admin'
    file_type   = request.form.get('import_type', 'csv')  # 'csv' or 'pdf'
    source_tag  = request.form.get('source_tag', 'Import').strip() or 'Import'

    if is_admin:
        assigned_to = request.form.get('assigned_to', '').strip() or username
    else:
        assigned_to = username

    rows_list = []
    err       = None

    if file_type == 'pdf':
        f = request.files.get('import_file')
        if not f or not f.filename:
            flash('No file uploaded.', 'danger')
            db.close()
            return redirect(url_for('leads'))
        if not f.filename.lower().endswith('.pdf'):
            flash('Please upload a .pdf file.', 'danger')
            db.close()
            return redirect(url_for('leads'))
        rows_list, err = _extract_leads_from_pdf(f.stream)
        if err:
            flash(err, 'danger')
            db.close()
            return redirect(url_for('leads'))

    else:  # csv
        f = request.files.get('import_file')
        if not f or not f.filename:
            flash('No file uploaded.', 'danger')
            db.close()
            return redirect(url_for('leads'))
        if not f.filename.lower().endswith('.csv'):
            flash('Please upload a .csv file.', 'danger')
            db.close()
            return redirect(url_for('leads'))
        try:
            content   = f.read().decode('utf-8-sig', errors='replace')
            reader    = csv.DictReader(io.StringIO(content))
            raw_rows  = list(reader)
        except Exception as e:
            flash(f'Could not parse CSV: {e}', 'danger')
            db.close()
            return redirect(url_for('leads'))

        for row in raw_rows:
            name  = (row.get('Full Name') or row.get('full_name') or
                     row.get('name') or row.get('Name') or '').strip()
            phone = (row.get('Phone Number (Calling Number)') or
                     row.get('phone_number') or row.get('phone') or
                     row.get('Phone') or row.get('Phone Number') or '').strip()
            email = (row.get('email') or row.get('Email') or
                     row.get('email_address') or '').strip()
            city  = (row.get('Your City Name') or row.get('city') or
                     row.get('City') or '').strip()
            src   = (row.get('source') or row.get('Source') or '').strip()
            rows_list.append({'name': name, 'phone': phone,
                              'email': email, 'city': city, 'source': src})

    # Pre-fetch existing phones (non-pool, non-deleted) for dedup
    existing_phones = {
        r[0] for r in db.execute(
            "SELECT phone FROM leads WHERE in_pool=0 AND deleted_at=''"
        ).fetchall()
    }

    imported = skipped = 0
    for row in rows_list:
        name  = row.get('name', '').strip()
        phone = row.get('phone', '').strip()
        email = row.get('email', '').strip()
        city  = row.get('city', '').strip()
        src   = row.get('source', '').strip() or source_tag

        if not name and not phone:
            skipped += 1
            continue
        if not name:
            name = phone
        if not phone:
            phone = 'N/A'

        if phone in existing_phones:
            skipped += 1
            continue
        existing_phones.add(phone)

        db.execute("""
            INSERT INTO leads
                (name, phone, email, assigned_to, source, status,
                 in_pool, pool_price, claimed_at, city, notes)
            VALUES (?, ?, ?, ?, ?, 'New', 0, 0, '', ?, '')
        """, (name, phone, email, assigned_to, src, city))
        imported += 1

    db.commit()
    db.close()
    flash(f'Import complete: {imported} leads added, {skipped} skipped (duplicates/empty).', 'success')
    return redirect(url_for('leads'))


# ─────────────────────────────────────────────
#  Announcements (Notice Board)
# ─────────────────────────────────────────────

@app.route('/announcements', methods=['GET'])
@login_required
def announcements():
    db   = get_db()
    rows = db.execute(
        "SELECT * FROM announcements ORDER BY pin DESC, created_at DESC LIMIT 20"
    ).fetchall()
    db.close()
    return render_template('announcements.html', announcements=rows)


@app.route('/announcements/post', methods=['POST'])
@admin_required
def post_announcement():
    msg = request.form.get('message', '').strip()
    pin = 1 if request.form.get('pin') else 0
    if not msg:
        flash('Message cannot be empty.', 'danger')
        return redirect(url_for('announcements'))
    db = get_db()
    db.execute(
        "INSERT INTO announcements (message, created_by, pin) VALUES (?, ?, ?)",
        (msg, session['username'], pin)
    )
    db.commit()
    # Push to all team members
    preview = msg[:80] + ('…' if len(msg) > 80 else '')
    _push_all_team(db, '📢 New Announcement', preview, url_for('announcements'))
    db.close()
    flash('Announcement posted!', 'success')
    return redirect(url_for('announcements'))


@app.route('/announcements/<int:ann_id>/delete', methods=['POST'])
@admin_required
def delete_announcement(ann_id):
    db = get_db()
    db.execute("DELETE FROM announcements WHERE id=?", (ann_id,))
    db.commit()
    db.close()
    flash('Announcement deleted.', 'warning')
    return redirect(url_for('announcements'))


@app.route('/announcements/<int:ann_id>/toggle-pin', methods=['POST'])
@admin_required
def toggle_pin(ann_id):
    db  = get_db()
    ann = db.execute("SELECT pin FROM announcements WHERE id=?", (ann_id,)).fetchone()
    if ann:
        db.execute("UPDATE announcements SET pin=? WHERE id=?",
                   (0 if ann['pin'] else 1, ann_id))
        db.commit()
    db.close()
    return redirect(url_for('announcements'))


# ─────────────────────────────────────────────
#  Leaderboard
# ─────────────────────────────────────────────

@app.route('/leaderboard')
@login_required
def leaderboard():
    db       = get_db()
    username = session['username']

    LEADER_SQL = """
        SELECT
            u.username,
            u.display_picture,
            COUNT(l.id)                                                   AS total,
            SUM(CASE WHEN l.status='Converted' THEN 1 ELSE 0 END)        AS converted,
            SUM(CASE WHEN l.payment_done=1     THEN 1 ELSE 0 END)        AS paid,
            COALESCE(SUM(l.payment_amount),0)                             AS revenue,
            ROUND(
              CAST(SUM(CASE WHEN l.payment_done=1 THEN 1 ELSE 0 END) AS REAL)
              / NULLIF(COUNT(l.id),0)*100, 1)                             AS paid_pct
        FROM users u
        LEFT JOIN leads l ON l.assigned_to=u.username AND l.in_pool=0
        WHERE u.role='team' AND u.status='approved' {extra}
        GROUP BY u.username
        ORDER BY paid DESC, converted DESC, total DESC
    """

    if session.get('role') == 'admin':
        # Admin sees the full leaderboard
        rows = db.execute(LEADER_SQL.format(extra='')).fetchall()
    else:
        # Non-admin: only show own network (self + all recursive downlines)
        network = _get_network_usernames(db, username)
        if network:
            placeholders = ','.join('?' for _ in network)
            rows = db.execute(
                LEADER_SQL.format(extra=f"AND u.username IN ({placeholders})"),
                network
            ).fetchall()
        else:
            rows = []

    db.close()
    return render_template('leaderboard.html', rows=rows,
                           current_user=username)


# ─────────────────────────────────────────────
#  Admin – Reset any user's password
# ─────────────────────────────────────────────

@app.route('/admin/users/<int:user_id>/reset-password', methods=['POST'])
@admin_required
def admin_reset_password(user_id):
    new_pw = request.form.get('new_password', '').strip()
    if len(new_pw) < 6:
        flash('Password must be at least 6 characters.', 'danger')
        return redirect(url_for('admin_approvals'))
    db   = get_db()
    user = db.execute("SELECT username FROM users WHERE id=?", (user_id,)).fetchone()
    if user:
        db.execute("UPDATE users SET password=? WHERE id=?",
                   (generate_password_hash(new_pw, method='pbkdf2:sha256'), user_id))
        db.commit()
        flash(f'Password for @{user["username"]} reset successfully.', 'success')
    db.close()
    return redirect(url_for('admin_approvals'))


# ─────────────────────────────────────────────
#  Lead Notes / Timeline
# ─────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/notes', methods=['POST'])
@login_required
def add_lead_note(lead_id):
    note = request.form.get('note', '').strip()
    if not note:
        flash('Note cannot be empty.', 'danger')
        return redirect(url_for('edit_lead', lead_id=lead_id))

    db = get_db()
    # Verify access
    if session.get('role') != 'admin':
        lead = db.execute(
            "SELECT id FROM leads WHERE id=? AND assigned_to=? AND in_pool=0",
            (lead_id, session['username'])
        ).fetchone()
        if not lead:
            db.close()
            flash('Access denied.', 'danger')
            return redirect(url_for('leads'))

    db.execute(
        "INSERT INTO lead_notes (lead_id, username, note) VALUES (?, ?, ?)",
        (lead_id, session['username'], note)
    )
    db.commit()
    db.close()
    flash('Note added.', 'success')
    return redirect(url_for('edit_lead', lead_id=lead_id))


@app.route('/leads/<int:lead_id>/notes/<int:note_id>/delete', methods=['POST'])
@login_required
def delete_lead_note(lead_id, note_id):
    db   = get_db()
    note = db.execute("SELECT username FROM lead_notes WHERE id=?", (note_id,)).fetchone()
    if note and (note['username'] == session['username'] or session.get('role') == 'admin'):
        db.execute("DELETE FROM lead_notes WHERE id=?", (note_id,))
        db.commit()
    db.close()
    return redirect(url_for('edit_lead', lead_id=lead_id))


# ─────────────────────────────────────────────
#  Bulk Actions on Leads
# ─────────────────────────────────────────────

@app.route('/leads/bulk-action', methods=['POST'])
@login_required
def bulk_action():
    action   = request.form.get('bulk_action', '')
    lead_ids = request.form.getlist('lead_ids')
    if not lead_ids:
        flash('No leads selected.', 'warning')
        return redirect(url_for('leads'))

    # Deduplicate — mobile + desktop both render checkboxes in DOM
    lead_ids = list(set(int(i) for i in lead_ids if i.isdigit()))
    db       = get_db()

    # Build safe WHERE clause limiting to user's own leads (unless admin)
    if session.get('role') == 'admin':
        placeholders = ','.join('?' for _ in lead_ids)
        where  = f"id IN ({placeholders}) AND in_pool=0"
        params = lead_ids
    else:
        placeholders = ','.join('?' for _ in lead_ids)
        where  = f"id IN ({placeholders}) AND assigned_to=? AND in_pool=0"
        params = lead_ids + [session['username']]

    if action == 'delete':
        db.execute(
            f"UPDATE leads SET deleted_at=datetime('now','localtime') WHERE {where}",
            params
        )
        db.commit()
        flash(f'Moved {len(lead_ids)} leads to Recycle Bin.', 'warning')

    elif action.startswith('status:'):
        new_status = action.split(':', 1)[1]
        if new_status in STATUSES:
            db.execute(
                f"UPDATE leads SET status=?, updated_at=datetime('now','localtime') WHERE {where}",
                [new_status] + params
            )
            db.commit()
            flash(f'Status updated to "{new_status}" for {len(lead_ids)} leads.', 'success')

    elif action == 'mark_paid':
        db.execute(
            f"UPDATE leads SET payment_done=1, payment_amount=?, "
            f"updated_at=datetime('now','localtime') WHERE {where}",
            [PAYMENT_AMOUNT] + params
        )
        db.commit()
        flash(f'Marked {len(lead_ids)} leads as paid (₹{PAYMENT_AMOUNT:.0f} each).', 'success')

    db.close()
    return redirect(url_for('leads'))


# ─────────────────────────────────────────────
#  Push Notification API
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


# ─────────────────────────────────────────────
#  Scheduled Reminder Jobs
# ─────────────────────────────────────────────

def _reminder_lock(db, key):
    """
    Atomic lock using INSERT OR IGNORE. Returns True if this process
    is the first to claim the lock for today (safe across gunicorn workers).
    """
    today = datetime.date.today().isoformat()
    lock_key = f'{key}_{today}'
    cur = db.execute(
        "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, 'sent')",
        (lock_key,)
    )
    db.commit()
    return cur.rowcount == 1  # True = we won the lock


def job_followup_reminders():
    """Push individual follow-up reminders to each team member at 9 AM IST."""
    db = get_db()
    try:
        if not _reminder_lock(db, 'followup_reminder'):
            return
        today = datetime.date.today().isoformat()
        rows = db.execute("""
            SELECT assigned_to, COUNT(*) as cnt
            FROM leads
            WHERE in_pool=0
              AND follow_up_date=?
              AND follow_up_date != ''
              AND status NOT IN ('Converted','Lost')
              AND assigned_to != ''
            GROUP BY assigned_to
        """, (today,)).fetchall()
        for row in rows:
            cnt = row['cnt']
            _push_to_users(db, row['assigned_to'],
                           '📅 Follow-up Reminder',
                           f'{cnt} lead{"s" if cnt > 1 else ""} due for follow-up today!',
                           '/dashboard')
        db.commit()
    finally:
        db.close()


def job_calling_reminder():
    """
    Minutely job: push calling reminder to each user whose calling_reminder_time
    matches the current HH:MM (IST). Per-user per-day lock prevents duplicates.
    """
    now_hhmm = datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
    today    = datetime.date.today().isoformat()
    db = get_db()
    try:
        users = db.execute(
            "SELECT username FROM users WHERE role='team' AND status='approved'"
            " AND calling_reminder_time=?", (now_hhmm,)
        ).fetchall()
        for u in users:
            lock_key = f'call_reminder_{u["username"]}_{today}'
            cur = db.execute(
                "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, 'sent')",
                (lock_key,)
            )
            db.commit()
            if cur.rowcount == 1:
                _push_to_users(db, u['username'],
                               '📞 Calling Reminder',
                               'Time to start your calls! Don\'t forget your daily report.',
                               '/reports/submit')
                db.commit()
    finally:
        db.close()


# ─────────────────────────────────────────────
#  Boot – runs on every startup
# ─────────────────────────────────────────────

init_db()
migrate_db()
seed_users()

# Start scheduler — guard against Flask reloader double-start
if SCHEDULER_AVAILABLE and not os.environ.get('SCHEDULER_STARTED'):
    os.environ['SCHEDULER_STARTED'] = '1'
    _scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
    _scheduler.add_job(job_followup_reminders, 'cron', hour=9, minute=0,
                       id='followup_reminders', replace_existing=True)
    _scheduler.add_job(job_calling_reminder, 'interval', minutes=1,
                       id='calling_reminder', replace_existing=True)
    _scheduler.start()
    atexit.register(lambda: _scheduler.shutdown(wait=False))

# ─────────────────────────────────────────────
#  Live Session
# ─────────────────────────────────────────────

@app.route('/live-session')
@login_required
def live_session():
    db    = get_db()
    link  = _get_setting(db, 'zoom_link', '')
    title = _get_setting(db, 'zoom_title', "Today's Live Session")
    time_ = _get_setting(db, 'zoom_time', '2:00 PM')
    db.close()
    return render_template('live_session.html',
                           zoom_link=link, zoom_title=title, zoom_time=time_)


@app.route('/admin/live-session', methods=['GET', 'POST'])
@admin_required
def admin_live_session():
    db = get_db()
    if request.method == 'POST':
        link  = request.form.get('zoom_link', '').strip()
        title = request.form.get('zoom_title', '').strip() or "Today's Live Session"
        time_ = request.form.get('zoom_time', '').strip() or '2:00 PM'
        _set_setting(db, 'zoom_link',  link)
        _set_setting(db, 'zoom_title', title)
        _set_setting(db, 'zoom_time',  time_)
        db.commit()
        db.close()
        flash('Live session updated.', 'success')
        return redirect(url_for('admin_live_session'))
    link  = _get_setting(db, 'zoom_link', '')
    title = _get_setting(db, 'zoom_title', "Today's Live Session")
    time_ = _get_setting(db, 'zoom_time', '2:00 PM')
    db.close()
    return render_template('live_session_admin.html',
                           zoom_link=link, zoom_title=title, zoom_time=time_)


# ─────────────────────────────────────────────
#  Admin – All Members List + Individual Activity
# ─────────────────────────────────────────────

@app.route('/admin/members')
@admin_required
def admin_members():
    db = get_db()
    users = db.execute(
        "SELECT * FROM users WHERE role='team' ORDER BY status, created_at DESC"
    ).fetchall()

    # Quick stats per member in one query
    _rows = db.execute("""
        SELECT assigned_to,
            COUNT(*) as total_leads,
            SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN payment_done=1 THEN 1 ELSE 0 END) as paid
        FROM leads WHERE in_pool=0
        GROUP BY assigned_to
    """).fetchall()
    stats_map = {r['assigned_to']: r for r in _rows}

    # Report count per member
    _rep_rows = db.execute(
        "SELECT username, COUNT(*) as report_count FROM daily_reports GROUP BY username"
    ).fetchall()
    report_map = {r['username']: r['report_count'] for r in _rep_rows}

    db.close()
    return render_template('all_members.html',
                           users=users,
                           stats_map=stats_map,
                           report_map=report_map)


@app.route('/admin/members/<username>')
@admin_required
def member_detail(username):
    db   = get_db()
    user = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if not user:
        flash('Member not found.', 'danger')
        db.close()
        return redirect(url_for('admin_members'))

    metrics = _get_metrics(db, username=username)
    wallet  = _get_wallet(db, username)

    recent_leads = db.execute(
        "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 ORDER BY created_at DESC LIMIT 20",
        (username,)
    ).fetchall()

    recent_reports = db.execute(
        "SELECT * FROM daily_reports WHERE username=? ORDER BY report_date DESC LIMIT 10",
        (username,)
    ).fetchall()

    # Status breakdown
    _sc = db.execute(
        "SELECT status, COUNT(*) as c FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' GROUP BY status",
        (username,)
    ).fetchall()
    status_data = {s: 0 for s in STATUSES}
    for row in _sc:
        if row['status'] in status_data:
            status_data[row['status']] = row['c']

    # Downline
    downlines = db.execute(
        "SELECT username, status FROM users WHERE upline_name=? ORDER BY username",
        (username,)
    ).fetchall()

    db.close()
    return render_template('member_detail.html',
                           member=user,
                           metrics=metrics,
                           wallet=wallet,
                           recent_leads=recent_leads,
                           recent_reports=recent_reports,
                           status_data=status_data,
                           downlines=downlines,
                           statuses=STATUSES,
                           payment_amount=PAYMENT_AMOUNT)




# ─────────────────────────────────────────────
#  Drill-Down Analytics
# ─────────────────────────────────────────────

@app.route('/drill-down/<metric>')
@login_required
def drilldown(metric):
    db = get_db()
    is_admin = session.get('role') == 'admin'

    if metric not in DRILL_LEAD_METRICS and metric not in DRILL_REPORT_METRICS:
        db.close()
        return redirect(url_for('admin_dashboard' if is_admin else 'team_dashboard'))

    # Scope: admin sees all, team sees self + downline
    if is_admin:
        network = None
    else:
        network = _get_downline_usernames(db, session['username'])

    fmt = request.args.get('format', '')

    # ── Lead metrics ─────────────────────────────────
    if metric in DRILL_LEAD_METRICS:
        label, icon, color, condition = DRILL_LEAD_METRICS[metric]

        if network is not None:
            ph = ','.join('?' * len(network))
            base = f"in_pool=0 AND deleted_at='' AND assigned_to IN ({ph})"
            base_params = list(network)
        else:
            base = "in_pool=0 AND deleted_at=''"
            base_params = []

        extra = f" AND {condition}" if condition else ''

        leads_rows = db.execute(
            f"SELECT id, name, phone, status, payment_done, payment_amount, revenue, "
            f"created_at, updated_at, assigned_to "
            f"FROM leads WHERE {base}{extra} ORDER BY created_at DESC LIMIT 500",
            base_params
        ).fetchall()

        breakdown = db.execute(
            f"SELECT assigned_to, COUNT(*) as cnt FROM leads WHERE {base}{extra} "
            f"GROUP BY assigned_to ORDER BY cnt DESC",
            base_params
        ).fetchall()

        trend_rows = db.execute(
            f"SELECT date(created_at) as d, COUNT(*) as cnt FROM leads "
            f"WHERE {base}{extra} AND date(created_at) >= date('now','-30 days') "
            f"GROUP BY d ORDER BY d",
            base_params
        ).fetchall()
        trend = [{'d': r['d'], 'cnt': r['cnt']} for r in trend_rows]

        if fmt == 'csv':
            db.close()
            out = io.StringIO()
            w = csv.writer(out)
            w.writerow(['Name', 'Phone', 'Status', 'Payment Done', 'Amount', 'Assigned To', 'Added', 'Updated'])
            for r in leads_rows:
                w.writerow([r['name'], r['phone'], r['status'],
                            'Yes' if r['payment_done'] else 'No',
                            r['payment_amount'] or 0,
                            r['assigned_to'], r['created_at'][:10], r['updated_at'][:10]])
            return Response(out.getvalue(), mimetype='text/csv',
                            headers={'Content-Disposition': f'attachment;filename=drill_{metric}.csv'})

        db.close()
        return render_template('drill_down.html',
                               metric=metric, label=label, icon=icon, color=color,
                               leads=leads_rows, report_rows=None,
                               breakdown=breakdown, trend=trend,
                               is_report=False, is_admin=is_admin)

    # ── Report metrics ────────────────────────────────
    else:
        label, icon, color = DRILL_REPORT_METRICS[metric]
        col = metric  # column name in daily_reports table

        if network is not None:
            ph = ','.join('?' * len(network))
            where = f"username IN ({ph})"
            where_params = list(network)
        else:
            where = '1=1'
            where_params = []

        report_rows = db.execute(
            f"SELECT username, report_date, {col} as val, remarks "
            f"FROM daily_reports WHERE {where} AND {col} > 0 "
            f"ORDER BY report_date DESC, username LIMIT 500",
            where_params
        ).fetchall()

        breakdown = db.execute(
            f"SELECT username as assigned_to, SUM({col}) as cnt "
            f"FROM daily_reports WHERE {where} GROUP BY username ORDER BY cnt DESC",
            where_params
        ).fetchall()

        trend_rows = db.execute(
            f"SELECT report_date as d, SUM({col}) as cnt FROM daily_reports "
            f"WHERE {where} AND report_date >= date('now','-30 days') "
            f"GROUP BY report_date ORDER BY report_date",
            where_params
        ).fetchall()
        trend = [{'d': r['d'], 'cnt': r['cnt']} for r in trend_rows]

        if fmt == 'csv':
            db.close()
            out = io.StringIO()
            w = csv.writer(out)
            w.writerow(['Member', 'Date', label, 'Remarks'])
            for r in report_rows:
                w.writerow([r['username'], r['report_date'], r['val'], r['remarks'] or ''])
            return Response(out.getvalue(), mimetype='text/csv',
                            headers={'Content-Disposition': f'attachment;filename=drill_{metric}.csv'})

        db.close()
        return render_template('drill_down.html',
                               metric=metric, label=label, icon=icon, color=color,
                               leads=None, report_rows=report_rows,
                               breakdown=breakdown, trend=trend,
                               is_report=True, is_admin=is_admin)


@app.route('/health')
def health():
    return {'status': 'ok'}, 200


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5001)
