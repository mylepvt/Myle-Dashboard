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
from urllib.parse import quote as _url_quote
from flask import (Flask, render_template, request, redirect, url_for,
                   flash, session, Response, make_response, abort, send_from_directory, jsonify)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.exceptions import HTTPException
from database import get_db, init_db, migrate_db, seed_users, seed_training_questions
from pathlib import Path
from helpers import (  # noqa: F401 — shared constants & utility functions
    STATUSES, STATUS_TO_STAGE, PIPELINE_AUTO_EXPIRE_STATUSES, CALL_STATUS_VALUES, TRACKS,
    CALL_RESULT_TAGS, RETARGET_TAGS, FOLLOWUP_TAGS, SOURCES,
    BADGE_DEFS, PAYMENT_AMOUNT, BADGE_META, STAGE_TO_DEFAULT_STATUS,
    _now_ist, _today_ist,
    _log_activity, _log_lead_event,
    _get_setting, _set_setting,
    _get_wallet, _get_metrics,
    _get_downline_usernames, _get_network_usernames,
    _get_admin_username, _get_leader_for_user,
    _calculate_priority, _leads_with_priority,
    _calculate_heat_score, _get_next_action, _generate_ai_tip,
    _enrich_lead, _enrich_leads,
    _transition_stage, _trigger_training_unlock, _check_seat_hold_expiry, _auto_expire_pipeline_leads,
    _check_and_award_badges, _check_and_award_badges_inner,
    _get_user_badges_emoji,
    _upsert_daily_score, _get_today_score, _get_actual_daily_counts,
)

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
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False

# Optional Anthropic AI (Maya assistant — fallback)
try:
    import anthropic as _anthropic_lib
    ANTHROPIC_AVAILABLE = True
except ImportError:
    _anthropic_lib = None
    ANTHROPIC_AVAILABLE = False

# Optional Google Gemini AI (Maya assistant — primary, free tier)
try:
    import google.generativeai as _gemini_lib
    import PIL.Image as _PIL_Image
    import io as _io_lib
    GEMINI_AVAILABLE = True
except ImportError:
    _gemini_lib  = None
    _PIL_Image   = None
    GEMINI_AVAILABLE = False

app = Flask(__name__)

app.config['TEMPLATES_AUTO_RELOAD'] = True

# ── Structured logging ───────────────────────────────────────
import logging as _logging
_log_level = _logging.DEBUG if os.environ.get('FLASK_DEBUG') else _logging.INFO
_log_fmt = _logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
_stream_handler = _logging.StreamHandler()
_stream_handler.setFormatter(_log_fmt)
_stream_handler.setLevel(_log_level)
app.logger.handlers.clear()
app.logger.addHandler(_stream_handler)
app.logger.setLevel(_log_level)

# ── Secret key & cookie security ─────────────────────────────
_env_secret = os.environ.get('SECRET_KEY')
if _env_secret:
    app.secret_key = _env_secret
else:
    # IMPORTANT (multi-worker): secret MUST be identical in every Gunicorn worker process.
    # Using secrets.token_hex(32) here caused a *different* key per worker → session cookies
    # signed on worker A failed on worker B → users randomly "logged out".
    # Stable shared fallback keeps sessions valid until SECRET_KEY is set in the environment.
    import sys as _sys

    app.secret_key = os.environ.get(
        'FLASK_DEV_SECRET_FALLBACK',
        'myle_community_secret_2024_local',
    )
    print(
        '[SECURITY WARNING] SECRET_KEY env var not set — using shared dev fallback '
        '(set SECRET_KEY on Render for strong signing + consistent sessions across deploys).',
        file=_sys.stderr,
    )

app.config['SESSION_PERMANENT'] = True                                    # every session permanent by default
app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(days=30)    # 30-day rolling sessions
# SESSION_TYPE is intentionally NOT set to filesystem/redis:
# Render's disk is ephemeral — server-side filesystem sessions are wiped on every deploy.
# Flask's default client-side signed cookies survive deploys as long as SECRET_KEY is stable.
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True


def _use_secure_cookies():
    """True when app is served over HTTPS (Render, explicit env, or legacy SECRET_KEY signal)."""
    override = (os.environ.get('SESSION_COOKIE_SECURE') or '').strip().lower()
    if override in ('0', 'false', 'no'):
        return False
    if override in ('1', 'true', 'yes'):
        return True
    if (os.environ.get('RENDER') or '').lower() == 'true':
        return True
    if (os.environ.get('FLASK_ENV') or '').lower() == 'production':
        return True
    return bool(_env_secret)


app.config['SESSION_COOKIE_SECURE'] = _use_secure_cookies()


@app.after_request
def _security_headers(response):
    """Baseline security headers for HTML/API responses (disable with SECURITY_HEADERS=0)."""
    if (os.environ.get('SECURITY_HEADERS') or '1').strip().lower() in ('0', 'false', 'no'):
        return response
    # Do not override if something else already set (e.g. future middleware)
    response.headers.setdefault('X-Content-Type-Options', 'nosniff')
    response.headers.setdefault('X-Frame-Options', 'SAMEORIGIN')
    response.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
    response.headers.setdefault(
        'Permissions-Policy',
        'accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), '
        'microphone=(), payment=(), usb=()'
    )
    if _use_secure_cookies():
        response.headers.setdefault('Strict-Transport-Security', 'max-age=31536000')
    return response

# Persistent upload root: set UPLOAD_ROOT to a persistent path (e.g. /data on Render) so
# PDF/audio uploads survive restarts; default is project directory (ephemeral on Render).
def _upload_root():
    return os.environ.get('UPLOAD_ROOT') or os.path.abspath(os.path.dirname(__file__))


_upload_root_warned = False


def _warn_upload_root_once():
    global _upload_root_warned
    if _upload_root_warned:
        return
    if os.environ.get('SECRET_KEY') and not os.environ.get('UPLOAD_ROOT'):
        _upload_root_warned = True
        import sys
        print('[UPLOAD] Production par UPLOAD_ROOT set nahi hai — training PDF/audio restart ke baad gayab ho jayenge. '
              'Render par Persistent Disk mount karo (e.g. /data) aur env UPLOAD_ROOT=/data set karo.', file=sys.stderr)

# ── Maya AI System Prompt ─────────────────────────────────────
MAYA_SYSTEM_PROMPT = """You are Maya — the AI assistant for Myle Community, a network marketing team management platform.

You help team members (and admins) with:
1. WhatsApp scripts for inviting prospects
2. Objection handling — professional, empathetic responses to common objections
3. Lead management and follow-up advice
4. Training guidance (7-day program, test prep)
5. App usage help (leads, wallet, daily reports, training)
6. Network marketing Q&A and motivation

## About Myle Community
- Network marketing business based on product sales (Forever Living)
- Members invite prospects and guide them through a conversion journey
- 3 investment tracks: Slow Track (₹8,000), Medium Track (₹18,000), Fast Track (₹38,000)
- ₹196 initial payment gives prospect access to a presentation video
- 3-day enrollment window after video to commit to a track
- Seat Hold deposit collected before full track payment

## About the App
- **Leads page**: Add prospects, track them by status (New Lead → Contacted → Invited → Video Sent → Video Watched → Paid ₹196 → Mindset Lock → Day 1 → Day 2 → Interview → Track Selected → Seat Hold Confirmed → Fully Converted)
- **Wallet**: Team members recharge via UPI QR code, spend on claiming leads from pool
- **Training**: 7-day video training + MCQ test (60/100 pass mark) → certificate → app unlocked
- **Daily Reports**: Submit daily KPIs every day
- **Lead Pool**: Admin imports or adds leads; team claims them by spending wallet balance

## WhatsApp Scripts

### First Approach
"Hey [Name]! 👋 Ek exciting business opportunity hai jo main share karna chahta/chahti tha. Kya 10 minute milenge ek quick call ke liye? Properly explain karta/karti hoon — koi pressure nahi, bas ek conversation."

### After Adding to Leads / Sending Video
"Hey [Name]! Ek short presentation bheja/bheja hai — sirf 20-25 minute ka hai. Dekh lena jab time mile. Uske baad 3 din ka window hota hai decide karne ke liye. Koi bhi question ho toh seedha poochh lena! 😊"

### 24-Hour Follow-up
"Hey [Name]! Bas check karne ke liye — kya time mila presentation dekhne ka? Koi questions? Main hoon yahan. 🙏"

### 3-Day Follow-up
"Hey [Name]! Presentation ke baare mein kuch thoughts? Aaj window ka last day hai actually. No pressure — seedha bata do. 👍"

### After Video Watched — Invitation
"Hey! Video dekha toh laga? Main chahta/chahti hoon tum sach mein samjho sab — kab ek 15-minute call kar sakte ho? Main sab doubts clear kar deta/deti hoon. 🎯"

## Common Objections & Answers

### "Mujhe time nahi hai"
"Bilkul samjha/samjhi! Aaj kal sab busy hain. Actually, iss business ki khoobsoorat yahi hai — tum apna time khud set karte ho. Part-time se start karo, sirf 1-2 ghante daily bhi kafi hai shuruat mein. Ek 15-minute call pe puri picture clear kar deta/deti hoon — kab free ho thoda?"

### "Ye sab fraud/MLM/pyramid scheme hai"
"Bilkul valid concern hai, seriously! Direct selling aur illegal pyramid scheme mein bahut fark hai. Yahan actual products sell hote hain, aur income product sales se aati hai — kisi ko sirf 'join' karne ke paise nahi milte. Ye industry India mein government-regulated hai (IDSA registered). Ek legit comparison share karoon kya?"

### "Kaafi mehnga hai / paisa nahi hai"
"Samajh sakta/sakti hoon. Isko ek investment ki tarah dekho — koi bhi offline business start karne mein lakhs lagte hain, yahan starting bahut kam hai. Aur agar seriously kaam karo toh 2-3 mahine mein investment waapas aa jata hai. Numbers dekhna chahoge ek baar?"

### "Soch ke batata/batati hoon"
"Bilkul! Sochna important hai. Main bas jaanna chahta/chahti tha — specifically kya soch rahe/rahi ho? Koi ek main concern ho toh seedha bata do, main directly address kar sakta/sakti hoon. Zyada helpful hoga woh."

### "Mera koi network nahi hai"
"Ye sabse common misconception hai actually! Network marketing ka matlab sirf family/friends nahi — aaj digital marketing se complete strangers bhi join karte hain. Humara training specifically sikhata hai kaise strangers se baat karein. Aur genuinely 5-7 seriously interested log toh milte hi hain shuruat mein."

### "Maine pehle kisi aur company mein try kiya tha, nahi hua"
"Honest feedback ke liye shukriya! Kisi bhi business mein success system + mentorship pe depend karti hai. Yahan specific daily training, personal guidance, aur proper follow-up system hai. Kya jaanna chahoge kya alag hai is baar?"

### "Ye sirf upar walon ke liye hai"
"Samjha/Samjhi! Ye ek common doubt hai. Actually iss model mein jo aaj start karta hai wo bhi same earning potential rakhta hai — kyunki earnings apni team ki performance se aati hai, na kisi ke 'upar' hone se. Main numbers ke saath explain karta/karti hoon?"

## Communication Style
- Reply in Hinglish (Hindi + English mix) — casual, warm, and professional
- Be encouraging, empathetic, and solution-focused
- Keep answers concise unless a full script is requested
- When user shares a screenshot, analyze it carefully and give specific, actionable advice
- Use emojis occasionally to keep the tone friendly but not excessive
- Always address the emotional side of objections before giving logical answers"""





# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Auth Decorators (see decorators.py)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

from decorators import admin_required, login_required, safe_route

# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Helpers
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

# Drill-down metric config
DRILL_LEAD_METRICS = {
    'total':          ('Total Leads',    'bi bi-people-fill',              'primary', None),
    'converted':      ('Converted',      'bi bi-check-circle-fill',        'success', "status IN ('Converted','Fully Converted')"),
    'paid':           ('Payments ₹196',  'bi bi-credit-card-2-front-fill', 'info',    'payment_done=1'),
    'day1':           ('Day 1 Done',     'bi bi-1-circle-fill',            'info',    'day1_done=1'),
    'day2':           ('Day 2 Done',     'bi bi-2-circle-fill',            'warning', 'day2_done=1'),
    'interview':      ('Interview Done', 'bi bi-mic-fill',                 'danger',  'interview_done=1'),
    'revenue':        ('Total Revenue',  'bi bi-currency-rupee',           'warning', 'payment_done=1'),
    'mindset_lock':   ('Mindset Lock',   'bi bi-lock-fill',                'primary', "status='Mindset Lock'"),
    'track_selected': ('Track Selected', 'bi bi-bookmark-check-fill',      'info',    "status='Track Selected'"),
    'seat_hold':      ('Seat Hold',      'bi bi-shield-check-fill',        'purple',  "status='Seat Hold Confirmed'"),
    'fully_converted':('Fully Converted','bi bi-trophy-fill',              'success', "status='Fully Converted'"),
}

DRILL_REPORT_METRICS = {
    'total_calling':    ('Total Calls',   'bi bi-telephone-fill',         'primary'),
    'pdf_covered':      ('PDF Covered',   'bi bi-file-earmark-pdf-fill',  'danger'),
    'calls_picked':     ('Calls Picked',  'bi bi-telephone-inbound-fill', 'success'),
    'enrollments_done': ('Enrollments',   'bi bi-person-check-fill',      'success'),
    'plan_2cc':         ('2CC Plan',      'bi bi-star-fill',              'warning'),
}


_qr_cache: dict = {}   # {upi_id: (bytes, b64_str)}

def _generate_upi_qr_bytes(upi_id):
    """Generate UPI QR code PNG bytes. Returns None if qrcode not available."""
    if not QR_AVAILABLE or not upi_id:
        return None
    if upi_id in _qr_cache:
        return _qr_cache[upi_id][0]
    upi_string = f"upi://pay?pa={_url_quote(upi_id)}&pn=Myle+Community&cu=INR"
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
                # \u2500\u2500 Try table extraction first \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
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
                    # \u2500\u2500 Fall back to line-by-line text scan \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
                    text = page.extract_text() or ''
                    for line in text.split('\n'):
                        m = _PHONE_RE.search(line)
                        if not m:
                            continue
                        phone = m.group(1)
                        # strip phone (and +91 prefix) from line \u2192 remaining text = name
                        name = _PHONE_RE.sub('', line).strip(' -|,;:\t')
                        leads.append({'name': name, 'phone': phone,
                                      'email': '', 'city': ''})
    except Exception as exc:
        return None, f"Could not parse PDF: {exc}"

    return leads, None


def _get_or_create_vapid_keys(db):
    """
    Return (private_scalar_b64url, public_b64url).
    Stores the raw 32-byte private key scalar as base64url — the format
    pywebpush accepts unconditionally across all versions.
    Any old PEM-based key is wiped and regenerated automatically.
    """
    if not PUSH_AVAILABLE:
        return None, None

    private_scalar = _get_setting(db, 'vapid_private_pem', '')   # reuse same DB key name
    public_b64     = _get_setting(db, 'vapid_public_key',  '')

    if private_scalar and public_b64:
        # If it looks like a PEM block (old format), wipe and regenerate
        if '-----' in private_scalar:
            app.logger.warning('[Push] Old PEM VAPID key detected — regenerating as raw scalar.')
            private_scalar = ''
            public_b64     = ''
            _set_setting(db, 'vapid_private_pem', '')
            _set_setting(db, 'vapid_public_key',  '')
            db.commit()
        else:
            return private_scalar, public_b64

    # Generate new P-256 key pair and store as raw base64url scalars
    private_key = ec.generate_private_key(ec.SECP256R1())

    # Private scalar: raw 32 bytes of the private key integer
    private_numbers = private_key.private_numbers()
    private_bytes_raw = private_numbers.private_value.to_bytes(32, 'big')
    private_scalar = base64.urlsafe_b64encode(private_bytes_raw).rstrip(b'=').decode()

    # Public key: uncompressed point (65 bytes), base64url-encoded
    pub_raw = private_key.public_key().public_bytes(
        _crypto_serial.Encoding.X962,
        _crypto_serial.PublicFormat.UncompressedPoint
    )
    public_b64 = base64.urlsafe_b64encode(pub_raw).rstrip(b'=').decode()

    _set_setting(db, 'vapid_private_pem', private_scalar)
    _set_setting(db, 'vapid_public_key',  public_b64)
    db.commit()
    app.logger.info('[Push] New VAPID key pair generated.')
    return private_scalar, public_b64


def _push_to_users(db, usernames, title, body, url='/'):
    """
    Send a Web Push notification to all subscriptions of the given usernames.
    Automatically removes dead (410/404) subscriptions.
    Fails silently \u2014 never breaks the calling route.
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
                    vapid_claims={'sub': 'mailto:' + (_get_setting(db, 'smtp_user') or 'admin@mylecommunity.com')}
                )
            except Exception as exc:
                # 410 Gone / 404 Not Found \u2192 subscription expired, clean up
                resp = getattr(exc, 'response', None)
                if resp is not None and getattr(resp, 'status_code', 0) in (404, 410):
                    dead_ids.append(sub['id'])
                else:
                    app.logger.error(f'[Push] Send failed: {exc}')

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

    subject = 'Welcome to Myle Community \u2013 Account Approved!'

    html_body = f"""
    <div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;border:1px solid #e0e0e0;">
      <div style="background:linear-gradient(135deg,#1a1a2e,#0f3460);padding:32px;text-align:center;">
        <h2 style="color:#fff;margin:0;font-size:22px;">Myle Community</h2>
        <p style="color:rgba(255,255,255,0.7);margin:8px 0 0;font-size:14px;">Team Dashboard</p>
      </div>
      <div style="padding:32px;">
        <h3 style="color:#1a1a2e;margin-top:0;">Hi {username}, your account is approved! \U0001f389</h3>
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
        return True
    except Exception as e:
        app.logger.error(f'[Email] Welcome email failed: {e}')
        return False  # Don't break approval flow if email fails


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

    subject   = 'Myle Community \u2013 Password Reset Request'
    html_body = f"""
    <div style="font-family:Arial,sans-serif;max-width:520px;margin:auto;padding:24px;background:#f8f9fa;border-radius:12px;">
      <div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:8px;padding:20px;text-align:center;margin-bottom:24px;">
        <h2 style="color:#fff;margin:0;">\U0001f510 Password Reset</h2>
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
    except Exception as e:
        app.logger.error(f'[Email] Password reset email failed: {e}')
        return False




# ─────────────────────────────────────────────────
#  Enrollment / Watch routes (see routes/enrollment_routes.py)
# ─────────────────────────────────────────────────

# Shared helpers used by enrollment routes AND other parts of app.py
def _youtube_embed_url(raw_url):
    """Extract YouTube video ID from any common URL and return embed URL. Returns '' if not valid."""
    if not raw_url or not isinstance(raw_url, str):
        return ''
    s = raw_url.strip()
    # Support: watch?v=, youtu.be/, embed/, shorts/
    m = _re.search(
        # Also supports live stream URLs: youtube.com/live/<id>
        r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/|youtube\.com/shorts/|youtube\.com/live/)([a-zA-Z0-9_-]{11})',
        s
    )
    if m:
        vid = m.group(1)
        return 'https://www.youtube-nocookie.com/embed/' + vid + '?rel=0&modestbranding=1&playsinline=1'
    return ''


def _public_external_url(endpoint, **values):
    """Build stable absolute URLs behind proxies (Render/Cloudflare/Nginx)."""
    path = url_for(endpoint, _external=False, **values)
    try:
        proto = (request.headers.get('X-Forwarded-Proto') or request.scheme or 'https').split(',')[0].strip()
        host = (request.headers.get('X-Forwarded-Host') or request.host or '').split(',')[0].strip()
        if host:
            return f"{proto}://{host}{path}"
    except RuntimeError:
        pass
    return url_for(endpoint, _external=True, **values)


_BATCH_SLOTS = ('d1_morning', 'd1_afternoon', 'd1_evening', 'd2_morning', 'd2_afternoon', 'd2_evening')
_BATCH_LABELS = {
    'd1_morning': 'Day 1 — Morning Batch', 'd1_afternoon': 'Day 1 — Afternoon Batch', 'd1_evening': 'Day 1 — Evening Batch',
    'd2_morning': 'Day 2 — Morning Batch', 'd2_afternoon': 'Day 2 — Afternoon Batch', 'd2_evening': 'Day 2 — Evening Batch',
}


def _batch_watch_urls():
    """In-app watch URLs for each batch slot (v1, v2). Prospect opens our page, not YouTube."""
    return {
        slot: {'v1': _public_external_url('watch_batch', slot=slot, v=1),
               'v2': _public_external_url('watch_batch', slot=slot, v=2)}
        for slot in _BATCH_SLOTS
    }

# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Template Filters
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.template_filter('wa_phone')
def wa_phone_filter(phone):
    """Clean phone number for WhatsApp wa.me link."""
    import re
    digits = re.sub(r'[^\d]', '', str(phone))
    if len(digits) == 10 and digits[0] in '6789':
        digits = '91' + digits          # Indian mobile \u2013 prepend country code
    elif digits.startswith('0') and len(digits) == 11:
        digits = '91' + digits[1:]      # 0XXXXXXXXXX \u2192 91XXXXXXXXXX
    return digits


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Context processor \u2013 inject counts for nav badges
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.context_processor
def inject_pending_count():
    try:
        if session.get('role') == 'admin':
            db  = get_db()
            row = db.execute("""
                SELECT
                  (SELECT COUNT(*) FROM users           WHERE status='pending') as pu,
                  (SELECT COUNT(*) FROM wallet_recharges WHERE status='pending') as wp,
                  (SELECT COUNT(*) FROM leads WHERE in_pool=0 AND deleted_at='' AND status='Lost') as lc
            """).fetchone()
            db.close()
            return {'pending_count': row['pu'], 'wallet_pending': row['wp'],
                    'has_pending_work': False, 'lost_count': row['lc']}
        uname = session.get('username')
        role  = session.get('role')
        if uname:
            db = get_db()
            has_pending_work = db.execute(
                "SELECT COUNT(*) FROM leads "
                "WHERE in_pool=0 AND deleted_at='' AND assigned_to=? AND status IN ('Day 1','Paid ₹196') AND d1_morning=0",
                (uname,)
            ).fetchone()[0] > 0
            if role == 'leader':
                downline = _get_downline_usernames(db, uname)
                ph = ','.join('?' * len(downline))
                lc = db.execute(
                    f"SELECT COUNT(*) FROM leads WHERE in_pool=0 AND deleted_at='' AND status='Lost' AND assigned_to IN ({ph})",
                    downline
                ).fetchone()[0]
            else:
                lc = db.execute(
                    "SELECT COUNT(*) FROM leads WHERE in_pool=0 AND deleted_at='' AND assigned_to=? AND status='Lost'",
                    (uname,)
                ).fetchone()[0]
            db.close()
            return {'pending_count': 0, 'wallet_pending': 0,
                    'has_pending_work': has_pending_work, 'lost_count': lc}
    except Exception as e:
        app.logger.error(f"inject_pending_count() failed: {e}")
    return {'pending_count': 0, 'wallet_pending': 0, 'has_pending_work': False, 'lost_count': 0}


# ──────────────────────────────────────────────────────────────
#  CSRF Protection
# ──────────────────────────────────────────────────────────────

# Routes exempt from CSRF:
#   - /login, /register  → standalone pages (don't extend base.html, so the
#                          auto-inject JS never runs → form has no csrf_token)
#   - /meta/webhook      → external service uses its own HMAC signature
_CSRF_EXEMPT_PREFIXES = ('/meta/webhook', '/login', '/register')

# Local dev: bypass login — set DEV_BYPASS_AUTH=1 and open app → auto admin session
_DEV_BYPASS_AUTH = os.environ.get('DEV_BYPASS_AUTH', '').lower() in ('1', 'true', 'yes')

@app.before_request
def dev_bypass_auth():
    """Allow auth bypass only for localhost development."""
    if not _DEV_BYPASS_AUTH or session.get('username'):
        return
    is_dev = os.environ.get('FLASK_ENV', '').lower() == 'development' or bool(os.environ.get('FLASK_DEBUG'))
    remote = (request.remote_addr or '').strip()
    if not is_dev or remote not in ('127.0.0.1', '::1'):
        return
    if request.path.startswith('/static') or request.path.startswith('/watch/'):
        return
    session['username'] = 'admin'
    session['role'] = 'admin'
    session.permanent = True


@app.before_request
def csrf_protect():
    """Generate a CSRF token for the session and validate it on unsafe methods."""
    # Always ensure a token exists in the session
    if '_csrf_token' not in session:
        session['_csrf_token'] = secrets.token_hex(32)

    # Only validate on state-changing methods
    if request.method in ('GET', 'HEAD', 'OPTIONS', 'TRACE'):
        return

    # Exempt external webhook endpoints (they use their own HMAC signature)
    if any(request.path.startswith(p) for p in _CSRF_EXEMPT_PREFIXES):
        return

    submitted = (
        request.form.get('csrf_token') or
        request.headers.get('X-CSRF-Token')
    )
    if not submitted or not hmac.compare_digest(submitted, session.get('_csrf_token', '')):
        abort(403, description='CSRF token missing or invalid. Please refresh and try again.')


@app.context_processor
def inject_csrf_token():
    """Make csrf_token available to every template."""
    return {'csrf_token': session.get('_csrf_token', '')}


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Global Error Handlers
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.errorhandler(404)
def not_found_error(error):
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return {'ok': False, 'error': 'Page not found'}, 404
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    import traceback as _tb
    app.logger.error(f"500 Error: {error}\n{_tb.format_exc()}")
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return {'ok': False, 'error': 'Server error, please try again'}, 500
    return render_template('500.html'), 500


@app.errorhandler(Exception)
def unhandled_exception(error):
    if isinstance(error, HTTPException):
        return error
    import traceback as _tb
    app.logger.error(f"Unhandled exception: {error}\n{_tb.format_exc()}")
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return {'ok': False, 'error': 'Something went wrong'}, 500
    return render_template('500.html'), 500


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Register / Login / Logout / Password reset (see routes/auth_routes.py)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

from routes.auth_routes import register_auth_routes

register_auth_routes(app)

from routes.webhook_routes import register_webhook_routes
from routes.misc_routes import register_misc_routes
from routes.profile_routes import register_profile_routes
from routes.social_routes import register_social_routes
from routes.wallet_routes import register_wallet_routes
from routes.enrollment_routes import register_enrollment_routes
from routes.training_routes import register_training_routes
from routes.report_routes import register_report_routes

register_webhook_routes(app)
register_misc_routes(app)
register_profile_routes(app)
register_social_routes(app)
register_wallet_routes(app)
register_enrollment_routes(app)
register_training_routes(app)
register_report_routes(app)


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Admin \u2013 Approvals
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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


@app.route('/admin/approvals/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user(user_id):
    db   = get_db()
    user = db.execute("SELECT username, status FROM users WHERE id=?", (user_id,)).fetchone()
    if user:
        if user['status'] == 'approved':
            flash('Cannot delete an approved user. Reject them first.', 'danger')
        else:
            db.execute("DELETE FROM users WHERE id=?", (user_id,))
            db.commit()
            flash(f'User "{user["username"]}" has been permanently deleted.', 'success')
    db.close()
    return redirect(url_for('admin_approvals', filter=request.form.get('current_filter', 'all')))


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Root redirect
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/')
@login_required
def index():
    if session.get('role') == 'admin':
        return redirect(url_for('admin_dashboard'))
    return redirect(url_for('team_dashboard'))


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Admin Dashboard
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/admin')
@admin_required
@safe_route
def admin_dashboard():
    db      = get_db()

    # Check seat hold expiry for all team/leader members
    seat_hold_users = db.execute(
        "SELECT username FROM users WHERE role IN ('team','leader') AND status='approved'"
    ).fetchall()
    for u in seat_hold_users:
        _check_seat_hold_expiry(db, u['username'])

    metrics = _get_metrics(db)
    today   = _today_ist().isoformat()
    _base_w = "in_pool=0 AND deleted_at=''"

    # ── 1. Live Pipeline Funnel (current leads at each stage) ────────
    _s1_ph = ','.join('?' * len(STAGE1_STATUSES))
    pipeline = db.execute(f"""
        SELECT
            SUM(CASE WHEN status IN ('New Lead','New','Contacted','Invited',
                         'Video Sent','Video Watched') THEN 1 ELSE 0 END) AS enrollment,
            SUM(CASE WHEN status IN ({_s1_ph}) THEN 1 ELSE 0 END) AS ready,
            SUM(CASE WHEN status='Day 1'       THEN 1 ELSE 0 END) AS day1,
            SUM(CASE WHEN status='Day 2'       THEN 1 ELSE 0 END) AS day2,
            SUM(CASE WHEN status IN ('Interview','Track Selected') THEN 1 ELSE 0 END) AS day3,
            SUM(CASE WHEN status='Seat Hold Confirmed' THEN 1 ELSE 0 END) AS seat_hold,
            SUM(CASE WHEN status IN ('Fully Converted','Converted') THEN 1 ELSE 0 END) AS converted
        FROM leads WHERE {_base_w}
    """, list(STAGE1_STATUSES)).fetchone()
    pipeline = dict(pipeline) if pipeline else {}
    for k in ('enrollment','ready','day1','day2','day3','seat_hold','converted'):
        pipeline[k] = pipeline.get(k) or 0

    pipeline_value = db.execute(
        f"SELECT COALESCE(SUM(track_price),0) FROM leads WHERE {_base_w} "
        "AND status IN ('Seat Hold Confirmed','Track Selected')"
    ).fetchone()[0] or 0

    # ── 2. Today's Pulse ─────────────────────────────────────────────
    approved_members = db.execute(
        "SELECT username, fbo_id FROM users WHERE role IN ('team','leader') AND status='approved' ORDER BY username"
    ).fetchall()
    today_reports = db.execute(
        "SELECT * FROM daily_reports WHERE report_date=? ORDER BY submitted_at DESC",
        (today,)
    ).fetchall()
    submitted_set   = {r['username'] for r in today_reports}
    missing_reports = [u['username'] for u in approved_members
                       if u['username'] not in submitted_set]

    _pulse_calls = sum(r['total_calling'] or 0 for r in today_reports)

    _pay_today = db.execute(
        f"SELECT COUNT(*), COALESCE(SUM(payment_amount),0) FROM leads "
        f"WHERE payment_done=1 AND date(updated_at)=? AND {_base_w}",
        (today,)
    ).fetchone()

    _d1_total = db.execute(f"SELECT COUNT(*) FROM leads WHERE {_base_w} AND status='Day 1'").fetchone()[0] or 0
    _d1_done  = db.execute(f"SELECT COUNT(*) FROM leads WHERE {_base_w} AND status='Day 1' AND d1_morning=1 AND d1_afternoon=1 AND d1_evening=1").fetchone()[0] or 0
    _d2_total = db.execute(f"SELECT COUNT(*) FROM leads WHERE {_base_w} AND status='Day 2'").fetchone()[0] or 0
    _d2_done  = db.execute(f"SELECT COUNT(*) FROM leads WHERE {_base_w} AND status='Day 2' AND d2_morning=1 AND d2_afternoon=1 AND d2_evening=1").fetchone()[0] or 0

    stale_cutoff = (_now_ist() - datetime.timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')
    stale_leads = db.execute(
        f"SELECT id, name, phone, assigned_to, status, updated_at FROM leads "
        f"WHERE {_base_w} AND assigned_to != '' "
        "AND status NOT IN ('Fully Converted','Converted','Lost','Seat Hold Confirmed') "
        "AND updated_at < ? ORDER BY updated_at ASC LIMIT 20",
        (stale_cutoff,)
    ).fetchall()

    pulse = {
        'reports_done':    len(today_reports),
        'reports_total':   len(approved_members),
        'total_calls':     _pulse_calls,
        'payments_count':  _pay_today[0] or 0,
        'payments_amount': _pay_today[1] or 0,
        'batch_d1_done':   _d1_done, 'batch_d1_total': _d1_total,
        'batch_d1_pct':    round(_d1_done / _d1_total * 100) if _d1_total else 0,
        'batch_d2_done':   _d2_done, 'batch_d2_total': _d2_total,
        'batch_d2_pct':    round(_d2_done / _d2_total * 100) if _d2_total else 0,
        'stale_count':     len(stale_leads),
    }

    # ── 3. Team Leaderboard ──────────────────────────────────────────
    _verif_rows = db.execute(f"""
        SELECT assigned_to, COUNT(*) as cnt FROM leads
        WHERE payment_done=1 AND date(updated_at)=? AND {_base_w}
        GROUP BY assigned_to
    """, (today,)).fetchall()
    report_verification = {r['assigned_to']: r['cnt'] for r in _verif_rows}

    team_board = []
    for m in approved_members:
        uname = m['username']
        score_pts, streak = _get_today_score(db, uname)
        counts = db.execute(f"""
            SELECT
                SUM(CASE WHEN status IN ({_s1_ph}) THEN 1 ELSE 0 END) AS stage1,
                SUM(CASE WHEN status='Day 1' THEN 1 ELSE 0 END) AS day1,
                SUM(CASE WHEN status='Day 2' THEN 1 ELSE 0 END) AS day2,
                SUM(CASE WHEN status IN ('Interview','Track Selected') THEN 1 ELSE 0 END) AS day3,
                SUM(CASE WHEN status='Seat Hold Confirmed' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status IN ('Fully Converted','Converted') THEN 1 ELSE 0 END) AS converted,
                COUNT(*) AS total
            FROM leads WHERE assigned_to=? AND {_base_w}
        """, (*STAGE1_STATUSES, uname)).fetchone()

        _m_d1_total = db.execute(f"SELECT COUNT(*) FROM leads WHERE assigned_to=? AND {_base_w} AND status='Day 1'", (uname,)).fetchone()[0] or 0
        _m_d1_done  = db.execute(f"SELECT COUNT(*) FROM leads WHERE assigned_to=? AND {_base_w} AND status='Day 1' AND d1_morning=1 AND d1_afternoon=1 AND d1_evening=1", (uname,)).fetchone()[0] or 0
        _m_d2_total = db.execute(f"SELECT COUNT(*) FROM leads WHERE assigned_to=? AND {_base_w} AND status='Day 2'", (uname,)).fetchone()[0] or 0
        _m_d2_done  = db.execute(f"SELECT COUNT(*) FROM leads WHERE assigned_to=? AND {_base_w} AND status='Day 2' AND d2_morning=1 AND d2_afternoon=1 AND d2_evening=1", (uname,)).fetchone()[0] or 0
        _m_batch_total = _m_d1_total + _m_d2_total
        _m_batch_done  = _m_d1_done + _m_d2_done
        batch_pct = round(_m_batch_done / _m_batch_total * 100) if _m_batch_total else -1

        team_board.append({
            'username': uname, 'fbo_id': m['fbo_id'] or '',
            'score': score_pts, 'streak': streak,
            'stage1': counts['stage1'] or 0, 'day1': counts['day1'] or 0,
            'day2': counts['day2'] or 0, 'day3': counts['day3'] or 0,
            'pending': counts['pending'] or 0, 'converted': counts['converted'] or 0,
            'total': counts['total'] or 0,
            'batch_pct': batch_pct,
            'report_done': uname in submitted_set,
        })
    team_board.sort(key=lambda x: x['score'], reverse=True)

    # ── 4. Recent Live Activity ───────────────────────────────────────
    _stage_acts = db.execute(f"""
        SELECT lsh.created_at, 'stage' AS type,
               COALESCE(l.name,'Unknown') AS lead_name, lsh.lead_id,
               lsh.stage, lsh.triggered_by AS actor
        FROM lead_stage_history lsh
        LEFT JOIN leads l ON l.id = lsh.lead_id
        WHERE lsh.lead_id IN (SELECT id FROM leads WHERE {_base_w})
        ORDER BY lsh.created_at DESC LIMIT 12
    """).fetchall()
    _new_acts = db.execute(f"""
        SELECT created_at, 'new_lead' AS type,
               COALESCE(name,'Unknown') AS lead_name, id AS lead_id,
               status AS stage, assigned_to AS actor
        FROM leads WHERE {_base_w} AND in_pool=0
        AND created_at >= datetime('now','-7 days','localtime')
        ORDER BY created_at DESC LIMIT 6
    """).fetchall()
    _pay_acts = db.execute(f"""
        SELECT updated_at AS created_at, 'payment' AS type,
               COALESCE(name,'Unknown') AS lead_name, id AS lead_id,
               status AS stage, assigned_to AS actor
        FROM leads WHERE {_base_w} AND payment_done=1
        AND updated_at >= datetime('now','-7 days','localtime')
        ORDER BY updated_at DESC LIMIT 6
    """).fetchall()
    _all_acts = [dict(r) for r in list(_stage_acts) + list(_new_acts) + list(_pay_acts)]
    _all_acts.sort(key=lambda x: x.get('created_at') or '', reverse=True)
    recent_activity = _all_acts[:12]

    recent = []  # kept for template compat but unused

    _sc = db.execute(
        f"SELECT status, COUNT(*) as c FROM leads WHERE {_base_w} GROUP BY status"
    ).fetchall()
    status_data = {s: 0 for s in STATUSES}
    for row in _sc:
        if row['status'] in status_data:
            status_data[row['status']] = row['c']

    monthly = db.execute(f"""
        SELECT strftime('%Y-%m', created_at) as month,
               SUM(payment_amount) as total
        FROM leads
        WHERE payment_done=1 AND {_base_w}
        GROUP BY month ORDER BY month DESC LIMIT 6
    """).fetchall()

    pending_users = db.execute(
        "SELECT * FROM users WHERE status='pending' ORDER BY created_at DESC"
    ).fetchall()

    wallet_pending_count = db.execute(
        "SELECT COUNT(*) FROM wallet_recharges WHERE status='pending'"
    ).fetchone()[0]

    pool_count = db.execute("SELECT COUNT(*) FROM leads WHERE in_pool=1").fetchone()[0]

    # ── 5. Daily conversion trend (7 days) ───────────────────────────
    daily_trend = db.execute(f"""
        SELECT date(updated_at) AS d,
               SUM(CASE WHEN status IN ('Converted','Fully Converted') THEN 1 ELSE 0 END) AS conversions,
               SUM(CASE WHEN payment_done=1 THEN 1 ELSE 0 END) AS payments
        FROM leads WHERE {_base_w} AND date(updated_at) >= date(?, '-6 days')
        GROUP BY d ORDER BY d
    """, (today,)).fetchall()

    db.close()
    resp = make_response(render_template('admin.html',
                           metrics=metrics,
                           pipeline=pipeline,
                           pipeline_value=pipeline_value,
                           pulse=pulse,
                           team_board=team_board,
                           stale_leads=stale_leads,
                           recent_activity=recent_activity,
                           status_data=status_data,
                           monthly=monthly,
                           daily_trend=daily_trend,
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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Team Dashboard  (scoped to logged-in user)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/dashboard')
@login_required
@safe_route
def team_dashboard():
    username = session['username']
    db       = get_db()

    # Check seat_hold expiry and auto-expire 24hr-old pipeline leads on every dashboard load
    _check_seat_hold_expiry(db, username)
    _auto_expire_pipeline_leads(db, username)

    metrics  = _get_metrics(db, username=username)
    wallet   = _get_wallet(db, username)

    recent = db.execute(
        "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 ORDER BY created_at DESC LIMIT 5",
        (username,)
    ).fetchall()

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

    today = _today_ist().isoformat()
    today_report = db.execute(
        "SELECT * FROM daily_reports WHERE username=? AND report_date=?",
        (username, today)
    ).fetchone()

    pool_count = db.execute("SELECT COUNT(*) FROM leads WHERE in_pool=1").fetchone()[0]

    _rt_ph = ','.join('?' * len(RETARGET_TAGS))
    retarget_count = db.execute(
        f"SELECT COUNT(*) FROM leads WHERE in_pool=0 AND deleted_at='' "
        f"AND assigned_to=? AND status NOT IN ('Converted','Fully Converted','Lost') "
        f"AND (call_result IN ({_rt_ph}) OR status='Retarget')",
        (username, *RETARGET_TAGS)
    ).fetchone()[0]

    zoom_link  = _get_setting(db, 'zoom_link', '')
    zoom_title = _get_setting(db, 'zoom_title', "Today's Live Session")
    zoom_time  = _get_setting(db, 'zoom_time', '2:00 PM')

    _today_stats = db.execute("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(payment_amount),0) as total
        FROM leads
        WHERE assigned_to=? AND payment_done=1 AND in_pool=0 AND deleted_at=''
          AND date(updated_at)=?
    """, (username, today)).fetchone()
    today_paid     = _today_stats['cnt'] or 0
    today_earnings = _today_stats['total'] or 0

    followups = db.execute("""
        SELECT id, name, phone, follow_up_date FROM leads
        WHERE assigned_to=? AND in_pool=0 AND deleted_at=''
          AND follow_up_date != ''
          AND follow_up_date <= ?
          AND status NOT IN ('Converted','Fully Converted','Lost')
        ORDER BY follow_up_date ASC LIMIT 10
    """, (username, today)).fetchall()

    notices = db.execute(
        "SELECT * FROM announcements ORDER BY pin DESC, created_at DESC LIMIT 5"
    ).fetchall()

    _cr_row = db.execute(
        "SELECT calling_reminder_time FROM users WHERE username=?", (username,)
    ).fetchone()
    calling_reminder_time = _cr_row['calling_reminder_time'] if _cr_row else ''

    funnel_leads = {}
    for _mk, _cond in [
        ('day1',      'day1_done=1'),
        ('day2',      'day2_done=1'),
        ('interview', 'interview_done=1'),
        ('converted', "status IN ('Converted','Fully Converted')"),
    ]:
        _rows = db.execute(
            f"SELECT name FROM leads "
            f"WHERE in_pool=0 AND deleted_at='' AND assigned_to=? AND {_cond} "
            f"ORDER BY updated_at DESC LIMIT 5",
            (username,)
        ).fetchall()
        funnel_leads[_mk] = [r['name'] for r in _rows]

    # Follow-up queue count (IST date so no timezone mismatch)
    now_date_ist = _now_ist().strftime('%Y-%m-%d')
    fu_placeholders = ','.join('?' * len(FOLLOWUP_TAGS))
    followup_count = db.execute(f"""
        SELECT COUNT(*) FROM leads
        WHERE in_pool=0 AND deleted_at=''
          AND assigned_to=?
          AND status NOT IN ('Converted','Fully Converted','Lost')
          AND (
            (follow_up_date != '' AND DATE(follow_up_date) <= ?)
            OR call_result IN ({fu_placeholders})
          )
    """, [username, now_date_ist] + list(FOLLOWUP_TAGS)).fetchone()[0]

    # Pipeline full lead objects
    _s1_ph = ','.join('?' * len(STAGE1_STATUSES))
    stage1_leads = db.execute(
        f"SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' AND status IN ({_s1_ph}) ORDER BY updated_at ASC",
        (username, *STAGE1_STATUSES)
    ).fetchall()
    day1_leads_db = db.execute(
        "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' AND status='Day 1' ORDER BY updated_at ASC",
        (username,)
    ).fetchall()
    day2_leads_db = db.execute(
        "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' AND status='Day 2' ORDER BY updated_at ASC",
        (username,)
    ).fetchall()
    day3_leads = db.execute(
        "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' AND status IN ('Interview','Track Selected') ORDER BY updated_at ASC",
        (username,)
    ).fetchall()
    pending_leads = db.execute(
        "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' AND status='Seat Hold Confirmed' ORDER BY updated_at ASC",
        (username,)
    ).fetchall()
    score_row = db.execute(
        "SELECT * FROM daily_scores WHERE username=? AND score_date=?",
        (username, today)
    ).fetchone()
    today_score  = score_row['total_points'] if score_row else 0
    today_streak = score_row['streak_days']  if score_row else 0
    pending_batches = (
        sum(1 for l in day1_leads_db if not (l['d1_morning'] and l['d1_afternoon'] and l['d1_evening'])) +
        sum(1 for l in day2_leads_db if not (l['d2_morning'] and l['d2_afternoon'] and l['d2_evening']))
    )
    # Counts derived from lists
    stage1_count         = len(stage1_leads)
    day1_count           = len(day1_leads_db)
    day2_count           = len(day2_leads_db)
    day3_count           = len(day3_leads)
    pending_count_pipeline = len(pending_leads)

    # Monthly goals + actuals
    current_month = _now_ist().strftime('%Y-%m')
    target_rows = db.execute(
        "SELECT metric, target_value FROM targets WHERE username=? AND month=?",
        (username, current_month)
    ).fetchall()
    m = _get_metrics(db, username)
    metric_actuals = {
        'leads':       m.get('total', 0),
        'payments':    m.get('paid', 0),
        'conversions': m.get('converted', 0),
        'revenue':     m.get('revenue', 0),
    }
    metric_labels = {'leads': 'Leads Added', 'payments': '₹196 Payments',
                     'conversions': 'Conversions', 'revenue': 'Revenue ₹'}
    targets_data = []
    for tr in target_rows:
        key    = tr['metric']
        target = tr['target_value']
        actual = metric_actuals.get(key, 0)
        pct    = round(actual / target * 100, 1) if target else 0
        targets_data.append({'label': metric_labels.get(key, key),
                              'actual': actual, 'target': int(target), 'pct': pct})

    # Build batch_videos BEFORE closing database
    batch_videos = {
        'd1_morning_v1':   _get_setting(db, 'batch_d1_morning_v1', ''),
        'd1_morning_v2':   _get_setting(db, 'batch_d1_morning_v2', ''),
        'd1_afternoon_v1': _get_setting(db, 'batch_d1_afternoon_v1', ''),
        'd1_afternoon_v2': _get_setting(db, 'batch_d1_afternoon_v2', ''),
        'd1_evening_v1':   _get_setting(db, 'batch_d1_evening_v1', ''),
        'd1_evening_v2':   _get_setting(db, 'batch_d1_evening_v2', ''),
        'd2_morning_v1':   _get_setting(db, 'batch_d2_morning_v1', ''),
        'd2_morning_v2':   _get_setting(db, 'batch_d2_morning_v2', ''),
        'd2_afternoon_v1': _get_setting(db, 'batch_d2_afternoon_v1', ''),
        'd2_afternoon_v2': _get_setting(db, 'batch_d2_afternoon_v2', ''),
        'd2_evening_v1':   _get_setting(db, 'batch_d2_evening_v1', ''),
        'd2_evening_v2':   _get_setting(db, 'batch_d2_evening_v2', ''),
    }
    enrollment_video_url   = _get_setting(db, 'enrollment_video_url', '')
    enrollment_video_title  = _get_setting(db, 'enrollment_video_title', 'Enrollment Video')

    # Enrich leads with heat + next_action
    stage1_leads_e  = _enrich_leads(stage1_leads)
    day1_leads_e    = _enrich_leads(day1_leads_db)
    day2_leads_e    = _enrich_leads(day2_leads_db)
    day3_leads_e    = _enrich_leads(day3_leads)
    pending_leads_e = _enrich_leads(pending_leads)
    recent_e        = _enrich_leads(recent)

    # Leader-specific: team snapshot data (downline pipeline + report compliance)
    show_day1_batches = session.get('role') in ('leader', 'admin')
    team_snapshot = []
    leader_report_stats = {}
    downline_missing_reports = []
    if session.get('role') == 'leader':
        # Get all direct + recursive downline usernames (excluding self)
        try:
            downline_usernames = _get_network_usernames(db, username)
        except Exception:
            downline_usernames = []
        downline_usernames = [u for u in downline_usernames if u != username]

        stage_ph = ','.join('?' * len(STAGE1_STATUSES))
        for member in downline_usernames:
            counts = db.execute(f"""
                SELECT
                    SUM(CASE WHEN status IN ({stage_ph}) THEN 1 ELSE 0 END) as stage1,
                    SUM(CASE WHEN status='Day 1' THEN 1 ELSE 0 END) as day1,
                    SUM(CASE WHEN status='Day 2' THEN 1 ELSE 0 END) as day2,
                    SUM(CASE WHEN status IN ('Interview','Track Selected')
                        THEN 1 ELSE 0 END) as day3,
                    SUM(CASE WHEN status='Seat Hold Confirmed' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status IN ('Fully Converted','Converted')
                        THEN 1 ELSE 0 END) as converted
                FROM leads
                WHERE assigned_to=? AND in_pool=0 AND deleted_at=''
            """, (*STAGE1_STATUSES, member)).fetchone()

            score_row = db.execute(
                "SELECT total_points, streak_days FROM daily_scores WHERE username=? AND score_date=?",
                (member, today)
            ).fetchone()
            today_pts = score_row['total_points'] if score_row else 0

            report_row = db.execute(
                "SELECT id FROM daily_reports WHERE username=? AND report_date=?",
                (member, today)
            ).fetchone()
            report_done = bool(report_row)

            team_snapshot.append({
                'username':    member,
                'stage1':      counts['stage1'] or 0,
                'day1':        counts['day1']   or 0,
                'day2':        counts['day2']   or 0,
                'day3':        counts['day3']   or 0,
                'pending':     counts['pending'] or 0,
                'converted':   counts['converted'] or 0,
                'score':       today_pts,
                'report_done': report_done,
            })
            if not report_done:
                downline_missing_reports.append(member)

        leader_report_stats = {
            'total':     len(downline_usernames),
            'submitted': len([m for m in team_snapshot if m['report_done']]),
            'missing':   downline_missing_reports,
        }

    db.close()
    resp = make_response(render_template('dashboard.html',
                           metrics=metrics,
                           wallet=wallet,
                           recent=recent_e,
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
                           followup_count=followup_count,
                           targets_data=targets_data,
                           zoom_link=zoom_link,
                           zoom_title=zoom_title,
                           zoom_time=zoom_time,
                           funnel_leads=funnel_leads,
                           stage1_count=stage1_count,
                           day1_count=day1_count,
                           day2_count=day2_count,
                           day3_count=day3_count,
                           pending_count_pipeline=pending_count_pipeline,
                           stage1_leads=stage1_leads_e,
                           day1_leads=day1_leads_e,
                           day2_leads=day2_leads_e,
                           day3_leads=day3_leads_e,
                           pending_leads=pending_leads_e,
                           today_score=today_score,
                           today_streak=today_streak,
                           pending_batches=pending_batches,
                           batch_videos=batch_videos,
                           batch_watch_urls=_batch_watch_urls(),
                           enrollment_video_url=enrollment_video_url,
                           enrollment_watch_url=url_for('watch_enrollment', _external=True) if enrollment_video_url else '',
                           enrollment_video_title=enrollment_video_title,
                           show_day1_batches=show_day1_batches,
                           user_role=session.get('role', 'team'),
                           team_snapshot=team_snapshot,
                           leader_report_stats=leader_report_stats,
                           downline_missing_reports=downline_missing_reports,
                           call_status_values=CALL_STATUS_VALUES,
                           csrf_token=session.get('_csrf_token', '')))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Leads \u2013 List
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/leads')
@login_required
@safe_route
def leads():
    import traceback as _tb
    try:
        return _leads_inner()
    except Exception as e:
        app.logger.error(f"leads() CRASH: {e}\n{_tb.format_exc()}")
        flash(f'Leads page error: {e}', 'danger')
        return redirect(url_for('dashboard'))

def _leads_inner():
    from datetime import datetime as _dt, timedelta as _td
    db     = get_db()
    status = request.args.get('status', '')
    search = request.args.get('q', '').strip()
    page   = max(1, int(request.args.get('page', 1)))
    today      = _dt.now().strftime('%Y-%m-%d')
    today_lo   = today + ' 00:00:00'
    tomorrow_lo = (_dt.now() + _td(days=1)).strftime('%Y-%m-%d') + ' 00:00:00'

    base   = "SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' AND status NOT IN ('Lost','Retarget')"
    role   = session.get('role')
    uname  = session.get('username')

    today_cond = ("(created_at >= ? AND created_at < ?"
                  " OR (claimed_at != '' AND claimed_at >= ? AND claimed_at < ?))")

    def _apply_filters(base_q, base_p, extra_cond, extra_p):
        q = base_q + f" AND {extra_cond}"
        p = list(base_p) + list(extra_p)
        if status:
            q += " AND status=?"; p.append(status)
        if search:
            if role == 'admin':
                q += " AND (name LIKE ? OR phone LIKE ? OR email LIKE ? OR assigned_to LIKE ?)"
                p += [f'%{search}%', f'%{search}%', f'%{search}%', f'%{search}%']
            else:
                q += " AND (name LIKE ? OR phone LIKE ? OR email LIKE ?)"
                p += [f'%{search}%', f'%{search}%', f'%{search}%']
        return q, p

    base_params = []
    if role != 'admin':
        base_params = [uname]
        base += " AND assigned_to=?"

    today_q, today_p = _apply_filters(
        base, base_params,
        today_cond, [today_lo, tomorrow_lo, today_lo, tomorrow_lo]
    )
    hist_q, hist_p = _apply_filters(
        base, base_params,
        f"NOT {today_cond}", [today_lo, tomorrow_lo, today_lo, tomorrow_lo]
    )

    _today_limit = 60
    _hist_limit  = 80
    _hist_offset = (page - 1) * _hist_limit
    try:
        today_leads_raw = db.execute(
            today_q + " ORDER BY created_at DESC LIMIT " + str(_today_limit), today_p
        ).fetchall()
        hist_leads_raw  = db.execute(
            hist_q  + " ORDER BY created_at DESC LIMIT ? OFFSET ?",
            hist_p + [_hist_limit + 1, _hist_offset]
        ).fetchall()
    except Exception as e:
        app.logger.error(f"leads() query failed: {e}")
        today_leads_raw, hist_leads_raw = [], []
    has_more_hist = len(hist_leads_raw) > _hist_limit
    if has_more_hist:
        hist_leads_raw = hist_leads_raw[:_hist_limit]
    # Use actual usernames from users table so assigned_to matches session['username']
    team            = db.execute(
        "SELECT username AS name FROM users "
        "WHERE role IN ('team','leader') AND status='approved' ORDER BY username"
    ).fetchall()
    db.close()

    # Enrich with heat + next_action
    try:
        today_leads = _enrich_leads(today_leads_raw)
        hist_leads  = _enrich_leads(hist_leads_raw)
    except Exception as e:
        app.logger.error(f"leads() enrichment failed: {e}")
        today_leads = [dict(l) for l in today_leads_raw]
        hist_leads  = [dict(l) for l in hist_leads_raw]

    # Split today_leads by tab
    day1_leads   = [l for l in today_leads if l.get('status') == 'Day 1']
    day2_leads   = [l for l in today_leads if l.get('status') == 'Day 2']
    day3_leads   = [l for l in today_leads if l.get('status') == 'Interview']
    active_leads = [l for l in today_leads
                    if l.get('status') not in ('Day 1', 'Day 2', 'Interview')]

    # Split hist_leads by status so after status change lead appears in current stage
    hist_active_leads = [l for l in hist_leads if l.get('status') not in ('Day 1', 'Day 2', 'Interview')]
    hist_day1_leads   = [l for l in hist_leads if l.get('status') == 'Day 1']
    hist_day2_leads   = [l for l in hist_leads if l.get('status') == 'Day 2']
    hist_day3_leads   = [l for l in hist_leads if l.get('status') == 'Interview']

    return render_template('leads.html',
                           leads=hist_leads,
                           today_leads=today_leads,
                           hist_leads=hist_leads,
                           day1_leads=day1_leads,
                           day2_leads=day2_leads,
                           day3_leads=day3_leads,
                           active_leads=active_leads,
                           hist_active_leads=hist_active_leads,
                           hist_day1_leads=hist_day1_leads,
                           hist_day2_leads=hist_day2_leads,
                           hist_day3_leads=hist_day3_leads,
                           statuses=STATUSES,
                           call_result_tags=CALL_RESULT_TAGS,
                           call_status_values=CALL_STATUS_VALUES,
                           user_role=session.get('role', 'team'),
                           sources=SOURCES,
                           selected_status=status,
                           search=search,
                           team=team,
                           page=page,
                           has_more_hist=has_more_hist)


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Leads \u2013 Add
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500



# ─────────────────────────────────────────────────────────────────
#  Leads – Set Batch (AJAX)
# ─────────────────────────────────────────────────────────────────

@app.route('/leads/<int:lid>/set-batch', methods=['POST'])
@login_required
def set_lead_batch(lid):
    data  = request.get_json(silent=True) or {}
    day   = str(data.get('day', ''))
    batch = str(data.get('batch', ''))
    if day not in ('1', '2', '3'):
        return {'ok': False, 'error': 'Invalid day'}, 400
    col = f'day{day}_batch'
    db  = get_db()
    row = db.execute("SELECT assigned_to FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''",
                     [lid]).fetchone()
    if not row:
        db.close(); return {'ok': False, 'error': 'Not found'}, 404
    if session.get('role') != 'admin' and row['assigned_to'] != session.get('username'):
        db.close(); return {'ok': False, 'error': 'Forbidden'}, 403
    db.execute(f"UPDATE leads SET {col}=?, updated_at=datetime('now','localtime') WHERE id=?",
               [batch, lid])
    db.commit()
    db.close()
    return {'ok': True}

@app.route('/leads/add', methods=['GET', 'POST'])
@login_required
@safe_route
def add_lead():
    db   = get_db()
    team = db.execute(
        "SELECT username AS name FROM users "
        "WHERE role IN ('team','leader') AND status='approved' ORDER BY username"
    ).fetchall()

    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'

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
            if is_ajax:
                db.close()
                return {'ok': False, 'error': 'Name and Phone are required.'}, 400
            flash('Name and Phone are required.', 'danger')
            db.close()
            return render_template('add_lead.html',
                                   statuses=STATUSES, sources=SOURCES, team=team,
                                   call_result_tags=CALL_RESULT_TAGS)

        dup = db.execute(
            "SELECT name FROM leads WHERE phone=? AND in_pool=0 AND deleted_at=''", (phone,)
        ).fetchone()
        if dup:
            msg = f'A lead with phone {phone} already exists ({dup["name"]}).'
            if is_ajax:
                db.close()
                return {'ok': False, 'error': msg}, 409
            flash(msg + ' Duplicate entries are not allowed.', 'danger')
            db.close()
            return render_template('add_lead.html',
                                   statuses=STATUSES, sources=SOURCES, team=team,
                                   call_result_tags=CALL_RESULT_TAGS)

        if status not in STATUSES:
            status = 'New'

        pipeline_stage = STATUS_TO_STAGE.get(status, 'enrollment')
        db.execute("""
            INSERT INTO leads
                (name, phone, email, referred_by, assigned_to, source,
                 status, payment_done, payment_amount, revenue,
                 follow_up_date, call_result, notes, city, in_pool, pool_price, claimed_at,
                 pipeline_stage, current_owner)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', ?, ?)
        """, (name, phone, email, referred_by, assigned_to, source,
              status, payment_done, payment_amount, revenue,
              follow_up_date, call_result, notes, city,
              pipeline_stage, assigned_to))
        new_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.commit()
        db.close()

        if is_ajax:
            return {'ok': True, 'id': new_id, 'name': name, 'phone': phone,
                    'city': city, 'status': status, 'source': source}

        flash(f'Lead "{name}" added successfully.', 'success')
        return redirect(url_for('leads'))

    db.close()
    return render_template('add_lead.html',
                           statuses=STATUSES, sources=SOURCES, team=team,
                           call_result_tags=CALL_RESULT_TAGS)


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Leads \u2013 Edit / Update
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/leads/<int:lead_id>/edit', methods=['GET', 'POST'])
@login_required
@safe_route
def edit_lead(lead_id):
    db   = get_db()
    team = db.execute(
        "SELECT username AS name FROM users "
        "WHERE role IN ('team','leader') AND status='approved' ORDER BY username"
    ).fetchall()

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

        track_selected_val   = request.form.get('track_selected', lead['track_selected'] or '').strip()
        track_price_val      = float(request.form.get('track_price', lead['track_price'] or 0) or 0)
        seat_hold_amount_val = float(request.form.get('seat_hold_amount', lead['seat_hold_amount'] or 0) or 0)
        seat_hold_received   = bool(request.form.get('seat_hold_received'))
        final_payment_received = bool(request.form.get('final_payment_received'))

        # Auto-fill from track defaults if track just selected
        if track_selected_val and track_selected_val in TRACKS:
            if not track_price_val:
                track_price_val = TRACKS[track_selected_val]['price']
            if not seat_hold_amount_val:
                seat_hold_amount_val = TRACKS[track_selected_val]['seat_hold']
            if status not in ('Seat Hold Confirmed', 'Fully Converted'):
                status = 'Track Selected'

        pending_amount_val = max(0.0, track_price_val - seat_hold_amount_val)

        # ── Checkbox-driven status (checkboxes override dropdown) ────────
        if final_payment_received and not seat_hold_received:
            # ❌ Cannot be Fully Converted without Seat Hold
            flash('Seat Hold pehle confirm hona chahiye — Fully Converted tab ho sakta hai jab Seat Hold Received bhi checked ho.', 'danger')
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
        elif final_payment_received:
            # Both seat_hold + final_payment checked
            status = 'Fully Converted'
            pending_amount_val = 0.0
        elif seat_hold_received:
            status = 'Seat Hold Confirmed'
        else:
            # Both unchecked — if status was set by checkboxes, revert it
            if status in ('Seat Hold Confirmed', 'Fully Converted'):
                status = 'Track Selected' if track_selected_val else 'New'
            # Clear seat hold amount & recalculate pending
            seat_hold_amount_val = 0.0
            pending_amount_val   = track_price_val

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

        # ₹196 paid → auto-advance to Day 1
        if status == 'Paid ₹196':
            status = 'Day 1'

        if session.get('role') == 'admin':
            # Guard against lead['assigned_to'] being None (unassigned leads)
            assigned_to = (request.form.get('assigned_to') or lead['assigned_to'] or '').strip()
        else:
            assigned_to = lead['assigned_to'] or ''

        # Sync pipeline_stage from status (one status -> one pipeline_stage)
        new_pipeline_stage = STATUS_TO_STAGE.get(status, 'enrollment')
        lead_pipeline_stage = lead['pipeline_stage'] if 'pipeline_stage' in lead.keys() else 'enrollment'
        stage_changed = new_pipeline_stage != lead_pipeline_stage
        _updated_at = _now_ist().strftime('%Y-%m-%d %H:%M:%S')

        # Reset pipeline_entered_at on every status change for auto-expirable statuses
        _entering_pipeline = status in PIPELINE_AUTO_EXPIRE_STATUSES
        _pipeline_entered_at_val = _updated_at if _entering_pipeline else ''

        # Single UPDATE: always set status and pipeline_stage together
        db.execute("""
            UPDATE leads
            SET name=?, phone=?, email=?, referred_by=?, assigned_to=?, status=?,
                payment_done=?, payment_amount=?,
                day1_done=?, day2_done=?, interview_done=?,
                follow_up_date=?, call_result=?, notes=?, city=?,
                track_selected=?, track_price=?, seat_hold_amount=?, pending_amount=?,
                pipeline_stage=?,
                updated_at=?,
                pipeline_entered_at=?
            WHERE id=?
        """, (name, phone, email, referred_by, assigned_to, status,
              payment_done, payment_amount,
              day1_done, day2_done, interview_done,
              follow_up_date, call_result, notes, city,
              track_selected_val, track_price_val, seat_hold_amount_val, pending_amount_val,
              new_pipeline_stage,
              _updated_at,
              _pipeline_entered_at_val,
              lead_id))
        db.commit()

        # If stage changed, run _transition_stage for current_owner + history (status already set above)
        if stage_changed:
            try:
                _transition_stage(db, lead_id, new_pipeline_stage, session['username'], status_override=status)
                db.commit()
            except Exception:
                pass

        try:
            _log_activity(db, session['username'], 'lead_update',
                          f"Lead #{lead_id} updated to status: {status} stage: {new_pipeline_stage}")
        except Exception:
            pass  # Non-fatal: commit already succeeded
        finally:
            db.close()
        flash(f'Lead "{name}" updated.', 'success')
        return redirect(url_for('leads'))

    lead_notes_rows = db.execute(
        "SELECT * FROM lead_notes WHERE lead_id=? ORDER BY created_at ASC",
        (lead_id,)
    ).fetchall()
    timeline = db.execute(
        "SELECT * FROM lead_notes WHERE lead_id=? ORDER BY created_at DESC LIMIT 50",
        (lead_id,)
    ).fetchall()
    db.close()
    return render_template('edit_lead.html',
                           lead=lead,
                           statuses=STATUSES,
                           team=team,
                           payment_amount=PAYMENT_AMOUNT,
                           lead_notes=lead_notes_rows,
                           timeline=timeline,
                           call_result_tags=CALL_RESULT_TAGS)


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Retarget List
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/follow-up')
@login_required
def follow_up_queue():
    db   = get_db()
    role = session.get('role')
    now_date = _now_ist().strftime('%Y-%m-%d')   # IST so user/server timezone match
    now_time = _now_ist().strftime('%H:%M')
    fu_placeholders = ','.join('?' * len(FOLLOWUP_TAGS))
    query = f"""
        SELECT * FROM leads
        WHERE in_pool=0 AND deleted_at=''
          AND status NOT IN ('Converted','Fully Converted','Lost')
          AND (
            (follow_up_date != '' AND DATE(follow_up_date) <= ?)
            OR call_result IN ({fu_placeholders})
          )
    """
    params = [now_date] + list(FOLLOWUP_TAGS)
    if role != 'admin':
        query += " AND assigned_to=?"
        params.append(session['username'])
    query += """
        ORDER BY
          CASE WHEN follow_up_date != '' AND DATE(follow_up_date) = ? THEN 0 ELSE 1 END,
          follow_up_date ASC,
          last_contacted ASC
    """
    params.append(now_date)
    leads_list = db.execute(query, params).fetchall()
    today_count    = sum(1 for l in leads_list
                         if l['follow_up_date'] and l['follow_up_date'][:10] == now_date)
    overdue_count  = sum(1 for l in leads_list
                         if l['follow_up_date'] and l['follow_up_date'][:10] < now_date)

    db.close()
    return render_template('follow_up.html',
                           leads=leads_list,
                           today_count=today_count,
                           overdue_count=overdue_count,
                           now_date=now_date,
                           now_time=now_time,
                           statuses=STATUSES,
                           call_result_tags=CALL_RESULT_TAGS)


@app.route('/leads/<int:lead_id>/mark-called', methods=['POST'])
@login_required
def mark_called(lead_id):
    db   = get_db()
    lead = db.execute("SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)).fetchone()
    if not lead:
        db.close()
        return {'ok': False, 'error': 'not found'}, 404
    if session.get('role') != 'admin' and lead['assigned_to'] != session['username']:
        db.close()
        return {'ok': False, 'error': 'forbidden'}, 403

    now = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    db.execute("""
        UPDATE leads SET last_contacted=?, contact_count=contact_count+1, updated_at=?
        WHERE id=?
    """, (now, now, lead_id))
    _log_lead_event(db, lead_id, session['username'], 'Called / contacted')
    # Auto-advance status to "Contacted" if currently below
    _STATUS_ORDER_MC = [
        'New Lead', 'New', 'Contacted', 'Invited',
        'Video Sent', 'Video Watched', 'Paid ₹196', 'Mindset Lock'
    ]
    lead_status = lead['status'] or 'New'
    if lead_status in ('New Lead', 'New'):
        db.execute(
            "UPDATE leads SET status='Contacted', updated_at=? WHERE id=?",
            (now, lead_id)
        )
    db.commit()
    db.close()
    return {'ok': True}


@app.route('/leads/<int:lead_id>/follow-up-time', methods=['POST'])
@login_required
def set_follow_up_time(lead_id):
    """Set follow-up reminder time (HH:MM). Accepts key 'time' or 'reminder_time'. Persists and shows in Follow-up Queue."""
    data = request.get_json(silent=True) or {}
    reminder_time = (data.get('reminder_time') or data.get('time') or '').strip()

    # Validate: allow empty to clear; if non-empty, expect HH:MM (optional strict check)
    if reminder_time:
        import re
        if not re.match(r'^([01]?\d|2[0-3]):[0-5]\d$', reminder_time):
            return {'ok': False, 'error': 'Use HH:MM format (e.g. 09:30)'}, 400

    db = get_db()
    lead = db.execute("SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)).fetchone()
    if not lead:
        db.close()
        return {'ok': False, 'error': 'Not found'}, 404

    role = session.get('role', 'team')
    username = session['username']
    if role == 'admin':
        pass
    elif role == 'leader':
        downline = _get_network_usernames(db, username)
        if lead['assigned_to'] != username and lead['assigned_to'] not in downline:
            db.close()
            return {'ok': False, 'error': 'You can only set reminder for your own or downline leads'}, 403
    else:
        if lead['assigned_to'] != username:
            db.close()
            return {'ok': False, 'error': 'Forbidden'}, 403

    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    # If setting a time and lead has no follow_up_date, set to today so it appears in Follow-up Queue
    lead_keys = lead.keys()
    follow_up_date = (lead['follow_up_date'] if 'follow_up_date' in lead_keys else '') or ''
    if reminder_time and not (follow_up_date and follow_up_date.strip()):
        follow_up_date = _now_ist().strftime('%Y-%m-%d')
        db.execute(
            "UPDATE leads SET follow_up_time=?, follow_up_date=?, updated_at=? WHERE id=?",
            (reminder_time, follow_up_date, now_str, lead_id)
        )
    else:
        db.execute(
            "UPDATE leads SET follow_up_time=?, updated_at=? WHERE id=?",
            (reminder_time, now_str, lead_id)
        )
    db.commit()
    db.close()
    return {'ok': True}


@app.route('/retarget')
@login_required
def retarget():
    db = get_db()
    rt_placeholders = ','.join('?' * len(RETARGET_TAGS))
    query  = f"""SELECT * FROM leads
                WHERE in_pool=0 AND deleted_at=''
                AND status NOT IN ('Converted','Fully Converted','Lost')
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


# ─────────────────────────────────────────────────────────────
#  Old Leads – Lost leads archive (can be restored / retargeted)
# ─────────────────────────────────────────────────────────────

@app.route('/old-leads')
@login_required
@safe_route
def old_leads():
    db     = get_db()
    search = request.args.get('q', '').strip()
    role   = session.get('role')

    base   = "SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' AND status IN ('Lost','Pending')"
    params = []

    if role == 'admin':
        pass  # admin sees all lost leads
    elif role == 'leader':
        downline = _get_downline_usernames(db, session['username'])
        placeholders = ','.join('?' * len(downline))
        base   += f" AND assigned_to IN ({placeholders})"
        params += downline
    else:
        base  += " AND assigned_to=?"
        params.append(session['username'])

    if search:
        if role in ('admin', 'leader'):
            base  += " AND (name LIKE ? OR phone LIKE ? OR email LIKE ? OR assigned_to LIKE ?)"
            params += [f'%{search}%', f'%{search}%', f'%{search}%', f'%{search}%']
        else:
            base  += " AND (name LIKE ? OR phone LIKE ? OR email LIKE ?)"
            params += [f'%{search}%', f'%{search}%', f'%{search}%']

    base += " ORDER BY updated_at DESC"
    leads_list = db.execute(base, params).fetchall()
    db.close()
    return render_template('old_leads.html', leads=leads_list, search=search, role=role)


@app.route('/leads/<int:lead_id>/restore-from-lost', methods=['POST'])
@login_required
@safe_route
def restore_from_lost(lead_id):
    """Move a Lost lead back to Retarget so it can be worked again."""
    db   = get_db()
    lead = db.execute(
        "SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)
    ).fetchone()

    if not lead:
        db.close()
        flash('Lead not found.', 'danger')
        return redirect(url_for('old_leads'))

    role = session.get('role')
    if role not in ('admin', 'leader') and lead['assigned_to'] != session['username']:
        db.close()
        flash('Access denied.', 'danger')
        return redirect(url_for('old_leads'))

    if lead['status'] not in ('Lost', 'Pending'):
        db.close()
        flash('Only Lost or Pending leads can be restored.', 'warning')
        return redirect(url_for('old_leads'))

    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    db.execute(
        """UPDATE leads
              SET status='Retarget',
                  pipeline_stage='enrollment',
                  pipeline_entered_at=?,
                  updated_at=?
            WHERE id=?""",
        (now_str, now_str, lead_id)
    )
    db.commit()
    db.close()

    flash(f'✅ "{lead["name"]}" restored to Retarget list.', 'success')
    return redirect(url_for('old_leads'))


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Leads \u2013 Quick status toggle
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/leads/<int:lead_id>/status', methods=['POST'])
@login_required
def update_status(lead_id):
    new_status = request.form.get('status')
    is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'

    if new_status not in STATUSES:
        if is_ajax:
            return {'ok': False, 'error': 'Invalid status'}, 400
        flash('Invalid status.', 'danger')
        return redirect(url_for('leads'))

    db = get_db()
    lead = db.execute("SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)).fetchone()
    if not lead:
        db.close()
        if is_ajax:
            return {'ok': False, 'error': 'Not found'}, 404
        flash('Lead not found.', 'danger')
        return redirect(url_for('leads'))

    # Allow assigned_to OR current_owner (stage may have changed ownership)
    if session.get('role') != 'admin':
        lead_keys = lead.keys()
        allowed = {lead['assigned_to']}
        if 'current_owner' in lead_keys and lead['current_owner']:
            allowed.add(lead['current_owner'])
        if session['username'] not in allowed:
            db.close()
            if is_ajax:
                return {'ok': False, 'error': 'Access denied'}, 403
            flash('Access denied.', 'danger')
            return redirect(url_for('leads'))

    # ₹196 paid → auto-advance to Day 1 and mark payment_done
    if new_status == 'Paid ₹196':
        new_status = 'Day 1'

    new_pipeline_stage = STATUS_TO_STAGE.get(new_status, 'enrollment')
    lead_pipeline_stage = lead['pipeline_stage'] if 'pipeline_stage' in lead.keys() else 'enrollment'
    stage_changed = new_pipeline_stage != lead_pipeline_stage
    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')

    if stage_changed:
        # Single update: _transition_stage sets pipeline_stage, current_owner, status (status_override)
        _transition_stage(db, lead_id, new_pipeline_stage, session['username'], status_override=new_status)
        if new_status == 'Day 1':
            db.execute(
                "UPDATE leads SET payment_done=1, payment_amount=?, updated_at=? WHERE id=? AND in_pool=0",
                (PAYMENT_AMOUNT, now_str, lead_id)
            )
        elif new_status in ('Seat Hold Confirmed', 'Fully Converted'):
            db.execute(
                "UPDATE leads SET payment_done=1, updated_at=? WHERE id=? AND in_pool=0",
                (now_str, lead_id)
            )
    else:
        _entering_pipeline = new_status in PIPELINE_AUTO_EXPIRE_STATUSES
        _pipe_entered = now_str if _entering_pipeline else ''
        db.execute(
            "UPDATE leads SET status=?, pipeline_stage=?, updated_at=?, pipeline_entered_at=? WHERE id=? AND in_pool=0",
            (new_status, new_pipeline_stage, now_str, _pipe_entered, lead_id)
        )

    _log_lead_event(db, lead_id, session['username'], f'Status to {new_status}')
    _log_activity(db, session['username'], 'lead_status_change',
                  f'{lead["name"]} to {new_status}')
    new_badges = _check_and_award_badges(db, lead['assigned_to'])
    db.commit()
    db.close()

    if is_ajax:
        return {'ok': True, 'status': new_status,
                'stage_changed': stage_changed,
                'new_stage': new_pipeline_stage if stage_changed else None,
                'new_badges': [BADGE_DEFS[k]['label'] for k in new_badges if k in BADGE_DEFS]}

    flash('Status updated.', 'success')
    return redirect(request.referrer or url_for('leads'))


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Leads \u2013 Quick call-result update (AJAX)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
        "UPDATE leads SET call_result=?, updated_at=? WHERE id=? AND in_pool=0",
        (tag, _now_ist().strftime('%Y-%m-%d %H:%M:%S'), lead_id)
    )
    db.commit()
    db.close()
    return {'ok': True, 'call_result': tag}


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Leads \u2013 Delete
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
            "UPDATE leads SET deleted_at=? WHERE id=?", (_now_ist().strftime('%Y-%m-%d %H:%M:%S'), lead_id)
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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Team  (Admin only)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/team')
@admin_required
def team():
    db      = get_db()
    members = db.execute("SELECT * FROM team_members ORDER BY name").fetchall()

    _rows = db.execute("""
        SELECT
            referred_by,
            COUNT(*) as total,
            SUM(CASE WHEN status IN ('Converted','Fully Converted') THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN payment_done=1              THEN 1 ELSE 0 END) as paid,
            SUM(payment_amount)                                           as revenue,
            SUM(CASE WHEN day1_done=1                 THEN 1 ELSE 0 END) as day1,
            SUM(CASE WHEN day2_done=1                 THEN 1 ELSE 0 END) as day2,
            SUM(CASE WHEN interview_done=1            THEN 1 ELSE 0 END) as interviews,
            SUM(CASE WHEN status='Seat Hold Confirmed' THEN 1 ELSE 0 END) as seat_holds,
            SUM(CASE WHEN status='Fully Converted'    THEN 1 ELSE 0 END) as fully_conv
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


# Report routes extracted to routes/report_routes.py

# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Admin \u2013 Settings
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/admin/settings', methods=['GET', 'POST'])
@admin_required
def admin_settings():
    db = get_db()
    if request.method == 'POST':
        upi_id           = request.form.get('upi_id', '').strip()
        lead_price       = request.form.get('lead_price', '50').strip()
        webhook_token    = request.form.get('webhook_token', '').strip()
        meta_page_token  = request.form.get('meta_page_token', '').strip()
        smtp_host        = request.form.get('smtp_host', '').strip()
        smtp_port        = request.form.get('smtp_port', '587').strip()
        smtp_user        = request.form.get('smtp_user', '').strip()
        smtp_from_name   = request.form.get('smtp_from_name', 'Myle Community').strip()
        smtp_password    = request.form.get('smtp_password', '').strip()
        anthropic_key    = request.form.get('anthropic_api_key', '').strip()

        _qr_cache.clear()
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
        if anthropic_key:
            _set_setting(db, 'anthropic_api_key', anthropic_key)

        # ── Batch Video Links (12 new settings) ────────────────────
        batch_video_keys = [
            'batch_d1_morning_v1', 'batch_d1_morning_v2',
            'batch_d1_afternoon_v1', 'batch_d1_afternoon_v2',
            'batch_d1_evening_v1', 'batch_d1_evening_v2',
            'batch_d2_morning_v1', 'batch_d2_morning_v2',
            'batch_d2_afternoon_v1', 'batch_d2_afternoon_v2',
            'batch_d2_evening_v1', 'batch_d2_evening_v2',
        ]
        for key in batch_video_keys:
            val = request.form.get(key, '').strip()
            _set_setting(db, key, val)

        # Enrollment video (Stage 1 — visible to team + leader)
        _set_setting(db, 'enrollment_video_url', request.form.get('enrollment_video_url', '').strip())
        _set_setting(db, 'enrollment_video_title', request.form.get('enrollment_video_title', '').strip())

        # App tutorial link (sent to fully converted leads by leader)
        _set_setting(db, 'app_tutorial_link', request.form.get('app_tutorial_link', '').strip())

        db.commit()
        db.close()
        flash('Settings saved successfully.', 'success')
        return redirect(url_for('admin_settings'))

    settings = {
        'upi_id':               _get_setting(db, 'upi_id'),
        'default_lead_price':   _get_setting(db, 'default_lead_price', '50'),
        'meta_webhook_token':   _get_setting(db, 'meta_webhook_token'),
        'meta_page_token':      _get_setting(db, 'meta_page_token'),
        'smtp_host':            _get_setting(db, 'smtp_host', 'smtp.gmail.com'),
        'smtp_port':            _get_setting(db, 'smtp_port', '587'),
        'smtp_user':            _get_setting(db, 'smtp_user'),
        'smtp_from_name':       _get_setting(db, 'smtp_from_name', 'Myle Community'),
        'smtp_password_set':     bool(_get_setting(db, 'smtp_password')),
        'anthropic_api_key_set': bool(_get_setting(db, 'anthropic_api_key') or os.environ.get('ANTHROPIC_API_KEY')),
    }

    # ── Enrollment Video (Stage 1) ────────────────────────────────
    enrollment_video_url   = _get_setting(db, 'enrollment_video_url', '')
    enrollment_video_title = _get_setting(db, 'enrollment_video_title', 'Enrollment Video')
    app_tutorial_link      = _get_setting(db, 'app_tutorial_link', '')

    # ── Batch Video Links ────────────────────────────────────────
    batch_videos = {
        'd1_morning_v1':   _get_setting(db, 'batch_d1_morning_v1', ''),
        'd1_morning_v2':   _get_setting(db, 'batch_d1_morning_v2', ''),
        'd1_afternoon_v1': _get_setting(db, 'batch_d1_afternoon_v1', ''),
        'd1_afternoon_v2': _get_setting(db, 'batch_d1_afternoon_v2', ''),
        'd1_evening_v1':   _get_setting(db, 'batch_d1_evening_v1', ''),
        'd1_evening_v2':   _get_setting(db, 'batch_d1_evening_v2', ''),
        'd2_morning_v1':   _get_setting(db, 'batch_d2_morning_v1', ''),
        'd2_morning_v2':   _get_setting(db, 'batch_d2_morning_v2', ''),
        'd2_afternoon_v1': _get_setting(db, 'batch_d2_afternoon_v1', ''),
        'd2_afternoon_v2': _get_setting(db, 'batch_d2_afternoon_v2', ''),
        'd2_evening_v1':   _get_setting(db, 'batch_d2_evening_v1', ''),
        'd2_evening_v2':   _get_setting(db, 'batch_d2_evening_v2', ''),
    }

    db.close()
    return render_template('admin_settings.html', settings=settings, batch_videos=batch_videos,
                           enrollment_video_url=enrollment_video_url, enrollment_video_title=enrollment_video_title,
                           app_tutorial_link=app_tutorial_link)


# ──────────────────────────────────────────────────────────────
#  Admin – Test Email
# ──────────────────────────────────────────────────────────────

@app.route('/admin/settings/test-email', methods=['POST'])
@admin_required
def admin_test_email():
    """Send a test email to verify SMTP configuration."""
    db = get_db()
    smtp_host     = _get_setting(db, 'smtp_host', '')
    smtp_port     = int(_get_setting(db, 'smtp_port', '465') or 465)
    smtp_user     = _get_setting(db, 'smtp_user', '')
    smtp_password = _get_setting(db, 'smtp_password', '')
    from_name     = _get_setting(db, 'smtp_from_name', 'Myle Community')
    db.close()

    test_to = request.form.get('test_email', '').strip()
    if not test_to:
        flash('Please enter a recipient email address.', 'danger')
        return redirect(url_for('admin_settings'))
    if not smtp_host or not smtp_user or not smtp_password:
        flash('SMTP is not fully configured. Please fill in host, user, and password.', 'danger')
        return redirect(url_for('admin_settings'))

    html_body = f"""
    <div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;padding:24px;background:#f8f9fa;border-radius:12px;">
      <div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);border-radius:8px;padding:20px;text-align:center;margin-bottom:24px;">
        <h2 style="color:#fff;margin:0;">✅ SMTP Test</h2>
      </div>
      <p style="color:#333;">This is a test email from <strong>{from_name}</strong>.</p>
      <p style="color:#555;">If you received this, your SMTP configuration is working correctly!</p>
      <p style="color:#888;font-size:12px;">Sent via: {smtp_host}:{smtp_port} as {smtp_user}</p>
    </div>
    """
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f'[{from_name}] SMTP Test Email'
    msg['From']    = f'{from_name} <{smtp_user}>'
    msg['To']      = test_to
    msg.attach(MIMEText(html_body, 'html'))

    try:
        import ssl as _ssl
        context = _ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, test_to, msg.as_string())
        flash(f'✅ Test email sent successfully to {test_to}!', 'success')
    except Exception as e:
        flash(f'❌ SMTP Error: {e}', 'danger')
    return redirect(url_for('admin_settings'))


# ──────────────────────────────────────────────────────────────
#  Admin – Test Push Notification
# ──────────────────────────────────────────────────────────────

@app.route('/admin/settings/reset-vapid', methods=['POST'])
@admin_required
def admin_reset_vapid():
    """Wipe VAPID keys AND all push subscriptions so everything starts fresh."""
    db = get_db()
    _set_setting(db, 'vapid_private_pem', '')
    _set_setting(db, 'vapid_public_key', '')
    sub_count = db.execute('SELECT COUNT(*) FROM push_subscriptions').fetchone()[0]
    db.execute('DELETE FROM push_subscriptions')
    db.commit()
    # Immediately generate a fresh key pair so the new public key is ready
    _get_or_create_vapid_keys(db)
    db.close()
    flash(f'VAPID keys reset and {sub_count} old subscription(s) cleared. '
          'All users must refresh their browser once to re-subscribe — notifications will work after that.',
          'warning')
    return redirect(url_for('admin_settings'))


@app.route('/admin/settings/test-push', methods=['POST'])
@admin_required
def admin_test_push():
    """Send a test push notification to the current admin user."""
    if not PUSH_AVAILABLE:
        flash('Push notifications are not available (pywebpush not installed).', 'danger')
        return redirect(url_for('admin_settings'))
    db = get_db()
    username = session['username']
    subs = db.execute(
        "SELECT id FROM push_subscriptions WHERE username=?", (username,)
    ).fetchall()
    if not subs:
        db.close()
        flash('No push subscription found for your account. Please click the bell icon to enable notifications first.', 'warning')
        return redirect(url_for('admin_settings'))
    _push_to_users(db, username,
                   title='✅ Push Test Successful!',
                   body='Push notifications are working correctly on your Myle Dashboard.',
                   url='/admin/settings')
    db.commit()
    db.close()
    flash('Test push notification sent! Check your browser/device notifications.', 'success')
    return redirect(url_for('admin_settings'))


@app.route('/admin/settings/test-calling-reminder', methods=['POST'])
@admin_required
def admin_test_calling_reminder():
    """Trigger the calling reminder job immediately for debugging."""
    if not SCHEDULER_AVAILABLE:
        flash('Scheduler (APScheduler) is not available — calling reminders cannot run.', 'danger')
        return redirect(url_for('admin_settings'))
    if not PUSH_AVAILABLE:
        flash('Push notifications are not available (pywebpush not installed).', 'danger')
        return redirect(url_for('admin_settings'))
    try:
        job_calling_reminder()
        ist_now = _now_ist()
        flash(
            f'Calling reminder job executed at {ist_now.strftime("%H:%M")} IST. '
            'If any users had this time set as their reminder, they received a notification.',
            'success'
        )
    except Exception as ex:
        flash(f'Calling reminder job error: {ex}', 'danger')
    return redirect(url_for('admin_settings'))


# ──────────────────────────────────────────────────────────────
#  Admin – Edit Member (username / email) + Permanent Delete
# ──────────────────────────────────────────────────────────────

@app.route('/admin/members/<username>/edit', methods=['POST'])
@admin_required
def admin_edit_member(username):
    """Change a member's username and/or email address."""
    db = get_db()
    member = db.execute("SELECT id, username, email FROM users WHERE username=?", (username,)).fetchone()
    if not member:
        db.close()
        flash('Member not found.', 'danger')
        return redirect(url_for('admin_members'))

    new_username = request.form.get('new_username', '').strip().lower()
    new_email    = request.form.get('new_email', '').strip().lower()

    errors = []

    if new_username and new_username != username:
        # Check uniqueness
        existing = db.execute("SELECT id FROM users WHERE username=? AND id!=?", (new_username, member['id'])).fetchone()
        if existing:
            errors.append(f'Username @{new_username} is already taken.')
        elif len(new_username) < 3:
            errors.append('Username must be at least 3 characters.')
        else:
            # Update all related tables
            db.execute("UPDATE leads SET assigned_to=? WHERE assigned_to=?", (new_username, username))
            try:
                db.execute("UPDATE leads SET added_by=? WHERE added_by=?", (new_username, username))
            except Exception:
                pass
            db.execute("UPDATE wallet_recharges SET username=? WHERE username=?", (new_username, username))
            db.execute("UPDATE push_subscriptions SET username=? WHERE username=?", (new_username, username))
            db.execute("UPDATE daily_reports SET username=? WHERE username=?", (new_username, username))
            try:
                db.execute("UPDATE lead_notes SET username=? WHERE username=?", (new_username, username))
            except Exception:
                pass
            try:
                db.execute("UPDATE activity_log SET username=? WHERE username=?", (new_username, username))
            except Exception:
                pass
            db.execute("UPDATE users SET username=? WHERE id=?", (new_username, member['id']))
            flash(f'Username changed from @{username} to @{new_username}.', 'success')
            username = new_username  # use new username for redirect

    if new_email:
        # Check uniqueness
        existing = db.execute("SELECT id FROM users WHERE LOWER(email)=? AND id!=?", (new_email, member['id'])).fetchone()
        if existing:
            errors.append(f'Email {new_email} is already in use by another account.')
        else:
            db.execute("UPDATE users SET email=? WHERE id=?", (new_email, member['id']))
            flash(f'Email updated to {new_email}.', 'success')

    for err in errors:
        flash(err, 'danger')

    db.commit()
    db.close()
    return redirect(url_for('member_detail', username=username))


@app.route('/admin/members/<username>/delete', methods=['POST'])
@admin_required
def admin_delete_member(username):
    """Permanently delete a member and all their data from the database."""
    db = get_db()
    member = db.execute("SELECT id, username FROM users WHERE username=? AND role='team'", (username,)).fetchone()
    if not member:
        db.close()
        flash('Member not found or cannot delete admin accounts.', 'danger')
        return redirect(url_for('admin_members'))

    confirm = request.form.get('confirm_username', '').strip()
    if confirm != username:
        db.close()
        flash('Confirmation username did not match. Member was NOT deleted.', 'danger')
        return redirect(url_for('member_detail', username=username))

    # Delete all member data
    db.execute("DELETE FROM wallet_recharges WHERE username=?", (username,))
    db.execute("DELETE FROM push_subscriptions WHERE username=?", (username,))
    db.execute("DELETE FROM daily_reports WHERE username=?", (username,))
    # Move their leads back to pool instead of deleting
    db.execute("UPDATE leads SET assigned_to='', in_pool=1, claimed_at='' WHERE assigned_to=? AND in_pool=0", (username,))
    try:
        db.execute("DELETE FROM activity_log WHERE username=?", (username,))
    except Exception:
        pass
    db.execute("DELETE FROM users WHERE id=?", (member['id'],))
    db.commit()
    db.close()
    flash(f'Member @{username} has been permanently deleted. Their leads have been returned to the pool.', 'success')
    return redirect(url_for('admin_members'))


# ──────────────────────────────────────────────────────────────
#  Admin – Monthly Targets
# ──────────────────────────────────────────────────────────────

@app.route('/admin/targets', methods=['GET', 'POST'])
@admin_required
def admin_targets():
    db    = get_db()
    month = request.args.get('month', _now_ist().strftime('%Y-%m'))

    if request.method == 'POST':
        month_p = request.form.get('month', month)
        members = db.execute(
            "SELECT username FROM users WHERE role='team' AND status='approved' ORDER BY username"
        ).fetchall()
        for m in members:
            uname = m['username']
            for metric in ('leads', 'payments', 'conversions', 'revenue'):
                val = request.form.get(f'{uname}_{metric}', '').strip()
                if val:
                    try:
                        db.execute("""
                            INSERT INTO targets (username, metric, target_value, month, created_by)
                            VALUES (?,?,?,?,?)
                            ON CONFLICT(username, metric, month)
                            DO UPDATE SET target_value=excluded.target_value
                        """, (uname, metric, float(val), month_p, session['username']))
                    except Exception:
                        pass
        db.commit()
        flash('Targets saved!', 'success')
        db.close()
        return redirect(url_for('admin_targets', month=month_p))

    members = db.execute(
        "SELECT username FROM users WHERE role='team' AND status='approved' ORDER BY username"
    ).fetchall()
    targets_map = {}
    rows = db.execute(
        "SELECT username, metric, target_value FROM targets WHERE month=?", (month,)
    ).fetchall()
    for r in rows:
        targets_map[(r['username'], r['metric'])] = r['target_value']

    db.close()
    return render_template('admin_targets.html',
                           members=members,
                           targets_map=targets_map,
                           month=month)


# ──────────────────────────────────────────────────────────────
#  Admin – Budget Summary Export (CSV)
# ──────────────────────────────────────────────────────────────

@app.route('/admin/budget-export')
@admin_required
def admin_budget_export():
    """Export wallet/lead budget summary for all members as CSV with optional date range."""
    import csv, io as _io
    # If no download param, show the filter page
    if not request.args.get('download'):
        db = get_db()
        member_count = db.execute("SELECT COUNT(*) FROM users WHERE role='team' AND status='approved'").fetchone()[0]
        db.close()
        return render_template('budget_export.html', member_count=member_count)
    db = get_db()

    date_from = request.args.get('date_from', '')
    date_to   = request.args.get('date_to', '')

    # Build date filter for leads claimed in range
    leads_filter = "in_pool=0 AND claimed_at!=''"
    leads_params = []
    recharge_filter = "status='approved'"
    recharge_params = []

    if date_from:
        leads_filter    += " AND claimed_at >= ?"
        leads_params.append(date_from)
        recharge_filter += " AND processed_at >= ?"
        recharge_params.append(date_from)
    if date_to:
        leads_filter    += " AND claimed_at <= ?"
        leads_params.append(date_to + ' 23:59:59')
        recharge_filter += " AND processed_at <= ?"
        recharge_params.append(date_to + ' 23:59:59')

    members = db.execute(
        "SELECT username, email, fbo_id, phone FROM users WHERE role='team' AND status='approved' ORDER BY username"
    ).fetchall()

    output = _io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'Username', 'Email', 'FBO ID', 'Phone',
        'Total Recharged (₹)', 'Total Spent on Leads (₹)', 'Wallet Balance (₹)',
        'Leads Claimed (count)', 'Admin Adjustments (₹)',
        'Date From', 'Date To'
    ])

    for m in members:
        uname = m['username']

        # Total wallet recharged (approved, from users not admin adjustments)
        total_recharged = db.execute(
            f"SELECT COALESCE(SUM(amount),0) FROM wallet_recharges WHERE username=? AND {recharge_filter} AND utr_number!='ADMIN-ADJUST'",
            [uname] + recharge_params
        ).fetchone()[0] or 0.0

        # Admin adjustments separately
        admin_adj = db.execute(
            f"SELECT COALESCE(SUM(amount),0) FROM wallet_recharges WHERE username=? AND status='approved' AND utr_number='ADMIN-ADJUST'",
            [uname]
        ).fetchone()[0] or 0.0

        # Leads spent
        total_spent = db.execute(
            f"SELECT COALESCE(SUM(pool_price),0) FROM leads WHERE assigned_to=? AND {leads_filter}",
            [uname] + leads_params
        ).fetchone()[0] or 0.0

        # Leads count
        leads_count = db.execute(
            f"SELECT COUNT(*) FROM leads WHERE assigned_to=? AND {leads_filter}",
            [uname] + leads_params
        ).fetchone()[0] or 0

        # Current wallet balance (always full, not date-filtered)
        wallet = _get_wallet(db, uname)
        balance = wallet['balance']

        writer.writerow([
            uname, m['email'] or '', m['fbo_id'] or '', m['phone'] or '',
            f"{total_recharged:.2f}", f"{total_spent:.2f}", f"{balance:.2f}",
            leads_count, f"{admin_adj:.2f}",
            date_from or 'All time', date_to or 'All time'
        ])

    db.close()
    output.seek(0)
    from flask import Response
    filename = f"budget_summary_{date_from or 'all'}_{date_to or 'all'}.csv"
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )


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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Admin \u2013 Lead Pool Management
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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

    # Reject oversized uploads before reading into memory (5 MB hard limit)
    _CSV_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
    f.seek(0, 2)           # seek to end
    _file_size = f.tell()
    f.seek(0)              # rewind
    if _file_size > _CSV_MAX_BYTES:
        flash(f'CSV file too large ({_file_size // 1024} KB). Maximum allowed is 5 MB.', 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    try:
        content = f.read().decode('utf-8-sig', errors='replace')
        reader  = csv.DictReader(io.StringIO(content))
        rows_list = list(reader)
    except Exception as e:
        flash(f'Could not parse CSV: {e}', 'danger')
        db.close()
        return redirect(url_for('admin_lead_pool'))

    existing_phones = {
        r[0] for r in db.execute(
            "SELECT phone FROM leads WHERE in_pool=1"
        ).fetchall()
    }

    imported = 0
    skipped  = 0

    for row in rows_list:
        _fn = (row.get('First Name') or row.get('first_name') or '').strip()
        _ln = (row.get('Last Name') or row.get('last_name') or '').strip()
        name  = (row.get('Full Name') or row.get('full_name') or
                 row.get('name') or row.get('Name') or
                 ((_fn + ' ' + _ln).strip() if _fn or _ln else '') or '').strip()
        phone = (row.get('Phone Number (Calling Number)') or
                 row.get('phone_number') or row.get('phone') or
                 row.get('Phone') or row.get('Phone Number') or '').strip()
        email = (row.get('email') or row.get('Email') or
                 row.get('email_address') or '').strip()

        age         = (row.get('Age') or row.get('age') or '').strip()
        gender      = (row.get('Gender') or row.get('gender') or '').strip()
        city        = (row.get('Your City Name') or row.get('city') or
                       row.get('City') or '').strip()
        ad_name     = (row.get('Ad Name') or row.get('ad_name') or '').strip()
        submit_time = (row.get('Submit Time') or row.get('submit_time') or '').strip()

        lead_source = ad_name if ad_name else source_tag

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
        if phone in existing_phones:
            skipped += 1
            continue
        existing_phones.add(phone)

        db.execute("""
            INSERT INTO leads
                (name, phone, email, assigned_to, source, status,
                 in_pool, pool_price, claimed_at, city, notes)
            VALUES (?, ?, ?, '', ?, 'New', 1, ?, '', ?, ?)
        """, (name, phone, email, lead_source, price_per_lead, city, notes_str))
        imported += 1

    db.commit()
    if imported > 0:
        _count = imported
        def _bg_push_csv():
            _db = get_db()
            try:
                _push_all_team(
                    _db,
                    '🎯 New Leads Available!',
                    f'{_count} new lead{"s" if _count != 1 else ""} just added to the Lead Pool — claim yours now!',
                    '/lead-pool'
                )
                _db.commit()
            finally:
                _db.close()
        threading.Thread(target=_bg_push_csv, daemon=True).start()
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
    if imported > 0:
        _count = imported
        def _bg_push_pdf():
            _db = get_db()
            try:
                _push_all_team(
                    _db,
                    '🎯 New Leads Available!',
                    f'{_count} new lead{"s" if _count != 1 else ""} just added to the Lead Pool — claim yours now!',
                    '/lead-pool'
                )
                _db.commit()
            finally:
                _db.close()
        threading.Thread(target=_bg_push_pdf, daemon=True).start()
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
    _lead_name = name
    def _bg_push_single():
        _db = get_db()
        try:
            _push_all_team(
                _db,
                '🎯 New Lead Available!',
                f'A new lead "{_lead_name}" has been added to the Lead Pool — claim it now!',
                '/lead-pool'
            )
            _db.commit()
        finally:
            _db.close()
    threading.Thread(target=_bg_push_single, daemon=True).start()
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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Admin \u2013 Wallet Recharge Requests
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
            "processed_at=? WHERE id=?",
            (_now_ist().strftime('%Y-%m-%d %H:%M:%S'), req_id)
        )
        db.commit()
        flash(f'Recharge of \u20b9{recharge["amount"]:.0f} for @{recharge["username"]} approved!', 'success')
        _username = recharge['username']
        _amount   = recharge['amount']
        def _bg_push_recharge(u, amt):
            _db = get_db()
            try:
                _push_to_users(_db, u, '\u2705 Wallet Recharged!',
                               f'\u20b9{amt:.0f} has been added to your wallet.',
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
            "processed_at=?, admin_note=? WHERE id=?",
            (_now_ist().strftime('%Y-%m-%d %H:%M:%S'), admin_note, req_id)
        )
        db.commit()
        flash(f'Recharge request from @{recharge["username"]} rejected.', 'warning')
    db.close()
    return redirect(url_for('admin_wallet_requests', status='pending'))


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Admin \u2013 Manual Wallet Adjustment
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/admin/members/<username>/wallet-adjust', methods=['POST'])
@admin_required
def admin_wallet_adjust(username):
    amount = request.form.get('amount', '').strip()
    note   = request.form.get('note', '').strip() or 'Manual adjustment by admin'
    try:
        amount = float(amount)
        if amount == 0:
            flash('Amount cannot be zero.', 'warning')
            return redirect(url_for('member_detail', username=username))
    except ValueError:
        flash('Invalid amount.', 'danger')
        return redirect(url_for('member_detail', username=username))

    db = get_db()
    db.execute(
        "INSERT INTO wallet_recharges (username, amount, utr_number, status, "
        "requested_at, processed_at, admin_note) "
        "VALUES (?, ?, 'ADMIN-ADJUST', 'approved', ?, ?, ?)",
        (username, amount, _now_ist().strftime('%Y-%m-%d %H:%M:%S'), _now_ist().strftime('%Y-%m-%d %H:%M:%S'), note)
    )
    db.commit()
    db.close()

    action = 'credited to' if amount > 0 else 'debited from'
    flash(f'\u20b9{abs(amount):.0f} {action} @{username}\'s wallet. Note: {note}', 'success')
    return redirect(url_for('member_detail', username=username))



# ─────────────────────────────────────────────────
#  Team – Wallet / Lead Pool / Calling Reminder (see routes/wallet_routes.py)
# ─────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
#  Profile / password / help / earnings (see routes/profile_routes.py)
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────
#  CSV Export
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
    fname = f"leads_{_today_ist().isoformat()}.csv"
    return Response(buf.getvalue(), mimetype='application/octet-stream',
                    headers={'Content-Disposition': f'attachment; filename="{fname}"'})


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Leads \u2013 Bulk Import (CSV / PDF)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/leads/import', methods=['POST'])
@login_required
def import_leads():
    db          = get_db()
    username    = session['username']
    is_admin    = session.get('role') == 'admin'
    file_type   = request.form.get('import_type', 'csv')
    source_tag  = request.form.get('source_tag', 'Import').strip() or 'Import'

    if is_admin:
        assigned_to = request.form.get('assigned_to', '').strip() or username
    else:
        assigned_to = username

    rows_list = []

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
            _fn2 = (row.get('First Name') or row.get('first_name') or '').strip()
            _ln2 = (row.get('Last Name') or row.get('last_name') or '').strip()
            name  = (row.get('Full Name') or row.get('full_name') or
                     row.get('name') or row.get('Name') or
                     ((_fn2 + ' ' + _ln2).strip() if _fn2 or _ln2 else '') or '').strip()
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

    existing_phones = {
        r[0] for r in db.execute(
            "SELECT phone FROM leads WHERE in_pool=0 AND deleted_at=''"
        ).fetchall()
    }

    imported = skipped = 0
    batch_values = []
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
        batch_values.append((name, phone, email, assigned_to, src, city))
        imported += 1

    _BATCH_SZ = 50
    for i in range(0, len(batch_values), _BATCH_SZ):
        chunk = batch_values[i:i + _BATCH_SZ]
        db.executemany("""
            INSERT INTO leads
                (name, phone, email, assigned_to, source, status,
                 in_pool, pool_price, claimed_at, city, notes)
            VALUES (?, ?, ?, ?, ?, 'New', 0, 0, '', ?, '')
        """, chunk)
        db.commit()

    db.close()
    flash(f'Import complete: {imported} leads added, {skipped} skipped (duplicates/empty).', 'success')
    return redirect(url_for('leads'))



# ─────────────────────────────────────────────────────────────
#  Announcements / Leaderboard (see routes/social_routes.py)
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
#  AI Lead Intelligence
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/intelligence')
@login_required
@safe_route
def intelligence():
    db       = get_db()
    username = session['username']
    role     = session.get('role', 'team')

    try:
        if role == 'admin':
            raw_leads = db.execute(
                "SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' ORDER BY updated_at DESC LIMIT 150"
            ).fetchall()
        elif role == 'leader':
            downline = _get_network_usernames(db, username)
            if downline:
                phs = ','.join('?' for _ in downline)
                raw_leads = db.execute(
                    f"SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' AND assigned_to IN ({phs}) ORDER BY updated_at DESC",
                    downline
                ).fetchall()
            else:
                raw_leads = []
        else:
            raw_leads = db.execute(
                "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' ORDER BY updated_at DESC",
                (username,)
            ).fetchall()
    except Exception as e:
        app.logger.error(f"intelligence() leads query failed: {e}")
        raw_leads = []

    # ── Leaderboard data (weekly scores) ──
    try:
        lb_rows = db.execute("""
            SELECT u.username,
                   COALESCE(SUM(ds.total_points), 0)   AS week_pts,
                   COALESCE(SUM(ds.batches_marked), 0) AS batches,
                   MAX(ds.streak_days)                 AS streak
            FROM users u
            LEFT JOIN daily_scores ds
                   ON ds.username = u.username
                  AND ds.score_date >= date('now', '-6 days')
            WHERE u.role IN ('team','leader') AND u.status='approved'
            GROUP BY u.username
            ORDER BY week_pts DESC
            LIMIT 10
        """).fetchall()
        lb_board = [dict(r) for r in lb_rows]
    except Exception:
        lb_board = []

    db.close()

    try:
        enriched = _enrich_leads(raw_leads)
    except Exception as e:
        app.logger.error(f"intelligence() enrichment failed: {e}")
        enriched = []
    for d in enriched:
        try:
            d['ai_tip'] = _generate_ai_tip(d)
        except Exception:
            d['ai_tip'] = ''
    enriched.sort(key=lambda x: (
        {'urgent': 0, 'today': 1, 'followup': 2, 'cold': 3}.get(x.get('next_action_type', 'cold'), 9),
        -x.get('heat', 0),
    ))

    urgent_count = sum(1 for l in enriched if l.get('next_action_type') == 'urgent')
    hot_count    = sum(1 for l in enriched if l.get('heat', 0) >= 75)

    return render_template('intelligence.html',
                           leads=enriched,
                           urgent_count=urgent_count,
                           hot_count=hot_count,
                           user_role=role,
                           badge_meta=BADGE_META,
                           lb_board=lb_board,
                           current_user=username)


@app.route('/ai/lead-intelligence')
@login_required
def ai_lead_intelligence():
    db       = get_db()
    username = session['username']
    role     = session.get('role')

    if role == 'admin':
        raw_leads = db.execute(
            "SELECT * FROM leads WHERE in_pool=0 AND deleted_at=\'\' ORDER BY updated_at DESC LIMIT 150"
        ).fetchall()
    else:
        raw_leads = db.execute(
            "SELECT * FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at=\'\' ORDER BY updated_at DESC",
            (username,)
        ).fetchall()

    db.close()

    enriched = _enrich_leads(raw_leads)
    for d in enriched:
        d['ai_tip'] = _generate_ai_tip(d)

    enriched.sort(key=lambda x: (
        {'urgent': 0, 'today': 1, 'followup': 2, 'cold': 3}.get(x.get('next_action_type', 'cold'), 9),
        -x.get('heat', 0),
    ))

    urgent_count = sum(1 for l in enriched if l.get('next_action_type') == 'urgent')
    hot_count    = sum(1 for l in enriched if l.get('heat', 0) >= 75)

    return jsonify({
        'leads': [{
            'id':               l.get('id'),
            'name':             l.get('name', ''),
            'stage':            l.get('pipeline_stage', 'enrollment'),
            'heat':             l.get('heat', 0),
            'next_action':      l.get('next_action', ''),
            'next_action_type': l.get('next_action_type', 'followup'),
            'call_status':      l.get('call_status', ''),
            'ai_tip':           l.get('ai_tip', ''),
            'owner':            l.get('assigned_to', ''),
        } for l in enriched],
        'urgent_count': urgent_count,
        'hot_count':    hot_count,
    })


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Admin \u2013 Reset any user's password
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Lead Notes / Timeline
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/leads/<int:lead_id>/notes', methods=['POST'])
@login_required
def add_lead_note(lead_id):
    note = request.form.get('note', '').strip()
    if not note:
        flash('Note cannot be empty.', 'danger')
        return redirect(url_for('edit_lead', lead_id=lead_id))

    db = get_db()
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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Bulk Actions on Leads
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/leads/bulk-action', methods=['POST'])
@login_required
def bulk_action():
    action   = request.form.get('bulk_action', '')
    lead_ids = request.form.getlist('lead_ids')
    if not lead_ids:
        flash('No leads selected.', 'warning')
        return redirect(url_for('leads'))

    lead_ids = list(set(int(i) for i in lead_ids if i.isdigit()))
    db       = get_db()

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
            f"UPDATE leads SET deleted_at=? WHERE {where}",
            [_now_ist().strftime('%Y-%m-%d %H:%M:%S')] + params
        )
        db.commit()
        flash(f'Moved {len(lead_ids)} leads to Recycle Bin.', 'warning')

    elif action.startswith('status:'):
        new_status = action.split(':', 1)[1]
        if new_status in STATUSES:
            db.execute(
                f"UPDATE leads SET status=?, updated_at=? WHERE {where}",
                [new_status, _now_ist().strftime('%Y-%m-%d %H:%M:%S')] + params
            )
            db.commit()
            flash(f'Status updated to "{new_status}" for {len(lead_ids)} leads.', 'success')

    elif action == 'mark_paid':
        db.execute(
            f"UPDATE leads SET payment_done=1, payment_amount=?, "
            f"updated_at=? WHERE {where}",
            [PAYMENT_AMOUNT, _now_ist().strftime('%Y-%m-%d %H:%M:%S')] + params
        )
        db.commit()
        flash(f'Marked {len(lead_ids)} leads as paid (\u20b9{PAYMENT_AMOUNT:.0f} each).', 'success')

    db.close()
    return redirect(url_for('leads'))


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Push Notification API
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/leads/bulk-update', methods=['POST'])
@login_required
def bulk_update_leads():
    data       = request.get_json() or {}
    ids        = data.get('ids', [])
    new_status = data.get('status', '').strip()
    if not ids or new_status not in STATUSES:
        return {'ok': False, 'error': 'invalid'}, 400

    db       = get_db()
    username = session['username']
    role     = session.get('role')
    updated  = 0
    now      = _now_ist().strftime('%Y-%m-%d %H:%M:%S')

    for lead_id in ids:
        lead = db.execute("SELECT assigned_to FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''",
                          (lead_id,)).fetchone()
        if not lead:
            continue
        if role != 'admin' and lead['assigned_to'] != username:
            continue
        new_stage = STATUS_TO_STAGE.get(new_status, 'enrollment')
        db.execute("UPDATE leads SET status=?, pipeline_stage=?, updated_at=? WHERE id=?",
                   (new_status, new_stage, now, lead_id))
        _log_lead_event(db, lead_id, username, f'[Bulk] Status → {new_status}')
        updated += 1

    _check_and_award_badges(db, username)
    db.commit()
    db.close()
    return {'ok': True, 'updated': updated}



# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Scheduled Reminder Jobs
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

def _reminder_lock(db, key):
    today = _today_ist().isoformat()   # IST date
    lock_key = f'{key}_{today}'
    cur = db.execute(
        "INSERT OR IGNORE INTO app_settings (key, value) VALUES (?, 'sent')",
        (lock_key,)
    )
    db.commit()
    return cur.rowcount == 1


def job_followup_reminders():
    """Push individual follow-up reminders to each team member at 9 AM IST."""
    db = get_db()
    try:
        if not _reminder_lock(db, 'followup_reminder'):
            return
        today = _today_ist().isoformat()
        rows = db.execute("""
            SELECT assigned_to, COUNT(*) as cnt
            FROM leads
            WHERE in_pool=0
              AND follow_up_date=?
              AND follow_up_date != ''
              AND status NOT IN ('Converted','Fully Converted','Lost')
              AND assigned_to != ''
            GROUP BY assigned_to
        """, (today,)).fetchall()
        for row in rows:
            cnt = row['cnt']
            _push_to_users(db, row['assigned_to'],
                           '\U0001f4c5 Follow-up Reminder',
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
    ist_now  = _now_ist()
    now_hhmm = ist_now.strftime('%H:%M')
    today    = _today_ist().isoformat()   # IST date — must match IST time, not server UTC
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
                app.logger.info(f'[Scheduler] Calling reminder sent @{u["username"]} at {now_hhmm} IST')
                _push_to_users(db, u['username'],
                               '\U0001f4de Calling Reminder',
                               'Time to start your calls! Don\'t forget your daily report.',
                               '/reports/submit')
                db.commit()
    finally:
        db.close()


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Boot \u2013 runs on every startup
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500



def migrate_pipeline_stages(db):
    """One-time startup migration: sync pipeline_stage + current_owner for all existing leads."""
    leads = db.execute("""
        SELECT id, status, assigned_to, pipeline_stage, current_owner
        FROM leads WHERE in_pool=0 AND deleted_at=''
    """).fetchall()
    for lead in leads:
        # Legacy normalization: "Paid ₹196" is now treated as Day 1 everywhere
        # (older data may still have the old status value).
        if lead['status'] == 'Paid ₹196':
            try:
                db.execute(
                    "UPDATE leads SET status='Day 1', payment_done=1, payment_amount=? WHERE id=?",
                    (PAYMENT_AMOUNT, lead['id'])
                )
            except Exception:
                pass
        current_stage = lead['pipeline_stage'] if 'pipeline_stage' in lead.keys() else ''
        # If we normalized status above, re-read expected stage accordingly
        expected_status = 'Day 1' if lead['status'] == 'Paid ₹196' else lead['status']
        expected_stage = STATUS_TO_STAGE.get(expected_status, 'enrollment')
        needs_update = (not current_stage or current_stage == '' or current_stage != expected_stage)
        if needs_update:
            stage = expected_stage
            owner = lead['current_owner'] if 'current_owner' in lead.keys() else ''
            if not owner:
                if stage == 'enrollment':
                    owner = lead['assigned_to'] or ''
                elif stage in ('day1', 'day3', 'seat_hold'):
                    owner = _get_leader_for_user(db, lead['assigned_to'])
                else:
                    owner = _get_admin_username(db)
            db.execute(
                "UPDATE leads SET pipeline_stage=?, current_owner=? WHERE id=?",
                (stage, owner, lead['id'])
            )
    db.commit()

init_db()
migrate_db()
seed_users()
seed_training_questions()

# Sync existing leads to pipeline stages
try:
    _boot_db = get_db()
    migrate_pipeline_stages(_boot_db)
    _boot_db.close()
except Exception as _e:
    import sys
    print(f'[Pipeline] migrate_pipeline_stages failed: {_e}', file=sys.stderr)

# ── Scheduler startup ───────────────────────────────────────────────────────
# start_scheduler() uses a file lock so exactly ONE worker process runs it.
# gunicorn.conf.py post_fork hook calls this after each fork.

_scheduler = None


def start_scheduler():
    """Start APScheduler (idempotent, file-lock guarded for multi-worker gunicorn)."""
    global _scheduler
    if not SCHEDULER_AVAILABLE:
        app.logger.warning('[Scheduler] APScheduler not available — reminders disabled.')
        return
    if _scheduler is not None and _scheduler.running:
        return

    import fcntl
    lock_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.scheduler.lock')
    try:
        lock_fd = open(lock_path, 'w')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
    except OSError:
        app.logger.info('[Scheduler] Another worker owns the scheduler lock — skipping.')
        return

    _scheduler = BackgroundScheduler(timezone='Asia/Kolkata')
    _scheduler.add_job(job_followup_reminders, 'cron', hour=9, minute=0,
                       id='followup_reminders', replace_existing=True)
    _scheduler.add_job(job_calling_reminder, 'interval', minutes=1,
                       id='calling_reminder', replace_existing=True)
    _scheduler.start()
    app.logger.info(f'[Scheduler] Started in PID {os.getpid()}')

    def _shutdown():
        try:
            _scheduler.shutdown(wait=False)
        except Exception:
            pass
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()
            os.unlink(lock_path)
        except Exception:
            pass

    atexit.register(_shutdown)


# Auto-start for Flask dev server / single-worker gunicorn.
# Multi-worker gunicorn uses gunicorn.conf.py post_fork hook instead.
if not os.environ.get('GUNICORN_MULTI_WORKER'):
    start_scheduler()



# ─────────────────────────────────────────────────────────────
#  Live Session (team) (see routes/social_routes.py)
# ─────────────────────────────────────────────────────────────

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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Admin \u2013 All Members List + Individual Activity
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/admin/members')
@admin_required
@safe_route
def admin_members():
    db = get_db()
    # Include team AND leader roles (admin manages both)
    users = db.execute(
        "SELECT * FROM users WHERE role IN ('team','leader') ORDER BY role, status, created_at DESC"
    ).fetchall()

    _rows = db.execute("""
        SELECT assigned_to,
            COUNT(*) as total_leads,
            SUM(CASE WHEN status IN ('Converted','Fully Converted') THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN payment_done=1 THEN 1 ELSE 0 END) as paid
        FROM leads WHERE in_pool=0
        GROUP BY assigned_to
    """).fetchall()
    stats_map = {r['assigned_to']: r for r in _rows}

    _rep_rows = db.execute(
        "SELECT username, COUNT(*) as report_count FROM daily_reports GROUP BY username"
    ).fetchall()
    report_map = {r['username']: r['report_count'] for r in _rep_rows}

    # Leader metrics: for each leader, count day1, seat_hold, converted in their downline
    leader_metrics = {}
    for u in users:
        if u['role'] == 'leader':
            uname = u['username']
            downline = _get_downline_usernames(db, uname)
            dl_ph = ','.join('?' * len(downline)) if downline else "''"
            day1_c = db.execute(
                f"SELECT COUNT(*) FROM leads WHERE pipeline_stage='day1' AND assigned_to IN ({dl_ph}) AND in_pool=0",
                downline
            ).fetchone()[0] if downline else 0
            seat_c = db.execute(
                f"SELECT COUNT(*) FROM leads WHERE pipeline_stage='seat_hold' AND current_owner=? AND in_pool=0",
                (uname,)
            ).fetchone()[0]
            conv_c = db.execute(
                f"SELECT COUNT(*) FROM leads WHERE pipeline_stage='complete' AND assigned_to IN ({dl_ph}) AND in_pool=0",
                downline
            ).fetchone()[0] if downline else 0
            conv_pct = round(conv_c / day1_c * 100, 1) if day1_c > 0 else 0
            leader_metrics[uname] = {
                'downline_count': len(downline),
                'day1_leads': day1_c,
                'seat_holds': seat_c,
                'converted': conv_c,
                'conv_pct': conv_pct,
            }

    leaders = db.execute(
        "SELECT username FROM users WHERE role='leader' AND status='approved' ORDER BY username"
    ).fetchall()
    db.close()
    return render_template('all_members.html',
                           users=users,
                           stats_map=stats_map,
                           report_map=report_map,
                           leader_metrics=leader_metrics,
                           leaders=leaders)


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

    _sc = db.execute(
        "SELECT status, COUNT(*) as c FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at='' GROUP BY status",
        (username,)
    ).fetchall()
    status_data = {s: 0 for s in STATUSES}
    for row in _sc:
        if row['status'] in status_data:
            status_data[row['status']] = row['c']

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


@app.route('/admin/activity')
@admin_required
def admin_activity():
    db = get_db()

    try:
        page = int(request.args.get('page', 1))
    except (TypeError, ValueError):
        page = 1
    page = max(1, page)
    per_pg  = 50
    offset  = (page - 1) * per_pg
    filter_user  = request.args.get('user', '')
    filter_event = request.args.get('event', '')

    where, params = ['1=1'], []
    if filter_user:
        where.append('username=?')
        params.append(filter_user)
    if filter_event:
        where.append('event_type=?')
        params.append(filter_event)

    total = db.execute(
        f"SELECT COUNT(*) FROM activity_log WHERE {' AND '.join(where)}", params
    ).fetchone()[0]

    logs = db.execute(
        f"SELECT * FROM activity_log WHERE {' AND '.join(where)} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [per_pg, offset]
    ).fetchall()

    # Last seen per team member — all approved team members, even those without activity
    last_seen_rows = db.execute("""
        SELECT u.username, u.display_picture,
               a.event_type, a.created_at
        FROM users u
        LEFT JOIN activity_log a
            ON a.username = u.username
            AND a.created_at = (
                SELECT MAX(a2.created_at) FROM activity_log a2
                WHERE a2.username = u.username
            )
        WHERE u.role = 'team' AND u.status = 'approved'
        ORDER BY a.created_at DESC
    """).fetchall()

    team_members = db.execute(
        "SELECT username FROM users WHERE role='team' AND status='approved' ORDER BY username"
    ).fetchall()

    db.close()

    total_pages = max(1, (total + per_pg - 1) // per_pg)
    return render_template('activity_log.html',
                           logs=logs, total=total, page=page,
                           total_pages=total_pages,
                           last_seen=last_seen_rows,
                           team_members=team_members,
                           filter_user=filter_user,
                           filter_event=filter_event,
                           event_types=['login','logout','lead_update','report_submit','lead_claim'])


# ─────────────────────────────────────────────
#  Drill-Down Analytics
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/drill-down/<metric>')
@login_required
def drilldown(metric):
    db = get_db()
    is_admin = session.get('role') == 'admin'

    if metric not in DRILL_LEAD_METRICS and metric not in DRILL_REPORT_METRICS:
        db.close()
        return redirect(url_for('admin_dashboard' if is_admin else 'team_dashboard'))

    if is_admin:
        network = None
    else:
        network = _get_downline_usernames(db, session['username'])

    fmt  = request.args.get('format', '')
    view = request.args.get('view', 'daily')  # 'daily' or 'monthly'

    # \u2500\u2500 Lead metrics \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
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

        if view == 'monthly':
            trend_rows = db.execute(
                f"SELECT strftime('%Y-%m', created_at) as d, COUNT(*) as cnt FROM leads "
                f"WHERE {base}{extra} AND date(created_at) >= date('now','-365 days') "
                f"GROUP BY d ORDER BY d",
                base_params
            ).fetchall()
        else:
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
                               is_report=False, is_admin=is_admin, view=view)

    # \u2500\u2500 Report metrics \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    else:
        label, icon, color = DRILL_REPORT_METRICS[metric]
        col = metric

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

        if view == 'monthly':
            trend_rows = db.execute(
                f"SELECT strftime('%Y-%m', report_date) as d, SUM({col}) as cnt FROM daily_reports "
                f"WHERE {where} AND report_date >= date('now','-365 days') "
                f"GROUP BY d ORDER BY d",
                where_params
            ).fetchall()
        else:
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
                               is_report=True, is_admin=is_admin, view=view)


# ─────────────────────────────────────────────────────────────
#  Training Gate (before_request)
# ─────────────────────────────────────────────────────────────

_TRAINING_EXEMPT = (
    '/static', '/training', '/profile/dp', '/meta',
    '/login', '/register', '/logout', '/health',
    '/manifest.json', '/sw.js', '/forgot-password',
    '/reset-password', '/push', '/calling-reminder',
    '/admin/training/signature-preview',
)

@app.before_request
def refresh_session_role():
    """Auto-sync session role from DB periodically (not every request)."""
    if 'username' not in session:
        return
    if request.path.startswith('/static'):
        return
    import time
    last_check = session.get('_role_checked', 0)
    if time.time() - last_check < 60:
        return
    try:
        db = get_db()
        row = db.execute(
            "SELECT role FROM users WHERE username=? AND status='approved'",
            (session['username'],)
        ).fetchone()
        db.close()
        session['_role_checked'] = time.time()
        if row and row['role'] != session.get('role'):
            session['role'] = row['role']
    except Exception:
        pass


@app.before_request
def training_gate():
    if any(request.path.startswith(p) for p in _TRAINING_EXEMPT):
        return
    if 'username' not in session:
        return
    if session.get('role') == 'admin':
        return
    ts = session.get('training_status', 'not_required')
    if ts not in ('not_required', 'unlocked'):
        return redirect(url_for('training_home'))


# Training routes extracted to routes/training_routes.py

# ──────────────────────────────────────────────────────────────
#  Maya AI Chat API
# ──────────────────────────────────────────────────────────────

@app.route('/api/chat', methods=['POST'])
@login_required
def api_chat():
    data = request.get_json(silent=True) or {}
    message    = (data.get('message') or '').strip()
    image_data = data.get('image')   # base64 data URL

    if not message and not image_data:
        return {'error': 'Empty message.'}, 400

    # ── Resolve Anthropic API key ───────────────────────────────────
    db            = get_db()
    anthropic_key = (_get_setting(db, 'anthropic_api_key', '') or '').strip() or os.environ.get('ANTHROPIC_API_KEY', '').strip()
    db.close()

    if not anthropic_key or not ANTHROPIC_AVAILABLE:
        return {'error': 'AI assistant not configured. Add Anthropic API key in Admin → Settings.'}, 503

    # ── Conversation history from session ──────────────────────────
    history = list(session.get('maya_history', []))

    # ── Decode image if provided ────────────────────────────────────
    b64_data   = None
    media_type = 'image/jpeg'
    if image_data:
        if ',' in image_data:
            header, b64_data = image_data.split(',', 1)
            media_type = header.split(';')[0].split(':')[1] if ':' in header else 'image/jpeg'
        else:
            b64_data = image_data

    text_for_ai = message or 'Is screenshot ko dekho aur specific, actionable advice do.'

    # ── Call Anthropic ───────────────────────────────────────────────
    reply    = None
    last_err = ''

    try:
        content = []
        if b64_data:
            content.append({'type': 'image', 'source': {
                'type': 'base64', 'media_type': media_type, 'data': b64_data}})
        content.append({'type': 'text', 'text': text_for_ai})

        ant_history = []
        for h in history:
            ant_history.append({'role': h['role'], 'content': h['content']})
        ant_history.append({'role': 'user', 'content': content})
        if len(ant_history) > 16:
            ant_history = ant_history[-16:]

        client   = _anthropic_lib.Anthropic(api_key=anthropic_key)
        response = client.messages.create(
            model='claude-haiku-4-5-20251001', max_tokens=1024,
            system=MAYA_SYSTEM_PROMPT, messages=ant_history
        )
        reply = response.content[0].text
    except Exception as e:
        last_err = str(e)

    # ── Failed ───────────────────────────────────────────────────────
    if reply is None:
        if '401' in last_err or '403' in last_err or 'api_key' in last_err.lower():
            return {'error': 'AI key is invalid — contact Admin.'}, 401
        return {'error': 'Maya is not available right now. Try again in a moment.'}, 503

    # ── Save history (text only) ────────────────────────────────────
    if image_data and not message:
        user_hist = '📸 [Screenshot shared]'
    elif image_data:
        user_hist = f'📸 {message}'
    else:
        user_hist = message

    history.append({'role': 'user',      'content': user_hist})
    history.append({'role': 'assistant', 'content': reply})
    if len(history) > 16:
        history = history[-16:]
    session['maya_history'] = history
    session.modified = True

    return {'reply': reply}


@app.route('/api/chat/clear', methods=['POST'])
@login_required
def api_chat_clear():
    session.pop('maya_history', None)
    session.modified = True
    return {'ok': True}



# ─────────────────────────────────────────────────────────────────
#  Working Section
# ─────────────────────────────────────────────────────────────────

# Working section Stage 1 = only leads ready for Day 1
STAGE1_STATUSES = ('Paid ₹196', 'Mindset Lock')

# My Leads enrollment statuses (for leads page filtering and pending-calls count)
ENROLLMENT_STATUSES = ('New Lead', 'New', 'Contacted', 'Invited',
                       'Video Sent', 'Video Watched', 'Paid ₹196', 'Mindset Lock')

PAST_STATUSES   = ('Fully Converted', 'Converted', 'Lost')


def _working_assigned_where(db, role, username, scope='all', downline_usernames=None):
    """
    Returns (sql_fragment, params) for WHERE clause in working() lead queries.
    scope: 'all' = admin sees all (assigned_to != ''); 'own' = leader/team own leads;
           'downline' = leader sees downline's leads (excludes self).
    downline_usernames: optional pre-fetched list for scope='downline' to avoid refetch.
    """
    if role == 'admin':
        return "AND assigned_to != ''", [] if scope == 'all' else (None, None)
    if role == 'leader':
        if scope == 'own':
            return "AND assigned_to=?", [username]
        if scope == 'downline':
            usernames = downline_usernames if downline_usernames is not None else [
                u for u in _get_network_usernames(db, username) if u != username
            ]
            if not usernames:
                return "AND 1=0", []
            ph = ','.join('?' * len(usernames))
            return f"AND assigned_to IN ({ph})", usernames
    # team
    return "AND assigned_to=?", [username]


@app.route('/working')
@login_required
@safe_route
def working():
    db       = get_db()
    username = session['username']
    today    = _today_ist().strftime('%Y-%m-%d')

    # Always fetch fresh role from DB so promotions take effect without re-login
    fresh_user = db.execute("SELECT role FROM users WHERE username=?", (username,)).fetchone()
    role = fresh_user['role'] if fresh_user else session.get('role', 'team')
    if role != session.get('role'):
        session['role'] = role   # sync session silently

    # Check seat_hold expiry
    _check_seat_hold_expiry(db, username)

    if role == 'admin':
        # ── Admin view (all leads with assigned_to set) ─────────────
        _admin_where, _admin_params = _working_assigned_where(db, 'admin', username, 'all')
        _admin_base = "FROM leads WHERE in_pool=0 AND deleted_at='' " + _admin_where + " "
        stage_placeholders = ','.join('?' * len(STAGE1_STATUSES))

        stage_counts = db.execute(
            "SELECT "
            "SUM(CASE WHEN status IN (" + stage_placeholders + ") THEN 1 ELSE 0 END) AS stage1, "
            "SUM(CASE WHEN status='Day 1' THEN 1 ELSE 0 END) AS day1, "
            "SUM(CASE WHEN status='Day 2' THEN 1 ELSE 0 END) AS day2, "
            "SUM(CASE WHEN status IN ('Interview','Track Selected') THEN 1 ELSE 0 END) AS day3, "
            "SUM(CASE WHEN status='Seat Hold Confirmed' THEN 1 ELSE 0 END) AS pending, "
            "SUM(CASE WHEN status IN ('Fully Converted','Converted') THEN 1 ELSE 0 END) AS converted "
            + _admin_base,
            list(STAGE1_STATUSES) + _admin_params
        ).fetchone()

        total_pipeline_value = db.execute(
            "SELECT COALESCE(SUM(track_price), 0) " + _admin_base + "AND status IN ('Seat Hold Confirmed','Track Selected')",
            _admin_params
        ).fetchone()[0] or 0

        # Team pipeline per member
        members = db.execute(
            "SELECT username, fbo_id FROM users WHERE role IN ('team','leader') AND status='approved' ORDER BY username"
        ).fetchall()
        team_pipeline = {}
        for m in members:
            uname = m['username']
            row = db.execute(f"""
                SELECT
                    SUM(CASE WHEN status IN ({stage_placeholders}) THEN 1 ELSE 0 END) AS stage1,
                    SUM(CASE WHEN status='Day 1' THEN 1 ELSE 0 END) AS day1,
                    SUM(CASE WHEN status='Day 2'             THEN 1 ELSE 0 END) AS day2,
                    SUM(CASE WHEN status IN ('Interview','Track Selected') THEN 1 ELSE 0 END) AS day3,
                    SUM(CASE WHEN status='Seat Hold Confirmed' THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status IN ('Fully Converted','Converted') THEN 1 ELSE 0 END) AS converted
                FROM leads WHERE assigned_to=? AND in_pool=0 AND deleted_at=''
            """, (*STAGE1_STATUSES, uname)).fetchone()
            score_pts, streak = _get_today_score(db, uname)
            team_pipeline[uname] = {
                'stage1': row['stage1'] or 0,
                'day1':   row['day1']   or 0,
                'day2':   row['day2']   or 0,
                'day3':   row['day3']   or 0,
                'pending': row['pending'] or 0,
                'converted': row['converted'] or 0,
                'score': score_pts,
                'fbo_id': m['fbo_id'] or '',
            }

        # Stale leads (not updated in 48h, not closed/lost)
        stale_cutoff = (_now_ist() - datetime.timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')
        stale_leads  = db.execute(
            "SELECT id, name, phone, assigned_to, status, updated_at "
            + _admin_base + "AND status NOT IN ('Fully Converted','Converted','Lost','Seat Hold Confirmed') AND updated_at < ? ORDER BY updated_at ASC",
            _admin_params + [stale_cutoff]
        ).fetchall()

        # Day 1/2 batch completion rate
        d1_total = db.execute(
            "SELECT COUNT(*) " + _admin_base + "AND status='Day 1'",
            _admin_params
        ).fetchone()[0] or 0
        d1_done  = db.execute(
            "SELECT COUNT(*) " + _admin_base + "AND status='Day 1' AND d1_morning=1 AND d1_afternoon=1 AND d1_evening=1",
            _admin_params
        ).fetchone()[0] or 0
        d2_total = db.execute(
            "SELECT COUNT(*) " + _admin_base + "AND status='Day 2'",
            _admin_params
        ).fetchone()[0] or 0
        d2_done  = db.execute(
            "SELECT COUNT(*) " + _admin_base + "AND status='Day 2' AND d2_morning=1 AND d2_afternoon=1 AND d2_evening=1",
            _admin_params
        ).fetchone()[0] or 0

        batch_completion = {
            'd1_total': d1_total, 'd1_done': d1_done,
            'd1_pct': round(d1_done / d1_total * 100) if d1_total else 0,
            'd2_total': d2_total, 'd2_done': d2_done,
            'd2_pct': round(d2_done / d2_total * 100) if d2_total else 0,
        }

        # Build batch_videos BEFORE closing database
        admin_batch_videos = {
            'd1_morning_v1': _get_setting(db, 'batch_d1_morning_v1', ''),
            'd1_morning_v2': _get_setting(db, 'batch_d1_morning_v2', ''),
            'd1_afternoon_v1': _get_setting(db, 'batch_d1_afternoon_v1', ''),
            'd1_afternoon_v2': _get_setting(db, 'batch_d1_afternoon_v2', ''),
            'd1_evening_v1': _get_setting(db, 'batch_d1_evening_v1', ''),
            'd1_evening_v2': _get_setting(db, 'batch_d1_evening_v2', ''),
            'd2_morning_v1': _get_setting(db, 'batch_d2_morning_v1', ''),
            'd2_morning_v2': _get_setting(db, 'batch_d2_morning_v2', ''),
            'd2_afternoon_v1': _get_setting(db, 'batch_d2_afternoon_v1', ''),
            'd2_afternoon_v2': _get_setting(db, 'batch_d2_afternoon_v2', ''),
            'd2_evening_v1': _get_setting(db, 'batch_d2_evening_v1', ''),
            'd2_evening_v2': _get_setting(db, 'batch_d2_evening_v2', ''),
        }
        enrollment_video_url   = _get_setting(db, 'enrollment_video_url', '')
        enrollment_video_title = _get_setting(db, 'enrollment_video_title', 'Enrollment Video')

        db.close()
        return render_template('working.html',
            is_admin=True,
            team_pipeline=team_pipeline,
            stage_counts=stage_counts,
            total_pipeline_value=total_pipeline_value,
            stale_leads=stale_leads,
            batch_completion=batch_completion,
            tracks=TRACKS,
            batch_videos=admin_batch_videos,
            batch_watch_urls=_batch_watch_urls(),
            enrollment_video_url=enrollment_video_url,
            enrollment_watch_url=url_for('watch_enrollment', _external=True) if enrollment_video_url else '',
            enrollment_video_title=enrollment_video_title,
            show_day1_batches=True,
            user_role='admin',
            call_status_values=CALL_STATUS_VALUES,
            csrf_token=session.get('_csrf_token', ''),
        )

    if role == 'leader':
        # Assigned filter: own = leader's leads, downline = team's leads (excludes self)
        _own_where, _own_params = _working_assigned_where(db, 'leader', username, 'own')
        try:
            _downline_only = [u for u in _get_network_usernames(db, username) if u != username]
        except Exception:
            _downline_only = []
        _team_where, _team_params = _working_assigned_where(db, 'leader', username, 'downline', _downline_only)

        _base = "SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' "
        _s1_ph = ','.join('?' * len(STAGE1_STATUSES))
        _e_ph  = ','.join('?' * len(ENROLLMENT_STATUSES))

        # ── OWN LEADS (leader's personal work) ──────────────────
        # own_stage1 includes ALL pre-Day-1 leads so enrollment video button is reachable
        own_stage1 = db.execute(
            _base + _own_where + f" AND status IN ({_e_ph}) ORDER BY updated_at DESC",
            _own_params + list(ENROLLMENT_STATUSES)
        ).fetchall()
        own_day1 = db.execute(
            _base + _own_where + " AND status='Day 1' ORDER BY updated_at DESC",
            _own_params
        ).fetchall()
        own_day2 = db.execute(
            _base + _own_where + " AND status='Day 2' ORDER BY updated_at DESC",
            _own_params
        ).fetchall()
        own_day3 = db.execute(
            _base + _own_where + " AND status IN ('Interview','Track Selected') ORDER BY updated_at DESC",
            _own_params
        ).fetchall()
        own_pending = db.execute(
            _base + _own_where + " AND status='Seat Hold Confirmed' ORDER BY updated_at DESC",
            _own_params
        ).fetchall()
        own_closing = db.execute(
            _base + _own_where + " AND status='Fully Converted' ORDER BY updated_at DESC",
            _own_params
        ).fetchall()
        own_past = db.execute(
            _base + _own_where + " AND status IN ('Converted','Lost') ORDER BY updated_at DESC LIMIT 20",
            _own_params
        ).fetchall()

        # ── TEAM LEADS (downline's work) ─────────────────────────
        if _team_params:
            _t_ph = ','.join('?' * len(_team_params))
            _team_base = f"SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' AND assigned_to IN ({_t_ph}) "
            # team_stage1: all pre-Day-1 leads so leader can track enrollment progress
            team_stage1 = db.execute(
                _team_base + f"AND status IN ({_e_ph}) ORDER BY assigned_to, updated_at DESC",
                _team_params + list(ENROLLMENT_STATUSES)
            ).fetchall()
            team_day1 = db.execute(
                _team_base + "AND status='Day 1' ORDER BY assigned_to, updated_at DESC",
                _team_params
            ).fetchall()
            team_day2 = db.execute(
                _team_base + "AND status='Day 2' ORDER BY assigned_to, updated_at DESC",
                _team_params
            ).fetchall()
            team_day3 = db.execute(
                _team_base + "AND status IN ('Interview','Track Selected') ORDER BY assigned_to, updated_at DESC",
                _team_params
            ).fetchall()
            team_pending = db.execute(
                _team_base + "AND status='Seat Hold Confirmed' ORDER BY assigned_to, updated_at DESC",
                _team_params
            ).fetchall()
            team_closing = db.execute(
                _team_base + "AND status='Fully Converted' ORDER BY assigned_to, updated_at DESC",
                _team_params
            ).fetchall()
            team_past = db.execute(
                _team_base + "AND status IN ('Converted','Lost') ORDER BY assigned_to, updated_at DESC LIMIT 50",
                _team_params
            ).fetchall()
            _d_phs = ','.join('?' * len(_downline_only))
            downline_members = db.execute(
                f"SELECT username, fbo_id FROM users WHERE username IN ({_d_phs}) AND status='approved' ORDER BY username",
                _downline_only
            ).fetchall()
        else:
            team_stage1 = team_day1 = team_day2 = team_day3 = []
            team_pending = team_closing = team_past = []
            downline_members = []

        # ── Pending action counts ────────────────────────────────
        def _row_val(r, key, default=None):
            try:
                return r[key] if key in r.keys() else default
            except Exception:
                return default
        own_pending_calls = sum(
            1 for l in own_stage1
            if not _row_val(l, 'call_result') or _row_val(l, 'call_result') in ('Follow Up Later','Callback Requested')
        )
        team_pending_calls = sum(
            1 for l in team_stage1
            if not _row_val(l, 'call_result') or _row_val(l, 'call_result') in ('Follow Up Later','Callback Requested')
        )
        own_batches_due = (
            sum(1 for l in own_day1 if not (_row_val(l, 'd1_morning') and _row_val(l, 'd1_afternoon') and _row_val(l, 'd1_evening'))) +
            sum(1 for l in own_day2 if not (_row_val(l, 'd2_morning') and _row_val(l, 'd2_afternoon') and _row_val(l, 'd2_evening')))
        )
        team_batches_due = (
            sum(1 for l in team_day1 if not (_row_val(l, 'd1_morning') and _row_val(l, 'd1_afternoon') and _row_val(l, 'd1_evening'))) +
            sum(1 for l in team_day2 if not (_row_val(l, 'd2_morning') and _row_val(l, 'd2_afternoon') and _row_val(l, 'd2_evening')))
        )

        # Leader today_actions (for sidebar badge on Working Section)
        _count_own = "SELECT COUNT(*) FROM leads WHERE in_pool=0 AND deleted_at='' " + _own_where + " "
        own_videos_to_send = db.execute(
            _count_own + " AND status='Contacted' AND (call_status IS NULL OR call_status='' OR call_status NOT IN ('Video Sent','Video Watched','Payment Done'))",
            _own_params
        ).fetchone()[0] or 0
        own_closings_due = db.execute(
            _count_own + " AND status IN ('Interview','Track Selected','Seat Hold Confirmed')",
            _own_params
        ).fetchone()[0] or 0
        leader_today_actions = {
            'pending_calls':  own_pending_calls,
            'videos_to_send': own_videos_to_send,
            'batches_due':    own_batches_due,
            'closings_due':   own_closings_due,
        }

        # ── Enrich all lists ─────────────────────────────────────
        own_stage1   = _enrich_leads(own_stage1)
        own_day1     = _enrich_leads(own_day1)
        own_day2     = _enrich_leads(own_day2)
        own_day3     = _enrich_leads(own_day3)
        own_pending  = _enrich_leads(own_pending)
        own_closing  = _enrich_leads(own_closing)
        team_stage1  = _enrich_leads(team_stage1)
        team_day1    = _enrich_leads(team_day1)
        team_day2    = _enrich_leads(team_day2)
        team_day3    = _enrich_leads(team_day3)
        team_pending = _enrich_leads(team_pending)
        team_closing = _enrich_leads(team_closing)

        today_score, streak = _get_today_score(db, username)

        # ── Batch videos (same as team member) ──────────────────
        leader_batch_videos = {
            'd1_morning_v1':   _get_setting(db, 'batch_d1_morning_v1', ''),
            'd1_morning_v2':   _get_setting(db, 'batch_d1_morning_v2', ''),
            'd1_afternoon_v1': _get_setting(db, 'batch_d1_afternoon_v1', ''),
            'd1_afternoon_v2': _get_setting(db, 'batch_d1_afternoon_v2', ''),
            'd1_evening_v1':   _get_setting(db, 'batch_d1_evening_v1', ''),
            'd1_evening_v2':   _get_setting(db, 'batch_d1_evening_v2', ''),
            'd2_morning_v1':   _get_setting(db, 'batch_d2_morning_v1', ''),
            'd2_morning_v2':   _get_setting(db, 'batch_d2_morning_v2', ''),
            'd2_afternoon_v1': _get_setting(db, 'batch_d2_afternoon_v1', ''),
            'd2_afternoon_v2': _get_setting(db, 'batch_d2_afternoon_v2', ''),
            'd2_evening_v1':   _get_setting(db, 'batch_d2_evening_v1', ''),
            'd2_evening_v2':   _get_setting(db, 'batch_d2_evening_v2', ''),
        }

        # ── Enroll To data (guarded so old DB or missing tables never crash leader view) ──
        enroll_days = {}
        enroll_pdfs = []
        recent_shares = []
        team_leads_for_enroll = []
        try:
            _ec_rows = db.execute(
                "SELECT * FROM enroll_content WHERE is_active=1 ORDER BY day_number, sort_order"
            ).fetchall()
            for _r in _ec_rows:
                _row_d = dict(_r)
                _d = _row_d.get('day_number', 1) or 1
                if _d not in enroll_days:
                    enroll_days[_d] = []
                enroll_days[_d].append(_row_d)

            enroll_pdfs = db.execute(
                "SELECT * FROM enroll_pdfs WHERE is_active=1 ORDER BY sort_order"
            ).fetchall()

            recent_shares = db.execute("""
                SELECT esl.*, ec.curiosity_title as video_title, ec.day_number as video_day
                FROM enroll_share_links esl
                JOIN enroll_content ec ON ec.id = esl.content_id
                WHERE esl.shared_by=?
                ORDER BY esl.created_at DESC LIMIT 15
            """, (username,)).fetchall()

            _all_leader_leads_phs = ','.join('?' * len([username] + list(_downline_only)))
            team_leads_for_enroll = db.execute(f"""
                SELECT id, name, phone, assigned_to FROM leads
                WHERE assigned_to IN ({_all_leader_leads_phs})
                  AND in_pool=0 AND deleted_at=''
                  AND status NOT IN ('Lost','Converted','Fully Converted')
                ORDER BY assigned_to, name
            """, [username] + list(_downline_only)).fetchall()
        except Exception:
            pass

        enrollment_video_url   = _get_setting(db, 'enrollment_video_url', '')
        enrollment_video_title = _get_setting(db, 'enrollment_video_title', 'Enrollment Video')

        # Tutorial / onboarding data for fully converted leads
        app_tutorial_link = _get_setting(db, 'app_tutorial_link', '')
        _leader_row = db.execute("SELECT fbo_id FROM users WHERE username=?", (username,)).fetchone()
        leader_fbo_id = (_leader_row['fbo_id'] if _leader_row and _leader_row['fbo_id'] else '')

        db.close()
        app_register_url = url_for('register', _external=True)

        return render_template('working.html',
            is_admin=False,
            is_leader=True,

            # Own leads
            own_stage1=own_stage1,
            own_day1=own_day1,
            own_day2=own_day2,
            own_day3=own_day3,
            own_pending=own_pending,
            own_closing=own_closing,
            own_past=own_past,

            # Team leads
            team_stage1=team_stage1,
            team_day1=team_day1,
            team_day2=team_day2,
            team_day3=team_day3,
            team_pending=team_pending,
            team_closing=team_closing,
            team_past=team_past,

            # Tutorial onboarding
            leader_fbo_id=leader_fbo_id,
            app_register_url=app_register_url,
            app_tutorial_link=app_tutorial_link,

            # Counts
            own_pending_calls=own_pending_calls,
            team_pending_calls=team_pending_calls,
            own_batches_due=own_batches_due,
            team_batches_due=team_batches_due,

            # Downline info
            downline_members=downline_members,
            has_team=bool(_downline_only),

            # Backward compatibility (some template parts may use these)
            stage1_leads=own_stage1 + team_stage1,
            day1_leads=own_day1 + team_day1,
            day2_leads=own_day2 + team_day2,
            day3_leads=own_day3 + team_day3,
            pending_leads=own_pending + team_pending,
            past_leads=own_past,

            today_score=today_score,
            streak=streak,
            today_actions=leader_today_actions,
            tracks=TRACKS,
            statuses=STATUSES,
            batch_videos=leader_batch_videos,
            user_role='leader',
            call_status_values=CALL_STATUS_VALUES,
            csrf_token=session.get('_csrf_token', ''),
            enroll_days=enroll_days,
            enroll_pdfs=enroll_pdfs,
            recent_shares=recent_shares,
            team_leads=team_leads_for_enroll,
            batch_watch_urls=_batch_watch_urls(),
            enrollment_video_url=enrollment_video_url,
            enrollment_watch_url=url_for('watch_enrollment', _external=True) if enrollment_video_url else '',
            enrollment_video_title=enrollment_video_title,
            show_day1_batches=True,
        )

    # ── Team member view (own leads only) ───────────────────────────
    _tw, _tp = _working_assigned_where(db, 'team', username, 'own')
    _base_team = "SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' " + _tw + " "
    _s1_ph = ','.join('?' * len(STAGE1_STATUSES))
    _e_ph = ','.join('?' * len(ENROLLMENT_STATUSES))

    # stage1_leads shows ALL pre-Day-1 leads (New Lead → Paid ₹196 → Mindset Lock)
    # so team members can see and send enrollment video to newly claimed leads
    stage1_leads = db.execute(
        _base_team + f"AND status IN ({_e_ph}) ORDER BY updated_at DESC",
        _tp + list(ENROLLMENT_STATUSES)
    ).fetchall()
    day1_leads = db.execute(
        _base_team + "AND status='Day 1' ORDER BY updated_at DESC",
        _tp
    ).fetchall()
    day2_leads = db.execute(
        _base_team + "AND status='Day 2' ORDER BY updated_at DESC",
        _tp
    ).fetchall()
    day3_leads = db.execute(
        _base_team + "AND status IN ('Interview','Track Selected') ORDER BY updated_at DESC",
        _tp
    ).fetchall()
    pending_leads = db.execute(
        _base_team + "AND status='Seat Hold Confirmed' ORDER BY updated_at DESC",
        _tp
    ).fetchall()
    past_leads = db.execute(
        _base_team + "AND status IN ('Fully Converted','Converted','Lost') ORDER BY updated_at DESC LIMIT 30",
        _tp
    ).fetchall()

    today_score, streak = _get_today_score(db, username)

    # Pending action counts (same assigned_to filter)
    _count_base = "SELECT COUNT(*) FROM leads WHERE in_pool=0 AND deleted_at='' " + _tw + " "
    pending_calls = db.execute(
        _count_base + f"AND status IN ({_e_ph}) AND (call_result='' OR call_result='Follow Up Later' OR call_result='Callback Requested')",
        _tp + list(ENROLLMENT_STATUSES)
    ).fetchone()[0] or 0
    videos_to_send = db.execute(
        _count_base + "AND status='Contacted' AND (call_status IS NULL OR call_status='' OR call_status NOT IN ('Video Sent','Video Watched','Payment Done'))",
        _tp
    ).fetchone()[0] or 0
    batches_due = (
        db.execute(
            _count_base + "AND status='Day 1' AND (d1_morning+d1_afternoon+d1_evening) < 3",
            _tp
        ).fetchone()[0] or 0
    ) + (
        db.execute(
            _count_base + "AND status='Day 2' AND (d2_morning+d2_afternoon+d2_evening) < 3",
            _tp
        ).fetchone()[0] or 0
    )
    closings_due = db.execute(
        _count_base + "AND status IN ('Interview','Track Selected','Seat Hold Confirmed')",
        _tp
    ).fetchone()[0] or 0

    today_actions = {
        'pending_calls':  pending_calls,
        'videos_to_send': videos_to_send,
        'batches_due':    batches_due,
        'closings_due':   closings_due,
    }

    # Build batch_videos BEFORE closing database
    team_batch_videos = {
        'd1_morning_v1': _get_setting(db, 'batch_d1_morning_v1', ''),
        'd1_morning_v2': _get_setting(db, 'batch_d1_morning_v2', ''),
        'd1_afternoon_v1': _get_setting(db, 'batch_d1_afternoon_v1', ''),
        'd1_afternoon_v2': _get_setting(db, 'batch_d1_afternoon_v2', ''),
        'd1_evening_v1': _get_setting(db, 'batch_d1_evening_v1', ''),
        'd1_evening_v2': _get_setting(db, 'batch_d1_evening_v2', ''),
        'd2_morning_v1': _get_setting(db, 'batch_d2_morning_v1', ''),
        'd2_morning_v2': _get_setting(db, 'batch_d2_morning_v2', ''),
        'd2_afternoon_v1': _get_setting(db, 'batch_d2_afternoon_v1', ''),
        'd2_afternoon_v2': _get_setting(db, 'batch_d2_afternoon_v2', ''),
        'd2_evening_v1': _get_setting(db, 'batch_d2_evening_v1', ''),
        'd2_evening_v2': _get_setting(db, 'batch_d2_evening_v2', ''),
    }
    enrollment_video_url   = _get_setting(db, 'enrollment_video_url', '')
    enrollment_video_title = _get_setting(db, 'enrollment_video_title', 'Enrollment Video')
    show_day1_batches     = (role or 'team') in ('leader', 'admin')

    # Enrich team view leads with heat + next_action
    stage1_leads  = _enrich_leads(stage1_leads)
    day1_leads    = _enrich_leads(day1_leads)
    day2_leads    = _enrich_leads(day2_leads)
    day3_leads    = _enrich_leads(day3_leads)
    pending_leads = _enrich_leads(pending_leads)
    db.close()
    return render_template('working.html',
        is_admin=False,
        stage1_leads=stage1_leads,
        day1_leads=day1_leads,
        day2_leads=day2_leads,
        day3_leads=day3_leads,
        pending_leads=pending_leads,
        past_leads=past_leads,
        today_score=today_score,
        streak=streak,
        today_actions=today_actions,
        tracks=TRACKS,
        statuses=STATUSES,
        batch_videos=team_batch_videos,
        batch_watch_urls=_batch_watch_urls(),
        enrollment_video_url=enrollment_video_url,
        enrollment_watch_url=url_for('watch_enrollment', _external=True) if enrollment_video_url else '',
        enrollment_video_title=enrollment_video_title,
        show_day1_batches=show_day1_batches,
        user_role=role or 'team',
        call_status_values=CALL_STATUS_VALUES,
        csrf_token=session.get('_csrf_token', ''),
    )


@app.route('/leads/<int:lead_id>/batch-share-url', methods=['POST'])
@login_required
def batch_share_url(lead_id):
    """Get tokenized watch URLs for this lead+slot. When prospect opens link, batch is auto-marked. No WhatsApp check needed."""
    data = request.get_json(silent=True) or {}
    slot = (data.get('slot') or '').strip()
    if slot not in _BATCH_SLOTS:
        return {'ok': False, 'error': 'Invalid slot'}, 400
    db = get_db()
    row = db.execute(
        "SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)
    ).fetchone()
    if not row:
        db.close()
        return {'ok': False, 'error': 'Not found'}, 404
    role = session.get('role', 'team')
    owner = row['assigned_to']
    if slot.startswith('d1_'):
        if role not in ('leader', 'admin'):
            db.close()
            return {'ok': False, 'error': 'Only leader/admin can share Day 1 batch links'}, 403
        if role == 'leader':
            downline = _get_network_usernames(db, session['username'])
            if owner != session['username'] and owner not in downline:
                db.close()
                return {'ok': False, 'error': 'Forbidden'}, 403
    else:
        if role != 'admin':
            db.close()
            return {'ok': False, 'error': 'Only admin can share Day 2 batch links'}, 403
    existing = db.execute(
        "SELECT token FROM batch_share_links WHERE lead_id=? AND slot=? AND used=0", (lead_id, slot)
    ).fetchone()
    if existing:
        token = existing['token']
    else:
        token = secrets.token_urlsafe(16)
        db.execute(
            "INSERT INTO batch_share_links (token, lead_id, slot) VALUES (?, ?, ?)",
            (token, lead_id, slot)
        )
        db.commit()
    db.close()
    watch_url_v1 = _public_external_url('watch_batch', slot=slot, v=1) + '?token=' + token
    watch_url_v2 = _public_external_url('watch_batch', slot=slot, v=2) + '?token=' + token
    return {'ok': True, 'watch_url_v1': watch_url_v1, 'watch_url_v2': watch_url_v2}


@app.route('/leads/<int:lead_id>/batch-toggle', methods=['POST'])
@login_required
def batch_toggle(lead_id):
    data  = request.get_json(silent=True) or {}
    batch = data.get('batch', '')
    VALID = ('d1_morning', 'd1_afternoon', 'd1_evening',
             'd2_morning', 'd2_afternoon', 'd2_evening')
    if batch not in VALID:
        return {'ok': False, 'error': 'Invalid batch'}, 400

    db  = get_db()
    row = db.execute(
        "SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)
    ).fetchone()
    if not row:
        db.close(); return {'ok': False, 'error': 'Not found'}, 404

    role  = session.get('role', 'team')
    owner = row['assigned_to']

    # Day 1 batches: only leader or admin can mark (leader runs/tracks Day 1 sessions)
    if batch.startswith('d1_'):
        if role not in ('leader', 'admin'):
            db.close(); return {'ok': False, 'error': 'Only leader/admin can mark Day 1 batches'}, 403
        if role == 'leader':
            downline = _get_network_usernames(db, session['username'])
            if owner != session['username'] and owner not in downline:
                db.close(); return {'ok': False, 'error': 'Forbidden'}, 403
    elif batch.startswith('d2_'):
        # Day 2 batches: admin only
        if role != 'admin':
            db.close(); return {'ok': False, 'error': 'Only admin can mark Day 2 batches'}, 403
    else:
        # Other batches: team can mark own; admin unrestricted
        if role != 'admin' and owner != session['username']:
            db.close(); return {'ok': False, 'error': 'Forbidden'}, 403

    # Toggle (or force-mark if force_mark=true, used by "already sent" button)
    force_mark = data.get('force_mark', False)
    current  = row[batch]
    if force_mark:
        new_val = 1
    else:
        new_val  = 0 if current else 1
    delta_pts = 15 if (new_val == 1 and current == 0) else (-15 if (new_val == 0 and current == 1) else 0)

    db.execute(
        f"UPDATE leads SET {batch}=?, updated_at=? WHERE id=?",
        (new_val, _now_ist().strftime('%Y-%m-%d %H:%M:%S'), lead_id)
    )

    # Check if all 3 batches done for the day
    day_prefix = batch[:2]  # 'd1' or 'd2'
    if day_prefix == 'd1':
        m, a, e = (
            (new_val if batch == 'd1_morning'   else row['d1_morning']),
            (new_val if batch == 'd1_afternoon' else row['d1_afternoon']),
            (new_val if batch == 'd1_evening'   else row['d1_evening']),
        )
        all_done = bool(m and a and e)
        if all_done:
            db.execute("UPDATE leads SET day1_done=1 WHERE id=?", (lead_id,))
        else:
            db.execute("UPDATE leads SET day1_done=0 WHERE id=?", (lead_id,))
        new_status = None
    else:
        m, a, e = (
            (new_val if batch == 'd2_morning'   else row['d2_morning']),
            (new_val if batch == 'd2_afternoon' else row['d2_afternoon']),
            (new_val if batch == 'd2_evening'   else row['d2_evening']),
        )
        all_done = bool(m and a and e)
        if all_done:
            db.execute("UPDATE leads SET day2_done=1 WHERE id=?", (lead_id,))
        else:
            db.execute("UPDATE leads SET day2_done=0 WHERE id=?", (lead_id,))
        new_status = None

    _upsert_daily_score(db, owner, delta_pts, delta_batches=(1 if new_val else -1))
    today_score, _ = _get_today_score(db, owner)
    db.commit()
    db.close()

    return {
        'ok': True,
        'new_val': new_val,
        'all_done': all_done,
        'points': delta_pts,
        'today_score': today_score,
    }


@app.route('/leads/<int:lead_id>/quick-advance', methods=['POST'])
@login_required
def quick_advance(lead_id):
    db  = get_db()
    row = db.execute(
        "SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)
    ).fetchone()
    if not row:
        db.close(); return {'ok': False, 'error': 'Not found'}, 404

    owner = row['assigned_to']
    if session.get('role') != 'admin' and owner != session['username']:
        db.close(); return {'ok': False, 'error': 'Forbidden'}, 403

    current  = row['status']
    new_status = None
    score_delta = 0

    # Stage advancement map
    if current == 'Mindset Lock':
        new_status = 'Day 1'
    elif current == 'Day 1':
        if row['d1_morning'] and row['d1_afternoon'] and row['d1_evening']:
            new_status = 'Day 2'
        else:
            db.close()
            return {'ok': False, 'error': 'Complete all Day 1 batches first'}, 400
    elif current == 'Day 2':
        if row['d2_morning'] and row['d2_afternoon'] and row['d2_evening']:
            new_status = 'Interview'
        else:
            db.close()
            return {'ok': False, 'error': 'Complete all Day 2 batches first'}, 400
    elif current == 'Interview':
        new_status = 'Track Selected'
    elif current == 'Track Selected':
        new_status = 'Seat Hold Confirmed'
        score_delta = 50
    elif current == 'Seat Hold Confirmed':
        new_status = 'Fully Converted'
        score_delta = 100
    else:
        db.close()
        return {'ok': False, 'error': f'No advance rule for status: {current}'}, 400

    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    db.execute(
        "UPDATE leads SET status=?, updated_at=? WHERE id=?",
        (new_status, now_str, lead_id)
    )
    # status → stage mapping
    if new_status == 'Day 1':
        new_stage = 'day1'
    elif new_status == 'Day 2':
        new_stage = 'day2'
    elif new_status == 'Interview':
        new_stage = 'day3'
    elif new_status == 'Seat Hold Confirmed':
        new_stage = 'seat_hold'
    elif new_status == 'Fully Converted':
        new_stage = 'closing'
    else:
        new_stage = None

    if new_stage:
        _transition_stage(db, lead_id, new_stage, session['username'], status_override=new_status)
    _log_lead_event(db, lead_id, session['username'], f'Status → {new_status} (quick advance)')
    _log_activity(db, session['username'], 'quick_advance',
                  f'{row["name"]} → {new_status}')

    if score_delta:
        payments = 1 if new_status == 'Seat Hold Confirmed' else 0
        _upsert_daily_score(db, owner, score_delta, delta_payments=payments)

    new_badges = _check_and_award_badges(db, owner)
    today_score, _ = _get_today_score(db, owner)
    db.commit()
    db.close()

    return {
        'ok': True,
        'new_status': new_status,
        'today_score': today_score,
        'new_badges': [BADGE_DEFS[k]['label'] for k in new_badges if k in BADGE_DEFS],
    }



# ──────────────────────────────────────────────────────────────────────
#  Pipeline Stage Advance (Part 5)
# ──────────────────────────────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/stage-advance', methods=['POST'])
@login_required
def stage_advance(lead_id):
    """Advance a lead to the next pipeline stage."""
    data = request.get_json(silent=True) or {}
    action = data.get('action', '')
    role = session.get('role', 'team')
    username = session['username']

    ACTION_MAP = {
        'enroll_complete':   (['admin'], 'day1'),          # team uses Payment Done call_status instead
        'day1_complete':     (['team', 'leader', 'admin'],  'day2'),
        'day2_complete':     (['admin'],                    'day3'),
        'interview_done':    (['leader', 'admin'],          'day3'),
        'seat_hold_done':    (['leader', 'admin'],          'seat_hold'),
        'fully_converted':   (['admin'],                    'closing'),
        'training_complete': (['admin'],                    'complete'),
        'mark_lost':         (['team', 'leader', 'admin'],  'lost'),
    }

    if action not in ACTION_MAP:
        return {'ok': False, 'error': 'Invalid action'}, 400

    allowed_roles, new_stage = ACTION_MAP[action]
    if role not in allowed_roles:
        return {'ok': False, 'error': 'Permission denied'}, 403

    db = get_db()
    lead = db.execute("SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)).fetchone()

    if not lead:
        db.close()
        return {'ok': False, 'error': 'Lead not found'}, 404

    lead_keys = lead.keys()

    # ── Ownership check: team can only act on own leads;
    #    leader can act on own + downline; admin unrestricted ──
    if role == 'team':
        if lead['assigned_to'] != username:
            db.close()
            return {'ok': False, 'error': 'You can only advance your own leads'}, 403
    elif role == 'leader':
        downline = _get_network_usernames(db, username)
        if lead['assigned_to'] != username and lead['assigned_to'] not in downline:
            db.close()
            return {'ok': False, 'error': 'You can only advance your own or downline leads'}, 403

    # ── Stage machine guard: validate the current stage allows this action ──
    VALID_FROM = {
        'enroll_complete':   ('enrollment',),
        'day1_complete':     ('day1',),
        'day2_complete':     ('day2',),
        'interview_done':    ('day2', 'day3'),
        'seat_hold_done':    ('day3',),
        'fully_converted':   ('seat_hold', 'closing'),
        'training_complete': ('training',),
        'mark_lost':         ('enrollment', 'day1', 'day2', 'day3', 'seat_hold', 'closing'),
    }
    current_stage = lead['pipeline_stage'] if 'pipeline_stage' in lead_keys else 'enrollment'
    valid_from = VALID_FROM.get(action, ())
    if valid_from and current_stage not in valid_from:
        db.close()
        return {'ok': False, 'error': f'Lead is at stage "{current_stage}" — cannot perform "{action}" from here'}, 400

    if action == 'seat_hold_done':
        track_sel = lead['track_selected'] if 'track_selected' in lead_keys else ''
        if not track_sel:
            db.close()
            return {'ok': False, 'error': 'Track select karo pehle (track_selected required)'}, 400
        expiry = (_now_ist() + datetime.timedelta(hours=48)).strftime('%Y-%m-%d %H:%M:%S')
        db.execute("UPDATE leads SET seat_hold_expiry=? WHERE id=?", (expiry, lead_id))

    if action == 'interview_done':
        now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
        new_stage_result, new_owner = _transition_stage(db, lead_id, 'day3', username, status_override='Track Selected')
        db.execute("UPDATE leads SET interview_done=1, updated_at=? WHERE id=?", (now_str, lead_id))
        db.commit()
        _log_activity(db, username, 'stage_advance', f'Lead #{lead_id} {action} to {new_stage_result}')
        db.close()
        stage_labels = {
            'enrollment': 'Enrollment',
            'day1': 'Day 1',
            'day2': 'Day 2',
            'day3': 'Day 3 / Interview',
            'seat_hold': 'Seat Hold',
            'closing': 'Closing / Fully Converted',
            'training': 'Training',
            'complete': 'Converted',
            'lost': 'Lost',
        }
        return {
            'ok': True,
            'new_stage': new_stage_result,
            'new_owner': new_owner,
            'message': f'Stage updated to {stage_labels.get(new_stage_result, new_stage_result)}',
        }

    new_stage_result, new_owner = _transition_stage(db, lead_id, new_stage, username)
    _log_activity(db, username, 'stage_advance', f'Lead #{lead_id} {action} to {new_stage_result}')
    db.close()

    stage_labels = {
        'enrollment': 'Enrollment',
        'day1': 'Day 1',
        'day2': 'Day 2',
        'day3': 'Day 3 / Interview',
        'seat_hold': 'Seat Hold',
        'closing': 'Closing / Fully Converted',
        'training': 'Training',
        'complete': 'Converted',
        'lost': 'Lost',
    }
    return {
        'ok': True,
        'new_stage': new_stage_result,
        'new_owner': new_owner,
        'message': f'Stage updated to {stage_labels.get(new_stage_result, new_stage_result)}',
    }


@app.route('/leads/<int:lead_id>/call-status', methods=['POST'])
@login_required
def update_call_status(lead_id):
    """Update call_status. Team/leader can update own or downline leads; admin can update any."""
    data = request.get_json(silent=True) or {}
    call_status = (data.get('call_status') or '').strip()

    if not call_status or call_status not in CALL_STATUS_VALUES:
        return {'ok': False, 'error': 'Invalid or missing call_status'}, 400

    db = get_db()
    lead = db.execute("SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)).fetchone()
    if not lead:
        db.close()
        return {'ok': False, 'error': 'Not found'}, 404

    role = session.get('role', 'team')
    username = session['username']

    # Admin: any lead. Team: only assigned_to self. Leader: self or downline.
    if role == 'admin':
        pass
    elif role == 'leader':
        downline = _get_network_usernames(db, username)
        if lead['assigned_to'] != username and lead['assigned_to'] not in downline:
            db.close()
            return {'ok': False, 'error': 'You can only update call status for your own or downline leads'}, 403
    else:
        if lead['assigned_to'] != username:
            db.close()
            return {'ok': False, 'error': 'Only the assigned member can update call status'}, 403

    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    db.execute("UPDATE leads SET call_status=?, updated_at=? WHERE id=?",
               (call_status, now_str, lead_id))
    _log_activity(db, username, 'call_status_update', f'Lead #{lead_id} call_status={call_status}')

    # ── AUTO STATUS UPDATE based on call_status ──────────────
    # Only move status FORWARD — never backward
    _STATUS_ORDER = [
        'New Lead', 'New', 'Contacted', 'Invited',
        'Video Sent', 'Video Watched', 'Paid ₹196', 'Mindset Lock',
        'Day 1', 'Day 2', 'Interview', 'Track Selected',
        'Seat Hold Confirmed', 'Fully Converted', 'Training', 'Converted'
    ]
    current_status = lead['status'] or 'New'
    cur_idx = _STATUS_ORDER.index(current_status) if current_status in _STATUS_ORDER else 0
    _call_to_status = {
        'Called - No Answer':    'Contacted',
        'Called - Interested':   'Contacted',
        'Called - Follow Up':    'Contacted',
        'Called - Not Interested': None,
        'Video Sent':            'Video Sent',
        'Video Watched':         'Video Watched',
        'Payment Done':          'Paid ₹196',
    }
    new_auto_status = _call_to_status.get(call_status)
    if new_auto_status:
        new_idx = _STATUS_ORDER.index(new_auto_status) if new_auto_status in _STATUS_ORDER else 0
        if new_idx > cur_idx:
            # Sync pipeline_stage alongside status to keep them consistent
            new_auto_stage = STATUS_TO_STAGE.get(new_auto_status, 'enrollment')
            db.execute(
                "UPDATE leads SET status=?, pipeline_stage=?, updated_at=? WHERE id=?",
                (new_auto_status, new_auto_stage, now_str, lead_id)
            )

    # Gamification: award points for call actions
    pts = 0
    delta_calls    = 0
    delta_payments = 0
    delta_videos   = 0
    if call_status in ('Called - Interested', 'Called - No Answer', 'Called - Follow Up'):
        pts += 5; delta_calls += 1
    elif call_status == 'Video Sent':
        pts += 10; delta_videos += 1
    elif call_status == 'Payment Done':
        pts += 25; delta_payments += 1
    if pts:
        _upsert_daily_score(db, username, pts,
                            delta_calls=delta_calls,
                            delta_videos=delta_videos,
                            delta_payments=delta_payments)

    # Auto-advance enrollment → day1 when "Payment Done" is set
    # Requires payment_done=1 to be recorded first to prevent bypass
    stage_advanced = False
    if call_status == 'Payment Done':
        # If user marks "Payment Done" from call status, record it here.
        # This removes the extra manual step and keeps the flow consistent.
        if not lead['payment_done']:
            try:
                db.execute(
                    "UPDATE leads SET payment_done=1, payment_amount=?, updated_at=? WHERE id=?",
                    (PAYMENT_AMOUNT, now_str, lead_id),
                )
                lead = dict(lead)
                lead['payment_done'] = 1
            except Exception:
                pass
        lead_stage = lead['pipeline_stage'] if 'pipeline_stage' in lead.keys() else 'enrollment'
        if lead_stage == 'enrollment':
            _transition_stage(db, lead_id, 'day1', username, status_override='Day 1')
            stage_advanced = True
            db.execute(
                "UPDATE leads SET payment_done=1, payment_amount=?, updated_at=? WHERE id=?",
                (PAYMENT_AMOUNT, now_str, lead_id)
            )

    db.commit()
    # Defer badge check so API returns immediately (no 3s wait for user)
    def _defer_badges():
        try:
            _db = get_db()
            _check_and_award_badges(_db, username)
            _db.commit()
            _db.close()
        except Exception:
            pass
    threading.Thread(target=_defer_badges, daemon=True).start()
    db.close()
    return {'ok': True, 'call_status': call_status, 'stage_advanced': stage_advanced}


@app.route('/admin/members/<username>/promote-leader', methods=['POST'])
@admin_required
def admin_promote_leader(username):
    """Toggle a team member between team and leader roles."""
    db = get_db()
    user = db.execute("SELECT role FROM users WHERE username=?", (username,)).fetchone()
    if not user:
        db.close()
        flash('Member not found.', 'danger')
        return redirect(url_for('admin_members'))

    current_role = user['role']
    if current_role == 'team':
        new_role = 'leader'
        msg = f'{username} promoted to Leader.'
    elif current_role == 'leader':
        new_role = 'team'
        msg = f'{username} demoted back to Team.'
    else:
        db.close()
        flash('Only team/leader roles can be toggled.', 'warning')
        return redirect(url_for('admin_members'))

    db.execute("UPDATE users SET role=? WHERE username=?", (new_role, username))
    db.commit()
    _log_activity(db, session['username'], 'role_change', f'{username}: {current_role} to {new_role}')
    db.close()
    flash(msg, 'success')
    return redirect(url_for('admin_members'))


@app.route('/admin/members/<username>/set-upline', methods=['POST'])
@admin_required
def admin_set_upline(username):
    """Admin assigns an upline leader to a team member."""
    upline_username = request.form.get('upline_username', '').strip()
    db = get_db()
    user = db.execute("SELECT role FROM users WHERE username=?", (username,)).fetchone()
    if not user or user['role'] not in ('team', 'leader'):
        db.close()
        flash('Member not found or invalid role.', 'danger')
        return redirect(url_for('admin_members'))

    if upline_username:
        leader = db.execute(
            "SELECT username FROM users WHERE username=? AND role='leader' AND status='approved'",
            (upline_username,)
        ).fetchone()
        if not leader:
            db.close()
            flash(f'Leader "{upline_username}" not found or not approved.', 'danger')
            return redirect(url_for('admin_members'))

    db.execute("UPDATE users SET upline_username=?, upline_name=? WHERE username=?", (upline_username, upline_username, username))
    db.commit()
    _log_activity(db, session['username'], 'set_upline',
                  f'{username} upline → {upline_username or "(none)"}')
    db.close()
    flash(f'Upline for @{username} set to @{upline_username or "none"}', 'success')
    return redirect(url_for('admin_members'))


@app.route('/api/today-score')
@login_required
def api_today_score():
    db = get_db()
    score, streak = _get_today_score(db, session['username'])
    db.close()
    return {'ok': True, 'score': score, 'streak': streak}


@app.route('/team/day2-progress')
@login_required
def day2_progress():
    if session.get('role') != 'admin':
        flash('Access denied. Day 2 Board is admin only.', 'danger')
        return redirect(url_for('team_dashboard'))
    db = get_db()
    now = _now_ist()

    # All Day 2 leads visible to admin
    day2_leads = db.execute("""
        SELECT l.*,
               CAST((julianday('now','localtime') - julianday(l.updated_at)) * 24 AS INTEGER) AS hours_since_update
        FROM leads l
        WHERE l.in_pool=0 AND l.deleted_at='' AND l.status='Day 2'
        ORDER BY (l.d2_morning + l.d2_afternoon + l.d2_evening) DESC, l.updated_at ASC
    """).fetchall()

    # Summary counts
    complete_count    = sum(1 for l in day2_leads if l['d2_morning'] and l['d2_afternoon'] and l['d2_evening'])
    in_progress_count = sum(1 for l in day2_leads if 0 < (l['d2_morning']+l['d2_afternoon']+l['d2_evening']) < 3)
    not_started_count = sum(1 for l in day2_leads if (l['d2_morning']+l['d2_afternoon']+l['d2_evening']) == 0)

    can_edit = session.get('role') == 'admin'
    username = session['username']

    # Build leader map: assigned_to → upline_name (for admin view)
    leader_map = {}
    if can_edit and day2_leads:
        usernames_list = list(set(l['assigned_to'] for l in day2_leads if l['assigned_to']))
        if usernames_list:
            ph = ','.join('?' * len(usernames_list))
            urows = db.execute(
                f"SELECT username, upline_username, upline_name FROM users WHERE username IN ({ph})",
                usernames_list
            ).fetchall()
            for r in urows:
                leader_map[r['username']] = r['upline_username'] or r['upline_name'] or '—'

    # Day 2 batch videos for quick access
    d2_videos = {
        'morning_v1':   _get_setting(db, 'batch_d2_morning_v1', ''),
        'morning_v2':   _get_setting(db, 'batch_d2_morning_v2', ''),
        'afternoon_v1': _get_setting(db, 'batch_d2_afternoon_v1', ''),
        'afternoon_v2': _get_setting(db, 'batch_d2_afternoon_v2', ''),
        'evening_v1':   _get_setting(db, 'batch_d2_evening_v1', ''),
        'evening_v2':   _get_setting(db, 'batch_d2_evening_v2', ''),
    }

    db.close()
    return render_template('day2_progress.html',
        day2_leads=day2_leads,
        complete_count=complete_count,
        in_progress_count=in_progress_count,
        not_started_count=not_started_count,
        can_edit=can_edit,
        current_user=username,
        leader_map=leader_map,
        d2_videos=d2_videos,
        csrf_token=session.get('_csrf_token', ''),
    )


# ─────────────────────────────────────────────────────────────────
#  Module 1 — Prospect Timeline (JSON endpoint)
# ─────────────────────────────────────────────────────────────────

@app.route('/leads/<int:lead_id>/timeline')
@login_required
@safe_route
def lead_timeline(lead_id):
    """Return full prospect history: stage transitions, notes, call events."""
    db       = get_db()
    role     = session.get('role', 'team')
    username = session['username']

    lead = db.execute(
        "SELECT id, name, phone, pipeline_stage, assigned_to, current_owner, status "
        "FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''",
        (lead_id,)
    ).fetchone()

    if not lead:
        db.close()
        return jsonify({'ok': False, 'error': 'Lead not found'}), 404

    # Ownership gate
    if role == 'team' and lead['assigned_to'] != username:
        db.close()
        return jsonify({'ok': False, 'error': 'Access denied'}), 403
    elif role == 'leader':
        network = _get_network_usernames(db, username)
        if lead['assigned_to'] not in network:
            db.close()
            return jsonify({'ok': False, 'error': 'Access denied'}), 403
    # admin: no restriction

    stages = db.execute(
        "SELECT stage, owner, triggered_by, created_at "
        "FROM lead_stage_history WHERE lead_id=? ORDER BY created_at ASC",
        (lead_id,)
    ).fetchall()

    notes = db.execute(
        "SELECT username, note, created_at "
        "FROM lead_notes WHERE lead_id=? ORDER BY created_at ASC",
        (lead_id,)
    ).fetchall()

    call_events = db.execute(
        "SELECT username, event_type, details, created_at FROM activity_log "
        "WHERE details LIKE ? ORDER BY created_at ASC LIMIT 60",
        (f'Lead #{lead_id} %',)
    ).fetchall()

    db.close()
    return jsonify({
        'ok':         True,
        'lead':       dict(lead),
        'stages':     [dict(r) for r in stages],
        'notes':      [dict(r) for r in notes],
        'call_events': [dict(r) for r in call_events],
    })


# ─────────────────────────────────────────────────────────────────
#  Module 3 — Leader Coaching Panel
# ─────────────────────────────────────────────────────────────────

@app.route('/leader/coaching')
@login_required
@safe_route
def leader_coaching():
    """Coaching panel: each downline member's pipeline state + stuck leads."""
    role     = session.get('role', 'team')
    username = session['username']

    if role not in ('leader', 'admin'):
        flash('Access denied.', 'danger')
        return redirect(url_for('team_dashboard'))

    db    = get_db()
    today = _today_ist().strftime('%Y-%m-%d')
    stale24_cutoff = (_now_ist() - datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')

    # Which leaders to show coaching cards for
    if role == 'admin':
        leader_rows = db.execute(
            "SELECT username FROM users WHERE role='leader' AND status='approved' ORDER BY username"
        ).fetchall()
        leaders_to_show = [r['username'] for r in leader_rows]
    else:
        leaders_to_show = [username]

    coaching_cards = []
    for leader_uname in leaders_to_show:
        downline_all = _get_downline_usernames(db, leader_uname)
        members      = [u for u in downline_all if u != leader_uname]

        for member in members:
            m_leads = db.execute("""
                SELECT id, name, pipeline_stage, updated_at,
                       d1_morning, d1_afternoon, d1_evening,
                       d2_morning, d2_afternoon, d2_evening
                FROM leads
                WHERE assigned_to=? AND in_pool=0 AND deleted_at=''
                  AND pipeline_stage NOT IN ('complete','lost')
            """, (member,)).fetchall()

            active_count = len(m_leads)
            stuck_leads  = [dict(l) for l in m_leads
                            if (l['updated_at'] or '') < stale24_cutoff]
            stuck_count  = len(stuck_leads)

            # Batch completion %
            d1_leads = [l for l in m_leads if l['pipeline_stage'] == 'day1']
            d2_leads = [l for l in m_leads if l['pipeline_stage'] == 'day2']
            total_possible = (len(d1_leads) + len(d2_leads)) * 3
            batches_done = (
                sum((l['d1_morning'] or 0) + (l['d1_afternoon'] or 0) + (l['d1_evening'] or 0)
                    for l in d1_leads) +
                sum((l['d2_morning'] or 0) + (l['d2_afternoon'] or 0) + (l['d2_evening'] or 0)
                    for l in d2_leads)
            )
            batch_pct = round(batches_done / total_possible * 100) if total_possible else 100

            # Today's calls from daily_scores
            score_row = db.execute(
                "SELECT calls_made, total_points, streak_days FROM daily_scores "
                "WHERE username=? AND score_date=?", (member, today)
            ).fetchone()
            calls_today = score_row['calls_made']   if score_row else 0
            pts_today   = score_row['total_points'] if score_row else 0

            # Stage breakdown
            stage_counts = {}
            for l in m_leads:
                s = l['pipeline_stage'] or 'enrollment'
                stage_counts[s] = stage_counts.get(s, 0) + 1

            coaching_cards.append({
                'username':     member,
                'upline':       leader_uname,
                'active_count': active_count,
                'stuck_count':  stuck_count,
                'stuck_leads':  stuck_leads[:3],
                'batch_pct':    batch_pct,
                'calls_today':  calls_today,
                'pts_today':    pts_today,
                'stage_counts': stage_counts,
            })

    # Sort: most stuck first, then most active
    coaching_cards.sort(key=lambda c: (-c['stuck_count'], -c['active_count']))

    # Summary totals
    total_active = sum(c['active_count'] for c in coaching_cards)
    total_stuck  = sum(c['stuck_count']  for c in coaching_cards)

    db.close()
    return render_template('leader_coaching.html',
        coaching_cards=coaching_cards,
        role=role,
        total_active=total_active,
        total_stuck=total_stuck,
        csrf_token=session.get('_csrf_token', ''),
    )


# ─────────────────────────────────────────────────────────────────
#  Module 4 — Admin Pipeline Funnel Analytics
# ─────────────────────────────────────────────────────────────────

@app.route('/admin/pipeline-analytics')
@admin_required
@safe_route
def admin_pipeline_analytics():
    """Pipeline health: stage funnel, time-in-stage, member comparison, bottlenecks."""
    db = get_db()

    # 1. Stage funnel counts
    STAGE_ORDER = ['enrollment', 'day1', 'day2', 'day3', 'seat_hold', 'closing', 'training']
    funnel_rows = db.execute("""
        SELECT pipeline_stage, COUNT(*) as lead_count
        FROM leads
        WHERE in_pool=0 AND deleted_at='' AND pipeline_stage NOT IN ('complete','lost','')
        GROUP BY pipeline_stage
    """).fetchall()
    funnel_map = {r['pipeline_stage']: r['lead_count'] for r in funnel_rows}
    stage_labels = ['Enrollment', 'Day 1', 'Day 2', 'Day 3', 'Seat Hold', 'Closing', 'Training']
    stage_counts = [funnel_map.get(s, 0) for s in STAGE_ORDER]

    # 2. Avg time-in-stage (correlated self-join on lead_stage_history)
    stage_time_rows = db.execute("""
        SELECT
            h1.stage,
            COUNT(*) AS transitions,
            CAST(AVG(
                (julianday(COALESCE(h2.created_at, datetime('now','localtime')))
               - julianday(h1.created_at)) * 86400
            ) AS INTEGER) AS avg_seconds,
            CAST(MAX(
                (julianday(COALESCE(h2.created_at, datetime('now','localtime')))
               - julianday(h1.created_at)) * 86400
            ) AS INTEGER) AS max_seconds
        FROM lead_stage_history h1
        LEFT JOIN lead_stage_history h2
          ON h2.lead_id = h1.lead_id
         AND h2.id = (
               SELECT MIN(id) FROM lead_stage_history
               WHERE lead_id = h1.lead_id AND id > h1.id
             )
        GROUP BY h1.stage
    """).fetchall()

    # Build ordered stage_time list
    stage_time_map = {r['stage']: dict(r) for r in stage_time_rows}
    stage_time = []
    for s in STAGE_ORDER:
        row = stage_time_map.get(s, {'stage': s, 'transitions': 0, 'avg_seconds': 0, 'max_seconds': 0})
        row['avg_hours'] = round((row['avg_seconds'] or 0) / 3600, 1)
        row['max_hours'] = round((row['max_seconds'] or 0) / 3600, 1)
        row['is_bottleneck'] = (row['avg_seconds'] or 0) > 86400 * 2  # > 48h average
        stage_time.append(row)

    # 3. Bottlenecks: stages with leads stuck > 48h
    bottleneck_rows = db.execute("""
        SELECT pipeline_stage, COUNT(*) as stuck_count
        FROM leads
        WHERE in_pool=0 AND deleted_at='' AND pipeline_stage NOT IN ('complete','lost','')
          AND updated_at < datetime('now','localtime','-48 hours')
        GROUP BY pipeline_stage
        ORDER BY stuck_count DESC
    """).fetchall()
    bottlenecks = [dict(r) for r in bottleneck_rows if r['stuck_count'] > 0]

    # 4. Member pipeline comparison
    member_rows = db.execute("""
        SELECT
            l.assigned_to,
            u.upline_username,
            u.upline_name,
            COUNT(*) AS total_leads,
            SUM(CASE WHEN l.pipeline_stage='day1' THEN 1 ELSE 0 END) AS day1_count,
            SUM(CASE WHEN l.pipeline_stage='day2' THEN 1 ELSE 0 END) AS day2_count,
            SUM(CASE WHEN l.pipeline_stage='day3' THEN 1 ELSE 0 END) AS day3_count,
            SUM(CASE WHEN l.pipeline_stage='seat_hold' THEN 1 ELSE 0 END) AS seat_hold_count,
            SUM(CASE WHEN l.pipeline_stage IN ('closing','complete') THEN 1 ELSE 0 END) AS converted_count,
            SUM(CASE WHEN l.pipeline_stage NOT IN ('complete','lost','')
                      AND l.updated_at < datetime('now','localtime','-48 hours') THEN 1 ELSE 0 END) AS stuck_count,
            ROUND(CAST(SUM(CASE WHEN l.pipeline_stage IN ('closing','complete') THEN 1 ELSE 0 END) AS REAL)
                  / NULLIF(COUNT(*), 0) * 100, 1) AS conv_pct
        FROM leads l
        JOIN users u ON u.username = l.assigned_to
        WHERE l.in_pool=0 AND l.deleted_at='' AND l.assigned_to != ''
          AND u.role IN ('team','leader') AND u.status='approved'
        GROUP BY l.assigned_to
        ORDER BY converted_count DESC, total_leads DESC
    """).fetchall()
    member_stats = [dict(r) for r in member_rows]

    # 5. Heat score distribution
    heat_rows = db.execute("""
        SELECT
            CASE
                WHEN (d1_morning+d1_afternoon+d1_evening+d2_morning+d2_afternoon+d2_evening) >= 5 THEN 'hot'
                WHEN pipeline_stage IN ('day3','seat_hold','closing') THEN 'hot'
                WHEN pipeline_stage IN ('day1','day2') THEN 'warm'
                ELSE 'cold'
            END as heat_band,
            COUNT(*) as cnt
        FROM leads
        WHERE in_pool=0 AND deleted_at='' AND pipeline_stage NOT IN ('complete','lost','')
        GROUP BY heat_band
    """).fetchall()
    heat_map   = {r['heat_band']: r['cnt'] for r in heat_rows}
    heat_data  = [heat_map.get('hot', 0), heat_map.get('warm', 0), heat_map.get('cold', 0)]

    db.close()
    return render_template('admin_pipeline.html',
        stage_labels=stage_labels,
        stage_counts=stage_counts,
        stage_time=stage_time,
        bottlenecks=bottlenecks,
        member_stats=member_stats,
        heat_data=heat_data,
        csrf_token=session.get('_csrf_token', ''),
    )


if __name__ == '__main__':
    _debug = os.environ.get('FLASK_DEBUG', '').lower() in ('1', 'true', 'yes')
    app.run(debug=_debug, host='0.0.0.0', port=int(os.environ.get('PORT', 5003)))
