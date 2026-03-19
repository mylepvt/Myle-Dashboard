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
try:
    import pytz
    _IST = pytz.timezone('Asia/Kolkata')
except ImportError:
    pytz = None
    _IST = None


def _now_ist():
    """Current datetime in IST as a naive datetime (safe for DB storage & strptime comparisons)."""
    if _IST:
        return datetime.datetime.now(_IST).replace(tzinfo=None)
    return datetime.datetime.now()   # fallback: server local time


def _today_ist():
    """Current date in IST."""
    if _IST:
        return datetime.datetime.now(_IST).date()
    return datetime.date.today()     # fallback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from urllib.parse import quote as _url_quote
from flask import (Flask, render_template, request, redirect, url_for,
                   flash, session, Response, make_response, abort, send_from_directory, jsonify)
from werkzeug.security import generate_password_hash, check_password_hash
from database import get_db, init_db, migrate_db, seed_users, seed_training_questions
from pathlib import Path

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
    # Stable fallback for dev / when SECRET_KEY env var is not set.
    # Sessions survive restarts but are NOT cryptographically secret if the
    # code is public.  Set SECRET_KEY env var on Render for full security.
    import sys as _sys
    app.secret_key = 'myle_community_secret_2024_local'
    print('[SECURITY WARNING] SECRET_KEY env var not set — using default fallback. '
          'Set SECRET_KEY in your Render environment for production security!',
          file=_sys.stderr)

app.permanent_session_lifetime = datetime.timedelta(days=3650)  # ~10 years = effectively forever
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
# Only send session cookie over HTTPS in production (when SECRET_KEY env var is set)
app.config['SESSION_COOKIE_SECURE'] = bool(os.environ.get('SECRET_KEY'))

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

STATUSES = ['New Lead', 'New', 'Contacted', 'Invited', 'Video Sent', 'Video Watched',
            'Paid ₹196', 'Mindset Lock',
            'Day 1', 'Day 2', 'Interview',
            'Track Selected', 'Seat Hold Confirmed', 'Fully Converted',
            'Training', 'Converted', 'Lost', 'Retarget']

# Pipeline stage mapping — always sync when status changes
STATUS_TO_STAGE = {
    'New Lead':            'enrollment',
    'New':                 'enrollment',
    'Contacted':           'enrollment',
    'Invited':             'enrollment',
    'Video Sent':          'enrollment',
    'Video Watched':       'enrollment',
    'Paid ₹196':           'enrollment',
    'Mindset Lock':        'enrollment',
    'Day 1':               'day1',
    'Day 2':               'day2',
    'Interview':           'day3',
    'Track Selected':      'day3',
    'Seat Hold Confirmed': 'seat_hold',
    'Fully Converted':     'closing',
    'Training':            'training',
    'Converted':           'complete',
    'Lost':                'lost',
    'Retarget':            'enrollment',
}

# Call status values (use regular hyphens, NOT em-dashes)
CALL_STATUS_VALUES = [
    'Not Called Yet',
    'Called - No Answer',
    'Called - Interested',
    'Called - Not Interested',
    'Called - Follow Up',
    'Called - Switch Off',
    'Called - Busy',
    'Call Back',
    'Wrong Number',
    'Video Sent',
    'Video Watched',
    'Payment Done',
    'Already forever',
    'Retarget',
]

TRACKS = {
    'Slow Track':   {'price': 8000,  'seat_hold': 2000},
    'Medium Track': {'price': 18000, 'seat_hold': 4000},
    'Fast Track':   {'price': 38000, 'seat_hold': 5000},
}

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
FOLLOWUP_TAGS = ('Follow Up Later', 'Callback Requested', 'Call Not Picked',
                 'Phone Switched Off', 'Not Reachable')
SOURCES  = ['WhatsApp', 'Facebook', 'Instagram', 'LinkedIn',
            'Walk-in', 'Referral', 'YouTube', 'Cold Call', 'Meta', 'Other']
BADGE_DEFS = {
    'first_sale':   {'label': 'First Sale',      'icon': 'bi-trophy-fill',       'color': '#f59e0b',
                     'desc': 'Convert your first lead'},
    'ten_leads':    {'label': 'Getting Started',  'icon': 'bi-person-plus-fill',  'color': '#6366f1',
                     'desc': 'Add 10 leads'},
    'century':      {'label': 'Century',          'icon': 'bi-123',               'color': '#0891b2',
                     'desc': 'Add 100 leads'},
    'payment_10':   {'label': '₹1960 Club',       'icon': 'bi-cash-stack',        'color': '#059669',
                     'desc': '10 payments collected'},
    'seat_hold_5':  {'label': 'Seat Holder',      'icon': 'bi-shield-fill-check', 'color': '#7c3aed',
                     'desc': '5 seat holds confirmed'},
    'fully_conv_1': {'label': 'Track Master',     'icon': 'bi-star-fill',         'color': '#d97706',
                     'desc': 'First fully converted lead'},
    'streak_7':     {'label': '7-Day Streak',     'icon': 'bi-fire',              'color': '#ef4444',
                     'desc': 'Submit daily report 7 days in a row'},
}
PAYMENT_AMOUNT = 196.0


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Auth Decorators
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
        app.logger.error(f"_check_session_valid() DB error: {e}")
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
            app.logger.error(f"Route {request.path} crashed: {e}\n{_tb.format_exc()}")
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
                return {'ok': False, 'error': 'Something went wrong, please try again'}, 500
            flash('Kuch gadbad ho gayi. Please dubara try karein.', 'danger')
            if session.get('role') == 'admin':
                return redirect(url_for('admin_dashboard'))
            if 'username' in session:
                return redirect(url_for('team_dashboard'))
            return redirect(url_for('login'))
    return decorated


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Helpers
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

def _get_downline_usernames(db, username):
    """Return [username] + all recursive downline usernames via upline_name chain."""
    rows = db.execute("""
        WITH RECURSIVE downline(uname) AS (
            SELECT ?
            UNION ALL
            SELECT u.username FROM users u JOIN downline d ON (u.upline_name = d.uname OR u.upline_username = d.uname)
        )
        SELECT uname FROM downline
    """, (username,)).fetchall()
    return [r['uname'] for r in rows]


def _log_activity(db, username, event_type, details=''):
    """Log a user activity event (login, lead_update, etc.)."""
    try:
        ip = request.remote_addr or ''
    except Exception:
        ip = ''
    try:
        db.execute(
            "INSERT INTO activity_log (username, event_type, details, ip_address, created_at) VALUES (?, ?, ?, ?, ?)",
            (username, event_type, details, ip, _now_ist().strftime('%Y-%m-%d %H:%M:%S'))
        )
        db.commit()
    except Exception:
        pass


def _log_lead_event(db, lead_id, username, note):
    """Insert a timeline entry for a lead."""
    db.execute(
        "INSERT INTO lead_notes (lead_id, username, note, created_at) VALUES (?,?,?,?)",
        (lead_id, username, note, _now_ist().strftime('%Y-%m-%d %H:%M:%S'))
    )


def _check_and_award_badges(db, username):
    """Check thresholds and award new badges. Returns list of newly awarded badge keys."""
    try:
        m = _get_metrics(db, username)
        existing = {r['badge_key'] for r in db.execute(
            "SELECT badge_key FROM user_badges WHERE username=?", (username,)
        ).fetchall()}

        streak = db.execute("""
            SELECT COUNT(*) FROM (
                SELECT report_date FROM daily_reports
                WHERE username=? ORDER BY report_date DESC LIMIT 7
            ) t
        """, (username,)).fetchone()[0]

        seat_holds = db.execute(
            "SELECT COUNT(*) FROM leads WHERE assigned_to=? AND status='Seat Hold Confirmed' AND in_pool=0",
            (username,)
        ).fetchone()[0]

        to_award = []
        checks = [
            ('first_sale',   m.get('converted', 0) >= 1),
            ('ten_leads',    m.get('total', 0) >= 10),
            ('century',      m.get('total', 0) >= 100),
            ('payment_10',   m.get('paid', 0) >= 10),
            ('seat_hold_5',  seat_holds >= 5),
            ('fully_conv_1', db.execute(
                "SELECT COUNT(*) FROM leads WHERE assigned_to=? AND status='Fully Converted' AND in_pool=0",
                (username,)).fetchone()[0] >= 1),
            ('streak_7',     streak >= 7),
        ]
        for key, condition in checks:
            if condition and key not in existing:
                to_award.append(key)

        for key in to_award:
            try:
                db.execute(
                    "INSERT OR IGNORE INTO user_badges (username, badge_key) VALUES (?,?)",
                    (username, key)
                )
            except Exception:
                pass

        return to_award
    except Exception:
        return []


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


def _get_metrics(db, username=None):
    """All dashboard KPIs. Excludes pool and soft-deleted leads."""
    if username:
        where_clause = "WHERE assigned_to = ? AND in_pool = 0 AND deleted_at = ''"
        params = (username,)
        base = "assigned_to = ? AND in_pool = 0 AND deleted_at = ''"
    else:
        where_clause = "WHERE in_pool = 0 AND deleted_at = ''"
        params = ()
        base = "in_pool = 0 AND deleted_at = ''"

    row = db.execute(f"""
        SELECT
            COUNT(*)                                                      AS total,
            SUM(CASE WHEN status IN ('Converted','Fully Converted')
                      THEN 1 ELSE 0 END)                                  AS converted,
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
                CAST(SUM(CASE WHEN status IN ('Converted','Fully Converted')
                              THEN 1 ELSE 0 END) AS REAL)
                / NULLIF(COUNT(*), 0) * 100
            , 1)                                                          AS close_pct,
            ROUND(
                SUM(COALESCE(payment_amount,0) + COALESCE(revenue,0))
                / NULLIF(COUNT(*), 0)
            , 2)                                                          AS rev_per_lead
        FROM leads {where_clause}
    """, params).fetchone()

    track_sel    = db.execute(f"SELECT COUNT(*) FROM leads WHERE {base} AND status='Track Selected'", params).fetchone()[0] or 0
    seat_hold    = db.execute(f"SELECT COUNT(*) FROM leads WHERE {base} AND status='Seat Hold Confirmed'", params).fetchone()[0] or 0
    fully_conv   = db.execute(f"SELECT COUNT(*) FROM leads WHERE {base} AND status='Fully Converted'", params).fetchone()[0] or 0
    seat_rev     = db.execute(f"SELECT COALESCE(SUM(seat_hold_amount),0) FROM leads WHERE {base} AND status='Seat Hold Confirmed'", params).fetchone()[0] or 0
    final_rev    = db.execute(f"SELECT COALESCE(SUM(track_price),0) FROM leads WHERE {base} AND status='Fully Converted'", params).fetchone()[0] or 0

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
        track_sel    = track_sel,
        seat_hold    = seat_hold,
        fully_conv   = fully_conv,
        seat_rev     = seat_rev,
        final_rev    = final_rev,
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
    """Compute wallet stats for a team member.
    Balance is not stored: recharged = SUM(wallet_recharges.amount) where status='approved',
    spent = SUM(leads.pool_price) where assigned_to=user and in_pool=0 and claimed_at set.
    Updated when: recharges approved or admin adjust (recharged); claim_leads (spent via leads).
    """
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

    balance = round(float(recharged) - float(spent), 2)
    return {
        'recharged': round(float(recharged), 2),
        'spent':     round(float(spent), 2),
        'balance':   balance,
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


def _get_network_usernames(db, username):
    """
    Return the list of usernames visible to `username`.
    Includes the user themselves + ALL recursive downlines (BFS tree walk).
    """
    visible = {username}
    queue   = [username]
    while queue:
        current  = queue.pop(0)
        downlines = db.execute(
            "SELECT username FROM users WHERE (upline_name=? OR upline_username=?) AND status='approved'",
            (current, current)
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



# ──────────────────────────────────────────────────────────────────────
#  Pipeline Helpers (Part 4)
# ──────────────────────────────────────────────────────────────────────

def _get_admin_username(db):
    """Return username of first admin user."""
    row = db.execute("SELECT username FROM users WHERE role='admin' LIMIT 1").fetchone()
    return row['username'] if row else 'admin'


def _get_leader_for_user(db, username):
    """Return the direct leader/upline username for a given user."""
    if not username:
        return _get_admin_username(db)
    row = db.execute(
        "SELECT upline_name, upline_username FROM users WHERE username=?", (username,)
    ).fetchone()
    if not row:
        return _get_admin_username(db)
    # Check upline_username (new field) then upline_name (existing populated field)
    leader = (row['upline_username'] or '').strip() or (row['upline_name'] or '').strip()
    if not leader:
        return _get_admin_username(db)
    lrow = db.execute(
        "SELECT username FROM users WHERE username=? AND status='approved'", (leader,)
    ).fetchone()
    return lrow['username'] if lrow else _get_admin_username(db)


def _calculate_priority(lead):
    """Compute a priority score for a lead (call on every query, never cache)."""
    score = 0
    today = _today_ist().isoformat()
    keys = lead.keys() if hasattr(lead, 'keys') else []
    status = lead['status'] if 'status' in keys else ''
    if status == 'Video Watched':
        score += 20
    payment_done = lead['payment_done'] if 'payment_done' in keys else 0
    if payment_done:
        score += 40
    follow_up_date = lead['follow_up_date'] if 'follow_up_date' in keys else ''
    if follow_up_date and follow_up_date[:10] == today:
        score += 50
    created_at = lead['created_at'] if 'created_at' in keys else ''
    if created_at and created_at[:10] == today:
        score += 10
    return score


def _leads_with_priority(raw_leads):
    """Return list of dicts sorted by priority score descending."""
    result = []
    for lead in raw_leads:
        d = dict(lead)
        d['priority_score'] = _calculate_priority(lead)
        result.append(d)
    result.sort(key=lambda x: (-x['priority_score'], x.get('created_at', '')))
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  Heat Score Engine
# ─────────────────────────────────────────────────────────────────────────────
def _calculate_heat_score(lead):
    """Return 0-100 heat score: call_status signal + stage + recency + follow-up."""
    score = 0
    keys  = lead.keys() if hasattr(lead, 'keys') else []
    get   = lambda k, d='': lead[k] if k in keys else d

    # Call status signal
    score += {
        'Payment Done':         40,
        'Video Watched':        25,
        'Called - Interested':  20,
        'Called - Follow Up':   15,
        'Video Sent':           10,
        'Called - No Answer':    5,
    }.get(get('call_status'), 0)

    # Pipeline stage signal
    stage = get('pipeline_stage', 'enrollment')
    if stage in ('day3', 'seat_hold'):
        score += 20
    elif stage in ('day1', 'day2'):
        score += 10

    # Recency (days since updated_at)
    upd = get('updated_at') or get('created_at', '')
    if upd:
        try:
            upd_date = datetime.datetime.strptime(upd[:10], '%Y-%m-%d').date()
            days_old = (_today_ist() - upd_date).days
            if   days_old <= 1: score += 20
            elif days_old <= 3: score += 10
            elif days_old >= 8: score -= 15
        except Exception:
            pass

    # Follow-up date
    today_str = _today_ist().isoformat()
    fu = get('follow_up_date', '')
    if fu:
        if   fu[:10] == today_str: score += 20
        elif fu[:10] <  today_str: score -= 10  # overdue

    return max(0, min(100, int(score)))


# ─────────────────────────────────────────────────────────────────────────────
#  Next Action Engine
# ─────────────────────────────────────────────────────────────────────────────
def _get_next_action(lead):
    """Return {action, type, priority} — the single most important next step."""
    keys = lead.keys() if hasattr(lead, 'keys') else []
    get  = lambda k, d='': lead[k] if k in keys else d

    stage        = get('pipeline_stage', 'enrollment')
    call_status  = get('call_status', '')
    status       = get('status', '')
    payment_done = int(get('payment_done', 0) or 0)

    if stage == 'enrollment':
        if not call_status or call_status == 'Not Called Yet':
            return {'action': 'Make first call', 'type': 'urgent', 'priority': 1}
        if call_status == 'Called - Interested' and status != 'Video Sent':
            return {'action': 'Send video now', 'type': 'urgent', 'priority': 1}
        if call_status in ('Called - No Answer', 'Called - Follow Up'):
            return {'action': 'Follow-up call', 'type': 'today', 'priority': 2}
        if call_status == 'Video Watched' and not payment_done:
            return {'action': 'Call for payment', 'type': 'urgent', 'priority': 1}
        if payment_done and status != 'Mindset Lock':
            return {'action': 'Mindset Lock call', 'type': 'today', 'priority': 2}
        if status == 'Mindset Lock':
            return {'action': 'Move to Day 1', 'type': 'today', 'priority': 2}
        return {'action': 'Follow up', 'type': 'followup', 'priority': 3}

    if stage == 'day1':
        d1_m = int(get('d1_morning', 0) or 0)
        d1_a = int(get('d1_afternoon', 0) or 0)
        d1_e = int(get('d1_evening', 0) or 0)
        rem   = 3 - (d1_m + d1_a + d1_e)
        if rem > 0:
            return {'action': f'{rem} batch(es) left', 'type': 'today', 'priority': 2}
        return {'action': 'Send to Day 2', 'type': 'today', 'priority': 2}

    if stage == 'day2':
        return {'action': 'Admin conducting', 'type': 'followup', 'priority': 4}

    if stage == 'day3':
        if not int(get('interview_done', 0) or 0):
            return {'action': 'Do interview', 'type': 'urgent', 'priority': 1}
        if not int(get('track_selected', 0) or 0):
            return {'action': 'Select track', 'type': 'urgent', 'priority': 1}
        return {'action': 'Confirm Seat Hold', 'type': 'urgent', 'priority': 1}

    if stage == 'seat_hold':
        expiry_str = get('seat_hold_expiry', '')
        if expiry_str:
            try:
                expiry = datetime.datetime.strptime(expiry_str[:19], '%Y-%m-%d %H:%M:%S')
                now    = _now_ist().replace(tzinfo=None)
                hours  = (expiry - now).total_seconds() / 3600
                if hours < 12:
                    return {'action': f'URGENT: {max(0,int(hours))}h mein expire!',
                            'type': 'urgent', 'priority': 0}
            except Exception:
                pass
        return {'action': 'Final payment follow up', 'type': 'followup', 'priority': 3}

    if stage in ('closing', 'training'):
        return {'action': 'In closing process', 'type': 'followup', 'priority': 4}

    if stage in ('complete', 'lost'):
        return {'action': '—', 'type': 'cold', 'priority': 9}

    return {'action': 'Follow up', 'type': 'followup', 'priority': 3}


def _generate_ai_tip(lead):
    """Return a Hindi/Hinglish AI tip string based on lead state."""
    get   = lambda k, d='': lead.get(k, d) if isinstance(lead, dict) else (
        lead[k] if hasattr(lead, 'keys') and k in lead.keys() else d)
    stage        = get('pipeline_stage', 'enrollment')
    heat         = int(get('heat', _calculate_heat_score(lead)))
    name         = get('name', 'Prospect')
    call_status  = get('call_status', '')
    payment_done = int(get('payment_done', 0) or 0)
    today_str    = _today_ist().isoformat()

    # Calculate days in pipeline
    created = get('created_at', '')
    days_in = 0
    if created:
        try:
            days_in = (_today_ist() - datetime.datetime.strptime(created[:10], '%Y-%m-%d').date()).days
        except Exception:
            pass

    # Seat hold expiry check
    expiry_str = get('seat_hold_expiry', '')
    expiry_soon = False
    if expiry_str:
        try:
            expiry = datetime.datetime.strptime(expiry_str[:19], '%Y-%m-%d %H:%M:%S')
            hours  = (expiry - _now_ist().replace(tzinfo=None)).total_seconds() / 3600
            expiry_soon = hours < 24
        except Exception:
            pass

    # Day1 batches
    d1_done = int(get('d1_morning', 0) or 0) + int(get('d1_afternoon', 0) or 0) + int(get('d1_evening', 0) or 0)

    # Rule-based tips (order matters — most specific first)
    if stage == 'seat_hold' and expiry_soon:
        return f"⚠️ {name}'s seat hold expires soon — final call today is a must!"
    if stage == 'day1' and d1_done == 3:
        return f"✅ All batches complete! Move {name} to Day 2 now."
    if stage == 'enrollment' and heat >= 75:
        return f"🔥 {name} looks very interested — try to convert today."
    if stage == 'enrollment' and call_status == 'Video Watched' and not payment_done:
        return f"👀 {name} has watched the video — make a strong payment call now."
    if stage == 'enrollment' and call_status == 'Payment Done':
        return f"💰 Payment confirmed! Move {name} to Day 1 and do Mindset Lock call."
    if stage == 'enrollment' and days_in > 5 and heat < 30:
        return f"❄️ {name} has been stuck for {days_in}d and going cold — do a strong follow-up call."
    if stage == 'enrollment' and (not call_status or call_status == 'Not Called Yet'):
        return f"📞 {name} has not been called yet — contact today."
    if stage == 'day1' and d1_done < 3:
        return f"⏳ {name} has {d1_done}/3 batches done — remind for the rest."
    if stage == 'day2':
        return f"🎓 {name} is in Day 2 — schedule interview with admin."
    if stage == 'day3':
        return f"🏁 {name} is at interview stage — get track selected and confirm seat hold."
    if stage == 'seat_hold':
        return f"🛡️ {name} is on seat hold — follow up for final payment."
    if heat < 40 and days_in > 3:
        return f"❄️ {name} inactive for {days_in}d — call once and update status."
    return f"📋 Maintain regular follow up with {name}."


def _enrich_lead(lead):
    """Add heat, next_action, next_action_type to a lead. Returns a dict."""
    d  = dict(lead)
    # Ensure fields exist that template accesses directly (prevent UndefinedError)
    for k in ('day1_batch', 'day2_batch', 'day3_batch',
              'heat', 'next_action', 'next_action_type'):
        d.setdefault(k, '' if 'batch' in k else 0 if k == 'heat' else '')
    # Guard NULL-prone fields used with [:10] slicing in templates
    for k in ('created_at', 'updated_at', 'claimed_at', 'follow_up_date'):
        if d.get(k) is None:
            d[k] = ''
    try:
        na = _get_next_action(lead)
        d['heat']             = _calculate_heat_score(lead)
        d['next_action']      = na['action']
        d['next_action_type'] = na['type']
    except Exception:
        d['heat']             = 0
        d['next_action']      = ''
        d['next_action_type'] = 'cold'
    return d


def _enrich_leads(lead_list):
    """Enrich a list of SQLite rows / dicts with heat + next_action fields."""
    return [_enrich_lead(l) for l in lead_list]


# ─────────────────────────────────────────────────────────────────────────────
#  Badge Award System
# ─────────────────────────────────────────────────────────────────────────────
BADGE_META = {
    'hot_streak':    ('🔥', 'Hot Streak',    '7+ din active raho'),
    'speed_closer':  ('⚡', 'Speed Closer',  'Enrollment → Day1 in ≤3 days'),
    'money_maker':   ('💰', 'Money Maker',   '5+ payments collected'),
    'first_convert': ('🏆', 'Converter',     'Pehli full conversion'),
    'rising_star':   ('⭐', 'Rising Star',   'Week ka top scorer'),
    'centurion':     ('💯', 'Centurion',     '10,000+ total points'),
    'batch_master':  ('📚', 'Batch Master',  '100 batches marked total'),
}


def _check_and_award_badges(db, username):
    """Check badge conditions and award new ones. Returns list of new badge keys."""
    try:
        return _check_and_award_badges_inner(db, username)
    except Exception:
        return []


def _check_and_award_badges_inner(db, username):
    new_badges = []
    today  = _today_ist().strftime('%Y-%m-%d')
    mon    = (_today_ist() - datetime.timedelta(days=_today_ist().weekday())).strftime('%Y-%m-%d')

    def _already_has(key):
        return db.execute(
            "SELECT 1 FROM user_badges WHERE username=? AND badge_key=?", (username, key)
        ).fetchone() is not None

    def _award(key):
        db.execute(
            "INSERT OR IGNORE INTO user_badges (username, badge_key) VALUES (?,?)",
            (username, key)
        )
        new_badges.append(key)

    # hot_streak: 7+ consecutive active days
    streak_row = db.execute(
        "SELECT streak_days FROM daily_scores WHERE username=? AND score_date=?",
        (username, today)
    ).fetchone()
    if streak_row and (streak_row['streak_days'] or 0) >= 7 and not _already_has('hot_streak'):
        _award('hot_streak')

    # speed_closer: any lead went from claimed to day1 in ≤3 days
    if not _already_has('speed_closer'):
        fast = db.execute("""
            SELECT COUNT(*) as cnt FROM leads
            WHERE assigned_to=? AND in_pool=0
              AND pipeline_stage IN ('day1','day2','day3','seat_hold','closing','complete')
              AND claimed_at != ''
              AND julianday(datetime(working_date)) - julianday(datetime(claimed_at)) <= 3
        """, (username,)).fetchone()
        if fast and (fast['cnt'] or 0) > 0:
            _award('speed_closer')

    # money_maker: 5+ payments
    payments = db.execute(
        "SELECT COUNT(*) as cnt FROM leads WHERE assigned_to=? AND payment_done=1 AND in_pool=0 AND deleted_at=''",
        (username,)
    ).fetchone()
    if payments and (payments['cnt'] or 0) >= 5 and not _already_has('money_maker'):
        _award('money_maker')

    # first_convert: any fully converted lead
    if not _already_has('first_convert'):
        conv = db.execute(
            "SELECT COUNT(*) as cnt FROM leads WHERE assigned_to=? AND status IN ('Converted','Fully Converted') AND in_pool=0",
            (username,)
        ).fetchone()
        if conv and (conv['cnt'] or 0) > 0:
            _award('first_convert')

    # centurion: 10000+ total points
    pts = db.execute(
        "SELECT COALESCE(SUM(total_points),0) as p FROM daily_scores WHERE username=?",
        (username,)
    ).fetchone()
    if pts and (pts['p'] or 0) >= 10000 and not _already_has('centurion'):
        _award('centurion')

    # batch_master: 100+ batches marked
    batches = db.execute(
        "SELECT COALESCE(SUM(batches_marked),0) as b FROM daily_scores WHERE username=?",
        (username,)
    ).fetchone()
    if batches and (batches['b'] or 0) >= 100 and not _already_has('batch_master'):
        _award('batch_master')

    # rising_star: top scorer this week (check at end of day actions)
    if not _already_has('rising_star'):
        top = db.execute("""
            SELECT username, SUM(total_points) as wpts
            FROM daily_scores WHERE score_date >= ?
            GROUP BY username ORDER BY wpts DESC LIMIT 1
        """, (mon,)).fetchone()
        if top and top['username'] == username:
            _award('rising_star')

    return new_badges


def _get_user_badges_emoji(db, username):
    """Return a space-joined emoji string for user's badges."""
    rows = db.execute(
        "SELECT badge_key FROM user_badges WHERE username=?", (username,)
    ).fetchall()
    return ' '.join(BADGE_META[r['badge_key']][0] for r in rows if r['badge_key'] in BADGE_META)


# Canonical stage -> default status (for transitions that don't provide an override)
STAGE_TO_DEFAULT_STATUS = {
    'enrollment': 'New Lead',   # fallback if a lead is reset to enrollment
    'day1':       'Day 1',
    'day2':       'Day 2',
    'day3':       'Interview',
    'seat_hold':  'Seat Hold Confirmed',
    'closing':    'Fully Converted',
    'training':   'Training',
    'complete':   'Converted',
    'lost':       'Lost',
}


def _transition_stage(db, lead_id, new_stage, triggered_by, status_override=None):
    """
    Move a lead to a new pipeline stage: update pipeline_stage, current_owner,
    optionally status (when status_override or stage default), and log to lead_stage_history.
    Returns (new_stage, new_owner).
    When status_override is provided, it is used so status always matches the caller's intent.
    """
    lead = db.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
    if not lead:
        return new_stage, ''

    lead_keys = lead.keys()
    current_stage = lead['pipeline_stage'] if 'pipeline_stage' in lead_keys else 'enrollment'
    current_owner = lead['current_owner'] if 'current_owner' in lead_keys else ''

    # Determine new_owner based on transition
    if current_stage == 'enrollment' and new_stage == 'day1':
        new_owner = _get_leader_for_user(db, lead['assigned_to'])
    elif current_stage == 'day1' and new_stage == 'day2':
        new_owner = _get_admin_username(db)
    elif current_stage == 'day2' and new_stage == 'day3':
        hist = db.execute(
            "SELECT owner FROM lead_stage_history WHERE lead_id=? AND stage='day1' ORDER BY created_at DESC LIMIT 1",
            (lead_id,)
        ).fetchone()
        new_owner = hist['owner'] if hist else _get_leader_for_user(db, lead['assigned_to'])
    elif current_stage == 'day3' and new_stage == 'seat_hold':
        new_owner = current_owner
    elif new_stage in ('closing', 'training', 'complete'):
        new_owner = _get_admin_username(db)
    elif new_stage == 'lost':
        new_owner = current_owner
    else:
        new_owner = current_owner or _get_admin_username(db)

    # One status value: caller override or stage default (ensures status maps to pipeline_stage)
    new_status = status_override if status_override is not None else STAGE_TO_DEFAULT_STATUS.get(new_stage)
    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')

    if new_status is not None:
        db.execute(
            "UPDATE leads SET pipeline_stage=?, current_owner=?, status=?, updated_at=? WHERE id=?",
            (new_stage, new_owner, new_status, now_str, lead_id)
        )
    else:
        db.execute(
            "UPDATE leads SET pipeline_stage=?, current_owner=?, updated_at=? WHERE id=?",
            (new_stage, new_owner, now_str, lead_id)
        )

    db.execute(
        "INSERT INTO lead_stage_history (lead_id, stage, owner, triggered_by, created_at) VALUES (?,?,?,?,?)",
        (lead_id, new_stage, new_owner, triggered_by, now_str)
    )

    if new_stage == 'training':
        _trigger_training_unlock(db, lead)

    db.commit()
    return new_stage, new_owner


def _sync_enroll_share_to_lead(db, token, username):
    """
    Called when a share link is generated.
    Auto-updates lead status, call_status, daily_scores.
    Safe to call multiple times — checks synced_to_lead flag.
    """
    try:
        link = db.execute(
            "SELECT * FROM enroll_share_links WHERE token=?", (token,)
        ).fetchone()
    except Exception:
        return
    if not link:
        return
    if link['synced_to_lead']:
        return

    lead_id = link['lead_id']
    if not lead_id:
        _upsert_daily_score(db, username, 10, delta_videos=1)
        try:
            db.execute(
                "UPDATE enroll_share_links SET synced_to_lead=1 WHERE token=?",
                (token,)
            )
        except Exception:
            pass
        return

    lead = db.execute(
        "SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''",
        (lead_id,)
    ).fetchone()
    if not lead:
        return

    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    FORWARD_ORDER = [
        'New Lead', 'New', 'Contacted', 'Invited',
        'Video Sent', 'Video Watched', 'Paid ₹196', 'Mindset Lock',
        'Day 1', 'Day 2', 'Interview', 'Track Selected',
        'Seat Hold Confirmed', 'Fully Converted', 'Training', 'Converted', 'Lost', 'Retarget'
    ]
    current_status = (lead['status'] or 'New')
    current_idx = FORWARD_ORDER.index(current_status) if current_status in FORWARD_ORDER else 0
    video_sent_idx = FORWARD_ORDER.index('Video Sent') if 'Video Sent' in FORWARD_ORDER else 4

    if current_idx < video_sent_idx:
        db.execute(
            "UPDATE leads SET status='Video Sent', call_status='Video Sent', pipeline_stage='enrollment', "
            "last_contacted=?, contact_count=COALESCE(contact_count,0)+1, updated_at=? "
            "WHERE id=?",
            (now_str, now_str, lead_id)
        )
    else:
        current_call = (lead['call_status'] or '')
        call_forward = ['Not Called Yet', 'Called - No Answer', 'Called - Not Interested',
                        'Called - Follow Up', 'Called - Interested',
                        'Video Sent', 'Video Watched', 'Payment Done']
        if current_call not in call_forward[5:]:
            db.execute(
                "UPDATE leads SET call_status='Video Sent', updated_at=? WHERE id=?",
                (now_str, lead_id)
            )

    content_id = link['content_id']
    video_name = 'Video'
    if content_id:
        try:
            content = db.execute(
                "SELECT curiosity_title, title FROM enroll_content WHERE id=?",
                (content_id,)
            ).fetchone()
            if content:
                video_name = (content['curiosity_title'] or content['title'] or video_name)
        except Exception:
            pass
    _log_lead_event(db, lead_id, username, f'Video shared via Enroll To: "{video_name}"')

    _upsert_daily_score(db, username, 10, delta_videos=1)
    try:
        db.execute(
            "UPDATE enroll_share_links SET synced_to_lead=1, lead_status_before=? WHERE token=?",
            (current_status, token)
        )
    except Exception:
        pass


def _sync_watch_event_to_lead(db, token):
    """
    Called when prospect opens watch page for the FIRST TIME (view_count 0→1).
    Auto-updates lead to Video Watched + notifies team member.
    """
    try:
        link = db.execute(
            "SELECT * FROM enroll_share_links WHERE token=?", (token,)
        ).fetchone()
    except Exception:
        return
    if not link or link['watch_synced'] or not link['lead_id']:
        return

    lead_id = link['lead_id']
    lead = db.execute(
        "SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''",
        (lead_id,)
    ).fetchone()
    if not lead:
        return

    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    shared_by = link['shared_by'] or ''

    FORWARD_ORDER = [
        'New Lead', 'New', 'Contacted', 'Invited',
        'Video Sent', 'Video Watched', 'Paid ₹196', 'Mindset Lock',
        'Day 1', 'Day 2', 'Interview', 'Track Selected',
        'Seat Hold Confirmed', 'Fully Converted', 'Training', 'Converted', 'Lost', 'Retarget'
    ]
    current_status = (lead['status'] or 'New')
    current_idx = FORWARD_ORDER.index(current_status) if current_status in FORWARD_ORDER else 0
    watched_idx = FORWARD_ORDER.index('Video Watched') if 'Video Watched' in FORWARD_ORDER else 5

    if current_idx < watched_idx:
        db.execute(
            "UPDATE leads SET status='Video Watched', call_status='Video Watched', pipeline_stage='enrollment', "
            "updated_at=? WHERE id=?",
            (now_str, lead_id)
        )

    content_id = link['content_id']
    video_name = 'Video'
    if content_id:
        try:
            content = db.execute(
                "SELECT curiosity_title FROM enroll_content WHERE id=?",
                (content_id,)
            ).fetchone()
            if content:
                video_name = (content['curiosity_title'] or 'Video')
        except Exception:
            pass
    _log_lead_event(db, lead_id, shared_by,
                   f'Prospect watched video: "{video_name}" — Call karo abhi!')

    _upsert_daily_score(db, shared_by, 5)

    try:
        _push_to_users(db, shared_by,
                       f'{lead["name"] or "Lead"} watched the video!',
                       'Call now — interest is at its peak!',
                       '/working')
    except Exception:
        pass

    try:
        db.execute(
            "UPDATE enroll_share_links SET watch_synced=1 WHERE token=?", (token,)
        )
    except Exception:
        pass


def _get_actual_daily_counts(db, username):
    """
    Returns system-verified counts for today.
    Tamper-proof — written by system via daily_scores.
    """
    today = _today_ist().strftime('%Y-%m-%d')
    try:
        row = db.execute(
            "SELECT * FROM daily_scores WHERE username=? AND score_date=?",
            (username, today)
        ).fetchone()
    except Exception:
        return {
            'videos_sent': 0,
            'calls_made': 0,
            'payments_collected': 0,
            'enroll_links_sent': 0,
            'prospect_views': 0,
        }
    if not row:
        return {
            'videos_sent': 0,
            'calls_made': 0,
            'payments_collected': 0,
            'enroll_links_sent': 0,
            'prospect_views': 0,
        }
    d = dict(row)
    return {
        'videos_sent': d.get('videos_sent', 0) or 0,
        'calls_made': d.get('calls_made', 0) or 0,
        'payments_collected': d.get('payments_collected', 0) or 0,
        'enroll_links_sent': d.get('enroll_links_sent', 0) or 0,
        'prospect_views': d.get('prospect_views', 0) or 0,
    }


@app.route('/enroll/generate-link', methods=['POST'])
@login_required
def enroll_generate_link():
    """Create a share link for a lead + content; sync to lead pipeline and daily_scores."""
    data = request.get_json(silent=True) or request.form
    lead_id = data.get('lead_id')
    content_id = data.get('content_id')
    if lead_id is not None:
        try:
            lead_id = int(lead_id)
        except (TypeError, ValueError):
            lead_id = None
    if content_id is not None:
        try:
            content_id = int(content_id)
        except (TypeError, ValueError):
            content_id = None

    username = session['username']
    token = secrets.token_urlsafe(16)
    db = get_db()
    try:
        db.execute("""
            INSERT INTO enroll_share_links (token, lead_id, content_id, shared_by, view_count)
            VALUES (?, ?, ?, ?, 0)
        """, (token, lead_id, content_id, username))
        db.commit()
    except Exception as e:
        db.close()
        return jsonify({'ok': False, 'error': 'Failed to create link'}), 400

    _sync_enroll_share_to_lead(db, token, username)
    today = _today_ist().strftime('%Y-%m-%d')
    try:
        db.execute("""
            UPDATE daily_scores SET enroll_links_sent = COALESCE(enroll_links_sent, 0) + 1
            WHERE username=? AND score_date=?
        """, (username, today))
    except Exception:
        pass
    db.commit()
    db.close()

    watch_url = url_for('watch_video', token=token, _external=True)
    return jsonify({'ok': True, 'token': token, 'watch_url': watch_url})


def _youtube_embed_url(raw_url):
    """Extract YouTube video ID from any common URL and return embed URL. Returns '' if not valid."""
    if not raw_url or not isinstance(raw_url, str):
        return ''
    s = raw_url.strip()
    # Support: watch?v=, youtu.be/, embed/, shorts/
    m = _re.search(
        r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/|youtube\.com/shorts/)([a-zA-Z0-9_-]{11})',
        s
    )
    if m:
        vid = m.group(1)
        return 'https://www.youtube.com/embed/' + vid + '?rel=0&modestbranding=1'
    return ''


@app.route('/watch/enrollment')
def watch_enrollment():
    """Public page: enrollment video in minimal embed (no YouTube UI). Share this link so prospect opens our page, not YouTube."""
    db = get_db()
    enrollment_video_url = _get_setting(db, 'enrollment_video_url', '')
    enrollment_video_title = _get_setting(db, 'enrollment_video_title', 'Enrollment Video')
    db.close()
    embed_url = _youtube_embed_url(enrollment_video_url)
    if not embed_url:
        return render_template('watch_video.html', error='Video not configured', title='Enrollment Video'), 404
    return render_template('watch_video.html', embed_url=embed_url, title=enrollment_video_title or 'Enrollment Video', error=None)


_BATCH_SLOTS = ('d1_morning', 'd1_afternoon', 'd1_evening', 'd2_morning', 'd2_afternoon', 'd2_evening')
_BATCH_LABELS = {
    'd1_morning': 'Day 1 — Morning Batch', 'd1_afternoon': 'Day 1 — Afternoon Batch', 'd1_evening': 'Day 1 — Evening Batch',
    'd2_morning': 'Day 2 — Morning Batch', 'd2_afternoon': 'Day 2 — Afternoon Batch', 'd2_evening': 'Day 2 — Evening Batch',
}


def _batch_watch_urls():
    """In-app watch URLs for each batch slot (v1, v2). Prospect opens our page, not YouTube."""
    return {
        slot: {'v1': url_for('watch_batch', slot=slot, v=1, _external=True),
               'v2': url_for('watch_batch', slot=slot, v=2, _external=True)}
        for slot in _BATCH_SLOTS
    }


def _mark_batch_done_for_lead(db, lead_id, slot):
    """When prospect opens batch link with token: mark that slot done, update day1_done/day2_done, add points for owner."""
    row = db.execute("SELECT * FROM leads WHERE id=? AND in_pool=0 AND deleted_at=''", (lead_id,)).fetchone()
    if not row:
        return
    owner = row['assigned_to']
    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    current = row[slot] if slot in row.keys() else 0
    if current:
        return
    db.execute(f"UPDATE leads SET {slot}=?, updated_at=? WHERE id=?", (1, now_str, lead_id))
    day_prefix = slot[:2]
    if day_prefix == 'd1':
        m = 1 if slot == 'd1_morning' else (row['d1_morning'] or 0)
        a = 1 if slot == 'd1_afternoon' else (row['d1_afternoon'] or 0)
        e = 1 if slot == 'd1_evening' else (row['d1_evening'] or 0)
        all_done = bool(m and a and e)
        db.execute("UPDATE leads SET day1_done=? WHERE id=?", (1 if all_done else 0, lead_id))
    else:
        m = 1 if slot == 'd2_morning' else (row['d2_morning'] or 0)
        a = 1 if slot == 'd2_afternoon' else (row['d2_afternoon'] or 0)
        e = 1 if slot == 'd2_evening' else (row['d2_evening'] or 0)
        all_done = bool(m and a and e)
        db.execute("UPDATE leads SET day2_done=? WHERE id=?", (1 if all_done else 0, lead_id))
    _upsert_daily_score(db, owner, 15, delta_batches=1)
    db.commit()


@app.route('/watch/batch/<slot>/<int:v>')
def watch_batch(slot, v):
    """Public page: 3-day batch video in minimal embed."""
    if slot not in _BATCH_SLOTS or v not in (1, 2):
        return render_template('watch_video.html', error='Invalid link', title='Batch Video'), 404
    db = get_db()
    setting_key = f'batch_{slot}_v{v}'
    yt_url = _get_setting(db, setting_key, '')
    db.close()
    embed_url = _youtube_embed_url(yt_url)
    if not embed_url:
        return render_template('watch_video.html', error='Video not configured', title=_BATCH_LABELS.get(slot, 'Batch Video')), 404
    title = _BATCH_LABELS.get(slot, 'Batch Video') + ' — Video ' + str(v)
    return render_template('watch_batch.html', embed_url=embed_url, title=title, slot=slot, v=v)


@app.route('/watch/<token>')
def watch_video(token):
    """Public watch page; first view syncs to lead (Video Watched) and notifies sharer."""
    db = get_db()
    link = db.execute(
        "SELECT * FROM enroll_share_links WHERE token=?", (token,)
    ).fetchone()
    if not link:
        db.close()
        return render_template('watch_video.html', error='Link not found or expired'), 404

    is_first_view = (link['view_count'] == 0)
    db.execute(
        "UPDATE enroll_share_links SET view_count = view_count + 1 WHERE token=?",
        (token,)
    )
    db.commit()

    if is_first_view:
        _sync_watch_event_to_lead(db, token)
        today = _today_ist().strftime('%Y-%m-%d')
        shared_by = link['shared_by'] or ''
        if shared_by:
            try:
                db.execute("""
                    UPDATE daily_scores SET prospect_views = COALESCE(prospect_views, 0) + 1
                    WHERE username=? AND score_date=?
                """, (shared_by, today))
            except Exception:
                pass
        db.commit()

    content = None
    if link['content_id']:
        try:
            content = db.execute(
                "SELECT curiosity_title, title FROM enroll_content WHERE id=?",
                (link['content_id'],)
            ).fetchone()
        except Exception:
            pass
    # Embed enrollment video so prospect watches in-app (no YouTube suggestions)
    enrollment_video_url = _get_setting(db, 'enrollment_video_url', '') if db else ''
    db.close()
    title = (content['curiosity_title'] or content['title']) if content else 'Video'
    embed_url = _youtube_embed_url(enrollment_video_url)
    return render_template('watch_video.html', token=token, title=title,
                           enrollment_video_url=enrollment_video_url or '', embed_url=embed_url, error=None)


def _trigger_training_unlock(db, lead):
    """When a lead reaches the training stage, unlock the assigned user training."""
    import re as _re2
    phone = lead['phone'] if 'phone' in lead.keys() else ''
    clean = _re2.sub(r'[^0-9]', '', phone)
    if clean.startswith('91') and len(clean) == 12:
        clean = clean[2:]
    if not clean:
        return
    user_row = db.execute("""
        SELECT * FROM users WHERE
        REPLACE(REPLACE(REPLACE(phone,'+91',''),'+',''),' ','') = ?
        OR REPLACE(REPLACE(phone,'+91',''),' ','') = ?
    """, (clean, clean)).fetchone()
    if user_row and user_row['training_status'] != 'completed':
        db.execute(
            "UPDATE users SET training_status='pending' WHERE username=?",
            (user_row['username'],)
        )
        try:
            _push_to_users(db, user_row['username'],
                           'Training Ready!',
                           'Start 7-day training. You will get a certificate!',
                           '/training')
        except Exception:
            pass
        _log_activity(db, user_row['username'], 'training_unlocked',
                      f'Lead #{lead["id"]} transitioned to training')


def _check_seat_hold_expiry(db, username):
    """Revert expired seat_hold leads back to day3 stage."""
    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    expired = db.execute("""
        SELECT * FROM leads
        WHERE current_owner=? AND pipeline_stage='seat_hold'
        AND in_pool=0 AND deleted_at=''
        AND seat_hold_expiry != '' AND seat_hold_expiry < ?
    """, (username, now_str)).fetchall()
    for lead in expired:
        _transition_stage(db, lead['id'], 'day3', 'system_expiry')
        _log_activity(db, 'system', 'seat_hold_expired',
                      f'Lead #{lead["id"]} seat hold expired')


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
                  (SELECT COUNT(*) FROM wallet_recharges WHERE status='pending') as wp
            """).fetchone()
            db.close()
            return {'pending_count': row['pu'], 'wallet_pending': row['wp'], 'has_pending_work': False}
        uname = session.get('username')
        if uname:
            db = get_db()
            has_pending_work = db.execute(
                "SELECT COUNT(*) FROM leads "
                "WHERE in_pool=0 AND deleted_at='' AND assigned_to=? AND status IN ('Day 1','Paid ₹196') AND d1_morning=0",
                (uname,)
            ).fetchone()[0] > 0
            db.close()
            return {'pending_count': 0, 'wallet_pending': 0, 'has_pending_work': has_pending_work}
    except Exception as e:
        app.logger.error(f"inject_pending_count() failed: {e}")
    return {'pending_count': 0, 'wallet_pending': 0, 'has_pending_work': False}


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
    """When DEV_BYPASS_AUTH=1 and no user in session, inject admin session so local run works without login."""
    if not _DEV_BYPASS_AUTH or session.get('username'):
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
    import traceback as _tb
    app.logger.error(f"Unhandled exception: {error}\n{_tb.format_exc()}")
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return {'ok': False, 'error': 'Something went wrong'}, 500
    return render_template('500.html'), 500


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Register
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
            flash(f'Upline FBO ID "{upline_fbo_id}" not found. Please ask your upline for their correct FBO ID.', 'danger')
            return render_template('register.html')
        upline_name = upline_user['username']

        is_new      = 1 if request.form.get('is_new_joining') else 0
        joining_dt  = request.form.get('joining_date', '').strip()
        t_status    = 'pending' if is_new else 'not_required'

        db.execute(
            "INSERT INTO users (username, password, role, fbo_id, upline_name, upline_username, phone, email, status, "
            "training_required, training_status, joining_date) "
            "VALUES (?, ?, 'team', ?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
            (username, generate_password_hash(password, method='pbkdf2:sha256'),
             fbo_id, upline_name, upline_name, phone, email,
             is_new, t_status, joining_dt)
        )
        db.commit()
        db.close()
        flash('Registration submitted! Your account is pending admin approval.', 'success')
        return redirect(url_for('login'))

    today = _today_ist().isoformat()
    return render_template('register.html', today=today)


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Login / Logout
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
            # Store only a boolean flag — full base64 in session overflows the 4 KB cookie limit.
            # Profile image is served via /profile/dp route.
            session['has_dp']   = bool(user['display_picture'])
            # Training gate — store status so before_request can redirect cheaply
            keys = user.keys() if hasattr(user, 'keys') else []
            session['training_status'] = user['training_status'] if 'training_status' in keys else 'not_required'
            db = get_db()
            _log_activity(db, user['username'], 'login', f"Role: {user['role']}")
            db.close()
            flash(f'Welcome back, {user["username"]}!', 'success')
            if user['role'] == 'admin':
                return redirect(url_for('admin_dashboard'))
            return redirect(url_for('team_dashboard'))
        else:
            flash('Invalid username or password.', 'danger')

    return render_template('login.html')


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Forgot / Reset Password
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
                expires_at = (_now_ist() + datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
                db.execute(
                    "INSERT INTO password_reset_tokens (username, token, expires_at) VALUES (?,?,?)",
                    (user['username'], token, expires_at)
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

    db  = get_db()
    row = db.execute(
        "SELECT * FROM password_reset_tokens WHERE token=? AND used=0",
        (token,)
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
    if 'username' in session:
        db = get_db()
        _log_activity(db, session['username'], 'logout', '')
        db.close()
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


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


# ─────────────────────────────────────────────
#  PWA support routes
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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

    # ── 4. Existing metrics kept ─────────────────────────────────────
    recent = db.execute(
        f"SELECT * FROM leads WHERE {_base_w} ORDER BY created_at DESC LIMIT 5"
    ).fetchall()

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

    recent = _enrich_leads(recent)
    db.close()
    resp = make_response(render_template('admin.html',
                           metrics=metrics,
                           pipeline=pipeline,
                           pipeline_value=pipeline_value,
                           pulse=pulse,
                           team_board=team_board,
                           stale_leads=stale_leads,
                           recent=recent,
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

    # Check seat_hold expiry on every dashboard load
    _check_seat_hold_expiry(db, username)

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

    base   = "SELECT * FROM leads WHERE in_pool=0 AND deleted_at=''"
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
    team            = db.execute("SELECT name FROM team_members ORDER BY name").fetchall()
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
    team = db.execute("SELECT name FROM team_members ORDER BY name").fetchall()

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

        # Single UPDATE: always set status and pipeline_stage together
        db.execute("""
            UPDATE leads
            SET name=?, phone=?, email=?, referred_by=?, assigned_to=?, status=?,
                payment_done=?, payment_amount=?,
                day1_done=?, day2_done=?, interview_done=?,
                follow_up_date=?, call_result=?, notes=?, city=?,
                track_selected=?, track_price=?, seat_hold_amount=?, pending_amount=?,
                pipeline_stage=?,
                updated_at=?
            WHERE id=?
        """, (name, phone, email, referred_by, assigned_to, status,
              payment_done, payment_amount,
              day1_done, day2_done, interview_done,
              follow_up_date, call_result, notes, city,
              track_selected_val, track_price_val, seat_hold_amount_val, pending_amount_val,
              new_pipeline_stage,
              _updated_at,
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
        db.execute(
            "UPDATE leads SET status=?, pipeline_stage=?, updated_at=? WHERE id=? AND in_pool=0",
            (new_status, new_pipeline_stage, now_str, lead_id)
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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Daily Reports \u2013 Submit (team member)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/reports/submit', methods=['GET', 'POST'])
@login_required
@safe_route
def report_submit():
    username = session['username']
    today    = _today_ist().isoformat()
    db       = get_db()

    existing = db.execute(
        "SELECT * FROM daily_reports WHERE username=? AND report_date=?",
        (username, today)
    ).fetchone()

    actual_counts = _get_actual_daily_counts(db, username)

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
                                   username=username, actual_counts=actual_counts)

        leads_educated = request.form.get('leads_educated', '')
        remarks        = request.form.get('remarks', '').strip()

        inflation_errors = []
        max_videos = (actual_counts['videos_sent'] + actual_counts['enroll_links_sent'] + 5)
        if pdf_covered > max_videos:
            inflation_errors.append(
                f"Videos/PDFs: System tracked {actual_counts['videos_sent']} videos today. "
                f"You cannot enter more than that."
            )
        if total_calling > (actual_counts['calls_made'] + 10):
            inflation_errors.append(
                f"Calls: System tracked {actual_counts['calls_made']} calls today. "
                f"You cannot enter more than that."
            )
        if inflation_errors:
            for err in inflation_errors:
                flash(err, 'danger')
            db.close()
            return render_template('report_form.html', existing=existing, today=today,
                                   username=username, actual_counts=actual_counts)

        now_ts = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
        db.execute("""
            INSERT INTO daily_reports
                (username, upline_name, report_date, total_calling, pdf_covered,
                 calls_picked, wrong_numbers, enrollments_done, pending_enroll,
                 underage, leads_educated, plan_2cc, seat_holdings, remarks,
                 submitted_at, videos_sent_actual, calls_made_actual, payments_actual, system_verified)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
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
                submitted_at=excluded.submitted_at,
                videos_sent_actual=excluded.videos_sent_actual,
                calls_made_actual=excluded.calls_made_actual,
                payments_actual=excluded.payments_actual,
                system_verified=1
        """, (username, upline_name, report_date, total_calling, pdf_covered,
              calls_picked, wrong_numbers, enrollments_done, pending_enroll,
              underage, leads_educated, plan_2cc, seat_holdings, remarks,
              now_ts,
              actual_counts['videos_sent'], actual_counts['calls_made'], actual_counts['payments_collected']))
        _upsert_daily_score(db, username, 20)
        new_badges = _check_and_award_badges(db, username)
        db.commit()
        _log_activity(db, username, 'report_submit', f"Date: {today}")
        db.close()
        flash('Daily report submitted successfully!', 'success')
        return redirect(url_for('team_dashboard'))

    db.close()
    return render_template('report_form.html', existing=existing, today=today,
                           username=username, actual_counts=actual_counts)


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Daily Reports \u2013 Admin View
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/reports')
@admin_required
@safe_route
def reports_admin():
    db          = get_db()
    filter_date = request.args.get('date', '')
    filter_user = request.args.get('user', '')
    view        = request.args.get('view', 'daily')  # 'daily' or 'monthly'

    query  = "SELECT * FROM daily_reports WHERE 1=1"
    params = []
    if view == 'daily':
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
        {'AND report_date=?' if (view == 'daily' and filter_date) else ''}
        {'AND username=?' if filter_user else ''}
    """, params).fetchone()

    members = db.execute(
        "SELECT DISTINCT username FROM daily_reports ORDER BY username"
    ).fetchall()

    today = _today_ist().isoformat()
    submitted_today = [r['username'] for r in db.execute(
        "SELECT username FROM daily_reports WHERE report_date=?", (today,)
    ).fetchall()]
    approved_team = [u['username'] for u in db.execute(
        "SELECT username FROM users WHERE role='team' AND status='approved'"
    ).fetchall()]
    missing_today = [u for u in approved_team if u not in submitted_today]

    user_filter_sql = 'AND username=?' if filter_user else ''
    user_filter_params = [filter_user] if filter_user else []

    if view == 'monthly':
        trend = db.execute(f"""
            SELECT strftime('%Y-%m', report_date) AS report_date,
                   COUNT(DISTINCT username)        AS reporters,
                   SUM(total_calling)              AS calling,
                   SUM(enrollments_done)           AS enrolments
            FROM daily_reports
            WHERE report_date >= date('now', '-365 days')
            {user_filter_sql}
            GROUP BY strftime('%Y-%m', report_date)
            ORDER BY report_date ASC
        """, user_filter_params).fetchall()

        monthly_reports = db.execute(f"""
            SELECT strftime('%Y-%m', report_date) AS month,
                   username,
                   SUM(total_calling)    AS total_calling,
                   SUM(pdf_covered)      AS pdf_covered,
                   SUM(calls_picked)     AS calls_picked,
                   SUM(enrollments_done) AS enrollments_done,
                   SUM(plan_2cc)         AS plan_2cc,
                   COUNT(*)              AS days_reported
            FROM daily_reports
            WHERE 1=1 {user_filter_sql}
            GROUP BY month, username
            ORDER BY month DESC, username
        """, user_filter_params).fetchall()
    else:
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
        monthly_reports = []

    # Additional summary for system vs reported videos
    system_total_videos = sum((r['videos_sent_actual'] or 0) for r in reports if (r['videos_sent_actual'] or -1) >= 0)
    reported_total_videos = sum((r['pdf_covered'] or 0) for r in reports)

    db.close()
    return render_template('reports_admin.html',
                           reports=reports,
                           totals=totals,
                           members=members,
                           submitted_today=submitted_today,
                           missing_today=missing_today,
                           trend=trend,
                           monthly_reports=monthly_reports,
                           filter_date=filter_date,
                           filter_user=filter_user,
                           view=view,
                           today=today,
                           system_total_videos=system_total_videos,
                           reported_total_videos=reported_total_videos)


# ─────────────────────────────────────────────────────────────
#  Leader – Team Reports (read-only)
# ─────────────────────────────────────────────────────────────
@app.route('/leader/team-reports')
@login_required
@safe_route
def leader_team_reports():
    """Leader sees daily reports for their downline — read only."""
    if session.get('role') not in ('leader', 'admin'):
        flash('Access denied.', 'danger')
        return redirect(url_for('team_dashboard'))

    username = session['username']
    db = get_db()

    # Get downline
    if session.get('role') == 'admin':
        members = [r['username'] for r in db.execute(
            "SELECT username FROM users WHERE role IN ('team','leader') AND status='approved'"
        ).fetchall()]
    else:
        try:
            members = _get_network_usernames(db, username)
        except Exception:
            members = []
        members = [m for m in members if m != username]

    # Date filter from query param, default today
    from datetime import datetime as _dt2
    date_filter = request.args.get('date', _today_ist().isoformat())

    reports = []
    if members:
        ph = ','.join('?' * len(members))
        reports = db.execute(f"""
            SELECT dr.*, u.phone as member_phone
            FROM daily_reports dr
            LEFT JOIN users u ON u.username = dr.username
            WHERE dr.username IN ({ph}) AND dr.report_date=?
            ORDER BY dr.submitted_at DESC
        """, members + [date_filter]).fetchall()

    # Who hasn't submitted
    submitted_set = {r['username'] for r in reports}
    missing = [m for m in members if m not in submitted_set]

    # Summary totals
    summary = {
        'total_calling':    sum((r['total_calling'] or 0) for r in reports),
        'pdf_covered':      sum((r['pdf_covered'] or 0) for r in reports),
        'calls_picked':     sum((r['calls_picked'] or 0) for r in reports),
        'enrollments_done': sum((r['enrollments_done'] or 0) for r in reports),
        'plan_2cc':         sum((r['plan_2cc'] or 0) for r in reports),
        'seat_holdings':    sum((r['seat_holdings'] or 0) for r in reports),
    }

    db.close()
    return render_template('leader_team_reports.html',
                           reports=reports,
                           missing=missing,
                           members=members,
                           summary=summary,
                           date_filter=date_filter,
                           today=_today_ist().isoformat())

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
                           enrollment_video_url=enrollment_video_url, enrollment_video_title=enrollment_video_title)


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
            db.execute("UPDATE leads SET added_by=? WHERE added_by=?", (new_username, username))
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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Team \u2013 Wallet
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
    flash(f'Recharge request of \u20b9{amount:.0f} submitted! UTR: {utr}. '
          f'Admin will credit your wallet within 24 hours.', 'success')
    return redirect(url_for('wallet'))


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Team \u2013 Lead Pool (Claim Leads)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Meta Webhook
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Change Password
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Profile (with display picture)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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


# ─────────────────────────────────────────────────────────────
#  Profile – with earned badges
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
#  Activity Feed API (polling)
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
#  Earnings / Commission Calculator
# ─────────────────────────────────────────────────────────────

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
    return Response(buf.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename={fname}'})


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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Announcements (Notice Board)
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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
    preview = msg[:80] + ('\u2026' if len(msg) > 80 else '')
    _push_all_team(db, '\U0001f4e2 New Announcement', preview, url_for('announcements'))
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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Leaderboard
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

@app.route('/leaderboard')
@login_required
def leaderboard():
    db       = get_db()
    username = session['username']

    LEADER_SQL = """
        SELECT
            u.username,
            u.display_picture,
            COUNT(l.id)                                                                AS total,
            SUM(CASE WHEN l.status IN ('Converted','Fully Converted')
                      THEN 1 ELSE 0 END)                                               AS converted,
            SUM(CASE WHEN l.payment_done=1 THEN 1 ELSE 0 END)                         AS paid,
            COALESCE(SUM(COALESCE(l.payment_amount,0)+COALESCE(l.revenue,0)),0)        AS revenue,
            ROUND(
              CAST(SUM(CASE WHEN l.payment_done=1 THEN 1 ELSE 0 END) AS REAL)
              / NULLIF(COUNT(l.id),0)*100, 1)                                          AS paid_pct,
            SUM(CASE WHEN l.status='Seat Hold Confirmed' THEN 1 ELSE 0 END)           AS seat_holds,
            SUM(CASE WHEN l.status='Fully Converted'     THEN 1 ELSE 0 END)           AS fully_conv,
            COALESCE(SUM(CASE WHEN l.status='Seat Hold Confirmed'
                              THEN l.seat_hold_amount ELSE 0 END), 0)                 AS seat_rev,
            COALESCE(SUM(CASE WHEN l.status='Fully Converted'
                              THEN l.track_price ELSE 0 END), 0)                      AS final_rev
        FROM users u
        LEFT JOIN leads l ON l.assigned_to=u.username AND l.in_pool=0 AND l.deleted_at=''
        WHERE u.role='team' AND u.status='approved' {extra}
        GROUP BY u.username
        ORDER BY paid DESC, converted DESC, total DESC
    """

    NETWORK_TREE_SQL = """
        WITH RECURSIVE tree(uname, level) AS (
            SELECT :me, 0
            UNION ALL
            SELECT u.username, t.level + 1
            FROM users u JOIN tree t ON u.upline_name = t.uname
            WHERE u.role='team' AND u.status='approved'
        )
        SELECT t.uname, t.level,
               u.display_picture,
               COUNT(l.id)                                                                AS total,
               SUM(CASE WHEN l.status IN ('Converted','Fully Converted') THEN 1 ELSE 0 END) AS converted,
               SUM(CASE WHEN l.payment_done=1 THEN 1 ELSE 0 END)                            AS paid,
               ROUND(CAST(SUM(CASE WHEN l.payment_done=1 THEN 1 ELSE 0 END) AS REAL)
                     / NULLIF(COUNT(l.id),0)*100, 1)                                        AS paid_pct,
               COALESCE(SUM(COALESCE(l.payment_amount,0)+COALESCE(l.revenue,0)),0)          AS revenue,
               SUM(CASE WHEN l.status='Seat Hold Confirmed' THEN 1 ELSE 0 END)             AS seat_holds,
               SUM(CASE WHEN l.status='Fully Converted'     THEN 1 ELSE 0 END)             AS fully_conv
        FROM tree t
        JOIN users u ON u.username = t.uname
        LEFT JOIN leads l ON l.assigned_to = t.uname AND l.in_pool=0 AND l.deleted_at=''
        WHERE t.uname != :me
        GROUP BY t.uname, t.level
        ORDER BY t.level, paid DESC, converted DESC
    """

    if session.get('role') == 'admin':
        rows = db.execute(LEADER_SQL.format(extra='')).fetchall()
    else:
        network = _get_network_usernames(db, username)
        if network:
            placeholders = ','.join('?' for _ in network)
            rows = db.execute(
                LEADER_SQL.format(extra=f"AND u.username IN ({placeholders})"),
                network
            ).fetchall()
        else:
            rows = []

    # Network tree — run for both admin (full org) and team (own downline)
    tree_rows = db.execute(NETWORK_TREE_SQL, {'me': username}).fetchall()
    network_by_gen = {}
    for r in tree_rows:
        network_by_gen.setdefault(r['level'], []).append(r)
    net_summary = {
        'total':   len(tree_rows),
        'direct':  len(network_by_gen.get(1, [])),
        'revenue': sum(r['revenue'] or 0 for r in tree_rows),
    }

    # Network growth by month
    if tree_rows:
        member_names = [r['uname'] for r in tree_rows]
        placeholders_g = ','.join('?' for _ in member_names)
        growth_rows = db.execute(f"""
            SELECT strftime('%Y-%m', CASE WHEN joining_date!='' THEN joining_date ELSE created_at END) as month,
                   COUNT(*) as new_members
            FROM users
            WHERE username IN ({placeholders_g})
              AND role='team' AND status='approved'
            GROUP BY month ORDER BY month ASC LIMIT 12
        """, member_names).fetchall()
        growth_data = [{'month': r['month'], 'count': r['new_members']} for r in growth_rows]
    else:
        growth_data = []

    # Weekly gamification scores
    monday_str = (_today_ist() - datetime.timedelta(days=_today_ist().weekday())).strftime('%Y-%m-%d')
    last_mon   = (_today_ist() - datetime.timedelta(days=_today_ist().weekday()+7)).strftime('%Y-%m-%d')

    weekly_rows = db.execute("""
        SELECT u.username, u.display_picture,
               COALESCE(SUM(CASE WHEN ds.score_date >= ? THEN ds.total_points ELSE 0 END),0) AS week_pts,
               COALESCE(SUM(CASE WHEN ds.score_date >= ? AND ds.score_date < ? THEN ds.total_points ELSE 0 END),0) AS last_week_pts,
               COALESCE(SUM(ds.total_points),0) AS all_time_pts,
               COALESCE(MAX(ds.streak_days),0) AS streak
        FROM users u
        LEFT JOIN daily_scores ds ON ds.username = u.username
        WHERE u.role='team' AND u.status='approved'
        GROUP BY u.username
        ORDER BY week_pts DESC, all_time_pts DESC
    """, (monday_str, last_mon, monday_str)).fetchall()

    # Attach badges
    weekly_board = []
    for r in weekly_rows:
        d = dict(r)
        badges_emoji = _get_user_badges_emoji(db, r['username'])
        d['badges']  = badges_emoji
        d['trend']   = (d['week_pts'] or 0) - (d['last_week_pts'] or 0)
        weekly_board.append(d)

    # Current user rank in weekly board
    weekly_usernames = [w['username'] for w in weekly_board]
    current_user_rank = (weekly_usernames.index(username) + 1) if username in weekly_usernames else None

    db.close()
    return render_template('leaderboard.html',
                           rows=rows,
                           current_user=username,
                           role=session.get('role'),
                           network_by_gen=network_by_gen,
                           net_summary=net_summary,
                           growth_data=growth_data,
                           weekly_board=weekly_board,
                           current_user_rank=current_user_rank)


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


# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
#  Live Session
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500

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

    page    = int(request.args.get('page', 1))
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


# ─────────────────────────────────────────────────────────────
#  Training Routes (Team)
# ─────────────────────────────────────────────────────────────

def _get_training_progress(db, username):
    """Return dict {day_number: completed} for the user."""
    rows = db.execute(
        "SELECT day_number, completed FROM training_progress WHERE username=?",
        (username,)
    ).fetchall()
    return {r['day_number']: r['completed'] for r in rows}


def _get_training_dates(db, username):
    """Return dict {day_number: completed_at_str} for the user."""
    rows = db.execute(
        "SELECT day_number, completed_at FROM training_progress WHERE username=? AND completed=1",
        (username,)
    ).fetchall()
    return {r['day_number']: r['completed_at'] for r in rows}


def _day_unlock_dates(dates_dict):
    """
    Given {day_number: completed_at_str}, return {day_number: earliest_unlock_date_str}
    for locked days (i.e. days that can't be done yet because of calendar enforcement).
    Day N is unlocked on: day1_date + (N-1) days.
    """
    if 1 not in dates_dict or not dates_dict[1]:
        return {}
    try:
        day1_date = datetime.datetime.strptime(dates_dict[1][:10], '%Y-%m-%d').date()
    except Exception:
        return {}
    result = {}
    for n in range(2, 8):
        earliest = day1_date + datetime.timedelta(days=n - 1)
        result[n] = earliest.strftime('%d %b %Y')
    return result


@app.route('/training')
@login_required
def training_home():
    username = session['username']
    ts = session.get('training_status', 'not_required')
    db = get_db()

    # ── Old members / fully unlocked: show videos freely + downline progress ──
    if ts in ('not_required', 'unlocked'):
        user_row = db.execute(
            "SELECT fbo_id FROM users WHERE username=?", (username,)
        ).fetchone()
        fbo_id = user_row['fbo_id'] if user_row else ''

        # All training videos (freely watchable)
        videos = {v['day_number']: v for v in
                  db.execute("SELECT * FROM training_videos ORDER BY day_number").fetchall()}

        # Bonus videos
        bonus_videos = db.execute(
            "SELECT * FROM bonus_videos ORDER BY sort_order, id"
        ).fetchall()

        # Direct downline who have training_required=1
        downline_rows = db.execute("""
            SELECT u.username, u.joining_date, u.training_status,
                   COALESCE(p.days_done, 0) AS days_done
            FROM users u
            LEFT JOIN (
                SELECT username, SUM(completed) AS days_done
                FROM training_progress GROUP BY username
            ) p ON p.username = u.username
            WHERE u.upline_name = ? AND u.training_required = 1
            ORDER BY u.username
        """, (fbo_id,)).fetchall()
        db.close()

        return render_template('training.html',
                               is_viewer=True,
                               training_status=ts,
                               downline=downline_rows,
                               days=range(1, 8),
                               videos=videos, progress={},
                               bonus_videos=bonus_videos,
                               current_day=None, current_video=None,
                               all_done=False, joining_date='',
                               test_score=-1, unlock_dates={})

    # ── Members currently in training ──
    videos = {v['day_number']: v for v in
              db.execute("SELECT * FROM training_videos ORDER BY day_number").fetchall()}

    progress = _get_training_progress(db, username)
    dates    = _get_training_dates(db, username)
    unlock_dates = _day_unlock_dates(dates)

    # Find current day (first incomplete, also respecting calendar lock)
    today = datetime.date.today()
    current_day = 1
    for d in range(1, 8):
        if not progress.get(d, 0):
            current_day = d
            break
    else:
        current_day = 8  # all done

    # Auto-promote status if all 7 completed
    all_done = all(progress.get(d, 0) for d in range(1, 8))
    if all_done and ts not in ('completed', 'unlocked'):
        db.execute(
            "UPDATE users SET training_status='completed' WHERE username=?",
            (username,)
        )
        db.commit()
        session['training_status'] = 'completed'
        ts = 'completed'

    current_video = videos.get(current_day)
    user_row = db.execute(
        "SELECT joining_date, training_status, test_score FROM users WHERE username=?",
        (username,)
    ).fetchone()

    test_score = user_row['test_score'] if user_row else -1

    # Bonus videos (shown after all days done)
    bonus_videos = []
    if all_done:
        bonus_videos = db.execute(
            "SELECT * FROM bonus_videos ORDER BY sort_order, id"
        ).fetchall()

    db.close()

    return render_template('training.html',
                           is_viewer=False,
                           videos=videos,
                           progress=progress,
                           current_day=current_day,
                           current_video=current_video,
                           all_done=all_done,
                           training_status=ts,
                           joining_date=user_row['joining_date'] if user_row else '',
                           days=range(1, 8),
                           downline=[],
                           test_score=test_score,
                           unlock_dates=unlock_dates,
                           bonus_videos=bonus_videos)


@app.route('/training/complete-day', methods=['POST'])
@login_required
def training_complete_day():
    username = session['username']
    day = request.form.get('day_number', type=int)
    if not day or day < 1 or day > 7:
        flash('Invalid day.', 'danger')
        return redirect(url_for('training_home'))

    db = get_db()
    progress = _get_training_progress(db, username)

    # Ensure user is on this day (can't skip)
    for prev in range(1, day):
        if not progress.get(prev, 0):
            db.close()
            flash('Please complete previous days first.', 'warning')
            return redirect(url_for('training_home'))

    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    db.execute(
        "INSERT INTO training_progress (username, day_number, completed, completed_at) "
        "VALUES (?, ?, 1, ?) ON CONFLICT(username, day_number) DO UPDATE SET completed=1, completed_at=?",
        (username, day, now_str, now_str)
    )

    # Check if all 7 now done
    progress[day] = 1
    all_done = all(progress.get(d, 0) for d in range(1, 8))
    if all_done:
        db.execute(
            "UPDATE users SET training_status='completed' WHERE username=?",
            (username,)
        )
        session['training_status'] = 'completed'
        flash('🎉 All 7 days complete! Take the training test — score 60/100 to unlock your certificate.', 'success')
    else:
        flash(f'✅ Day {day} complete! Keep going.', 'success')

    db.commit()
    db.close()
    return redirect(url_for('training_home'))


@app.route('/training/certificate')
@login_required
def training_certificate():
    ts = session.get('training_status', 'pending')
    if ts not in ('completed', 'unlocked'):
        flash('Complete all 7 training days first.', 'warning')
        return redirect(url_for('training_home'))

    username = session['username']
    db = get_db()
    user_row = db.execute(
        "SELECT joining_date, training_status, test_score FROM users WHERE username=?",
        (username,)
    ).fetchone()

    # Require test pass (score >= 60) unless already unlocked
    test_score = user_row['test_score'] if user_row else -1
    if ts != 'unlocked' and test_score < 60:
        db.close()
        flash('Training test pass karna zaroori hai (60/100 ya zyada). Pehle test do.', 'warning')
        return redirect(url_for('training_test'))

    # Find completion date (day 7)
    day7 = db.execute(
        "SELECT completed_at FROM training_progress WHERE username=? AND day_number=7",
        (username,)
    ).fetchone()

    # Admin signature
    sig_file = _get_setting(db, 'admin_signature_file', '')
    db.close()

    completion_date = ''
    if day7 and day7['completed_at']:
        try:
            completion_date = datetime.datetime.strptime(
                day7['completed_at'], '%Y-%m-%d %H:%M:%S'
            ).strftime('%d %B %Y')
        except Exception:
            completion_date = day7['completed_at'][:10]

    cert_number = f"MYLE-{_today_ist().year}-{username.upper()}"
    sig_url = url_for('training_signature_preview')

    return render_template('training_certificate.html',
                           username=username,
                           joining_date=user_row['joining_date'] if user_row else '',
                           completion_date=completion_date,
                           cert_number=cert_number,
                           training_status=ts,
                           test_score=test_score,
                           sig_url=sig_url)


@app.route('/training/upload-certificate', methods=['POST'])
@login_required
def training_upload_certificate():
    ts = session.get('training_status', 'pending')
    if ts not in ('completed', 'unlocked'):
        flash('Complete training first.', 'warning')
        return redirect(url_for('training_home'))

    f = request.files.get('certificate_file')
    if not f or not f.filename:
        flash('Please select a file to upload.', 'danger')
        return redirect(url_for('training_home'))

    ext = f.filename.rsplit('.', 1)[-1].lower() if '.' in f.filename else ''
    if ext not in ('pdf', 'jpg', 'jpeg', 'png'):
        flash('Only PDF, JPG, or PNG files are accepted.', 'danger')
        return redirect(url_for('training_home'))

    # Size check (max 5 MB)
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    if size > 5 * 1024 * 1024:
        flash('File too large. Maximum size is 5 MB.', 'danger')
        return redirect(url_for('training_home'))

    # Save file to persistent upload root (not /tmp)
    upload_dir = os.path.join(_upload_root(), 'uploads', 'training_certs')
    os.makedirs(upload_dir, exist_ok=True)
    filename = f"{session['username']}_cert.{ext}"
    f.save(os.path.join(upload_dir, filename))

    db = get_db()
    db.execute(
        "UPDATE users SET training_status='unlocked', certificate_path=? WHERE username=?",
        (filename, session['username'])
    )
    db.commit()
    db.close()

    session['training_status'] = 'unlocked'
    flash('🎉 Certificate uploaded! Full app access granted. Welcome to Myle Community!', 'success')
    return redirect(url_for('team_dashboard'))


# ─────────────────────────────────────────────────────────────
#  Admin Training Management
# ─────────────────────────────────────────────────────────────

@app.route('/admin/training')
@admin_required
def admin_training():
    _warn_upload_root_once()
    db = get_db()
    videos = {v['day_number']: v for v in
              db.execute("SELECT * FROM training_videos ORDER BY day_number").fetchall()}

    # Members who need training
    members = db.execute(
        "SELECT username, joining_date, training_status, training_required, certificate_path, test_score "
        "FROM users WHERE role='team' AND status='approved' ORDER BY username"
    ).fetchall()

    # Progress per member
    all_progress = {}
    for m in members:
        prog = _get_training_progress(db, m['username'])
        all_progress[m['username']] = sum(1 for d in range(1, 8) if prog.get(d, 0))

    questions = db.execute(
        "SELECT * FROM training_questions ORDER BY sort_order, id"
    ).fetchall()

    bonus_videos = db.execute(
        "SELECT * FROM bonus_videos ORDER BY sort_order, id"
    ).fetchall()

    sig_file = _get_setting(db, 'admin_signature_file', '')
    db.close()

    # Warn if PDF/audio uploads may not persist (e.g. on Render without UPLOAD_ROOT)
    upload_root_set = bool(os.environ.get('UPLOAD_ROOT'))
    in_production = bool(os.environ.get('SECRET_KEY'))

    return render_template('admin_training.html',
                           videos=videos,
                           members=members,
                           all_progress=all_progress,
                           days=range(1, 8),
                           questions=questions,
                           bonus_videos=bonus_videos,
                           sig_file=sig_file,
                           upload_root_set=upload_root_set,
                           in_production=in_production)


@app.route('/training/media/<path:filename>')
@login_required
def training_media(filename):
    """Serve uploaded training podcast audio / PDF files."""
    media_dir = os.path.join(_upload_root(), 'uploads', 'training')
    return send_from_directory(media_dir, filename)


@app.route('/admin/training/save-video', methods=['POST'])
@admin_required
def admin_training_save_video():
    day   = request.form.get('day_number', type=int)
    title = request.form.get('title', '').strip()
    url   = request.form.get('youtube_url', '').strip()
    desc  = request.form.get('description', '').strip()

    if not day or day < 1 or day > 7:
        flash('Invalid day number.', 'danger')
        return redirect(url_for('admin_training'))

    # Keep existing values as fallback if no new file/url provided
    podcast_url = request.form.get('podcast_url_existing', '').strip()
    pdf_url     = request.form.get('pdf_url_existing', '').strip()

    # External URL takes over existing (file upload below can override again)
    ext_podcast_url = request.form.get('podcast_external_url', '').strip()
    if ext_podcast_url:
        podcast_url = ext_podcast_url

    media_dir = os.path.join(_upload_root(), 'uploads', 'training')
    audio_dir = os.path.join(media_dir, 'audio')
    pdf_dir   = os.path.join(media_dir, 'pdf')
    os.makedirs(audio_dir, exist_ok=True)
    os.makedirs(pdf_dir,   exist_ok=True)

    podcast_file = request.files.get('podcast_file')
    if podcast_file and podcast_file.filename:
        ext   = podcast_file.filename.rsplit('.', 1)[-1].lower() if '.' in podcast_file.filename else 'mp3'
        fname = f'day{day}_podcast.{ext}'
        podcast_file.save(os.path.join(audio_dir, fname))
        podcast_url = f'audio/{fname}'

    pdf_file = request.files.get('pdf_file')
    if pdf_file and pdf_file.filename:
        fname = f'day{day}_resource.pdf'
        pdf_file.save(os.path.join(pdf_dir, fname))
        pdf_url = f'pdf/{fname}'

    db = get_db()
    db.execute(
        "INSERT INTO training_videos (day_number, title, youtube_url, podcast_url, pdf_url, description) "
        "VALUES (?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(day_number) DO UPDATE SET title=?, youtube_url=?, podcast_url=?, pdf_url=?, description=?",
        (day, title, url, podcast_url, pdf_url, desc,
         title, url, podcast_url, pdf_url, desc)
    )
    db.commit()
    db.close()
    flash(f'Day {day} video saved.', 'success')
    return redirect(url_for('admin_training'))


@app.route('/admin/training/<username>/toggle', methods=['POST'])
@admin_required
def admin_training_toggle(username):
    db = get_db()
    user = db.execute(
        "SELECT training_required, training_status FROM users WHERE username=?",
        (username,)
    ).fetchone()
    if not user:
        db.close()
        flash('User not found.', 'danger')
        return redirect(url_for('admin_training'))

    if user['training_required']:
        # Disable training — give full access
        db.execute(
            "UPDATE users SET training_required=0, training_status='not_required' WHERE username=?",
            (username,)
        )
        flash(f'{username}: Training requirement removed. Full access granted.', 'success')
    else:
        # Enable training
        ts = 'pending' if user['training_status'] == 'not_required' else user['training_status']
        db.execute(
            "UPDATE users SET training_required=1, training_status=? WHERE username=?",
            (ts, username)
        )
        flash(f'{username}: Training required. Access locked until completion.', 'warning')

    db.commit()
    db.close()
    return redirect(url_for('admin_training'))


@app.route('/admin/training/<username>/reset', methods=['POST'])
@admin_required
def admin_training_reset(username):
    db = get_db()
    db.execute("DELETE FROM training_progress WHERE username=?", (username,))
    db.execute(
        "UPDATE users SET training_status='pending', certificate_path='', "
        "test_score=-1, test_attempts=0 WHERE username=? AND training_required=1",
        (username,)
    )
    db.commit()
    db.close()
    flash(f'{username}: Training progress reset.', 'success')
    return redirect(url_for('admin_training'))


# ─────────────────────────────────────────────────────────────
#  Training Test Routes
# ─────────────────────────────────────────────────────────────

@app.route('/training/test')
@login_required
def training_test():
    ts = session.get('training_status', 'pending')
    if ts not in ('completed', 'unlocked'):
        flash('Complete all 7 days of training first.', 'warning')
        return redirect(url_for('training_home'))

    username = session['username']
    db = get_db()

    # Fetch up to 20 questions (random order for variety)
    questions = db.execute(
        "SELECT * FROM training_questions ORDER BY RANDOM() LIMIT 20"
    ).fetchall()

    user_row = db.execute(
        "SELECT test_score, test_attempts FROM users WHERE username=?", (username,)
    ).fetchone()
    db.close()

    test_score   = user_row['test_score']   if user_row else -1
    test_attempts = user_row['test_attempts'] if user_row else 0

    return render_template('training_test.html',
                           questions=questions,
                           test_score=test_score,
                           test_attempts=test_attempts,
                           training_status=ts)


@app.route('/training/test/submit', methods=['POST'])
@login_required
def training_test_submit():
    ts = session.get('training_status', 'pending')
    if ts not in ('completed', 'unlocked'):
        return redirect(url_for('training_home'))

    username = session['username']
    db = get_db()

    questions = db.execute("SELECT * FROM training_questions ORDER BY id").fetchall()
    if not questions:
        db.close()
        flash('No questions available. Contact admin.', 'warning')
        return redirect(url_for('training_home'))

    correct = 0
    total   = len(questions)
    for q in questions:
        ans = request.form.get(f'q_{q["id"]}', '').strip().lower()
        if ans == q['correct_answer'].lower():
            correct += 1

    score   = int(correct / total * 100)
    passed  = 1 if score >= 60 else 0
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    db.execute(
        "INSERT INTO training_test_attempts (username, score, total_questions, passed, attempted_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (username, score, total, passed, now_str)
    )
    db.execute(
        "UPDATE users SET test_score=?, test_attempts=test_attempts+1 WHERE username=?",
        (score, username)
    )
    db.commit()
    db.close()

    if passed:
        flash(f'🎉 Congratulations! Score: {score}/100. Test passed! Download your certificate now.', 'success')
        return redirect(url_for('training_certificate'))
    else:
        flash(f'Score: {score}/100. Pass nahi hua (60 chahiye). Dobara try karo!', 'danger')
        return redirect(url_for('training_test'))


# ─────────────────────────────────────────────────────────────
#  Admin: Test Question Management
# ─────────────────────────────────────────────────────────────

@app.route('/admin/training/test/add-question', methods=['POST'])
@admin_required
def admin_training_add_question():
    question = request.form.get('question', '').strip()
    option_a = request.form.get('option_a', '').strip()
    option_b = request.form.get('option_b', '').strip()
    option_c = request.form.get('option_c', '').strip()
    option_d = request.form.get('option_d', '').strip()
    correct  = request.form.get('correct_answer', 'a').strip().lower()

    if not question or not option_a or not option_b:
        flash('Question aur kam se kam do options zaroori hain.', 'danger')
        return redirect(url_for('admin_training') + '#testTab')

    if correct not in ('a', 'b', 'c', 'd'):
        correct = 'a'

    db = get_db()
    max_order = db.execute("SELECT COALESCE(MAX(sort_order),0) FROM training_questions").fetchone()[0]
    db.execute(
        "INSERT INTO training_questions (question, option_a, option_b, option_c, option_d, correct_answer, sort_order) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (question, option_a, option_b, option_c, option_d, correct, max_order + 1)
    )
    db.commit()
    db.close()
    flash('Question add ho gaya.', 'success')
    return redirect(url_for('admin_training') + '#testTab')


@app.route('/admin/training/test/delete-question/<int:qid>', methods=['POST'])
@admin_required
def admin_training_delete_question(qid):
    db = get_db()
    db.execute("DELETE FROM training_questions WHERE id=?", (qid,))
    db.commit()
    db.close()
    flash('Question delete ho gaya.', 'success')
    return redirect(url_for('admin_training') + '#testTab')


# ─────────────────────────────────────────────────────────────
#  Admin: Bonus Videos Management
# ─────────────────────────────────────────────────────────────

@app.route('/admin/training/save-bonus-video', methods=['POST'])
@admin_required
def admin_training_save_bonus_video():
    vid_id  = request.form.get('vid_id', type=int)
    title   = request.form.get('title', '').strip()
    yt_url  = request.form.get('youtube_url', '').strip()
    desc    = request.form.get('description', '').strip()

    if not title or not yt_url:
        flash('Title aur YouTube URL zaroori hain.', 'danger')
        return redirect(url_for('admin_training') + '#bonusTab')

    db = get_db()
    if vid_id:
        db.execute(
            "UPDATE bonus_videos SET title=?, youtube_url=?, description=? WHERE id=?",
            (title, yt_url, desc, vid_id)
        )
    else:
        max_order = db.execute("SELECT COALESCE(MAX(sort_order),0) FROM bonus_videos").fetchone()[0]
        db.execute(
            "INSERT INTO bonus_videos (title, youtube_url, description, sort_order) VALUES (?, ?, ?, ?)",
            (title, yt_url, desc, max_order + 1)
        )
    db.commit()
    db.close()
    flash('Bonus video saved.', 'success')
    return redirect(url_for('admin_training') + '#bonusTab')


@app.route('/admin/training/delete-bonus-video/<int:vid_id>', methods=['POST'])
@admin_required
def admin_training_delete_bonus_video(vid_id):
    db = get_db()
    db.execute("DELETE FROM bonus_videos WHERE id=?", (vid_id,))
    db.commit()
    db.close()
    flash('Bonus video delete ho gaya.', 'success')
    return redirect(url_for('admin_training') + '#bonusTab')


# ─────────────────────────────────────────────────────────────
#  Admin: Signature Management
# ─────────────────────────────────────────────────────────────

@app.route('/admin/training/upload-signature', methods=['POST'])
@admin_required
def admin_training_upload_signature():
    f = request.files.get('signature_file')
    if not f or not f.filename:
        flash('Koi file select nahi ki.', 'danger')
        return redirect(url_for('admin_training') + '#sigTab')

    ext = f.filename.rsplit('.', 1)[-1].lower() if '.' in f.filename else ''
    if ext not in ('png', 'jpg', 'jpeg'):
        flash('Sirf PNG ya JPG accept hai.', 'danger')
        return redirect(url_for('admin_training') + '#sigTab')

    upload_dir = os.path.join(_upload_root(), 'uploads', 'admin')
    os.makedirs(upload_dir, exist_ok=True)
    filename = f'admin_signature.{ext}'
    f.save(os.path.join(upload_dir, filename))

    db = get_db()
    _set_setting(db, 'admin_signature_file', filename)
    db.commit()
    db.close()

    flash('Signature upload ho gayi.', 'success')
    return redirect(url_for('admin_training') + '#sigTab')


@app.route('/admin/training/signature-preview')
def training_signature_preview():
    db = get_db()
    sig_file = _get_setting(db, 'admin_signature_file', '')
    db.close()
    upload_dir = os.path.join(_upload_root(), 'uploads', 'admin')
    if sig_file and os.path.exists(os.path.join(upload_dir, sig_file)):
        return send_from_directory(upload_dir, sig_file)
    # Fallback to static default signature
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    if os.path.exists(os.path.join(static_dir, 'admin_signature.png')):
        return send_from_directory(static_dir, 'admin_signature.png')
    return '', 404


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


@app.route('/health')
def health():
    return {'status': 'ok'}, 200


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


def _upsert_daily_score(db, username, delta_pts,
                        delta_calls=0, delta_videos=0,
                        delta_batches=0, delta_payments=0):
    """Atomically add to today's daily_scores row, creating it if needed.
    Uses CASE WHEN for floor-at-zero because SQLite does not allow MAX()
    as a scalar in SET clauses (only as an aggregate).
    Uses INSERT OR REPLACE for the new-row path to handle rare concurrent inserts."""
    today     = _today_ist().strftime('%Y-%m-%d')
    yesterday = (_today_ist() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    existing  = db.execute(
        "SELECT id FROM daily_scores WHERE username=? AND score_date=?",
        (username, today)
    ).fetchone()
    if existing:
        # CASE WHEN is the correct SQLite way to floor at zero in an UPDATE
        db.execute("""
            UPDATE daily_scores SET
                total_points       = CASE WHEN total_points + ? < 0 THEN 0 ELSE total_points + ? END,
                calls_made         = CASE WHEN calls_made + ? < 0 THEN 0 ELSE calls_made + ? END,
                videos_sent        = CASE WHEN videos_sent + ? < 0 THEN 0 ELSE videos_sent + ? END,
                batches_marked     = CASE WHEN batches_marked + ? < 0 THEN 0 ELSE batches_marked + ? END,
                payments_collected = CASE WHEN payments_collected + ? < 0 THEN 0 ELSE payments_collected + ? END
            WHERE username=? AND score_date=?
        """, (delta_pts, delta_pts,
              delta_calls, delta_calls,
              delta_videos, delta_videos,
              delta_batches, delta_batches,
              delta_payments, delta_payments,
              username, today))
    else:
        yrow = db.execute(
            "SELECT streak_days FROM daily_scores WHERE username=? AND score_date=?",
            (username, yesterday)
        ).fetchone()
        streak       = (yrow['streak_days'] + 1) if yrow else 1
        streak_bonus = 10 if yrow else 0
        # INSERT OR REPLACE handles the rare concurrent-insert race condition
        db.execute("""
            INSERT OR REPLACE INTO daily_scores
                (username, score_date, calls_made, videos_sent,
                 batches_marked, payments_collected, total_points, streak_days)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (username, today,
              max(0, delta_calls), max(0, delta_videos),
              max(0, delta_batches), max(0, delta_payments),
              max(0, delta_pts + streak_bonus), streak))


def _get_today_score(db, username):
    """Return (total_points, streak_days) for today. 0,1 if no row."""
    today = _today_ist().strftime('%Y-%m-%d')
    row   = db.execute(
        "SELECT total_points, streak_days FROM daily_scores WHERE username=? AND score_date=?",
        (username, today)
    ).fetchone()
    if row:
        return row['total_points'], row['streak_days']
    return 0, 1


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

        # ── OWN LEADS (leader's personal work) ──────────────────
        own_stage1 = db.execute(
            _base + _own_where + f" AND status IN ({_s1_ph}) ORDER BY updated_at DESC",
            _own_params + list(STAGE1_STATUSES)
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
        own_past = db.execute(
            _base + _own_where + " AND status IN ('Fully Converted','Converted','Lost') ORDER BY updated_at DESC LIMIT 20",
            _own_params
        ).fetchall()

        # ── TEAM LEADS (downline's work) ─────────────────────────
        if _team_params:
            _t_ph = ','.join('?' * len(_team_params))
            _team_base = f"SELECT * FROM leads WHERE in_pool=0 AND deleted_at='' AND assigned_to IN ({_t_ph}) "
            team_stage1 = db.execute(
                _team_base + f"AND status IN ({_s1_ph}) ORDER BY assigned_to, updated_at DESC",
                _team_params + list(STAGE1_STATUSES)
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
            team_past = db.execute(
                _team_base + "AND status IN ('Fully Converted','Converted','Lost') ORDER BY assigned_to, updated_at DESC LIMIT 50",
                _team_params
            ).fetchall()
            _d_phs = ','.join('?' * len(_downline_only))
            downline_members = db.execute(
                f"SELECT username, fbo_id FROM users WHERE username IN ({_d_phs}) AND status='approved' ORDER BY username",
                _downline_only
            ).fetchall()
        else:
            team_stage1 = team_day1 = team_day2 = team_day3 = []
            team_pending = team_past = []
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
        team_stage1  = _enrich_leads(team_stage1)
        team_day1    = _enrich_leads(team_day1)
        team_day2    = _enrich_leads(team_day2)
        team_day3    = _enrich_leads(team_day3)
        team_pending = _enrich_leads(team_pending)

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

        db.close()

        return render_template('working.html',
            is_admin=False,
            is_leader=True,

            # Own leads
            own_stage1=own_stage1,
            own_day1=own_day1,
            own_day2=own_day2,
            own_day3=own_day3,
            own_pending=own_pending,
            own_past=own_past,

            # Team leads
            team_stage1=team_stage1,
            team_day1=team_day1,
            team_day2=team_day2,
            team_day3=team_day3,
            team_pending=team_pending,
            team_past=team_past,

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

    stage1_leads = db.execute(
        _base_team + f"AND status IN ({_s1_ph}) ORDER BY updated_at DESC",
        _tp + list(STAGE1_STATUSES)
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
    watch_url_v1 = url_for('watch_batch', slot=slot, v=1, _external=True) + '?token=' + token
    watch_url_v2 = url_for('watch_batch', slot=slot, v=2, _external=True) + '?token=' + token
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

    # Day 1 batches: only leader or admin can mark (team cannot send Day 1 task from dashboard)
    if batch.startswith('d1_'):
        if role not in ('leader', 'admin'):
            db.close(); return {'ok': False, 'error': 'Only leader/admin can send Day 1 batches'}, 403
        if role == 'leader':
            downline = _get_network_usernames(db, session['username'])
            if owner != session['username'] and owner not in downline:
                db.close(); return {'ok': False, 'error': 'Forbidden'}, 403
    else:
        # Day 2 batches: admin only
        if batch.startswith('d2_'):
            if role != 'admin':
                db.close(); return {'ok': False, 'error': 'Only admin can mark Day 2 batches'}, 403
        else:
            # Other batches (d3_, etc.): team can mark own; admin unrestricted
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
        'interview_done':    ('day2',),
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
    db = get_db()
    now = _now_ist()

    # All Day 2 leads visible to everyone
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

    db.close()
    return render_template('day2_progress.html',
        day2_leads=day2_leads,
        complete_count=complete_count,
        in_progress_count=in_progress_count,
        not_started_count=not_started_count,
        can_edit=can_edit,
        current_user=username,
    )


if __name__ == '__main__':
    _debug = os.environ.get('FLASK_DEBUG', '').lower() in ('1', 'true', 'yes')
    app.run(debug=_debug, host='0.0.0.0', port=int(os.environ.get('PORT', 5003)))
