"""
helpers.py — Shared constants and utility functions extracted from app.py.

All functions here are pure helpers: they operate on DB connections, lead dicts,
or datetime values.  None of them depend on Flask's `app` object or `current_app`.
Some import `request` from flask for IP logging but gracefully fall back.
"""

import datetime
import re as _re

try:
    import pytz
    _IST = pytz.timezone('Asia/Kolkata')
except ImportError:
    pytz = None
    _IST = None


# ─────────────────────────────────────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────────────────────────────────────

STATUSES = ['New Lead', 'New', 'Contacted', 'Invited', 'Video Sent', 'Video Watched',
            'Paid ₹196', 'Mindset Lock',
            'Day 1', 'Day 2', 'Interview',
            '2cc Plan',
            'Track Selected', 'Seat Hold Confirmed',
            'Pending',
            'Level Up',
            'Fully Converted',
            'Training', 'Converted', 'Lost', 'Retarget', 'Inactive']

# All active pipeline statuses — auto-expire to Inactive after 24 hrs of no status change
# Terminal statuses (Fully Converted, Converted, Lost, Pending, Inactive) are excluded
PIPELINE_AUTO_EXPIRE_STATUSES = [
    'New Lead', 'New', 'Contacted', 'Invited', 'Video Sent', 'Video Watched',
    'Paid ₹196', 'Mindset Lock',
    'Day 1', 'Day 2', 'Interview', '2cc Plan', 'Track Selected', 'Seat Hold Confirmed',
    'Level Up',
    'Training', 'Retarget',
]

STATUS_TO_STAGE = {
    'New Lead':            'prospecting',
    'New':                 'prospecting',
    'Contacted':           'prospecting',
    'Invited':             'prospecting',
    'Video Sent':          'prospecting',
    'Video Watched':       'prospecting',
    'Paid ₹196':           'enrolled',
    'Mindset Lock':        'enrolled',
    'Day 1':               'day1',
    'Day 2':               'day2',
    'Interview':           'day3',
    '2cc Plan':            'plan_2cc',
    'Track Selected':      'day3',
    'Seat Hold Confirmed': 'seat_hold',
    'Pending':             'pending',
    'Level Up':            'level_up',
    'Fully Converted':     'closing',
    'Training':            'training',
    'Converted':           'complete',
    'Lost':                'lost',
    'Retarget':            'prospecting',
    'Inactive':            'inactive',
}

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

SOURCES = ['WhatsApp', 'Facebook', 'Instagram', 'LinkedIn',
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

BADGE_META = {
    'hot_streak':    ('\U0001f525', 'Hot Streak',    '7+ din active raho'),
    'speed_closer':  ('\u26a1', 'Speed Closer',  'Enrollment \u2192 Day1 in \u22643 days'),
    'money_maker':   ('\U0001f4b0', 'Money Maker',   '5+ payments collected'),
    'first_convert': ('\U0001f3c6', 'Converter',     'Pehli full conversion'),
    'rising_star':   ('\u2b50', 'Rising Star',   'Week ka top scorer'),
    'centurion':     ('\U0001f4af', 'Centurion',     '10,000+ total points'),
    'batch_master':  ('\U0001f4da', 'Batch Master',  '100 batches marked total'),
}

STAGE_TO_DEFAULT_STATUS = {
    'enrollment': 'New Lead',
    'day1':       'Day 1',
    'day2':       'Day 2',
    'day3':       'Interview',
    'seat_hold':  'Seat Hold Confirmed',
    'closing':    'Fully Converted',
    'training':   'Training',
    'complete':   'Converted',
    'lost':       'Lost',
}


# ─────────────────────────────────────────────────────────────────────────────
#  Time helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_ist():
    """Current datetime in IST as a naive datetime (safe for DB storage & strptime comparisons)."""
    if _IST:
        return datetime.datetime.now(_IST).replace(tzinfo=None)
    return datetime.datetime.now()


def _today_ist():
    """Current date in IST."""
    if _IST:
        return datetime.datetime.now(_IST).date()
    return datetime.date.today()


# ─────────────────────────────────────────────────────────────────────────────
#  DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _log_activity(db, username, event_type, details=''):
    """Log a user activity event (login, lead_update, etc.)."""
    try:
        from flask import request
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

    balance = round(float(recharged) - float(spent), 2)
    return {
        'recharged': round(float(recharged), 2),
        'spent':     round(float(spent), 2),
        'balance':   balance,
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
    leader = (row['upline_username'] or '').strip() or (row['upline_name'] or '').strip()
    if not leader:
        return _get_admin_username(db)
    lrow = db.execute(
        "SELECT username FROM users WHERE username=? AND status='approved'", (leader,)
    ).fetchone()
    return lrow['username'] if lrow else _get_admin_username(db)


# ─────────────────────────────────────────────────────────────────────────────
#  Priority / Heat / Next-Action / AI-Tip engines
# ─────────────────────────────────────────────────────────────────────────────

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


def _calculate_heat_score(lead):
    """Return 0-100 heat score: call_status signal + stage + recency + follow-up."""
    score = 0
    keys  = lead.keys() if hasattr(lead, 'keys') else []
    get   = lambda k, d='': lead[k] if k in keys else d

    score += {
        'Payment Done':         40,
        'Video Watched':        25,
        'Called - Interested':  20,
        'Called - Follow Up':   15,
        'Video Sent':           10,
        'Called - No Answer':    5,
    }.get(get('call_status'), 0)

    stage = get('pipeline_stage', 'enrollment')
    if stage in ('day3', 'seat_hold'):
        score += 20
    elif stage in ('day1', 'day2'):
        score += 10

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

    today_str = _today_ist().isoformat()
    fu = get('follow_up_date', '')
    if fu:
        if   fu[:10] == today_str: score += 20
        elif fu[:10] <  today_str: score -= 10

    return max(0, min(100, int(score)))


def _get_next_action(lead):
    """Return {action, type, priority} -- the single most important next step."""
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
        return {'action': '\u2014', 'type': 'cold', 'priority': 9}

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

    created = get('created_at', '')
    days_in = 0
    if created:
        try:
            days_in = (_today_ist() - datetime.datetime.strptime(created[:10], '%Y-%m-%d').date()).days
        except Exception:
            pass

    expiry_str = get('seat_hold_expiry', '')
    expiry_soon = False
    if expiry_str:
        try:
            expiry = datetime.datetime.strptime(expiry_str[:19], '%Y-%m-%d %H:%M:%S')
            hours  = (expiry - _now_ist().replace(tzinfo=None)).total_seconds() / 3600
            expiry_soon = hours < 24
        except Exception:
            pass

    d1_done = int(get('d1_morning', 0) or 0) + int(get('d1_afternoon', 0) or 0) + int(get('d1_evening', 0) or 0)

    if stage == 'seat_hold' and expiry_soon:
        return f"\u26a0\ufe0f {name}'s seat hold expires soon \u2014 final call today is a must!"
    if stage == 'day1' and d1_done == 3:
        return f"\u2705 All batches complete! Move {name} to Day 2 now."
    if stage == 'enrollment' and heat >= 75:
        return f"\U0001f525 {name} looks very interested \u2014 try to convert today."
    if stage == 'enrollment' and call_status == 'Video Watched' and not payment_done:
        return f"\U0001f440 {name} has watched the video \u2014 make a strong payment call now."
    if stage == 'enrollment' and call_status == 'Payment Done':
        return f"\U0001f4b0 Payment confirmed! Move {name} to Day 1 and do Mindset Lock call."
    if stage == 'enrollment' and days_in > 5 and heat < 30:
        return f"\u2744\ufe0f {name} has been stuck for {days_in}d and going cold \u2014 do a strong follow-up call."
    if stage == 'enrollment' and (not call_status or call_status == 'Not Called Yet'):
        return f"\U0001f4de {name} has not been called yet \u2014 contact today."
    if stage == 'day1' and d1_done < 3:
        return f"\u23f3 {name} has {d1_done}/3 batches done \u2014 remind for the rest."
    if stage == 'day2':
        return f"\U0001f393 {name} is in Day 2 \u2014 schedule interview with admin."
    if stage == 'day3':
        return f"\U0001f3c1 {name} is at interview stage \u2014 get track selected and confirm seat hold."
    if stage == 'seat_hold':
        return f"\U0001f6e1\ufe0f {name} is on seat hold \u2014 follow up for final payment."
    if heat < 40 and days_in > 3:
        return f"\u2744\ufe0f {name} inactive for {days_in}d \u2014 call once and update status."
    return f"\U0001f4cb Maintain regular follow up with {name}."


def _enrich_lead(lead):
    """Add heat, next_action, next_action_type to a lead. Returns a dict."""
    d  = dict(lead)
    for k in ('day1_batch', 'day2_batch', 'day3_batch',
              'heat', 'next_action', 'next_action_type'):
        d.setdefault(k, '' if 'batch' in k else 0 if k == 'heat' else '')
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
#  Pipeline transition helpers
# ─────────────────────────────────────────────────────────────────────────────

def _transition_stage(db, lead_id, new_stage, triggered_by, status_override=None):
    """
    Move a lead to a new pipeline stage: update pipeline_stage, current_owner,
    optionally status (when status_override or stage default), and log to lead_stage_history.
    Returns (new_stage, new_owner).
    """
    lead = db.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
    if not lead:
        return new_stage, ''

    lead_keys = lead.keys()
    current_stage = lead['pipeline_stage'] if 'pipeline_stage' in lead_keys else 'enrollment'
    current_owner = lead['current_owner'] if 'current_owner' in lead_keys else ''

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

    new_status = status_override if status_override is not None else STAGE_TO_DEFAULT_STATUS.get(new_stage)
    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')

    # Reset pipeline_entered_at on every stage change for auto-expirable statuses
    effective_status = new_status if new_status is not None else STAGE_TO_DEFAULT_STATUS.get(new_stage)
    entering_active_pipeline = effective_status in PIPELINE_AUTO_EXPIRE_STATUSES
    new_pipeline_entered_at = now_str if entering_active_pipeline else ''

    if new_status is not None:
        db.execute(
            "UPDATE leads SET pipeline_stage=?, current_owner=?, status=?, updated_at=?, pipeline_entered_at=? WHERE id=?",
            (new_stage, new_owner, new_status, now_str, new_pipeline_entered_at, lead_id)
        )
    else:
        db.execute(
            "UPDATE leads SET pipeline_stage=?, current_owner=?, updated_at=?, pipeline_entered_at=? WHERE id=?",
            (new_stage, new_owner, now_str, new_pipeline_entered_at, lead_id)
        )

    db.execute(
        "INSERT INTO lead_stage_history (lead_id, stage, owner, triggered_by, created_at) VALUES (?,?,?,?,?)",
        (lead_id, new_stage, new_owner, triggered_by, now_str)
    )

    if new_stage == 'training':
        _trigger_training_unlock(db, lead)

    db.commit()
    return new_stage, new_owner


def _trigger_training_unlock(db, lead):
    """When a lead reaches the training stage, unlock the assigned user training."""
    phone = lead['phone'] if 'phone' in lead.keys() else ''
    clean = _re.sub(r'[^0-9]', '', phone)
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
        # Push notification — late import to avoid circular dependency
        try:
            from app import _push_to_users
            _push_to_users(db, user_row['username'],
                           'Training Ready!',
                           'Start 7-day training. You will get a certificate!',
                           '/training')
        except Exception:
            pass
        _log_activity(db, user_row['username'], 'training_unlocked',
                      f'Lead #{lead["id"]} transitioned to training')


def _auto_expire_pipeline_leads(db, username):
    """
    Move leads to 'Pending' if they've been in an active pipeline stage for 24+ hours
    without any update. Runs on dashboard load for the given user's assigned leads.
    """
    from datetime import timedelta
    cutoff = (_now_ist() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
    placeholders = ','.join('?' * len(PIPELINE_AUTO_EXPIRE_STATUSES))
    expired = db.execute(f"""
        SELECT id, name FROM leads
        WHERE assigned_to=? AND in_pool=0 AND deleted_at=''
        AND status IN ({placeholders})
        AND pipeline_entered_at != '' AND pipeline_entered_at < ?
    """, (username, *PIPELINE_AUTO_EXPIRE_STATUSES, cutoff)).fetchall()

    now_str = _now_ist().strftime('%Y-%m-%d %H:%M:%S')
    for lead in expired:
        db.execute("""
            UPDATE leads SET status='Inactive', pipeline_stage='inactive', updated_at=?
            WHERE id=?
        """, (now_str, lead['id']))
        _log_activity(db, 'system', 'pipeline_expired',
                      f'Lead #{lead["id"]} ({lead["name"]}) auto-moved to Inactive after 24hr inactivity')
    if expired:
        db.commit()
    return len(expired)


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


# ─────────────────────────────────────────────────────────────────────────────
#  Badge system
# ─────────────────────────────────────────────────────────────────────────────

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

    # speed_closer
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

    # first_convert
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

    # rising_star: top scorer this week
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


# ─────────────────────────────────────────────────────────────────────────────
#  Daily scores
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_daily_score(db, username, delta_pts,
                        delta_calls=0, delta_videos=0,
                        delta_batches=0, delta_payments=0):
    """Atomically add to today's daily_scores row, creating it if needed."""
    today     = _today_ist().strftime('%Y-%m-%d')
    yesterday = (_today_ist() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    existing  = db.execute(
        "SELECT id FROM daily_scores WHERE username=? AND score_date=?",
        (username, today)
    ).fetchone()
    if existing:
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


def _get_actual_daily_counts(db, username):
    """
    Returns system-verified counts for today.
    Tamper-proof -- written by system via daily_scores.
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
