import os
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, flash, session
from werkzeug.security import generate_password_hash, check_password_hash
from database import get_db, init_db, migrate_db, seed_users

app = Flask(__name__)
# Use env variable on Render/production; fallback for local dev
app.secret_key = os.environ.get('SECRET_KEY', 'myle_community_secret_2024_local')

STATUSES = ['New', 'Contacted', 'Day 1', 'Day 2', 'Interview', 'Converted', 'Lost']
SOURCES  = ['WhatsApp', 'Facebook', 'Instagram', 'LinkedIn',
            'Walk-in', 'Referral', 'YouTube', 'Cold Call', 'Other']
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
    """
    All dashboard KPIs computed in a single SQL pass.
    Pass username to scope results to a specific team member.
    """
    if username:
        where_clause = "WHERE assigned_to = ?"
        params = (username,)
    else:
        where_clause = ""
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


# ─────────────────────────────────────────────
#  Context processor – inject pending count
# ─────────────────────────────────────────────

@app.context_processor
def inject_pending_count():
    if session.get('role') == 'admin':
        db    = get_db()
        count = db.execute(
            "SELECT COUNT(*) FROM users WHERE status='pending'"
        ).fetchone()[0]
        db.close()
        return {'pending_count': count}
    return {'pending_count': 0}


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
            (username, generate_password_hash(password, method='pbkdf2:sha256'), fbo_id, upline_name, phone, email)
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

        # Check password – support both hashed and legacy plain-text
        password_ok = False
        if user:
            stored = user['password']
            if stored.startswith(('pbkdf2:', 'scrypt:', 'argon2:')):
                password_ok = check_password_hash(stored, password)
            else:
                # Legacy plain-text: compare then auto-upgrade to hash
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
    user = db.execute("SELECT username FROM users WHERE id=?", (user_id,)).fetchone()
    if user:
        db.execute("UPDATE users SET status='approved' WHERE id=?", (user_id,))
        db.commit()
        flash(f'"{user["username"]}" has been approved and can now log in.', 'success')
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
        "SELECT * FROM leads ORDER BY created_at DESC LIMIT 5"
    ).fetchall()

    status_data = {}
    for s in STATUSES:
        count = db.execute(
            "SELECT COUNT(*) as c FROM leads WHERE status=?", (s,)
        ).fetchone()['c']
        status_data[s] = count

    monthly = db.execute("""
        SELECT strftime('%Y-%m', created_at) as month,
               SUM(payment_amount) as total
        FROM leads
        WHERE payment_done=1
        GROUP BY month
        ORDER BY month DESC
        LIMIT 6
    """).fetchall()

    # Per-member performance summary
    members = db.execute("SELECT * FROM team_members ORDER BY name").fetchall()
    team_stats = []
    for m in members:
        row = db.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='Converted' THEN 1 ELSE 0 END) as converted,
                SUM(CASE WHEN payment_done=1     THEN 1 ELSE 0 END) as paid,
                SUM(COALESCE(payment_amount,0) + COALESCE(revenue,0)) as revenue
            FROM leads WHERE assigned_to=?
        """, (m['name'],)).fetchone()
        team_stats.append({'member': m, 'stats': row})

    # Pending registration requests
    pending_users = db.execute(
        "SELECT * FROM users WHERE status='pending' ORDER BY created_at DESC"
    ).fetchall()

    # Today's report summary
    import datetime
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
                           today=today)


# ─────────────────────────────────────────────
#  Team Dashboard  (scoped to logged-in user)
# ─────────────────────────────────────────────

@app.route('/dashboard')
@login_required
def team_dashboard():
    username = session['username']
    db       = get_db()
    metrics  = _get_metrics(db, username=username)

    recent = db.execute(
        "SELECT * FROM leads WHERE assigned_to=? ORDER BY created_at DESC LIMIT 5",
        (username,)
    ).fetchall()

    status_data = {}
    for s in STATUSES:
        count = db.execute(
            "SELECT COUNT(*) as c FROM leads WHERE status=? AND assigned_to=?",
            (s, username)
        ).fetchone()['c']
        status_data[s] = count

    monthly = db.execute("""
        SELECT strftime('%Y-%m', created_at) as month,
               SUM(payment_amount) as total
        FROM leads
        WHERE payment_done=1 AND assigned_to=?
        GROUP BY month
        ORDER BY month DESC
        LIMIT 6
    """, (username,)).fetchall()

    import datetime
    today = datetime.date.today().isoformat()
    today_report = db.execute(
        "SELECT * FROM daily_reports WHERE username=? AND report_date=?",
        (username, today)
    ).fetchone()

    db.close()
    return render_template('dashboard.html',
                           metrics=metrics,
                           recent=recent,
                           status_data=status_data,
                           monthly=monthly,
                           payment_amount=PAYMENT_AMOUNT,
                           today_report=today_report,
                           today=today)


# ─────────────────────────────────────────────
#  Leads – List
# ─────────────────────────────────────────────

@app.route('/leads')
@login_required
def leads():
    db     = get_db()
    status = request.args.get('status', '')
    search = request.args.get('q', '').strip()

    query  = "SELECT * FROM leads WHERE 1=1"
    params = []

    # Team members only see their assigned leads
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

        # Admin can assign freely; team leads are always self-assigned
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
                 follow_up_date, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    # Admin sees any lead; team only sees their own
    if session.get('role') == 'admin':
        lead = db.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
    else:
        lead = db.execute(
            "SELECT * FROM leads WHERE id=? AND assigned_to=?",
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

        # Only admin can reassign leads
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

    # Team: verify ownership before updating
    if session.get('role') != 'admin':
        lead = db.execute(
            "SELECT id FROM leads WHERE id=? AND assigned_to=?",
            (lead_id, session['username'])
        ).fetchone()
        if not lead:
            flash('Access denied.', 'danger')
            db.close()
            return redirect(url_for('leads'))

    db.execute(
        "UPDATE leads SET status=?, updated_at=datetime('now','localtime') WHERE id=?",
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
        lead = db.execute("SELECT name FROM leads WHERE id=?", (lead_id,)).fetchone()
    else:
        lead = db.execute(
            "SELECT name FROM leads WHERE id=? AND assigned_to=?",
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
            FROM leads WHERE referred_by=?
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
    today    = __import__('datetime').date.today().isoformat()
    db       = get_db()

    # Load today's existing report (if re-submitting / editing)
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

        # Upsert: insert or replace if same user + date
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
        flash('✅ Daily report submitted successfully!', 'success')
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

    # KPI totals across filtered reports
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

    # All distinct members who ever submitted
    members = db.execute(
        "SELECT DISTINCT username FROM daily_reports ORDER BY username"
    ).fetchall()

    # Today's submitters vs approved team members (who hasn't reported today?)
    import datetime
    today = datetime.date.today().isoformat()
    submitted_today = [r['username'] for r in db.execute(
        "SELECT username FROM daily_reports WHERE report_date=?", (today,)
    ).fetchall()]
    approved_team = [u['username'] for u in db.execute(
        "SELECT username FROM users WHERE role='team' AND status='approved'"
    ).fetchall()]
    missing_today = [u for u in approved_team if u not in submitted_today]

    # Daily trend data (last 14 days) for chart
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
#  Boot
# ─────────────────────────────────────────────

if __name__ == '__main__':
    init_db()
    migrate_db()
    seed_users()
    app.run(debug=False, host='0.0.0.0', port=5001)
