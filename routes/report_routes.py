"""
Report routes (team daily report submission, admin reports view, leader team reports).

Registered via register_report_routes(app) at the end of app.py load so helpers
on the app module are available without circular import at import time.
"""
from __future__ import annotations

from flask import (
    flash, redirect, render_template, request, session, url_for,
)

from database import get_db
from helpers import _get_actual_daily_counts


def register_report_routes(app):
    """Attach report-related URL rules to the Flask app (preserves endpoint names)."""
    from app import (  # noqa: PLC0415 — late import after app module is populated
        admin_required,
        login_required,
        safe_route,
        _log_activity,
        _now_ist,
        _today_ist,
        _upsert_daily_score,
        _check_and_award_badges,
        _get_network_usernames,
    )

    # ─────────────────────────────────────────────────────────────
    #  Daily Reports – Submit (team member)
    # ─────────────────────────────────────────────────────────────

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

    # ─────────────────────────────────────────────────────────────
    #  Daily Reports – Admin View
    # ─────────────────────────────────────────────────────────────

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
