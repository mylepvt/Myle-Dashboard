"""
Announcements, leaderboard, and live-session routes.

Registered via register_social_routes(app) at the end of app.py load so helpers
on the app module are available without circular import at import time.
"""
from __future__ import annotations

import datetime

from flask import (
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from database import get_db


def register_social_routes(app):
    """Attach social/community URL rules to the Flask app (preserves endpoint names)."""
    from app import _push_all_team  # noqa: PLC0415 — late import
    from decorators import admin_required, login_required
    from helpers import (
        _get_network_usernames,
        _get_setting,
        _set_setting,
        _get_user_badges_emoji,
        _today_ist,
    )

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
