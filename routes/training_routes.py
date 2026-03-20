"""
Training routes (team training, admin training management, test, certificate, bonus videos, signature).

Registered via register_training_routes(app) at the end of app.py load so helpers
on the app module are available without circular import at import time.
"""
from __future__ import annotations

import datetime
import os

from flask import (
    flash, redirect, render_template, request, send_from_directory,
    session, url_for,
)

from database import get_db


# ── Local helpers (only used by training routes) ──────────────

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


def register_training_routes(app):
    """Attach training-related URL rules to the Flask app (preserves endpoint names)."""
    from app import (  # noqa: PLC0415 — late import after app module is populated
        admin_required,
        login_required,
        safe_route,
        _get_setting,
        _set_setting,
        _now_ist,
        _today_ist,
        _upload_root,
        _warn_upload_root_once,
    )

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

        # Sync session from DB so upload/certificate routes see correct status
        if user_row and user_row['training_status']:
            ts = user_row['training_status']
            session['training_status'] = ts

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
            flash('\U0001f389 All 7 days complete! Take the training test \u2014 score 60/100 to unlock your certificate.', 'success')
        else:
            flash(f'\u2705 Day {day} complete! Keep going.', 'success')

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
    @safe_route
    def training_upload_certificate():
        import base64 as _b64

        # Verify from DB (session can be stale — e.g. new tab, re-login, cache)
        db = get_db()
        user = db.execute(
            "SELECT training_status, test_score FROM users WHERE username=?",
            (session['username'],)
        ).fetchone()
        ts = (user['training_status'] if user else '') or 'pending'
        test_score = (user['test_score'] if user and user['test_score'] is not None else -1)

        if ts not in ('completed', 'unlocked'):
            db.close()
            flash('Complete training first.', 'warning')
            return redirect(url_for('training_home'))

        # If completed, must have passed test (60+) to upload
        if ts == 'completed' and test_score < 60:
            db.close()
            flash('Training test pass karna zaroori hai (60/100). Pehle test do.', 'warning')
            return redirect(url_for('training_test'))

        # Sync session so it stays correct
        session['training_status'] = ts
        db.close()

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

        # Read file bytes and encode as base64 for SQLite storage
        # This avoids any dependency on the filesystem (Render has an ephemeral FS)
        file_bytes = f.read()
        cert_blob  = _b64.b64encode(file_bytes).decode('utf-8')
        filename   = f"{session['username']}_cert.{ext}"

        # Best-effort: also try to save to disk, but NEVER block unlock on failure
        try:
            upload_dir = os.path.join(_upload_root(), 'uploads', 'training_certs')
            os.makedirs(upload_dir, exist_ok=True)
            with open(os.path.join(upload_dir, filename), 'wb') as fh:
                fh.write(file_bytes)
        except Exception as _e:
            app.logger.warning(f"training_upload_certificate: disk save failed for "
                               f"{session['username']} ({_e}) — blob stored in DB instead")

        # Always update DB — certificate_blob guarantees we have the file even on Render
        db = get_db()
        db.execute(
            """UPDATE users
                  SET training_status='unlocked',
                      certificate_path=?,
                      certificate_blob=?
                WHERE username=?""",
            (filename, cert_blob, session['username'])
        )
        db.commit()
        db.close()

        session['training_status'] = 'unlocked'
        flash('\U0001f389 Certificate uploaded! Full app access granted. Welcome to Myle Community!', 'success')
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
        username = session['username']
        db = get_db()
        user_row = db.execute(
            "SELECT training_status, test_score, test_attempts FROM users WHERE username=?",
            (username,)
        ).fetchone()
        ts = (user_row and user_row['training_status']) or 'pending'
        if ts not in ('completed', 'unlocked'):
            db.close()
            flash('Complete all 7 days of training first.', 'warning')
            return redirect(url_for('training_home'))

        session['training_status'] = ts  # keep session in sync

        questions = db.execute(
            "SELECT * FROM training_questions ORDER BY RANDOM() LIMIT 20"
        ).fetchall()

        test_score   = user_row['test_score']   if user_row else -1
        test_attempts = user_row['test_attempts'] if user_row else 0

        db.close()
        return render_template('training_test.html',
                               questions=questions,
                               test_score=test_score,
                               test_attempts=test_attempts,
                               training_status=ts)

    @app.route('/training/test/submit', methods=['POST'])
    @login_required
    def training_test_submit():
        username = session['username']
        db = get_db()
        user = db.execute("SELECT training_status FROM users WHERE username=?", (username,)).fetchone()
        ts = (user and user['training_status']) or 'pending'
        if ts not in ('completed', 'unlocked'):
            db.close()
            flash('Complete all 7 training days first.', 'warning')
            return redirect(url_for('training_home'))

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
            flash(f'\U0001f389 Congratulations! Score: {score}/100. Test passed! Download your certificate now.', 'success')
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
        static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static')
        if os.path.exists(os.path.join(static_dir, 'admin_signature.png')):
            return send_from_directory(static_dir, 'admin_signature.png')
        return '', 404
