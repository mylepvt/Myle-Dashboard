"""
Enrollment / watch-video routes and their local helpers.

Registered via register_enrollment_routes(app) at the end of app.py load so helpers
on the app module are available without circular import at import time.
"""
from __future__ import annotations

import re as _re
import secrets

from flask import jsonify, redirect, render_template, request, session, url_for

from database import get_db
from decorators import login_required


# ─────────────────────────────────────────────────
#  Local helpers (only used by enrollment routes)
# ─────────────────────────────────────────────────

def _sync_enroll_share_to_lead(db, token, username,
                                _now_ist=None, _upsert_daily_score=None,
                                _log_lead_event=None):
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
        'Video Sent', 'Video Watched', 'Paid \u20b9196', 'Mindset Lock',
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


def _sync_watch_event_to_lead(db, token,
                               _now_ist=None, _upsert_daily_score=None,
                               _log_lead_event=None, _push_to_users=None):
    """
    Called when prospect opens watch page for the FIRST TIME (view_count 0->1).
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
        'Video Sent', 'Video Watched', 'Paid \u20b9196', 'Mindset Lock',
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
                   f'Prospect watched video: "{video_name}" \u2014 Call karo abhi!')

    _upsert_daily_score(db, shared_by, 5)

    try:
        _push_to_users(db, shared_by,
                       f'{lead["name"] or "Lead"} watched the video!',
                       'Call now \u2014 interest is at its peak!',
                       '/working')
    except Exception:
        pass

    try:
        db.execute(
            "UPDATE enroll_share_links SET watch_synced=1 WHERE token=?", (token,)
        )
    except Exception:
        pass


def _mark_batch_done_for_lead(db, lead_id, slot,
                               _now_ist=None, _upsert_daily_score=None):
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


def register_enrollment_routes(app):
    """Attach enrollment / watch-video URL rules to the Flask app."""
    from app import (  # noqa: PLC0415 — late import after app module is populated
        _BATCH_LABELS,
        _BATCH_SLOTS,
        _get_setting,
        _log_lead_event,
        _now_ist,
        _public_external_url,
        _push_to_users,
        _today_ist,
        _upsert_daily_score,
        _youtube_embed_url,
    )

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

        _sync_enroll_share_to_lead(db, token, username,
                                    _now_ist=_now_ist,
                                    _upsert_daily_score=_upsert_daily_score,
                                    _log_lead_event=_log_lead_event)
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

        watch_url = _public_external_url('watch_video', token=token)
        return jsonify({'ok': True, 'token': token, 'watch_url': watch_url})


    @app.route('/watch/enrollment')
    def watch_enrollment():
        """Public page: enrollment video in minimal embed (no YouTube UI)."""
        db = get_db()
        enrollment_video_url = _get_setting(db, 'enrollment_video_url', '')
        enrollment_video_title = _get_setting(db, 'enrollment_video_title', 'Enrollment Video')
        db.close()
        embed_url = _youtube_embed_url(enrollment_video_url)
        if not embed_url:
            return render_template('watch_video.html', error='Video not configured', title='Enrollment Video'), 404
        return render_template('watch_video.html', embed_url=embed_url, title=enrollment_video_title or 'Enrollment Video', error=None)


    @app.route('/watch/batch/<slot>/<int:v>')
    def watch_batch(slot, v):
        """Public page: 3-day batch video in minimal embed.
        If ?token= is present, auto-marks that batch slot done for the lead."""
        if slot not in _BATCH_SLOTS or v not in (1, 2):
            return render_template('watch_video.html', error='Invalid link', title='Batch Video'), 404
        db = get_db()
        # Auto-mark batch done when prospect opens tokenized link
        token = (request.args.get('token', '') or '').strip()
        # WhatsApp/in-app browsers sometimes pass trailing punctuation in query text.
        token = _re.sub(r'[^A-Za-z0-9_-]', '', token)
        if token:
            try:
                link = db.execute(
                    "SELECT * FROM batch_share_links WHERE token=? AND used=0", (token,)
                ).fetchone()
                if link and link['slot'] == slot:
                    _mark_batch_done_for_lead(db, link['lead_id'], slot,
                                               _now_ist=_now_ist,
                                               _upsert_daily_score=_upsert_daily_score)
            except Exception:
                pass
        setting_key = f'batch_{slot}_v{v}'
        yt_url = _get_setting(db, setting_key, '')
        embed_url = _youtube_embed_url(yt_url)
        fallback_used = False

        # Fallback: If Watch 1's embed URL can't be derived (empty OR invalid URL),
        # open Video 2 embed instead.
        if int(v) == 1 and not embed_url:
            yt_url_v2 = _get_setting(db, f'batch_{slot}_v2', '')
            embed_url = _youtube_embed_url(yt_url_v2)
            yt_url = yt_url_v2
            fallback_used = bool(embed_url)

        db.close()

        if not embed_url:
            return render_template(
                'watch_video.html',
                error='Video not configured',
                title=_BATCH_LABELS.get(slot, 'Batch Video')
            ), 404

        title = _BATCH_LABELS.get(slot, 'Batch Video') + ' \u2014 Video ' + str(v)
        if fallback_used:
            title = _BATCH_LABELS.get(slot, 'Batch Video') + ' \u2014 Video 1 (using Video 2)'

        return render_template('watch_batch.html', embed_url=embed_url, title=title, slot=slot, v=v)


    @app.route('/watch/<token>')
    def watch_video(token):
        """Public watch page; first view syncs to lead (Video Watched) and notifies sharer."""
        token = (token or '').strip()
        token = _re.sub(r'[^A-Za-z0-9_-]', '', token)
        db = get_db()
        link = db.execute(
            "SELECT * FROM enroll_share_links WHERE token=?", (token,)
        ).fetchone()
        if not link:
            # Defensive fallback: if a batch token is opened on /watch/<token>,
            # redirect to the proper batch watch route instead of showing expired.
            try:
                b = db.execute(
                    "SELECT slot FROM batch_share_links WHERE token=? LIMIT 1", (token,)
                ).fetchone()
                if b and b['slot'] in _BATCH_SLOTS:
                    db.close()
                    return redirect(url_for('watch_batch', slot=b['slot'], v=1, token=token))
            except Exception:
                pass
            db.close()
            return render_template('watch_video.html', error='Link not found or expired'), 404

        is_first_view = (link['view_count'] == 0)
        db.execute(
            "UPDATE enroll_share_links SET view_count = view_count + 1 WHERE token=?",
            (token,)
        )
        db.commit()

        if is_first_view:
            _sync_watch_event_to_lead(db, token,
                                       _now_ist=_now_ist,
                                       _upsert_daily_score=_upsert_daily_score,
                                       _log_lead_event=_log_lead_event,
                                       _push_to_users=_push_to_users)
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
