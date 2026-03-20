# Pipeline & App Flow — Cross-Check Reference

**Last verified:** 2025-03-18

---

## 1. Working Section — Roles

| Role   | View        | Data source                          | Tabs / Sections |
|--------|-------------|--------------------------------------|------------------|
| Admin  | working()   | All leads, team_pipeline, batch_completion | Single view, no My Work / Team Work |
| Leader | working()   | Own leads + downline leads           | **My Work** (own), **Team Work** (downline), **Enroll** |
| Team   | working()   | Own leads only                       | Single view (Stage 1, Day 1, Day 2, Day 3, Pending) |

- **Leader:** `_downline_only` = `_get_network_usernames(db, username)` minus self. If that raises, we use `[]` (no crash). Team Work tab **always** shown; empty state if no downline.
- **Leader today_actions:** Pending calls, videos to send, batches due, closings due (own leads) — passed as `today_actions` for sidebar badge.

---

## 2. Leader — Enroll To & DB

- **enroll_content:** Columns `id`, `curiosity_title`, `title`, `created_at`, **is_active**, **day_number**, **sort_order** (migration in `database.py`).
- **enroll_pdfs:** Table exists with `id`, `title`, `url`, `is_active`, `sort_order`, `created_at`.
- Leader block loads enroll data in a **try/except**; on any failure it uses empty `enroll_days`, `enroll_pdfs`, `recent_shares`, `team_leads_for_enroll` (no 500).
- Lead row access uses **\_row_val(r, key, default)** so missing columns (e.g. `d1_morning`, `call_result`) don’t cause KeyError.

---

## 3. Enrollment Video — Prospect Link (No YouTube Distraction)

- **Admin setting:** `enrollment_video_url` (YouTube URL), `enrollment_video_title`.
- **Public route:** `GET /watch/enrollment` — no login. Reads settings, builds YouTube embed URL (`?rel=0&modestbranding=1`), renders `watch_video.html` with only embed. If no video configured → **404**.
- **Share link:** In app we use **enrollment_watch_url** = `url_for('watch_enrollment', _external=True)` (when `enrollment_video_url` is set). So the link shown/copied for prospects is **our domain** (e.g. `https://your-app.com/watch/enrollment`), not raw YouTube.
- **Templates:** Dashboard, Working (team view), Enroll To use `href="{{ enrollment_watch_url or enrollment_video_url }}"` so we always prefer in-app link when present.
- **before_request:** Skips auth for `request.path.startswith('/watch/')`, so `/watch/enrollment` and `/watch/<token>` are public.

---

## 4. Watch by Token (Enroll To Share Links)

- **Route:** `GET /watch/<token>` — looks up `enroll_share_links` by token. First view increments `view_count`, syncs to lead (Video Watched), updates daily_scores. Renders same minimal embed page.
- **Order:** `/watch/enrollment` is registered **before** `/watch/<token>` so `enrollment` is not treated as a token.

---

## 5. Payment Done → Day 1 (My Work)

- In **update_call_status**: when `call_status == 'Payment Done'` and lead’s `pipeline_stage == 'enrollment'`, we set `payment_done=1` and call **\_transition_stage(..., 'day1', ..., status_override='Day 1')**. Lead moves to Day 1 and appears in My Work → Day 1 after refresh.
- No extra API needed; UI hint in Ready for Day 1: “₹196 pay hua to Call Status → Payment Done karo”.

---

## 6. Batch Videos (Day 1) — Leader

- **batch_toggle:** Day 1 batches (`d1_*`) allowed for **leader** and **admin**. Leader can mark for **own** leads or **downline** leads (`assigned_to in {self} ∪ downline`).
- Leader sees batch buttons (M/A/E) in **My Work** (own Day 1) and **Team Work** (team Day 1). `batch_videos` = `leader_batch_videos` from settings; popup uses `BATCH_VIDEOS` from template.

---

## 7. Template Variables — Quick Check

| Template / Include     | Critical vars (leader/working) |
|------------------------|--------------------------------|
| working.html (leader)  | own_stage1, own_day1, …, team_*, downline_members, has_team, enroll_days, enroll_pdfs, recent_shares, team_leads, **enrollment_video_url**, **enrollment_watch_url**, batch_videos, today_actions, call_status_values |
| enroll_to.html         | enrollment_video_url, **enrollment_watch_url**, enroll_days, enroll_pdfs, recent_shares, team_leads |
| dashboard.html         | enrollment_video_url, **enrollment_watch_url**, enrollment_video_title, … |
| watch_video.html       | embed_url, title, error (optional) |

All views that pass `enrollment_video_url` also pass `enrollment_watch_url` when URL is set.

---

## 8. Verification

Run from project root:

```bash
./venv/bin/python scripts/verify_app_flow.py
```

Checks: `/watch/enrollment` 200 + embed, leader `/working` 200 + “My Work”/“Team Work”, `/watch/enrollment` 404 when video not set.

---

**Summary:** Leader flow (Working, Team Work, Enroll To, batch, today_actions), enrollment watch URL, and payment→Day 1 are wired and guarded; verification script passes.
