# Pipeline & Sync — Debug / Flow Reference

## 1. Enroll “Generate Share Link” flow

1. **POST /enroll/generate-link** (logged-in)
   - Reads `lead_id`, `content_id` from JSON/form.
   - Inserts row into `enroll_share_links` (token, lead_id, content_id, shared_by, view_count=0).
   - Commits.
   - Calls **`_sync_enroll_share_to_lead(db, token, username)`** (no commit inside).
   - Updates `daily_scores.enroll_links_sent += 1` for today (same connection).
   - **Commits** (so sync + enroll_links_sent are persisted).
   - Returns `{ ok, token, watch_url }`.

2. **`_sync_enroll_share_to_lead`**
   - Loads link by token; if `synced_to_lead` → return.
   - If no `lead_id`: `_upsert_daily_score(10, delta_videos=1)`, set `synced_to_lead=1`, return.
   - Else: load lead (in_pool=0, not deleted).
   - Move status/call_status forward to “Video Sent” only if below that in `FORWARD_ORDER`.
   - Update lead: `last_contacted`, `contact_count`, `updated_at`.
   - `_log_lead_event` → “Video shared via Enroll To: …”
   - `_upsert_daily_score(username, 10, delta_videos=1)`.
   - Set `enroll_share_links.synced_to_lead=1`, `lead_status_before=<previous status>`.

## 2. Watch page (first view) flow

1. **GET /watch/<token>** (public)
   - Load `enroll_share_links` by token; 404 if missing.
   - `is_first_view = (view_count == 0)`.
   - `UPDATE enroll_share_links SET view_count = view_count + 1`; **commit**.
   - If **is_first_view**:
     - **`_sync_watch_event_to_lead(db, token)`** (no commit inside).
     - `UPDATE daily_scores SET prospect_views += 1` for `shared_by` and today.
     - **Commit** (so watch sync + prospect_views are persisted).
   - Fetch content title; render `watch_video.html`.

2. **`_sync_watch_event_to_lead`**
   - Load link; if `watch_synced` or no `lead_id` → return.
   - Load lead; move status/call_status to “Video Watched” if not already past.
   - `_log_lead_event` → “Prospect watched video …”
   - `_upsert_daily_score(shared_by, 5)`.
   - Push notification to `shared_by`.
   - Set `enroll_share_links.watch_synced=1`.

## 3. Daily report flow

1. **GET /reports/submit**
   - `actual_counts = _get_actual_daily_counts(db, username)` (today’s `daily_scores`).
   - Render `report_form.html` with `actual_counts`, `existing`, `today`, `username`.

2. **POST /reports/submit**
   - Parse form (total_calling, pdf_covered, …).
   - `actual_counts = _get_actual_daily_counts(db, username)`.
   - **Validation:**  
     - `pdf_covered <= videos_sent + enroll_links_sent + 5`  
     - `total_calling <= calls_made + 10`  
     On failure: flash errors, re-render form with `actual_counts`.
   - INSERT/UPDATE `daily_reports` including  
     `videos_sent_actual`, `calls_made_actual`, `payments_actual`, `system_verified=1`  
     (18 bindings; `system_verified` is literal `1` in SQL).
   - `_upsert_daily_score(username, 20)`, badges, **commit**, redirect.

## 4. DB schema (sync-related)

- **daily_scores:** `enroll_links_sent`, `prospect_views` (ALTER in `migrate_db`).
- **daily_reports:** `videos_sent_actual`, `calls_made_actual`, `payments_actual`, `system_verified` (ALTER).
- **enroll_share_links:** `lead_status_before`, `synced_to_lead`, `watch_synced` (table + ALTERs in `migrate_db`).

## 5. Bugs fixed in this pass

- **Report submit:** Removed extra `now_ts` in INSERT bindings (was 19 values for 18 placeholders).
- **enroll_generate_link:** Commit moved after the try/except so sync is always committed even if `enroll_links_sent` update fails.
- **watch_video:** Commit after first-view block so watch sync is committed even if `prospect_views` update fails.
- **report_form.html:** `max` for PDF uses `|default(0)` for `enroll_links_sent` to avoid missing key.
- **reports_admin.html:** Verified badge uses `r.system_verified is defined and r.system_verified` for old rows.

## 6. Quick checks

- Run `migrate_db()` and confirm `daily_scores`, `daily_reports`, `enroll_share_links` have the new columns.
- Generate link with `lead_id` → lead status/call_status “Video Sent”, `daily_scores.videos_sent` and `enroll_links_sent` +1.
- Open watch URL first time → lead “Video Watched”, `prospect_views` +1, push to sharer.
- Submit report → form pre-filled from `actual_counts`; submit with values above system count → validation error; successful submit stores `system_verified=1` and actuals.
