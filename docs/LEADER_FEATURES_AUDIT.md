# Leader Features — Deep Audit Report

**Date:** 2025-03-17  
**Scope:** Leader-only and leader+admin features. **Audit only — no code changes.**

---

## 1. Executive Summary

| Area | Status | Notes |
|------|--------|--------|
| **Working Section (/working)** | **BROKEN** | Page crashes on load due to DB schema mismatch |
| **Leader Team Reports (/leader/team-reports)** | OK | Route, template, and variables align |
| **Dashboard (leader view)** | OK | team_snapshot, leader_report_stats passed and used |
| **Stage advance / Call status** | OK* | *Backend allows leader; UI never loads due to Working crash |
| **Sidebar (My Team link)** | OK | Links to leader_team_reports correctly |
| **Enroll To (Working tab)** | BROKEN | Depends on Working page load; also uses broken enroll queries |

---

## 2. Critical: Working Section Crashes for Leader

When a **leader** opens **Working Section** (`/working`), the server hits two SQL issues and the page never renders.

### 2.1 enroll_content — missing columns

- **Location:** `app.py` ~7739–7746 (leader branch of `working()`).
- **Query:**
  ```sql
  SELECT * FROM enroll_content WHERE is_active=1 ORDER BY day_number, sort_order
  ```
- **Schema in `database.py`:** Table `enroll_content` is created with only:
  - `id`, `curiosity_title`, `title`, `created_at`
- **Missing in schema:** `is_active`, `day_number`, `sort_order`
- **Result:** SQLite error (e.g. "no such column: is_active" or "day_number"/"sort_order"). Leader request fails; Working page does not load.
- **No migration:** There is no `ALTER TABLE enroll_content ADD COLUMN ...` in `database.py` for these columns.

### 2.2 enroll_pdfs — table does not exist

- **Location:** `app.py` ~7749–7750 (same leader branch).
- **Query:**
  ```sql
  SELECT * FROM enroll_pdfs WHERE is_active=1 ORDER BY sort_order
  ```
- **Schema in `database.py`:** There is **no** `CREATE TABLE enroll_pdfs` anywhere. Only `enroll_content` and `enroll_share_links` exist for enroll-related tables.
- **Result:** SQLite error "no such table: enroll_pdfs". Even if enroll_content were fixed, the leader branch would crash here.
- **Template:** `templates/enroll_to.html` uses `enroll_pdfs` (lines 48–62); it is included only in leader view of `working.html` (line 602).

**Conclusion:** Leader cannot use Working Section at all until (1) enroll_content has `is_active`, `day_number`, `sort_order` (or the query is changed to match current schema) and (2) enroll_pdfs table is created (or the query removed/guarded).

---

## 3. Leader Team Reports — OK

- **Route:** `app.py` `leader_team_reports()` (~3727–3786).
- **URL:** `/leader/team-reports`.
- **Access:** `role in ('leader', 'admin')`.
- **Template:** `leader_team_reports.html`.
- **Variables passed:** `reports`, `missing`, `members`, `summary`, `date_filter`, `today`.
- **Template usage:** Uses `date_filter`, `reports`, `summary` (total_calling, pdf_covered, calls_picked, enrollments_done, plan_2cc, seat_holdings), `missing`, `today`, and per-report fields including `videos_sent_actual`, `system_verified`. All of these exist on `daily_reports` (base table + ALTER columns in database.py ~679–689).
- **Sidebar:** Base template shows "My Team" → `url_for('leader_team_reports')` for `session.get('role') == 'leader'` (base.html ~169–174).

No issues found; feature should work.

---

## 4. Dashboard (Leader View) — OK

- **Route:** `team_dashboard()` used for both team and leader.
- **Leader-specific:** When `session.get('role') == 'leader'`, the route builds `downline_usernames` via `_get_network_usernames(db, username)`, then `team_snapshot` (per-member stage counts, today_pts, report_done), `leader_report_stats`, `downline_missing_reports`.
- **Passed to template:** `team_snapshot`, `leader_report_stats`, `downline_missing_reports`, `show_day1_batches=True` for leader.
- **Template:** `dashboard.html` uses `team_snapshot`, `downline_missing_reports` in leader-specific blocks; `user_role` is passed so pipeline/UI can branch correctly.

No issues found; leader dashboard should work.

---

## 5. Working Section — JS and Backend (when page loads)

- **Tabs:** `switchLeaderTab('own'|'team'|'enroll')` and `filterTeamLeads(member)` are defined in `working.html` (~1329, ~1367) and used by leader buttons.
- **Stage advance:** `stageAdvance(leadId, action, btnEl)` calls `POST /leads/<id>/stage-advance`. `ACTION_MAP` in app.py allows leader for: `day1_complete`, `interview_done`, `seat_hold_done`, `mark_lost`. So leader can advance stages for allowed actions.
- **Call status:** `updateCallStatus(leadId, newStatus, selectEl)` calls `POST /leads/<id>/call-status`. Backend allows leader to update own or downline leads.

Because the Working page does not load for leader (Section 2), these flows are currently unreachable for leader. Once the crash is fixed, they should work.

---

## 6. Enroll To Block

- **Rendered in:** `working.html` leader view only, `{% include 'enroll_to.html' %}` inside `#leader-section-enroll`.
- **Expects:** `enrollment_video_url`, `enrollment_video_title`, `enroll_days`, `enroll_pdfs`, `recent_shares`, `team_leads`.
- **Backend:** Leader branch of `working()` builds these from:
  - `enroll_content` (broken query → crash),
  - `enroll_pdfs` (missing table → crash),
  - `enroll_share_links` JOIN `enroll_content` (uses `ec.day_number` — again depends on enroll_content having day_number),
  - leads for own + downline for share-link generator.
- **Result:** Enroll To is broken for leader both because the page never loads and because the enroll queries are invalid.

---

## 7. Sidebar and today_actions

- **Leader nav:** "My Team" → `leader_team_reports` — correct.
- **Working Section badge:** Base template shows an alert badge when `today_actions` is defined and has pending_calls/videos_to_send/batches_due/closings_due. `today_actions` is built and passed only in the **team member** branch of `working()` (~7890, 7933), not in the leader branch. So when a leader is on any page (e.g. dashboard), the sidebar context does not include `today_actions` and the Working Section badge will not show. Minor; not a crash.

---

## 8. Summary Table

| Feature | Route/File | Issue |
|--------|------------|--------|
| Working Section (leader) | `app.py` working() leader branch | **CRASH:** enroll_content missing columns `is_active`, `day_number`, `sort_order` |
| Working Section (leader) | `app.py` same block | **CRASH:** table `enroll_pdfs` does not exist |
| Enroll To | Same as above + enroll_to.html | Broken by above; also recent_shares uses enroll_content.day_number |
| Leader Team Reports | /leader/team-reports | None found |
| Leader Dashboard | team_dashboard() | None found |
| Stage advance / Call status | /leads/<id>/stage-advance, call-status | Backend OK; unreachable until Working loads |
| Sidebar “My Team” | base.html | None found |
| today_actions badge (leader) | base.html + working() | today_actions not passed for leader (cosmetic) |

---

## 9. Recommended Fixes (for later implementation)

1. **enroll_content**
   - Either add columns to schema: `is_active` (INTEGER, default 1), `day_number` (INTEGER), `sort_order` (INTEGER, default 0), and backfill/migrate;  
   - Or change the query to use only existing columns (e.g. no filter/order or order by id/created_at) and build `enroll_days` in code if needed.

2. **enroll_pdfs**
   - Either create table `enroll_pdfs` with columns used in app/template (e.g. id, title, url, is_active, sort_order, created_at) and add migration;  
   - Or remove the query and pass an empty list for `enroll_pdfs` so the page loads and the PDF block shows “No PDFs” until the feature is implemented.

3. **recent_shares**  
   - Query uses `ec.day_number`; ensure enroll_content has `day_number` (or remove from SELECT and handle in template).

4. **Optional:** Pass `today_actions` in the leader branch of `working()` (and/or from team_dashboard for leader) if the sidebar badge for Working Section should apply to leaders.

---

## 10. Fixes Applied (post-audit)

**Date:** 2025-03-17

| Issue | Fix |
|-------|-----|
| **enroll_content missing columns** | In `database.py` (init_db): after creating `enroll_content`, added ALTER TABLE for `is_active` (INTEGER DEFAULT 1), `day_number` (INTEGER DEFAULT 1), `sort_order` (INTEGER DEFAULT 0). |
| **enroll_pdfs table missing** | In `database.py` (init_db): added CREATE TABLE `enroll_pdfs` with columns `id`, `title`, `url`, `is_active`, `sort_order`, `created_at`. |
| **Leader sidebar badge (today_actions)** | In `app.py` (working() leader branch): compute `leader_today_actions` (pending_calls, videos_to_send, batches_due, closings_due) for leader’s own leads and pass `today_actions=leader_today_actions` to `render_template('working.html', ...)`. |

After restarting the app, `init_db()` runs and applies the new columns and table. Leader can then open Working Section and use Enroll To; the Working Section badge in the sidebar will show when a leader has pending actions.

---

**End of audit. Fixes in §10 were applied.**
