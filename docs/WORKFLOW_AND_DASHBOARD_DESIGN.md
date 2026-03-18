# App Workflow & Dashboard Design — Team Leaders & Admin

Recommendations for how the app workflow and dashboards should be structured for **team members**, **team leaders**, and **admin**.

---

## 1. Overall App Workflow (Lead Lifecycle)

### 1.1 Pipeline stages (current)

```
Pool → Stage 1 (Enrollment) → Day 1 → Day 2 → Day 3 (Interview/Track) → Seat Hold → Converted
```

- **Pool:** Unassigned leads; team claims from here.
- **Stage 1:** New → Contacted → Video Sent → Video Watched → Paid ₹196 → Day 1.
- **Day 1 / Day 2:** Batch completion (morning/afternoon/evening); leader/admin send batches.
- **Day 3:** Interview / Track Selected; leader/admin mark progress.
- **Seat Hold:** Confirmed, expiry tracked; then Fully Converted / Converted.

### 1.2 Who does what (role-based)

| Action | Team | Leader | Admin |
|--------|------|--------|-------|
| Claim from pool | ✅ | ✅ | ✅ |
| Add new lead | ✅ | ✅ | ✅ |
| Call, WhatsApp, update call_status | ✅ | ✅ | ✅ |
| **Enroll To** — generate share link | ✅ | ✅ | ✅ |
| Mark Day 1 batches (send links) | ❌ | ✅ | ✅ |
| Mark Day 2 batches | ✅ (own) | ✅ | ✅ |
| Mark interview_done / advance to Day 3 | ❌ | ✅ | ✅ |
| Seat hold, track selection | ✅ | ✅ | ✅ |
| View **all** leads / pipeline | ❌ | ❌ | ✅ |
| Approve registrations, reports, wallet | ❌ | ❌ | ✅ |
| Settings, pool management, targets | ❌ | ❌ | ✅ |

### 1.3 Sync-first workflow (recommended)

- **Enroll To** is the main way to “send video” — share link → lead auto “Video Sent” → prospect opens → “Video Watched” + push to team member.
- **Daily report** is pre-filled from system counts (calls, videos, payments); member can only edit down, not inflate.
- **Call status** and **pipeline stage** are updated by real actions (share link, watch, payment, batch, advance) so admin/leader see truthful pipeline.

---

## 2. Team Member Dashboard (My Dashboard)

**Goal:** “Aaj kya karna hai?” — today’s score, follow-ups, pipeline, report.

### 2.1 Layout (top → bottom)

1. **Status bar (compact)**  
   - Today’s points + streak.  
   - Follow-ups due (count + link to Follow-up queue).  
   - Report status: “Report ✓” or “Submit Report” (prominent).

2. **Notice board + Live session**  
   - Pinned announcements (1–2 lines).  
   - Zoom/live link + time if set.

3. **Pipeline summary (my leads only)**  
   - Stage 1 / Day 1 / Day 2 / Day 3 / Pending / Converted counts.  
   - Optional: “Hot” (heat ≥ 75) and “Batches pending” hints.  
   - Single CTA: **Working Section** (full pipeline + actions).

4. **Today’s priorities (optional card)**  
   - “X follow-ups due” → link to follow_up.  
   - “X videos to send” (Stage 1, not yet Video Sent) → Working.  
   - “X batches due” (Day 1/2 incomplete) → Working.  
   - “Report not submitted” → Report form.

5. **Recent leads (5)**  
   - Name, status, last contact; link to My Leads / edit.

6. **Monthly targets (if set)**  
   - Leads, payments, conversions, revenue vs target (e.g. progress bars).

### 2.2 What team member should do in order

1. Open dashboard → see report status and follow-ups.  
2. Submit report if not done (pre-filled from system).  
3. Go to **Working** → Stage 1: call, Enroll To, update call_status.  
4. Day 2: complete batches (mark morning/afternoon/evening).  
5. Follow-up queue: call back and update.  
6. My Leads: deep edit, notes, status.

### 2.3 Nav (team)

- **Dashboard** (home)  
- **My Leads**  
- **Working** (pipeline + actions)  
- **Lead Pool** (claim)  
- **Follow-ups**  
- **Report**  
- **Leaderboard**  
- **Announcements**  
- **Training** (if applicable)  
- **Profile / Logout**

---

## 3. Team Leader Dashboard

**Goal:** “Meri team ka aaj kya hai?” — team pipeline, batch sending, report compliance, light oversight.

### 3.1 How leader is different from team

- **Same as team:** Own leads, Working (own pipeline), report, pool, follow-ups.  
- **Extra:**  
  - Send **Day 1** batch links (team cannot).  
  - Mark **interview_done** and advance to Day 3.  
  - See **team-level** view: downline’s pipeline and today’s activity (read-only or light actions).

### 3.2 Recommended leader dashboard layout

1. **Personal status (same as team)**  
   - Today’s score, streak, report status, follow-ups.

2. **Team snapshot (new block)**  
   - List of downline (from `upline_username` / `upline_name`).  
   - Per member: Stage 1 / Day 1 / Day 2 / Day 3 / Pending / Converted counts, today’s points.  
   - “X reports submitted today” vs “Y missing”.  
   - Link: “View team pipeline” → Working (leader view) or a dedicated **Leader Working** view.

3. **My pipeline (same as team)**  
   - Leader’s own Stage 1 → Converted summary + link to Working.

4. **Today’s team reports**  
   - Who submitted, who missing (if leader has access to this data; else only admin).

5. **Notices + Live session** (same as team).

6. **Recent leads (own)** (same as team).

### 3.3 Leader-specific pages

- **Working:** When role = leader, show **own** pipeline same as team; **plus** ability to send Day 1 batches and mark interview_done. Optionally a “Team” tab with aggregated stage counts per downline (no edit, or only “nudge”/remind).  
- **Reports:** If you grant leaders “reports for my team only”, a **Leader Reports** page: list of downline, who submitted today, and optionally today’s totals (calls, PDF, enrolled) per member — read-only, no edit of reports.

### 3.4 Nav (leader)

Same as team, plus (if implemented):

- **Team** or **My Team** → team snapshot + team pipeline/reports.  
- No access: Admin-only (approvals, all leads, pool config, settings, wallet admin).

---

## 4. Admin Dashboard

**Goal:** “Pure org ka control + visibility” — approvals, reports, pipeline, revenue, settings.

### 4.1 Layout (top → bottom)

1. **Alerts (always on top)**  
   - Pending registrations (count + “Review All”).  
   - Missing daily reports (count + “All Reports”).  
   - Wallet pending (if applicable).  
   - Pool count (and link to pool).

2. **Today’s daily reports**  
   - Table: Member, Calls, PDF, Enrolled, ₹196 Actual (system), 2CC, Educated, Remarks.  
   - Highlight mismatch: reported enrollments > system actual (e.g. yellow row).  
   - “Verified” badge where system_verified = 1.  
   - Link: “All Reports” → reports_admin (filter by date/user).

3. **KPI row**  
   - Total leads, Converted, Revenue, Payments (₹196).  
   - Each card links to drilldown by metric.

4. **Funnel / pipeline KPIs**  
   - Seat hold count + value, Day 1/2/Interview/Converted counts.  
   - Optional: trend (last 7/30 days).

5. **Team stats table**  
   - Per member (team + leaders): total leads, converted, paid, revenue.  
   - Link to member detail or drilldown.

6. **Recent leads (all)**  
   - Last 5–10 created; name, assigned_to, status.

7. **Charts (optional)**  
   - Monthly revenue; status distribution; reports trend.

### 4.2 Admin-specific pages (current + suggested)

- **Admin Dashboard** — above.  
- **All Leads** — filter by assigned_to, status, date.  
- **Lead Pool** — add/edit pool, pricing, assign/claim.  
- **Working** — full pipeline (all members), batch completion, stale leads.  
- **Reports** — daily/monthly, filters, Verified column, discrepancy note.  
- **Approvals** — pending users, approve/reject.  
- **Team / Members** — list team + leaders, roles, status.  
- **Training** — who completed, certificates.  
- **Targets** — set monthly targets per member.  
- **Budget / Export** — if applicable.  
- **Activity log** — audit trail.  
- **Wallet requests** — approve/reject recharges.  
- **Settings** — Zoom, batch links, announcements, app settings.

### 4.3 Nav (admin)

- **Admin Dashboard**  
- **All Leads**  
- **Lead Pool**  
- **Recycle Bin**  
- **Intelligence**  
- **Working** (full pipeline)  
- **Team**  
- **Reports**  
- **Approvals**  
- **Members**  
- **Training**  
- **Targets**  
- **Budget Export**  
- **Activity**  
- **Wallet Requests** (admin)  
- **Settings**  
- **Help**  
- Profile / Logout  

---

## 5. Summary: Workflow by role

| Area | Team | Leader | Admin |
|------|------|--------|-------|
| **Home** | My Dashboard (score, report, pipeline, follow-ups) | Leader dashboard (personal + team snapshot) | Admin dashboard (alerts, reports, KPIs, team) |
| **Leads** | My Leads only | My Leads only | All Leads |
| **Pipeline** | Working (own) | Working (own + Day 1 send + interview_done) | Working (all + batch/stale view) |
| **Report** | Submit (pre-filled, no inflate) | Submit | View all, verify, drilldown |
| **Pool** | Claim | Claim | Manage pool |
| **Team** | — | Team snapshot / team reports (optional) | Team list, members, roles |
| **Approvals / Settings** | — | — | Full access |

---

## 6. Implementation notes

- **Leader dashboard:** Add a “team snapshot” block that queries downline by `upline_username` (or upline_name mapped to username), then for each member loads stage counts + today’s score from `daily_scores` and lead counts. Reuse existing `_get_leader_for_user` in reverse: “users where upline_username = current user”.  
- **Leader reports:** Optional route “reports for my team” that filters `daily_reports` by `username IN (my downline)`.  
- **Working (leader):** Reuse same template; backend already allows leader to send Day 1 batches and mark interview_done. Optional: “Team” tab with read-only pipeline per downline.  
- **Admin:** Already has reports with Verified and mismatch highlight; ensure “System Total vs Reported” summary is visible where useful (e.g. reports_admin page).  
- **Sync:** Keep Enroll To → lead status + daily_scores + report pre-fill as the single source of truth; all dashboards should read from the same pipeline and report tables.

This gives a clear workflow and dashboard design for team, leaders, and admin that matches the current pipeline and sync behaviour.
