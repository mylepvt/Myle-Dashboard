#!/usr/bin/env python3
"""
Myle Community Dashboard — Hindi Tutorial Video Generator v3

REAL screen recording using Playwright's built-in video capture:
  - Browser is actually recorded (not screenshots)
  - Orange glowing highlights on relevant UI elements
  - Mouse cursor visible during clicks and navigation
  - Each step is padded to match TTS audio duration
  - Hindi TTS narration via edge-tts

Usage:
  python generate.py \\
    --url http://localhost:5002 \\
    --user testmember \\
    --password tutorial123 \\
    --db /path/to/leads.db \\
    [--output team_tutorial.mp4]
"""

import asyncio
import os
import sys
import argparse
import subprocess
import tempfile
import shutil
import sqlite3
import time
from pathlib import Path
from typing import List, Optional

try:
    import edge_tts
    from playwright.async_api import async_playwright, Page
except ImportError:
    print("❌ Missing deps: pip install -r requirements.txt && playwright install chromium")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════

VOICE    = "hi-IN-SwaraNeural"
VIEWPORT = {"width": 390, "height": 844}

# Orange glow highlight injected via JS on each page
HIGHLIGHT_CSS = """
.myle-hl {
    outline: 3px solid #ff6b35 !important;
    outline-offset: 4px !important;
    box-shadow: 0 0 0 6px rgba(255,107,53,0.35), 0 0 20px rgba(255,107,53,0.5) !important;
    border-radius: 6px !important;
    position: relative !important;
    z-index: 9000 !important;
}
"""

# ═══════════════════════════════════════════════════════════════════════════
# STEPS — each step: id, hindi narration, list of actions
#
# Action types:
#   navigate        : go to URL
#   click           : click a CSS selector
#   click_tab       : click a tab by text
#   type_into       : click input then type
#   fill_safe       : fill input (silent fail)
#   scroll_down     : scroll by pixels
#   wait            : pause N seconds
#   js              : evaluate JS
#   highlight       : add orange glow to selector
#   highlight_lead  : highlight lead card containing text
#   highlight_text  : highlight element containing text
#   unhighlight     : remove all highlights
# ═══════════════════════════════════════════════════════════════════════════

STEPS = [
    # ── 1. INTRO ─────────────────────────────────────────────────────────
    {
        "id": "01_intro",
        "hindi": (
            "नमस्ते! यह है Myle Community का Team Dashboard। "
            "इस video में हम live demo देखेंगे — "
            "lead add करना, Stage 1 से Day 1 में move करना, "
            "batches mark करना, और Push to Day 2 करना। "
            "चलो शुरू करते हैं।"
        ),
        "actions": [
            {"navigate": "/dashboard"},
            {"wait": 3},
            {"scroll_down": 250},
            {"wait": 3},
            {"scroll_down": -250},
            {"wait": 3},
        ],
    },

    # ── 2. DASHBOARD ─────────────────────────────────────────────────────
    {
        "id": "02_dashboard",
        "hindi": (
            "यह है team member का Dashboard। "
            "ऊपर score, streak, और बचा हुआ काम दिख रहा है। "
            "नीचे Working Section, My Leads, Wallet, Lead Pool के quick buttons हैं।"
        ),
        "actions": [
            {"navigate": "/dashboard"},
            {"wait": 2},
            {"scroll_down": 300},
            {"wait": 3},
            {"scroll_down": -300},
            {"wait": 2},
        ],
    },

    # ── 3. MY LEADS PAGE ─────────────────────────────────────────────────
    {
        "id": "03_my_leads",
        "hindi": (
            "My Leads page पर सभी leads दिखती हैं। "
            "ऊपर Quick Add button है — "
            "जिससे नई lead add होती है। "
            "अभी हम एक नई lead add करते हैं।"
        ),
        "actions": [
            {"navigate": "/leads"},
            {"wait": 2},
            {"highlight": "[data-bs-target='#quickAddModal']"},
            {"wait": 4},
            {"unhighlight": True},
        ],
    },

    # ── 4. OPEN ADD LEAD MODAL ────────────────────────────────────────────
    {
        "id": "04_add_lead_modal",
        "hindi": (
            "Quick Add button press करते हैं। "
            "एक form खुलता है — Quick Add Lead। "
            "इसमें lead का नाम, phone number, और city भरनी होती है।"
        ),
        "actions": [
            {"navigate": "/leads"},
            {"wait": 1},
            {"highlight": "[data-bs-target='#quickAddModal']"},
            {"wait": 1},
            {"click": "[data-bs-target='#quickAddModal']", "wait": 1.5},
            {"unhighlight": True},
            {"highlight": "#quickAddModal .modal-body"},
            {"wait": 4},
        ],
    },

    # ── 5. FILL THE FORM ─────────────────────────────────────────────────
    {
        "id": "05_fill_form",
        "hindi": (
            "Form में नाम भरते हैं — Neha Gupta। "
            "Phone number — 9876543212। "
            "City — Jaipur। "
            "सब भर दिया।"
        ),
        "actions": [
            {"navigate": "/leads"},
            {"click": "[data-bs-target='#quickAddModal']", "wait": 1.5},
            {"unhighlight": True},
            {"highlight": "#quickAddModal input[name='name']"},
            {"type_into": "#quickAddModal input[name='name']", "value": "Neha Gupta"},
            {"wait": 0.6},
            {"highlight": "#quickAddModal input[name='phone']"},
            {"type_into": "#quickAddModal input[name='phone']", "value": "9876543212"},
            {"wait": 0.6},
            {"highlight": "#quickAddModal input[name='city']"},
            {"type_into": "#quickAddModal input[name='city']", "value": "Jaipur"},
            {"wait": 0.5},
            {"unhighlight": True},
            {"wait": 1},
        ],
    },

    # ── 6. SUBMIT — LEAD ADDED ────────────────────────────────────────────
    {
        "id": "06_lead_added",
        "hindi": (
            "Add Lead button press करते हैं। "
            "Lead add हो गई! "
            "My Leads में Neha Gupta दिख रही है। "
            "यह lead अभी Stage 1 — Enrollment में है।"
        ),
        "actions": [
            {"navigate": "/leads"},
            {"click": "[data-bs-target='#quickAddModal']", "wait": 1.5},
            {"type_into": "#quickAddModal input[name='name']", "value": "Neha Gupta"},
            {"type_into": "#quickAddModal input[name='phone']", "value": "9876543212"},
            {"type_into": "#quickAddModal input[name='city']", "value": "Jaipur"},
            {"wait": 0.3},
            {"highlight": "#quickAddModal button[type='submit']"},
            {"wait": 0.8},
            {"click": "#quickAddModal button[type='submit']", "wait": 2.5},
            {"unhighlight": True},
            {"wait": 2},
        ],
    },

    # ── 7. WORKING SECTION — STAGE 1 ─────────────────────────────────────
    {
        "id": "07_stage1",
        "hindi": (
            "Working Section में जाते हैं। "
            "Stage 1 tab में Rahul Sharma की lead दिख रही है। "
            "यह enrollment stage है — Payment अभी नहीं हुई।"
        ),
        "actions": [
            {"navigate": "/working"},
            {"wait": 1},
            {"highlight": "button:has-text('Stage 1'), [data-tab='stage1']"},
            {"click_tab": "Stage 1"},
            {"wait": 1},
            {"unhighlight": True},
            {"scroll_down": 250},
            {"wait": 1},
            {"highlight_lead": "Rahul Sharma"},
            {"wait": 4},
            {"unhighlight": True},
        ],
    },

    # ── 8. LEAD CARD DETAILS ──────────────────────────────────────────────
    {
        "id": "08_lead_card",
        "hindi": (
            "Lead card में — नाम, phone, pipeline indicator E-1-2-3। "
            "Call और WhatsApp के direct buttons हैं। "
            "नीचे Call Status badge है। "
            "My Leads में जाकर Call Status update करते हैं।"
        ),
        "actions": [
            {"navigate": "/working"},
            {"click_tab": "Stage 1"},
            {"scroll_down": 200},
            {"wait": 1},
            {"highlight": ".pipeline-step, .pipeline-indicator, .pipeline-connector"},
            {"wait": 3},
            {"unhighlight": True},
            {"wait": 1},
        ],
    },

    # ── 9. CALL STATUS — PAYMENT DONE ────────────────────────────────────
    {
        "id": "09_call_status",
        "hindi": (
            "My Leads page पर जाते हैं। "
            "Rahul Sharma की lead में Call Status dropdown है। "
            "Lead ने payment कर दी — Payment Done select करते हैं। "
            "देखो — lead automatically Day 1 में move हो जाती है!"
        ),
        "actions": [
            {"navigate": "/leads"},
            {"wait": 1},
            {"highlight": "select[onchange*='updateCallStatus']"},
            {"wait": 2},
            {"unhighlight": True},
            {"js": """
                const cards = document.querySelectorAll('.wl-card');
                for (const card of cards) {
                    if (card.textContent.includes('Rahul Sharma')) {
                        const sel = card.querySelector('select[onchange*="updateCallStatus"]');
                        if (sel) {
                            sel.value = 'Payment Done';
                            sel.dispatchEvent(new Event('change'));
                            break;
                        }
                    }
                }
            """, "wait": 3},
            {"wait": 2},
        ],
    },

    # ── 10. DAY 1 TAB ─────────────────────────────────────────────────────
    {
        "id": "10_day1_lead",
        "hindi": (
            "Working Section में Day 1 tab खोलते हैं। "
            "Rahul Sharma अब यहाँ है! "
            "Payment Done करते ही lead automatically Stage 1 से Day 1 में आ गई।"
        ),
        "actions": [
            {"navigate": "/working"},
            {"wait": 1},
            {"click_tab": "Day 1"},
            {"wait": 1},
            {"scroll_down": 250},
            {"wait": 1},
            {"highlight_lead": "Rahul Sharma"},
            {"wait": 4},
            {"unhighlight": True},
        ],
    },

    # ── 11. MORNING BATCH POPUP ───────────────────────────────────────────
    {
        "id": "11_morning_batch",
        "hindi": (
            "Day 1 में तीन batches होती हैं — Morning, Afternoon, Evening। "
            "Morning batch button दबाते हैं। "
            "एक popup खुलता है — दो options हैं।"
        ),
        "actions": [
            {"navigate": "/working"},
            {"click_tab": "Day 1"},
            {"scroll_down": 180},
            {"wait": 1},
            {"highlight": ".wk-batch-btn[data-batch='d1_morning']"},
            {"wait": 2},
            {"click": ".wk-batch-btn[data-batch='d1_morning']", "wait": 1.2},
            {"wait": 3},
        ],
    },

    # ── 12. MARK BATCH DONE ───────────────────────────────────────────────
    {
        "id": "12_batch_marked",
        "hindi": (
            "Pehle Se Bhej Diya — Sirf Mark Karo दबाते हैं। "
            "Batch mark हो गई — button green हो गया। "
            "Plus 15 points भी मिले।"
        ),
        "actions": [
            {"navigate": "/working"},
            {"click_tab": "Day 1"},
            {"scroll_down": 180},
            {"click": ".wk-batch-btn[data-batch='d1_morning']", "wait": 0.8},
            {"highlight_text": "Sirf Mark"},
            {"wait": 1.5},
            {"js": """
                const btns = [...document.querySelectorAll('button')];
                const btn = btns.find(b =>
                    b.textContent.includes('Sirf Mark') ||
                    b.textContent.includes('Pehle Se'));
                if (btn) btn.click();
            """, "wait": 2},
            {"unhighlight": True},
            {"wait": 2},
        ],
    },

    # ── 13. ALL BATCHES DONE — PUSH TO DAY 2 ─────────────────────────────
    {
        "id": "13_push_day2",
        "hindi": (
            "यह है Priya Singh — तीनों batches complete। "
            "Card green हो गया है। "
            "नीचे Push to Day 2 button है — इसे press करते हैं। "
            "Lead Day 2 में move हो गई!"
        ),
        "actions": [
            {"navigate": "/working"},
            {"click_tab": "Day 1"},
            {"wait": 1},
            {"js": """
                const advBtn = document.querySelector('.wk-advance-btn');
                if (advBtn) advBtn.scrollIntoView({behavior:'smooth', block:'center'});
            """, "wait": 1.5},
            {"highlight": ".wk-advance-btn"},
            {"wait": 2},
            {"click": ".wk-advance-btn", "wait": 2.5},
            {"unhighlight": True},
            {"wait": 2},
        ],
    },

    # ── 14. DAY 2 VIEW ────────────────────────────────────────────────────
    {
        "id": "14_day2_view",
        "hindi": (
            "Day 2 tab में जाते हैं। "
            "Lead दिख रही है लेकिन batch buttons lock हैं — "
            "Day 2 की batches admin mark करता है। "
            "Team member यहाँ call और WhatsApp कर सकता है।"
        ),
        "actions": [
            {"navigate": "/working"},
            {"click_tab": "Day 2"},
            {"wait": 2},
            {"scroll_down": 250},
            {"wait": 3},
            {"scroll_down": -100},
            {"wait": 2},
        ],
    },

    # ── 15. PIPELINE + SCORE ──────────────────────────────────────────────
    {
        "id": "15_pipeline_score",
        "hindi": (
            "हर batch mark करने पर 15 points मिलते हैं। "
            "Working Section के ऊपर real-time score दिखती है। "
            "हर card पर pipeline indicator है — "
            "E, 1, 2, 3, S, C, T — जो current stage बताता है।"
        ),
        "actions": [
            {"navigate": "/working"},
            {"wait": 1},
            {"highlight": ".score-strip, #scoreStrip, .today-score-card"},
            {"wait": 3},
            {"unhighlight": True},
            {"click_tab": "Day 1"},
            {"wait": 1},
            {"highlight": ".pipeline-step, .pipeline-connector"},
            {"wait": 3},
            {"unhighlight": True},
        ],
    },

    # ── 16. DAILY REPORT ─────────────────────────────────────────────────
    {
        "id": "16_daily_report",
        "hindi": (
            "Daily Report हर दिन submit करनी होती है। "
            "Calls, WhatsApp, follow-ups fill करो। "
            "Report miss करने पर streak टूट जाती है।"
        ),
        "actions": [
            {"navigate": "/reports/submit"},
            {"wait": 2},
            {"fill_safe": "input[name='calls_made']", "value": "12"},
            {"wait": 0.4},
            {"fill_safe": "input[name='whatsapp_sent']", "value": "8"},
            {"wait": 0.4},
            {"fill_safe": "input[name='follow_ups']", "value": "5"},
            {"wait": 0.5},
            {"highlight": "button[type='submit']"},
            {"wait": 3},
            {"unhighlight": True},
        ],
    },

    # ── 17. WALLET ────────────────────────────────────────────────────────
    {
        "id": "17_wallet",
        "hindi": (
            "Wallet page पर balance दिखता है। "
            "Recharge के लिए UPI QR code scan करो, "
            "UTR number डालो, request submit करो। "
            "Admin approve करेगा तो balance add होगा।"
        ),
        "actions": [
            {"navigate": "/wallet"},
            {"wait": 2},
            {"scroll_down": 350},
            {"wait": 3},
            {"scroll_down": -200},
            {"wait": 2},
        ],
    },

    # ── 18. LEAD POOL ─────────────────────────────────────────────────────
    {
        "id": "18_lead_pool",
        "hindi": (
            "Lead Pool में available leads होती हैं। "
            "Number भरो, Claim करो — "
            "wallet से amount deduct होगा "
            "और leads My Leads में आ जाएंगी।"
        ),
        "actions": [
            {"navigate": "/lead-pool"},
            {"wait": 2},
            {"scroll_down": 250},
            {"wait": 3},
        ],
    },

    # ── 19. OUTRO ─────────────────────────────────────────────────────────
    {
        "id": "19_outro",
        "hindi": (
            "तो यह था Myle Community का complete team tutorial। "
            "याद रखो — नई lead add करो, "
            "Stage 1 में Payment Done करो जब payment आए, "
            "Day 1 में तीनों batches mark करो, "
            "Push to Day 2 करो, "
            "और हर दिन Daily Report submit करो। "
            "Score बढ़ाते रहो। All the best!"
        ),
        "actions": [
            {"navigate": "/dashboard"},
            {"wait": 4},
            {"scroll_down": 250},
            {"wait": 3},
            {"scroll_down": -250},
            {"wait": 3},
        ],
    },
]


# ═══════════════════════════════════════════════════════════════════════════
# DB SEEDING
# ═══════════════════════════════════════════════════════════════════════════

def seed_demo_data(db_path: str, username: str):
    if not os.path.exists(db_path):
        print(f"  ⚠️  DB not found: {db_path}")
        return
    conn = sqlite3.connect(db_path)
    conn.execute(
        "DELETE FROM leads WHERE assigned_to=? AND name IN ('Rahul Sharma','Priya Singh')",
        (username,))
    conn.execute("""
        INSERT INTO leads
          (name,phone,city,assigned_to,in_pool,pipeline_stage,status,
           current_owner,created_at,updated_at,deleted_at)
        VALUES ('Rahul Sharma','9876543210','Delhi',?,0,'enrollment','New Lead',
                ?,datetime('now','-1 hour'),datetime('now','-1 hour'),'')
    """, (username, username))
    conn.execute("""
        INSERT INTO leads
          (name,phone,city,assigned_to,in_pool,pipeline_stage,status,
           current_owner,d1_morning,d1_afternoon,d1_evening,
           created_at,updated_at,deleted_at)
        VALUES ('Priya Singh','9876543211','Mumbai',?,0,'day1','Day 1',
                ?,1,1,1,
                datetime('now','-2 hours'),datetime('now','-2 hours'),'')
    """, (username, username))
    conn.commit()
    conn.close()
    print(f"  ✓ Demo leads seeded for @{username}")


# ═══════════════════════════════════════════════════════════════════════════
# TTS
# ═══════════════════════════════════════════════════════════════════════════

async def generate_tts(text: str, path: str) -> float:
    communicate = edge_tts.Communicate(text, VOICE)
    await communicate.save(path)
    return max(4.0, len(text.split()) * 0.44)


# ═══════════════════════════════════════════════════════════════════════════
# BROWSER ACTIONS
# ═══════════════════════════════════════════════════════════════════════════

async def _hl(page: Page, selector: str):
    """Inject highlight CSS and add .myle-hl to matching elements."""
    try:
        await page.evaluate(f"""
            (() => {{
                if (!document.getElementById('myle-hl-style')) {{
                    const s = document.createElement('style');
                    s.id = 'myle-hl-style';
                    s.textContent = `{HIGHLIGHT_CSS}`;
                    document.head.appendChild(s);
                }}
                document.querySelectorAll('.myle-hl').forEach(el => el.classList.remove('myle-hl'));
                const targets = document.querySelectorAll('{selector}');
                targets.forEach(el => {{
                    el.classList.add('myle-hl');
                    if (targets.length === 1) el.scrollIntoView({{behavior:'smooth',block:'center'}});
                }});
            }})();
        """)
        await asyncio.sleep(0.6)
    except Exception:
        pass


async def _unhl(page: Page):
    try:
        await page.evaluate("document.querySelectorAll('.myle-hl').forEach(el => el.classList.remove('myle-hl'));")
    except Exception:
        pass


async def run_step_actions(page: Page, base_url: str, actions: list):
    for action in actions:
        try:
            if "navigate" in action:
                await page.goto(base_url + action["navigate"],
                                wait_until="networkidle", timeout=15000)
                await asyncio.sleep(1.5)

            elif "click_tab" in action:
                try:
                    await page.click(f"text={action['click_tab']}", timeout=4000)
                    await asyncio.sleep(1.0)
                except Exception:
                    pass

            elif "click" in action:
                try:
                    await page.click(action["click"], timeout=5000)
                    await asyncio.sleep(action.get("wait", 1.0))
                except Exception:
                    pass

            elif "type_into" in action:
                try:
                    await page.click(action["type_into"], timeout=3000)
                    await asyncio.sleep(0.2)
                    await page.keyboard.type(action.get("value", ""))
                except Exception:
                    pass

            elif "fill_safe" in action:
                try:
                    await page.fill(action["fill_safe"], action.get("value", ""), timeout=2000)
                except Exception:
                    pass

            elif "scroll_down" in action:
                await page.evaluate(f"window.scrollBy(0, {action['scroll_down']})")
                await asyncio.sleep(0.5)

            elif "wait" in action:
                await asyncio.sleep(action["wait"])

            elif "js" in action:
                try:
                    await page.evaluate(action["js"])
                    await asyncio.sleep(action.get("wait", 0.8))
                except Exception:
                    pass

            elif "highlight" in action:
                await _hl(page, action["highlight"])

            elif "unhighlight" in action:
                await _unhl(page)

            elif "highlight_lead" in action:
                name = action["highlight_lead"].replace("'", "\\'")
                try:
                    await page.evaluate(f"""
                        (() => {{
                            if (!document.getElementById('myle-hl-style')) {{
                                const s = document.createElement('style');
                                s.id = 'myle-hl-style';
                                s.textContent = `{HIGHLIGHT_CSS}`;
                                document.head.appendChild(s);
                            }}
                            document.querySelectorAll('.myle-hl').forEach(el => el.classList.remove('myle-hl'));
                            const cards = document.querySelectorAll('[id^=wcard], .wk-lead-card, .lead-card, .wl-card');
                            for (const c of cards) {{
                                if (c.textContent.includes('{name}')) {{
                                    c.classList.add('myle-hl');
                                    c.scrollIntoView({{behavior:'smooth', block:'center'}});
                                    break;
                                }}
                            }}
                        }})();
                    """)
                    await asyncio.sleep(0.8)
                except Exception:
                    pass

            elif "highlight_text" in action:
                txt = action["highlight_text"].replace("'", "\\'")
                try:
                    await page.evaluate(f"""
                        (() => {{
                            if (!document.getElementById('myle-hl-style')) {{
                                const s = document.createElement('style');
                                s.id = 'myle-hl-style';
                                s.textContent = `{HIGHLIGHT_CSS}`;
                                document.head.appendChild(s);
                            }}
                            document.querySelectorAll('.myle-hl').forEach(el => el.classList.remove('myle-hl'));
                            const all = [...document.querySelectorAll('button, a, label, span')];
                            const el = all.find(e => e.textContent.includes('{txt}'));
                            if (el) {{
                                el.classList.add('myle-hl');
                                el.scrollIntoView({{behavior:'smooth', block:'center'}});
                            }}
                        }})();
                    """)
                    await asyncio.sleep(0.6)
                except Exception:
                    pass

        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
# VIDEO HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def run_ffmpeg(cmd: list, timeout: int = 120) -> bool:
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=timeout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n  ✗ ffmpeg: {e.stderr.decode()[:300]}")
        return False
    except FileNotFoundError:
        print("\n  ✗ ffmpeg not found — brew install ffmpeg")
        return False


def webm_to_mp4(src: str, dst: str) -> bool:
    return run_ffmpeg([
        "ffmpeg", "-i", src,
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-vf", "scale=390:844:force_original_aspect_ratio=decrease,"
               "pad=390:844:(ow-iw)/2:(oh-ih)/2",
        "-pix_fmt", "yuv420p", "-r", "25",
        "-y", dst,
    ], timeout=300)


def cut_segment(src: str, start: float, duration: float, dst: str) -> bool:
    return run_ffmpeg([
        "ffmpeg", "-ss", f"{start:.3f}", "-i", src,
        "-t", f"{duration:.3f}",
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p",
        "-y", dst,
    ], timeout=120)


def merge_with_audio(video: str, audio: str, dst: str) -> bool:
    """Merge video with audio. Clone last frame if video is shorter than audio."""
    return run_ffmpeg([
        "ffmpeg",
        "-i", video,
        "-i", audio,
        "-filter_complex", "[0:v]tpad=stop_mode=clone:stop_duration=30[v]",
        "-map", "[v]", "-map", "1:a",
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-c:a", "aac", "-b:a", "192k",
        "-pix_fmt", "yuv420p",
        "-shortest",
        "-y", dst,
    ], timeout=120)


def concat_clips(clips: List[str], dst: str) -> bool:
    concat = "/tmp/myle_final_concat.txt"
    with open(concat, "w") as f:
        for c in clips:
            f.write(f"file '{c}'\n")
    ok = run_ffmpeg([
        "ffmpeg", "-f", "concat", "-safe", "0", "-i", concat,
        "-c", "copy", "-y", dst,
    ], timeout=600)
    if os.path.exists(concat):
        os.remove(concat)
    return ok


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

async def main(base_url: str, username: str, password: str,
               output_path: str, db_path: Optional[str]):

    temp_dir = tempfile.mkdtemp(prefix="myle_tut_v3_")
    print(f"📁 Temp: {temp_dir}\n")

    # Seed demo data
    if db_path and os.path.exists(db_path):
        print("🌱 Seeding demo data...")
        seed_demo_data(db_path, username)
    else:
        print(f"⚠️  DB not found: {db_path}")
    print()

    # TTS
    print("🎤 Generating Hindi TTS audio...")
    audio_files: dict = {}
    durations:   dict = {}
    for i, step in enumerate(STEPS):
        print(f"  [{i+1:02d}/{len(STEPS)}] {step['id']}", end="", flush=True)
        apath = os.path.join(temp_dir, f"audio_{i:02d}.mp3")
        dur   = await generate_tts(step["hindi"], apath)
        audio_files[step["id"]] = apath
        durations[step["id"]]   = dur
        print(f" → {dur:.1f}s ✓")
    print()

    # Browser recording
    print("🎬 Recording browser session...")
    video_dir = os.path.join(temp_dir, "video")
    os.makedirs(video_dir)

    step_start_times: List[float] = []
    login_duration   = 0.0

    async with async_playwright() as p:
        browser = await p.chromium.launch(args=["--no-sandbox"])
        context = await browser.new_context(
            viewport=VIEWPORT,
            record_video_dir=video_dir,
            record_video_size={"width": 390, "height": 844},
        )
        page = await context.new_page()

        # Inject highlight CSS on every page load
        await page.add_init_script(f"""
            (() => {{
                const inject = () => {{
                    if (document.getElementById('myle-hl-style')) return;
                    const s = document.createElement('style');
                    s.id = 'myle-hl-style';
                    s.textContent = `{HIGHLIGHT_CSS}`;
                    document.head.appendChild(s);
                }};
                if (document.readyState === 'loading')
                    document.addEventListener('DOMContentLoaded', inject);
                else inject();
            }})();
        """)

        # Login
        t0 = time.time()
        print("  → Logging in...", end="", flush=True)
        try:
            await page.goto(f"{base_url}/login", wait_until="networkidle", timeout=12000)
            await page.fill('input[name="username"]', username)
            await page.fill('input[name="password"]', password)
            await page.click('button[type="submit"]')
            await asyncio.sleep(2.5)
            if "/login" in page.url:
                print(" ✗ Login failed")
                await browser.close()
                return False
            login_duration = time.time() - t0
            print(f" ✓  (login took {login_duration:.1f}s)")
        except Exception as e:
            print(f" ✗ {e}")
            await browser.close()
            return False

        # Run steps
        recording_start = time.time()
        for i, step in enumerate(STEPS):
            sid      = step["id"]
            tts_dur  = durations[sid]
            rel_time = time.time() - recording_start + login_duration
            step_start_times.append(rel_time)

            print(f"  [{i+1:02d}/{len(STEPS)}] {sid}  ({tts_dur:.1f}s target)...",
                  end="", flush=True)

            action_t0 = time.time()
            await run_step_actions(page, base_url, step["actions"])
            elapsed = time.time() - action_t0

            # Pad remaining time so step fills its TTS slot
            pad = max(0.0, tts_dur - elapsed - 0.2)
            if pad > 0:
                await asyncio.sleep(pad)

            print(f" ✓  (actions {elapsed:.1f}s, pad {pad:.1f}s)")

        await context.close()  # Finalises the WebM

    print()

    # Find recorded WebM
    webm_files = list(Path(video_dir).glob("*.webm"))
    if not webm_files:
        print("❌ No WebM recorded")
        return False
    raw_webm = str(webm_files[0])
    print(f"  Raw WebM: {raw_webm}")

    # Convert WebM → MP4
    print("\n🔄 Converting WebM to MP4...")
    full_mp4 = os.path.join(temp_dir, "full_session.mp4")
    if not webm_to_mp4(raw_webm, full_mp4):
        return False
    print("  ✓")

    # Split + merge with audio
    print("\n✂️  Splitting into per-step clips and merging audio...")
    clips = []
    for i, step in enumerate(STEPS):
        sid   = step["id"]
        start = step_start_times[i]
        dur   = durations[sid]
        audio = audio_files[sid]

        seg_path  = os.path.join(temp_dir, f"seg_{i:02d}.mp4")
        clip_path = os.path.join(temp_dir, f"clip_{i:02d}.mp4")

        print(f"  [{i+1:02d}/{len(STEPS)}] {sid}  start={start:.1f}s  dur={dur:.1f}s...",
              end="", flush=True)

        if not cut_segment(full_mp4, start, dur + 1.0, seg_path):
            print(" ✗ cut failed")
            continue
        if not merge_with_audio(seg_path, audio, clip_path):
            print(" ✗ merge failed")
            continue

        clips.append(clip_path)
        print(" ✓")

    if not clips:
        print("❌ No clips")
        return False

    # Concatenate
    print(f"\n🔗 Concatenating {len(clips)} clips...")
    if concat_clips(clips, output_path):
        shutil.rmtree(temp_dir, ignore_errors=True)
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"\n✅  Tutorial video ready!")
        print(f"    File  : {output_path}")
        print(f"    Size  : {size_mb:.1f} MB")
        print(f"    Steps : {len(STEPS)}  |  Clips : {len(clips)}")
        print(f"\n▶   Open  : open \"{output_path}\"")
        return True
    else:
        print("❌ Concatenation failed")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Myle Dashboard Hindi Tutorial Video Generator v3 (Real Screen Recording)",
        epilog=(
            "Example:\n"
            "  python generate.py \\\n"
            "    --url http://localhost:5002 \\\n"
            "    --user testmember \\\n"
            "    --password tutorial123 \\\n"
            "    --db /path/to/leads.db"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--url",      required=True)
    parser.add_argument("--user",     required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--output",   default="team_tutorial.mp4")
    parser.add_argument("--db",       default="../leads.db")
    args = parser.parse_args()

    print("""
╔════════════════════════════════════════════════════════════════════╗
║  Myle Community Dashboard — Hindi Tutorial Video Generator v3      ║
║  REAL screen recording  •  Highlights  •  Hindi TTS narration      ║
╚════════════════════════════════════════════════════════════════════╝
    """)

    try:
        ok = asyncio.run(main(
            args.url, args.user, args.password, args.output, args.db
        ))
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\n⏹  Cancelled")
        sys.exit(1)
    except Exception as e:
        import traceback
        print(f"\n❌ Fatal: {e}")
        traceback.print_exc()
        sys.exit(1)
