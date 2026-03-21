import os
import json
import sqlite3
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, jsonify

try:
    import anthropic as _anthropic_lib
    ANTHROPIC_AVAILABLE = True
except ImportError:
    _anthropic_lib = None
    ANTHROPIC_AVAILABLE = False

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'myle-review-secret-2024')
# On Render free tier, /tmp is the only persistent-ish path
DB_PATH = os.environ.get('DB_PATH', os.path.join(os.path.dirname(__file__), 'reviews.db'))

ANALYSIS_PROMPT = """You are an expert human psychology analyst and behavioral profiler.

Analyse this Hindi/Hinglish sales conversation transcript and deeply understand the PSYCHOLOGY of the prospect.

Do NOT focus on sales techniques or objection handling.
Focus on: mindset, personality traits, emotional state, decision behavior.

Return ONLY valid JSON (no extra text, no markdown):

{
 "confidence_level": "X/10",
 "trust_level": "low / medium / high",
 "decision_speed": "fast / medium / slow",
 "personality_type": "analytical / emotional / confused / dominant / passive",
 "emotional_state": "excited / curious / doubtful / fearful / resistant / neutral",
 "intent_level": "low / medium / high",
 "seriousness": "timepass / exploring / serious / ready_to_buy",
 "hidden_fear": "",
 "dominant_thought_pattern": "",
 "logic_vs_emotion": "logic-driven / emotion-driven / mixed",
 "trust_barrier": "",
 "risk_appetite": "low / medium / high",
 "attention_level": "low / medium / high",
 "engagement_quality": "weak / average / strong",
 "decision_blocker": "",
 "behavior_summary": "",
 "prediction": "will_buy / needs_push / will_delay / will_not_buy"
}

Rules:
- Detect subtle signals like hesitation, delay language, tone shifts
- Interpret Hindi phrases psychologically:
   "soch ke batata hu" = avoidance / low commitment
   "dekhte hain" = low urgency
   "interest hai" = surface-level curiosity (not commitment)
- Do NOT assume buying intent unless strong signals exist
- Be accurate, not optimistic
- No motivation, no advice, only psychological truth

TRANSCRIPT:
"""

# ── DB ──────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_name   TEXT NOT NULL,
                phone       TEXT,
                batch       TEXT,
                transcript  TEXT NOT NULL,
                analysis    TEXT,
                prediction  TEXT,
                created_at  TEXT DEFAULT (datetime('now','localtime'))
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        db.commit()

def get_setting(key, default=''):
    with get_db() as db:
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row['value'] if row else default

def set_setting(key, value):
    with get_db() as db:
        db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))
        db.commit()

# ── Routes ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    with get_db() as db:
        reviews = db.execute(
            "SELECT id, lead_name, phone, batch, prediction, created_at FROM reviews ORDER BY id DESC"
        ).fetchall()
    return render_template('index.html', reviews=reviews)


@app.route('/analyze', methods=['GET', 'POST'])
def analyze():
    if request.method == 'GET':
        return render_template('analyze.html')

    lead_name  = request.form.get('lead_name', '').strip()
    phone      = request.form.get('phone', '').strip()
    batch      = request.form.get('batch', '').strip()
    transcript = request.form.get('transcript', '').strip()

    if not lead_name or not transcript:
        return render_template('analyze.html', error="Lead name aur transcript required hai.", form=request.form)

    # Get API key
    api_key = get_setting('anthropic_api_key') or os.environ.get('ANTHROPIC_API_KEY', '').strip()

    if not api_key or not ANTHROPIC_AVAILABLE:
        return render_template('analyze.html',
            error="Anthropic API key set nahi hai. Settings mein add karo.",
            form=request.form)

    try:
        client   = _anthropic_lib.Anthropic(api_key=api_key)
        response = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=1024,
            messages=[{"role": "user", "content": ANALYSIS_PROMPT + transcript}]
        )
        raw = response.content[0].text.strip()
        # Strip markdown code blocks if present
        if raw.startswith('```'):
            raw = raw.split('```')[1]
            if raw.startswith('json'):
                raw = raw[4:]
        analysis_json = json.loads(raw)
        analysis_str  = json.dumps(analysis_json, ensure_ascii=False)
        prediction    = analysis_json.get('prediction', '')
    except json.JSONDecodeError:
        analysis_str  = json.dumps({"error": "JSON parse failed", "raw": raw})
        prediction    = ''
    except Exception as e:
        return render_template('analyze.html',
            error=f"API Error: {str(e)}", form=request.form)

    with get_db() as db:
        cur = db.execute(
            "INSERT INTO reviews(lead_name, phone, batch, transcript, analysis, prediction) VALUES(?,?,?,?,?,?)",
            (lead_name, phone, batch, transcript, analysis_str, prediction)
        )
        db.commit()
        review_id = cur.lastrowid

    return redirect(url_for('detail', review_id=review_id))


@app.route('/review/<int:review_id>')
def detail(review_id):
    with get_db() as db:
        review = db.execute("SELECT * FROM reviews WHERE id=?", (review_id,)).fetchone()
    if not review:
        return redirect(url_for('index'))

    analysis = None
    if review['analysis']:
        try:
            analysis = json.loads(review['analysis'])
        except Exception:
            analysis = None

    return render_template('detail.html', review=review, analysis=analysis)


@app.route('/review/<int:review_id>/delete', methods=['POST'])
def delete_review(review_id):
    with get_db() as db:
        db.execute("DELETE FROM reviews WHERE id=?", (review_id,))
        db.commit()
    return redirect(url_for('index'))


@app.route('/settings', methods=['GET', 'POST'])
def settings():
    saved = False
    if request.method == 'POST':
        key = request.form.get('anthropic_api_key', '').strip()
        if key:
            set_setting('anthropic_api_key', key)
        saved = True
    api_key_set = bool(get_setting('anthropic_api_key') or os.environ.get('ANTHROPIC_API_KEY'))
    return render_template('settings.html', api_key_set=api_key_set, saved=saved)


# ── Run ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5050)
