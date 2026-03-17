# 🎬 Tutorial Video — Setup Guide

## Step 1️⃣ — Install FFmpeg

**On macOS:**
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install ffmpeg
```

**On Linux (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install ffmpeg
```

Verify:
```bash
ffmpeg -version
```

## Step 2️⃣ — Install Python Dependencies

```bash
cd "Team dashboard/tutorial_video"
pip install -r requirements.txt
playwright install chromium
```

## Step 3️⃣ — Start Your Flask App

```bash
cd "Team dashboard"
python app.py
# OR use gunicorn/your deployment method
```

App should be running at: **http://localhost:5001**

## Step 4️⃣ — Generate Tutorial Video

```bash
cd tutorial_video

# For admin view
python generate.py \
  --url http://localhost:5001 \
  --user admin \
  --password YOUR_ADMIN_PASSWORD

# For team view (if you have team credentials)
python generate.py \
  --url http://localhost:5001 \
  --user teamuser \
  --password TEAM_PASSWORD \
  --output team_tutorial.mp4
```

## ⏳ What to Expect

- Takes **5-8 minutes** to generate
- Shows progress bar for each stage
- Creates **`tutorial_video.mp4`** in current directory
- Size: ~30-50 MB
- Duration: ~2.5-3 minutes
- Language: **Hindi** with natural TTS voice

## 🎞️ After Generation

Open the video:
```bash
# macOS
open tutorial_video.mp4

# Linux
vlc tutorial_video.mp4

# Windows
ffplay tutorial_video.mp4
```

## 📤 Share Your Video

1. **YouTube:** Upload and add to channel
2. **Email:** Send to team
3. **Google Drive:** Share with link
4. **WhatsApp:** Share video file
5. **Website:** Embed on your site

## 🎨 Customization

Edit `tutorial_video/generate.py` — STEPS section to:
- Change Hindi narration
- Add/remove steps
- Change URLs being demoed
- Add pauses

Then regenerate!

## ❓ Troubleshooting

| Problem | Solution |
|---------|----------|
| `ffmpeg not found` | Install it (Step 1) |
| `playwright not found` | `pip install -r requirements.txt` |
| Login fails | Check username/password |
| App timeout | Make sure app is running |
| No audio in video | Check internet (edge-tts needs cloud) |

---

**Questions?** Check the detailed README in `tutorial_video/README.md`
