# 🎬 Myle Community Dashboard — Hindi Tutorial Video Generator

Automatically generate a **full walkthrough tutorial video in Hindi** for your Myle Community Dashboard app!

## ✨ What It Does

- **18 steps** covering all features (Admin + Team dashboards)
- **Hindi narration** using Microsoft Edge TTS (natural sounding voice)
- **Automated screenshots** of every page
- **Professional video** with synced audio
- **Mobile-optimized** (390×844 resolution)

## 📋 Features Covered

1. Admin Dashboard overview
2. Leads management & status tracking
3. Lead Pool (import & claiming)
4. Wallet system (recharge requests)
5. Admin reports & analytics
6. Settings (UPI, lead price, webhooks)
7. Team Dashboard (new minimal design)
8. Stage 1 enrollment
9. Day 1-2 batch toggles
10. Wallet recharge flow
11. Lead claiming from pool
12. Working Section (kanban view)
13. Daily report submission
14. ...and more!

## 🛠️ Installation

### Step 1: Install FFmpeg

**macOS:**
```bash
brew install ffmpeg
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install ffmpeg
```

**Windows:**
```bash
choco install ffmpeg
# OR download from https://ffmpeg.org/download.html
```

### Step 2: Install Python Dependencies

```bash
cd tutorial_video
pip install -r requirements.txt
playwright install chromium
```

## 🚀 Usage

### Basic Usage
```bash
python generate.py \
  --url http://localhost:5001 \
  --user admin \
  --password your_password
```

This will create `tutorial_video.mp4` in the current directory.

### Custom Output Path
```bash
python generate.py \
  --url http://localhost:5001 \
  --user admin \
  --password your_password \
  --output myle_tutorial_hindi.mp4
```

## ⏱️ How Long Does It Take?

- **TTS Generation:** 2-3 minutes (18 audio clips)
- **Screenshots:** 1-2 minutes (18 pages)
- **Video Creation:** 2-3 minutes (FFmpeg encoding)
- **Total:** ~5-8 minutes

## 📊 Output Format

- **Format:** MP4 (H.264 + AAC)
- **Resolution:** 390×844 px (mobile)
- **Audio:** Hindi (44.1kHz stereo)
- **Duration:** ~2.5 minutes
- **File Size:** ~30-50 MB

## 🎤 Voice Characteristics

- **Voice:** Indian Hindi, Female
- **Engine:** Microsoft Edge TTS (free, no API key)
- **Quality:** Natural sounding, clear pronunciation
- **Speed:** Normal (adjustable in code if needed)

## 📝 Customization

### Change Hindi Narration

Edit `STEPS` list in `generate.py`:

```python
{
    "id": "your_step",
    "hindi": "आपका नया narration यहाँ लिखें",
    "url": "/your/page/url",
    "wait": 2.5,
}
```

### Add/Remove Steps

Modify the `STEPS` list:
- Add new steps with `id`, `hindi`, `url`, `wait`
- Remove steps by deleting them from the list
- Reorder by changing list order

### Change Voice

Change `VOICE` variable to other Hindi voices:
- `hi-IN-SwaraNeural` (female, default)
- `hi-IN-BharatNeural` (male)

## ✅ Troubleshooting

### "ffmpeg not found"
**Fix:** Install FFmpeg (see installation section above)

### "Playwright timeout during screenshot"
**Fix:** Make sure app is running on the correct URL, and increase `--wait-until` parameter

### "Login failed"
**Fix:** Verify username and password are correct

### "Audio generation error"
**Fix:** Check internet connection (edge-tts needs to fetch from Microsoft cloud)

### "Video quality is low"
**Fix:** Edit `create_video_clip()` function and change `-tune stillimage` to `-tune animation`

## 📖 Example Run

```bash
$ python generate.py --url http://localhost:5001 --user admin --password mypass

╔════════════════════════════════════════════════════════════╗
║     Myle Community Dashboard                               ║
║     Hindi Tutorial Video Generator                         ║
║     18 steps | Full Walkthrough | Auto-generated           ║
╚════════════════════════════════════════════════════════════╝

📁 Working directory: /tmp/myle_tutorial_abc123

🎤 Generating Hindi audio narration...
[████████████████████] 18/18 — TTS: 18_outro
✅ Audio files generated

📸 Taking screenshots...
[████████████████████] 18/18 — Screenshot: 18_outro
✅ Screenshots taken

🎬 Creating video clips...
[████████████████████] 18/18 — Creating clip: 18_outro
✅ Video clips created

🔗 Concatenating 18 clips...
✅ Tutorial video created: tutorial_video.mp4

📊 Video stats:
   Duration: 45 seconds
   Size: 35.2 MB
   Language: Hindi
   Resolution: 390×844 (Mobile)

✨ Tutorial video ready! Open with: mpv tutorial_video.mp4
```

## 🎯 What Next?

1. **Watch:** Open `tutorial_video.mp4` in any video player
2. **Share:** Send to team or customers
3. **Host:** Upload to YouTube, Vimeo, or your website
4. **Customize:** Edit steps and regenerate for specific use cases

## 🔧 Advanced Options

### Regenerate with Different Credentials

```bash
python generate.py \
  --url http://localhost:5001 \
  --user teamuser \
  --password pass123 \
  --output team_tutorial.mp4
```

This will show the team member view of the dashboard.

### Multiple Videos

Create separate videos for admin and team views:

```bash
# Admin tutorial
python generate.py --url http://localhost:5001 --user admin --password xxx --output admin_guide.mp4

# Team tutorial
python generate.py --url http://localhost:5001 --user teamuser --password xxx --output team_guide.mp4
```

## 📦 Technical Details

- **Playwright:** Automates Chromium browser for screenshots
- **edge-tts:** Microsoft's free TTS API for Hindi audio
- **FFmpeg:** Creates video from images + audio
- **Asyncio:** Concurrent operations for speed

## 🤝 Support

If you encounter issues:

1. Check error message carefully
2. Verify app is running (`curl http://localhost:5001`)
3. Test login manually
4. Check Python version (requires 3.8+)
5. Ensure all dependencies installed

## 📄 License

Internal use only. Generated videos can be shared with your team.

---

**Happy tutorial making! 🎬✨**
