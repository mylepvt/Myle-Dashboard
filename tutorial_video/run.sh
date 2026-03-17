#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Myle Community Dashboard — Tutorial Video Generator      ║${NC}"
echo -e "${BLUE}║  Hindi में Automatic Full Walkthrough                    ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}\n"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: Check & Install FFmpeg
# ═══════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[STEP 1/5]${NC} Checking FFmpeg..."

if ! command -v ffmpeg &> /dev/null; then
    echo -e "${YELLOW}⚠️  FFmpeg not found. Installing...${NC}"

    if ! command -v brew &> /dev/null; then
        echo -e "${YELLOW}Installing Homebrew first...${NC}"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi

    brew install ffmpeg
    echo -e "${GREEN}✅ FFmpeg installed${NC}\n"
else
    echo -e "${GREEN}✅ FFmpeg already installed${NC}\n"
fi

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: Install Python Dependencies
# ═══════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[STEP 2/5]${NC} Installing Python dependencies..."

pip install -q playwright edge-tts 2>/dev/null || pip3 install -q playwright edge-tts
playwright install chromium 2>/dev/null

echo -e "${GREEN}✅ Python dependencies installed${NC}\n"

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: Get Admin Password
# ═══════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[STEP 3/5]${NC} Admin credentials needed"
echo -e "${BLUE}ℹ️  Make sure Flask app is running at http://localhost:5001${NC}\n"

read -sp "Enter admin password: " ADMIN_PASSWORD
echo ""

if [ -z "$ADMIN_PASSWORD" ]; then
    echo -e "${RED}❌ Password cannot be empty${NC}"
    exit 1
fi

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: Generate Video
# ═══════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[STEP 4/5]${NC} Generating tutorial video...\n"

python generate.py \
    --url http://localhost:5001 \
    --user admin \
    --password "$ADMIN_PASSWORD" \
    --output tutorial_video.mp4

if [ ! -f "tutorial_video.mp4" ]; then
    echo -e "${RED}❌ Video generation failed${NC}"
    exit 1
fi

echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5: Open Video
# ═══════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[STEP 5/5]${NC} Opening video..."

VIDEO_PATH="$(pwd)/tutorial_video.mp4"
open "$VIDEO_PATH"

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  ✅ Tutorial Video Ready!                                ║${NC}"
echo -e "${GREEN}║  📁 Location: $VIDEO_PATH  ║${NC}"
echo -e "${GREEN}║  ▶️  Video opening in default player...                 ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}\n"

echo -e "${BLUE}Video Sharing Options:${NC}"
echo "  📤 YouTube: Upload to your channel"
echo "  📧 Email: Send to team"
echo "  🔗 Drive: Upload to Google Drive"
echo "  💬 WhatsApp: Share video file"
echo ""

exit 0
