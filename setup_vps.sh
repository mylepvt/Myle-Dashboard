#!/bin/bash
# Myle Community Dashboard — Hostinger VPS Setup Script
# Run as root: bash setup_vps.sh

set -e

APP_DIR="/var/www/myle"
APP_USER="myle"
DOMAIN=""   # Fill this: e.g. dashboard.mylecommunity.com
PORT=8000

echo "========================================"
echo "  Myle Community VPS Setup"
echo "========================================"

# 1. System update + dependencies
echo "[1/8] Installing system packages..."
apt update -y && apt upgrade -y
apt install -y python3 python3-pip python3-venv nginx certbot python3-certbot-nginx git curl ufw

# 2. Create app user
echo "[2/8] Creating app user..."
id -u $APP_USER &>/dev/null || useradd -m -s /bin/bash $APP_USER

# 3. Clone repo
echo "[3/8] Cloning repository..."
mkdir -p $APP_DIR
cd $APP_DIR
if [ ! -d ".git" ]; then
  git clone https://github.com/mylepvt/Myle-Dashboard.git .
else
  git pull origin main
fi
chown -R $APP_USER:$APP_USER $APP_DIR

# 4. Python venv + packages
echo "[4/8] Setting up Python environment..."
sudo -u $APP_USER python3 -m venv $APP_DIR/venv
sudo -u $APP_USER $APP_DIR/venv/bin/pip install --upgrade pip
sudo -u $APP_USER $APP_DIR/venv/bin/pip install -r $APP_DIR/requirements.txt

# 5. Systemd service
echo "[5/8] Creating systemd service..."
cat > /etc/systemd/system/myle.service << EOF
[Unit]
Description=Myle Community Dashboard
After=network.target

[Service]
User=$APP_USER
Group=$APP_USER
WorkingDirectory=$APP_DIR
Environment="FLASK_ENV=production"
ExecStart=$APP_DIR/venv/bin/gunicorn \
    --workers 2 \
    --bind 127.0.0.1:$PORT \
    --timeout 120 \
    --preload \
    --access-logfile /var/log/myle/access.log \
    --error-logfile /var/log/myle/error.log \
    wsgi:application
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

mkdir -p /var/log/myle
chown -R $APP_USER:$APP_USER /var/log/myle

# 6. Nginx config
echo "[6/8] Configuring nginx..."
cat > /etc/nginx/sites-available/myle << EOF
server {
    listen 80;
    server_name $DOMAIN;

    client_max_body_size 20M;

    location / {
        proxy_pass         http://127.0.0.1:$PORT;
        proxy_set_header   Host \$host;
        proxy_set_header   X-Real-IP \$remote_addr;
        proxy_set_header   X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto \$scheme;
        proxy_read_timeout 120s;
    }

    location /static/ {
        alias $APP_DIR/static/;
        expires 7d;
        add_header Cache-Control "public, immutable";
    }
}
EOF

ln -sf /etc/nginx/sites-available/myle /etc/nginx/sites-enabled/myle
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

# 7. Firewall
echo "[7/8] Setting up firewall..."
ufw allow OpenSSH
ufw allow 'Nginx Full'
ufw --force enable

# 8. Start service
echo "[8/8] Starting Myle service..."
systemctl daemon-reload
systemctl enable myle
systemctl start myle

echo ""
echo "========================================"
echo "  Setup Complete!"
echo "  Service status: systemctl status myle"
echo "  Logs:           journalctl -u myle -f"
echo ""
echo "  NEXT: Upload leads.db then run:"
echo "  certbot --nginx -d $DOMAIN"
echo "========================================"
