#!/bin/bash
# KadenVerify Quick Deployment Script for Mundi Ralph
# Run this on the server: bash deploy.sh

set -e  # Exit on error

echo "🚀 KadenVerify Deployment Script"
echo "================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
  echo "⚠️  Please run as root (sudo bash deploy.sh)"
  exit 1
fi

# Configuration
INSTALL_DIR="/opt/kadenverify"
REPO_URL="https://github.com/Lua2147/kadenverify.git"
PYTHON_CMD="python3"

echo "📋 Configuration:"
echo "  Install dir: $INSTALL_DIR"
echo "  Repository: $REPO_URL"
echo ""

# Step 1: Install dependencies
echo "📦 Installing system dependencies..."
apt update
apt install -y python3 python3-venv python3-pip git nginx certbot python3-certbot-nginx

# Step 2: Clone or update repository
if [ -d "$INSTALL_DIR" ]; then
  echo "📂 Directory exists, pulling latest changes..."
  cd "$INSTALL_DIR"
  git pull origin main
else
  echo "📥 Cloning repository..."
  git clone "$REPO_URL" "$INSTALL_DIR"
  cd "$INSTALL_DIR"
fi

# Step 3: Create virtual environment
echo "🐍 Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
  $PYTHON_CMD -m venv venv
fi

source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Step 4: Configuration
echo "⚙️  Configuration..."

# Check if config.json exists
if [ ! -f "config.json" ]; then
  echo "📝 Creating config.json from example..."
  cp config.example.json config.json
  echo "⚠️  Please edit config.json with your settings:"
  echo "     nano $INSTALL_DIR/config.json"
  echo ""
  read -p "Press Enter to edit config now, or Ctrl+C to exit and edit later..."
  nano config.json
fi

# Check if .streamlit/secrets.toml exists
mkdir -p .streamlit
if [ ! -f ".streamlit/secrets.toml" ]; then
  echo "📝 Creating Streamlit secrets..."
  read -p "Enter API key (same as in config.json): " api_key
  echo "KADENVERIFY_API_KEY = \"$api_key\"" > .streamlit/secrets.toml
  chmod 600 .streamlit/secrets.toml
fi

# Step 5: Test verification
echo "🧪 Testing verification..."
echo "Testing: python3 cli.py verify test@gmail.com"
$PYTHON_CMD cli.py verify test@gmail.com || {
  echo "⚠️  Warning: Verification test failed. Port 25 might be blocked."
  echo "Continue anyway? (y/n)"
  read -r continue
  if [ "$continue" != "y" ]; then
    exit 1
  fi
}

# Step 6: Create systemd services
echo "🔧 Creating systemd services..."

# API service
cat > /etc/systemd/system/kadenverify-api.service <<EOF
[Unit]
Description=KadenVerify API Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$INSTALL_DIR/venv/bin"
ExecStart=$INSTALL_DIR/venv/bin/uvicorn server:app --host 0.0.0.0 --port 8025
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Dashboard service
cat > /etc/systemd/system/kadenverify-dashboard.service <<EOF
[Unit]
Description=KadenVerify Dashboard
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$INSTALL_DIR
Environment="PATH=$INSTALL_DIR/venv/bin"
ExecStart=$INSTALL_DIR/venv/bin/streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
systemctl daemon-reload

# Enable and start services
echo "🚀 Starting services..."
systemctl enable kadenverify-api
systemctl enable kadenverify-dashboard
systemctl restart kadenverify-api
systemctl restart kadenverify-dashboard

# Wait for services to start
sleep 3

# Check status
echo ""
echo "📊 Service Status:"
systemctl status kadenverify-api --no-pager -l | head -10
echo ""
systemctl status kadenverify-dashboard --no-pager -l | head -10

# Step 7: Configure firewall
echo ""
echo "🔥 Configuring firewall..."
if command -v ufw &> /dev/null; then
  ufw allow 8025/tcp
  ufw allow 8501/tcp
  ufw allow out 25/tcp
  echo "✅ Firewall rules added"
else
  echo "⚠️  UFW not installed, skipping firewall configuration"
fi

# Step 8: Test endpoints
echo ""
echo "🧪 Testing endpoints..."
sleep 2

# Test health endpoint
if curl -s http://localhost:8025/health | grep -q "ok"; then
  echo "✅ API health check passed"
else
  echo "❌ API health check failed"
fi

# Test dashboard
if curl -s http://localhost:8501 | grep -q "KadenVerify"; then
  echo "✅ Dashboard is running"
else
  echo "⚠️  Dashboard check inconclusive (may still be loading)"
fi

# Get server IP
SERVER_IP=$(curl -s ifconfig.me)

# Step 9: Summary
echo ""
echo "================================================================"
echo "✅ Deployment Complete!"
echo "================================================================"
echo ""
echo "📌 Access Points:"
echo "  API:       http://$SERVER_IP:8025"
echo "  Dashboard: http://$SERVER_IP:8501"
echo "  Health:    http://$SERVER_IP:8025/health"
echo "  API Docs:  http://$SERVER_IP:8025/docs"
echo ""
echo "📝 Next Steps:"
echo "  1. Test API: curl 'http://localhost:8025/verify?email=test@gmail.com' -H 'X-API-Key: YOUR_KEY'"
echo "  2. Open dashboard in browser: http://$SERVER_IP:8501"
echo "  3. Configure DNS (optional): See DEPLOYMENT_GUIDE.md"
echo "  4. Set up Nginx + SSL (optional): See DEPLOYMENT_GUIDE.md"
echo ""
echo "📚 Documentation:"
echo "  Full guide: $INSTALL_DIR/DEPLOYMENT_GUIDE.md"
echo "  README:     $INSTALL_DIR/README.md"
echo ""
echo "🔧 Useful Commands:"
echo "  View API logs:       journalctl -u kadenverify-api -f"
echo "  View dashboard logs: journalctl -u kadenverify-dashboard -f"
echo "  Restart API:         systemctl restart kadenverify-api"
echo "  Restart dashboard:   systemctl restart kadenverify-dashboard"
echo ""
echo "================================================================"
