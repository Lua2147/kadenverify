# KadenVerify Deployment Guide - Mundi Ralph

Complete deployment guide for mundi-ralph server (Vultr VPS with port 25 access).

## Prerequisites

- Server with port 25 outbound access (Vultr, DigitalOcean, etc.)
- Python 3.12+
- Git
- Domain with DNS configured (optional but recommended)

---

## Step 1: Server Preparation

```bash
# SSH to server
ssh root@149.28.37.34  # or ssh mundi-ralph

# Update system
apt update && apt upgrade -y

# Install Python 3.12 if needed
apt install -y python3.12 python3.12-venv python3-pip git

# Create app directory
mkdir -p /opt/kadenverify
cd /opt/kadenverify
```

---

## Step 2: Clone Repository

```bash
# Clone from GitHub
git clone https://github.com/Lua2147/kadenverify.git .

# Or if already cloned, pull latest
git pull origin main
```

---

## Step 3: Install Dependencies

```bash
# Create virtual environment
python3.12 -m venv venv
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

---

## Step 4: Configure

### Create config.json

```bash
cp config.example.json config.json
nano config.json
```

**Edit these values:**
```json
{
  "smtp": {
    "helo_domain": "verify.kadenwood.com",
    "from_address": "verify@kadenwood.com",
    "default_concurrency": 50
  },
  "api": {
    "api_key": "CHANGE_THIS_TO_SECURE_KEY"
  },
  "catchall_validation": {
    "enabled": true,
    "apollo_db_path": "/path/to/apollo.duckdb"
  }
}
```

### Create Streamlit secrets

```bash
mkdir -p .streamlit
nano .streamlit/secrets.toml
```

**Add:**
```toml
KADENVERIFY_API_KEY = "SAME_KEY_AS_CONFIG_JSON"
```

### Set environment variables

```bash
nano .env
```

**Add:**
```bash
KADENVERIFY_API_KEY=your-secure-api-key-here
KADENVERIFY_HELO_DOMAIN=verify.kadenwood.com
KADENVERIFY_FROM_ADDRESS=verify@kadenwood.com
KADENVERIFY_CONCURRENCY=50
```

---

## Step 5: Test Verification

```bash
# Test single email
python3 cli.py verify test@gmail.com

# Should see:
# ✓ test@gmail.com
# Reachability: safe (or risky/invalid)
# SMTP Code: 250 (not 0!)
```

**If SMTP Code is 0:**
- Port 25 might be blocked
- Check firewall: `ufw allow 25/tcp`
- Test port: `telnet gmail-smtp-in.l.google.com 25`

---

## Step 6: Start API Server

### Option A: Direct (for testing)

```bash
# Start API
uvicorn server:app --host 0.0.0.0 --port 8025

# Test from another terminal
curl 'http://localhost:8025/health'
# Should return: {"status": "ok"}
```

### Option B: Systemd Service (recommended)

```bash
# Create service file
sudo nano /etc/systemd/system/kadenverify-api.service
```

**Add:**
```ini
[Unit]
Description=KadenVerify API Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/kadenverify
Environment="PATH=/opt/kadenverify/venv/bin"
ExecStart=/opt/kadenverify/venv/bin/uvicorn server:app --host 0.0.0.0 --port 8025
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Enable and start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable kadenverify-api
sudo systemctl start kadenverify-api
sudo systemctl status kadenverify-api
```

---

## Step 7: Start Dashboard

### Option A: Direct (for testing)

```bash
# Start dashboard
streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0

# Access: http://149.28.37.34:8501
```

### Option B: Systemd Service (recommended)

```bash
# Create service file
sudo nano /etc/systemd/system/kadenverify-dashboard.service
```

**Add:**
```ini
[Unit]
Description=KadenVerify Dashboard
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/kadenverify
Environment="PATH=/opt/kadenverify/venv/bin"
ExecStart=/opt/kadenverify/venv/bin/streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Enable and start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable kadenverify-dashboard
sudo systemctl start kadenverify-dashboard
sudo systemctl status kadenverify-dashboard
```

---

## Step 8: Configure Firewall

```bash
# Allow API port
sudo ufw allow 8025/tcp

# Allow dashboard port
sudo ufw allow 8501/tcp

# Allow SMTP outbound (if not already)
sudo ufw allow out 25/tcp

# Reload firewall
sudo ufw reload
```

---

## Step 9: DNS Configuration (Optional)

### A Records

```
verify.kadenwood.com  →  149.28.37.34
dashboard.kadenwood.com  →  149.28.37.34
```

### PTR Record (Reverse DNS)

Configure in Vultr control panel:
```
149.28.37.34  →  verify.kadenwood.com
```

### SPF Record

Add to your domain DNS:
```
v=spf1 ip4:149.28.37.34 ~all
```

---

## Step 10: Nginx Reverse Proxy (Optional)

For HTTPS and custom domains:

```bash
# Install nginx
apt install -y nginx certbot python3-certbot-nginx

# Create API config
nano /etc/nginx/sites-available/kadenverify-api
```

**Add:**
```nginx
server {
    listen 80;
    server_name verify.kadenwood.com;

    location / {
        proxy_pass http://localhost:8025;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

**Create dashboard config:**
```bash
nano /etc/nginx/sites-available/kadenverify-dashboard
```

**Add:**
```nginx
server {
    listen 80;
    server_name dashboard.kadenwood.com;

    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

**Enable sites:**
```bash
ln -s /etc/nginx/sites-available/kadenverify-api /etc/nginx/sites-enabled/
ln -s /etc/nginx/sites-available/kadenverify-dashboard /etc/nginx/sites-enabled/
nginx -t
systemctl reload nginx
```

**Enable HTTPS:**
```bash
certbot --nginx -d verify.kadenwood.com -d dashboard.kadenwood.com
```

---

## Step 11: Verify Everything Works

### Test API

```bash
# Health check
curl http://localhost:8025/health

# Verify email
curl 'http://localhost:8025/verify?email=test@gmail.com' \
  -H 'X-API-Key: your-api-key'

# Should return status: valid/risky/invalid (NOT unknown)
```

### Test Dashboard

```bash
# Open in browser
http://149.28.37.34:8501

# Or with domain
https://dashboard.kadenwood.com
```

### Test from External

```bash
# From your local machine
curl 'http://149.28.37.34:8025/verify?email=test@gmail.com' \
  -H 'X-API-Key: your-api-key'
```

---

## Monitoring

### Check logs

```bash
# API logs
sudo journalctl -u kadenverify-api -f

# Dashboard logs
sudo journalctl -u kadenverify-dashboard -f
```

### Check status

```bash
# Services
sudo systemctl status kadenverify-api
sudo systemctl status kadenverify-dashboard

# Ports
netstat -tlnp | grep -E '8025|8501'
```

### View stats

```bash
# Via API
curl 'http://localhost:8025/stats' \
  -H 'X-API-Key: your-api-key'

# Via CLI
python3 cli.py stats
```

---

## Troubleshooting

### SMTP Code is 0 (port 25 blocked)

```bash
# Test port 25
telnet gmail-smtp-in.l.google.com 25

# If fails, contact Vultr to unblock port 25
```

### API returns 401 Unauthorized

- Check API key in config.json matches request header
- Ensure X-API-Key header is set correctly

### Dashboard shows "API Offline"

- Check API is running: `systemctl status kadenverify-api`
- Check API port: `netstat -tlnp | grep 8025`
- Check API_URL in dashboard.py matches server

### High "unknown" rate

- Port 25 blocked → contact provider
- IP blacklisted → check https://mxtoolbox.com/blacklists.aspx
- Timeout issues → reduce concurrency

---

## Maintenance

### Update code

```bash
cd /opt/kadenverify
git pull origin main
pip install -r requirements.txt
sudo systemctl restart kadenverify-api
sudo systemctl restart kadenverify-dashboard
```

### Backup database

```bash
# Backup verified results
cp verified.duckdb verified.duckdb.backup-$(date +%Y%m%d)
```

### Clean old cache

```bash
# Clear cache older than 30 days
python3 -c "
import duckdb
conn = duckdb.connect('verified.duckdb')
conn.execute(\"DELETE FROM verified_emails WHERE verified_at < NOW() - INTERVAL 30 DAYS\")
conn.close()
"
```

---

## Production Checklist

- [ ] Port 25 verified working
- [ ] API running and accessible
- [ ] Dashboard running and accessible
- [ ] API key configured securely
- [ ] Firewall rules configured
- [ ] DNS records configured (optional)
- [ ] SSL certificates installed (optional)
- [ ] Monitoring/logging enabled
- [ ] Backup strategy in place
- [ ] Test verification returns real results (not "unknown")

---

## Access URLs

**After deployment:**

- **API:** `http://149.28.37.34:8025` or `https://verify.kadenwood.com`
- **Dashboard:** `http://149.28.37.34:8501` or `https://dashboard.kadenwood.com`
- **Health Check:** `http://149.28.37.34:8025/health`
- **API Docs:** `http://149.28.37.34:8025/docs` (FastAPI auto-generated)

---

## Next Steps

1. Integrate with existing apps (investor-outreach, CRM)
2. Set up monitoring (Uptime Kuma, Grafana)
3. Configure backup automation
4. Add authentication to dashboard (if needed)
5. Set up alerts for failures

---

## Support

- **Logs:** `journalctl -u kadenverify-api -f`
- **Stats:** `curl localhost:8025/stats -H 'X-API-Key: your-key'`
- **GitHub:** https://github.com/Lua2147/kadenverify
- **Issues:** https://github.com/Lua2147/kadenverify/issues
