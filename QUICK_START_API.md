# KadenVerify API — Quick Start

## 1. Start the Server

```bash
cd "/Users/louis/Mundi Princeps/apps/email-verifier"
./start_server.sh
```

Server runs at `http://localhost:8000`

API docs at `http://localhost:8000/docs` (interactive Swagger UI)

## 2. Test with curl

```bash
# Health check (no auth required)
curl http://localhost:8000/health

# Single email verification
curl -H "X-API-Key: dev-key-replace-in-production" \
  "http://localhost:8000/verify?email=test@gmail.com"

# Batch verification
curl -X POST http://localhost:8000/verify/batch \
  -H "X-API-Key: dev-key-replace-in-production" \
  -H "Content-Type: application/json" \
  -d '{"emails": ["test@gmail.com", "invalid@example.com"]}'

# Statistics
curl -H "X-API-Key: dev-key-replace-in-production" \
  http://localhost:8000/stats
```

## 3. Use Python Client

```bash
# Verify single email
python api_client_example.py single test@gmail.com

# Verify batch from file
python api_client_example.py batch test_emails.txt

# Check health
python api_client_example.py health

# View stats
python api_client_example.py stats
```

## 4. Integration Examples

### Replace OmniVerifier in investor-outreach-platform

Change base URL from OmniVerifier to KadenVerify:
```python
# OLD
base_url = "https://api.omniverifier.com"

# NEW (local)
base_url = "http://localhost:8000"

# NEW (production)
base_url = "http://149.28.37.34:8000"
```

Endpoints are drop-in compatible:
- `GET /v1/validate/{email}`
- `POST /v1/verify`
- `GET /v1/validate/credits`

### JavaScript/TypeScript

```typescript
const API_BASE = 'http://localhost:8000';
const API_KEY = 'your-api-key';

async function verifyEmail(email: string) {
  const response = await fetch(
    `${API_BASE}/verify?email=${encodeURIComponent(email)}`,
    {
      headers: {
        'X-API-Key': API_KEY
      }
    }
  );
  return await response.json();
}

// Usage
const result = await verifyEmail('test@example.com');
console.log(result.status); // 'valid' | 'invalid' | 'catch_all' | 'unknown'
```

### Python Requests

```python
import requests

def verify_email(email: str) -> dict:
    response = requests.get(
        'http://localhost:8000/verify',
        params={'email': email},
        headers={'X-API-Key': 'your-api-key'}
    )
    return response.json()

# Usage
result = verify_email('test@example.com')
print(result['status'])  # 'valid', 'invalid', 'catch_all', or 'unknown'
```

## 5. Production Deployment

### On mundi-ralph server (149.28.37.34):

```bash
ssh root@149.28.37.34
cd /opt/kadenverify
source venv/bin/activate

# Start with systemd (persistent)
sudo systemctl start kadenverify
sudo systemctl enable kadenverify

# Or start manually
uvicorn server:app --host 0.0.0.0 --port 8000 --workers 4
```

### Configure nginx reverse proxy:

```nginx
server {
    listen 80;
    server_name verify.kadenwood.com;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Response Format

```json
{
  "email": "test@gmail.com",
  "status": "valid",             // 'valid' | 'invalid' | 'catch_all' | 'unknown'
  "is_deliverable": true,
  "is_catch_all": false,
  "is_disposable": false,
  "is_role": false,
  "is_free": true,
  "provider": "google",
  "mx_host": "gmail-smtp-in.l.google.com",
  "smtp_code": 250,
  "_kadenverify_tier": "cached", // 'cached' | 'fast' | 'full'
  "_kadenverify_reason": "Found in verification cache"
}
```

## Performance

- **Cached:** <50ms (instant from DuckDB)
- **Fast tier:** 100-300ms (DNS + metadata, no SMTP)
- **Full tier:** 500-2000ms (complete SMTP verification)

95%+ of repeat verifications hit cache for instant response.

## Cost

**Zero.** Self-hosted on your infrastructure. No API credits, no usage fees.
