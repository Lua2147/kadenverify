# KadenVerify API Usage Guide

## Starting the Server

### Local Development
```bash
cd "/Users/louis/Mundi Princeps/apps/email-verifier"

# Set environment variables
export KADENVERIFY_API_KEY="your-secret-key-here"
export KADENVERIFY_HELO_DOMAIN="verify.kadenwood.com"
export KADENVERIFY_FROM_ADDRESS="verify@kadenwood.com"
export KADENVERIFY_CONCURRENCY="50"
export KADENVERIFY_TIERED="true"

# Run server
uvicorn server:app --host 0.0.0.0 --port 8000 --reload
```

### Production (mundi-ralph)
```bash
ssh root@149.28.37.34
cd /opt/kadenverify
source venv/bin/activate
uvicorn server:app --host 0.0.0.0 --port 8000 --workers 4
```

Server will be available at `http://localhost:8000` (local) or `http://149.28.37.34:8000` (production).

## API Endpoints

### 1. Single Email Verification (GET)
```
GET /verify?email=test@example.com
Headers: X-API-Key: your-secret-key-here
```

**Response:**
```json
{
  "email": "test@example.com",
  "status": "valid",
  "is_deliverable": true,
  "is_catch_all": false,
  "is_disposable": false,
  "is_role": false,
  "is_free": false,
  "provider": "google",
  "mx_host": "gmail-smtp-in.l.google.com",
  "smtp_code": 250,
  "_kadenverify_tier": "cached",
  "_kadenverify_reason": "Found in verification cache"
}
```

### 2. Single Email Verification (POST)
```
POST /verify
Headers: X-API-Key: your-secret-key-here
Content-Type: application/json

{
  "email": "test@example.com"
}
```

### 3. Batch Verification
```
POST /verify/batch
Headers: X-API-Key: your-secret-key-here
Content-Type: application/json

{
  "emails": [
    "test1@example.com",
    "test2@example.com",
    "test3@example.com"
  ]
}
```

**Response:**
```json
[
  {
    "email": "test1@example.com",
    "status": "valid",
    "is_deliverable": true,
    ...
  },
  {
    "email": "test2@example.com",
    "status": "invalid",
    "is_deliverable": false,
    ...
  }
]
```

### 4. OmniVerifier-Compatible Endpoints

#### GET /v1/validate/{email}
Drop-in replacement for investor-outreach-platform
```
GET /v1/validate/test@example.com
Headers: x-api-key: your-secret-key-here
```

#### POST /v1/verify
Drop-in replacement for kadenwood-ui
```
POST /v1/verify
Headers: x-api-key: your-secret-key-here
Content-Type: application/json

{
  "email": "test@example.com"
}
```

#### GET /v1/validate/credits
Returns unlimited credits (always 999999)
```
GET /v1/validate/credits
Headers: x-api-key: your-secret-key-here
```

### 5. Health Check
```
GET /health
```

**Response:**
```json
{
  "status": "ok",
  "service": "kadenverify",
  "version": "0.1.0"
}
```

### 6. Statistics
```
GET /stats
Headers: X-API-Key: your-secret-key-here
```

**Response:**
```json
{
  "total": 1547,
  "valid": 892,
  "invalid": 433,
  "catch_all": 156,
  "unknown": 66
}
```

## Status Values

- **valid** — Email is deliverable
- **invalid** — Email does not exist or is undeliverable
- **catch_all** — Domain accepts all emails (cannot verify individual address)
- **unknown** — Could not determine status (temporary error, timeout, etc.)

## Rate Limiting

- **100 requests per minute** per IP address
- Returns `429 Too Many Requests` when exceeded

## Tiered Verification

KadenVerify uses a 3-tier verification system for <50ms response times:

1. **Tier 1 (Cached)** — Instant response from DuckDB cache
2. **Tier 2 (Fast)** — DNS + metadata only, no SMTP (when confidence > 85%)
3. **Tier 3 (Full)** — Full SMTP verification

The `_kadenverify_tier` and `_kadenverify_reason` fields show which tier was used.

## Authentication

API key can be provided in three ways:

1. `X-API-Key` header (recommended)
2. `x-api-key` header (OmniVerifier compatibility)
3. `Authorization: Bearer <key>` header (OmniVerifier compatibility)

## Error Responses

### 401 Unauthorized
```json
{
  "detail": "Invalid API key"
}
```

### 429 Rate Limit Exceeded
```json
{
  "detail": "Rate limit exceeded"
}
```

### 400 Bad Request (Batch)
```json
{
  "detail": "Batch size exceeds maximum of 1000"
}
```
