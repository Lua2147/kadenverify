# KadenVerify — Self-Hosted Email Verification

Zero-cost replacement for OmniVerifier/MillionVerifier. Matches SaaS speed (<50ms cached) with 95%+ SMTP accuracy.

## Stack

Python, FastAPI, DuckDB

## Key Commands

```bash
python cli.py              # CLI verification
python server.py           # Start API server
python dashboard.py        # Monitoring dashboard
python batch_verify.py     # Bulk verification
```

## 3-Tier Verification

1. **Cached** (~50ms) — DuckDB lookup, 30-day TTL
2. **Fast** (~300ms) — RFC syntax + DNS MX + provider detection
3. **SMTP** (~3s) — EHLO/MAIL FROM/RCPT TO handshake (never sends DATA)

## API Endpoints

- `GET /v1/validate/{email}` — Single email verification
- `POST /v1/verify` — Batch verification (up to 1000)

OmniVerifier-compatible response format (drop-in replacement).

## Features

- Catch-all detection via random address probe
- Domain-first batching (10-100x speedup)
- 20-100 parallel SMTP connections
- Apollo DB cross-reference for catch-all validation
- Error parsing for 40+ patterns in 7 languages
