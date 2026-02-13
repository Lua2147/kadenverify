# KadenVerify — Self-Hosted Email Verification

Zero-cost replacement for OmniVerifier/MillionVerifier. Matches SaaS speed (<50ms cached) with 95%+ SMTP accuracy.

## Stack

Python, FastAPI, DuckDB

## Key Commands

```bash
python cli.py              # CLI verification
python server.py           # Start API server
python dashboard.py        # Monitoring dashboard
python batch_verify.py     # Bulk verification (email-only)
python batch_process.py run ./contacts/  # Full pipeline: find → verify → squeeze → xlsx
python batch_process.py export state.csv -o results.xlsx  # Re-export from state
python batch_process.py stats state.csv  # Show stats from state
```

## 3-Tier Verification

1. **Cached** (~50ms) — DuckDB lookup, 30-day TTL
2. **Fast** (~300ms) — RFC syntax + DNS MX + provider detection
3. **SMTP** (~3s) — EHLO/MAIL FROM/RCPT TO handshake (never sends DATA)

## API Endpoints

- `GET /v1/validate/{email}` — Single email verification
- `POST /v1/verify` — Batch verification (up to 1000)

OmniVerifier-compatible response format (drop-in replacement).

## Batch Processor (`batch_process.py`)

End-to-end contact file processing: xlsx/csv in → find missing emails → verify → squeeze → xlsx out.

- Ingests PitchBook xlsx, generic xlsx, or csv (auto-detects column names)
- Splits contacts: has-email → verify, missing-email → find via waterfall then verify
- Iterative squeeze: re-verifies risky/unknown until dry (SMTP greylisting timer exploit)
- Preserves all contact metadata (name, company, position, phone, LinkedIn, etc.)
- Exports formatted xlsx with sheets: Summary, Deliverable, All Usable, All Contacts, No Email
- Saves state.csv for resume/re-export

## Features

- Catch-all detection via random address probe
- Domain-first batching (10-100x speedup)
- 20-100 parallel SMTP connections
- Apollo DB cross-reference for catch-all validation
- Error parsing for 40+ patterns in 7 languages
