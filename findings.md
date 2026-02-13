# Findings & Decisions

## Requirements
- User request: “Add to supa instead, migrate everything” (interpretation pending; needs scope confirmation).
- Preserve existing API contracts (notably `/ready`, `/metrics`, auth header compat, rate limit 429 shape).
- Avoid leaking secrets (Supabase URL/keys, DB passwords).

## Research Findings
- Current persistence is local `verified.duckdb` with `verified_emails` table.
- `server.py` uses `verified.duckdb` as the tiered-verification cache (Tier 1).
- `store/duckdb_io.py` writes verification results to `verified.duckdb` for CLI pipeline and `cli.py stats`.
- `dashboard.py` queries `verified.duckdb` directly for analytics/export.
- No existing Supabase integration in this app (`rg supabase` found nothing).

## Technical Decisions
| Decision | Rationale |
|----------|-----------|

## Issues Encountered
| Issue | Resolution |
|-------|------------|
- Git push blocked in sandbox (DNS resolution for github.com); needs user approval for network-enabled push.

## Resources
- Key files: `server.py`, `store/duckdb_io.py`, `cli.py`, `dashboard.py`, `verified.duckdb`
