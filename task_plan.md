# Task Plan: Supabase Migration (Email Verifier)

## Goal
Migrate the app’s persistent state from local DuckDB/Redis to Supabase Postgres (and ship a safe migration path for existing `verified.duckdb` data), while preserving API contracts.

## Current Phase
Phase 4

## Phases

### Phase 1: Requirements & Discovery
- [x] Confirm scope of “migrate everything” (API cache, /stats, CLI pipeline, Streamlit dashboard, rate limiting, metrics)
- [x] Confirm target Supabase project + auth method (service role key vs DB connection string)
- [x] Inventory current state usage (DuckDB cache, verified results DB, Redis rate limiting/cache options)
- [x] Document findings + constraints in findings.md
- **Status:** complete

### Phase 2: Schema & Design
- [x] Define canonical `verified_emails` schema for Supabase (include `error`, `verified_at`, indexes)
- [x] Decide access pattern for runtime (PostgREST via `requests` + async `to_thread`)
- [x] Decide behavior on Supabase outage (readiness shows degraded; cache lookup/update best-effort)
- [x] Define migration strategy from `verified.duckdb` (idempotent upsert, batching)
- **Status:** complete

### Phase 3: Implementation
- [x] Add Supabase backend for cache read/write
- [x] Update `/stats` to query Supabase when enabled
- [x] Update CLI pipeline + CLI stats to support Supabase target
- [x] Add migration command + SQL migration for table
- [x] Update docs/deploy (`deploy.sh`, docker env vars)
- **Status:** complete

### Phase 4: Testing & Verification
- [x] Unit tests: Supabase backend behavior (mock HTTP/DB)
- [x] Verify existing API contract tests still pass
- [ ] Verify migration command against a real Supabase project
- [x] Document verification results in progress.md
- **Status:** in_progress

### Phase 5: Delivery
- [ ] Commit + push
- [ ] Run/guide data migration
- [ ] Production rollout checklist (health, ready, metrics, stats)
- **Status:** pending

## Decisions Made
| Decision | Rationale |
|----------|-----------|

## Errors Encountered
| Error | Resolution |
|-------|------------|
