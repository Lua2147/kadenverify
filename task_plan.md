# Task Plan: Supabase Migration (Email Verifier)

## Goal
Migrate the app’s persistent state from local DuckDB/Redis to Supabase Postgres (and ship a safe migration path for existing `verified.duckdb` data), while preserving API contracts.

## Current Phase
Phase 1

## Phases

### Phase 1: Requirements & Discovery
- [ ] Confirm scope of “migrate everything” (API cache, /stats, CLI pipeline, Streamlit dashboard, rate limiting, metrics)
- [ ] Confirm target Supabase project + auth method (service role key vs DB connection string)
- [ ] Inventory current state usage (DuckDB cache, verified results DB, Redis rate limiting/cache options)
- [ ] Document findings + constraints in findings.md
- **Status:** in_progress

### Phase 2: Schema & Design
- [ ] Define canonical `verified_emails` schema for Supabase (include `error`, `verified_at`, indexes)
- [ ] Decide access pattern for runtime (PostgREST vs direct Postgres) + connection management
- [ ] Decide behavior on Supabase outage (fallback, degraded readiness, caching)
- [ ] Define migration strategy from `verified.duckdb` (idempotent upsert, batching, resume)
- **Status:** pending

### Phase 3: Implementation
- [ ] Add Supabase backend for cache read/write
- [ ] Update `/stats` to query Supabase (and/or provide compatibility mode)
- [ ] Update CLI pipeline + CLI stats to support Supabase target
- [ ] Add migration script: DuckDB -> Supabase table
- [ ] Update docs/deploy (`deploy.sh`, env vars)
- **Status:** pending

### Phase 4: Testing & Verification
- [ ] Unit tests: Supabase backend behavior (mock HTTP/DB)
- [ ] Verify existing API contract tests still pass
- [ ] Verify migration script on sample dataset (dry-run mode)
- [ ] Document verification results in progress.md
- **Status:** pending

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
