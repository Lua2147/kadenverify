"""KadenVerify HTTP API — drop-in replacement for OmniVerifier.

Endpoints:
  GET  /verify?email=...          Single email verification
  POST /verify                    Single email verification (JSON body)
  POST /verify/batch              Batch verification (JSON array)
  GET  /v1/validate/{email}       OmniVerifier-compatible (investor-outreach)
  POST /v1/verify                 OmniVerifier-compatible (kadenwood-ui)
  GET  /health                    Health check
  GET  /stats                     Verification statistics
"""

import asyncio
import hashlib
import json
import logging
import os
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).parent))

from engine.verifier import verify_email, verify_batch
from engine.tiered_verifier import verify_email_tiered
from engine.models import VerificationResult, Reachability

logger = logging.getLogger("kadenverify.server")

# Configuration from environment
API_KEY = os.environ.get("KADENVERIFY_API_KEY", "")
HELO_DOMAIN = os.environ.get("KADENVERIFY_HELO_DOMAIN", "verify.kadenwood.com")
FROM_ADDRESS = os.environ.get("KADENVERIFY_FROM_ADDRESS", "verify@kadenwood.com")
CONCURRENCY = int(os.environ.get("KADENVERIFY_CONCURRENCY", "5"))
MAX_BATCH_SIZE = 1000
ENABLE_TIERED = os.environ.get("KADENVERIFY_TIERED", "true").lower() == "true"
CACHE_BACKEND = os.environ.get("KADENVERIFY_CACHE_BACKEND", "duckdb").lower()
CACHE_REDIS_URL = os.environ.get("KADENVERIFY_CACHE_REDIS_URL", os.environ.get("KADENVERIFY_REDIS_URL", ""))
CACHE_TTL_SECONDS = int(os.environ.get("KADENVERIFY_CACHE_TTL_SECONDS", str(30 * 24 * 60 * 60)))
SUPABASE_URL = os.environ.get("KADENVERIFY_SUPABASE_URL", os.environ.get("SUPABASE_URL", ""))
SUPABASE_SERVICE_ROLE_KEY = os.environ.get(
    "KADENVERIFY_SUPABASE_SERVICE_ROLE_KEY",
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""),
)
SUPABASE_TABLE = os.environ.get("KADENVERIFY_SUPABASE_TABLE", "verified_emails")
SUPABASE_TIMEOUT_SECONDS = float(os.environ.get("KADENVERIFY_SUPABASE_TIMEOUT_SECONDS", "5.0"))

# If Supabase is configured and the cache backend wasn't explicitly set, prefer Supabase.
if (
    "KADENVERIFY_CACHE_BACKEND" not in os.environ
    and SUPABASE_URL
    and SUPABASE_SERVICE_ROLE_KEY
):
    CACHE_BACKEND = "supabase"
READINESS_DNS_HOST = os.environ.get("KADENVERIFY_READINESS_DNS_HOST", "gmail-smtp-in.l.google.com")
READINESS_SMTP_HOST = os.environ.get("KADENVERIFY_READINESS_SMTP_HOST", "gmail-smtp-in.l.google.com")
READINESS_SMTP_PORT = int(os.environ.get("KADENVERIFY_READINESS_SMTP_PORT", "25"))
READINESS_TIMEOUT_SECONDS = float(os.environ.get("KADENVERIFY_READINESS_TIMEOUT_SECONDS", "3.0"))

app = FastAPI(
    title="KadenVerify",
    description="Self-hosted email verification API",
    version="0.1.0",
)


class MetricsRegistry:
    """In-memory operational metrics snapshot for the API process."""

    def __init__(self, max_samples: int = 2000):
        self._lock = threading.Lock()
        self._max_samples = max_samples
        self.request_count = 0
        self.status_counts: dict[int, int] = {}
        self.endpoint_counts: dict[str, int] = {}
        self.endpoint_latencies_ms: dict[str, list[float]] = {}
        self.tier_counts: dict[int, int] = {}
        self.tier_latencies_ms: dict[int, list[float]] = {}
        self.cache_hits = 0
        self.cache_misses = 0
        self.rate_limited_429 = 0
        self.smtp_failure_reasons: dict[str, int] = {}
        self.enrichment_spend_total_usd = 0.0
        self.enrichment_events = 0

    def _push_latency(self, bucket: dict, key, value: float) -> None:
        sample = bucket.setdefault(key, [])
        sample.append(value)
        if len(sample) > self._max_samples:
            sample.pop(0)

    def record_http(self, endpoint: str, status_code: int, latency_ms: float) -> None:
        with self._lock:
            self.request_count += 1
            self.status_counts[status_code] = self.status_counts.get(status_code, 0) + 1
            self.endpoint_counts[endpoint] = self.endpoint_counts.get(endpoint, 0) + 1
            self._push_latency(self.endpoint_latencies_ms, endpoint, latency_ms)

    def record_tier_latency(self, tier: int, latency_ms: float) -> None:
        with self._lock:
            self.tier_counts[tier] = self.tier_counts.get(tier, 0) + 1
            self._push_latency(self.tier_latencies_ms, tier, latency_ms)

    def record_cache_lookup(self, hit: bool) -> None:
        with self._lock:
            if hit:
                self.cache_hits += 1
            else:
                self.cache_misses += 1

    def record_rate_limit_429(self) -> None:
        with self._lock:
            self.rate_limited_429 += 1

    def record_smtp_failure_reason(self, reason: Optional[str]) -> None:
        if not reason:
            return
        with self._lock:
            self.smtp_failure_reasons[reason] = self.smtp_failure_reasons.get(reason, 0) + 1

    def record_enrichment_spend(self, cost: float) -> None:
        if cost <= 0:
            return
        with self._lock:
            self.enrichment_spend_total_usd += cost
            self.enrichment_events += 1

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = int((len(ordered) - 1) * percentile)
        return round(ordered[index], 2)

    def snapshot(self) -> dict:
        with self._lock:
            cache_total = self.cache_hits + self.cache_misses
            cache_hit_rate = (self.cache_hits / cache_total) if cache_total else 0.0
            tier_latency = {
                str(tier): {
                    "count": self.tier_counts.get(tier, 0),
                    "p50": self._percentile(latencies, 0.50),
                    "p95": self._percentile(latencies, 0.95),
                }
                for tier, latencies in self.tier_latencies_ms.items()
            }

            endpoint_latency = {
                endpoint: {
                    "count": self.endpoint_counts.get(endpoint, 0),
                    "p50": self._percentile(latencies, 0.50),
                    "p95": self._percentile(latencies, 0.95),
                }
                for endpoint, latencies in self.endpoint_latencies_ms.items()
            }

            return {
                "requests_total": self.request_count,
                "status_codes": {str(k): v for k, v in self.status_counts.items()},
                "endpoint_latency_ms": endpoint_latency,
                "tier_distribution": {str(k): v for k, v in self.tier_counts.items()},
                "tier_latency_ms": tier_latency,
                "cache": {
                    "hits": self.cache_hits,
                    "misses": self.cache_misses,
                    "hit_rate": round(cache_hit_rate, 4),
                },
                "smtp_failure_reasons": dict(self.smtp_failure_reasons),
                "rate_limited_429": self.rate_limited_429,
                "enrichment": {
                    "events": self.enrichment_events,
                    "total_spend_usd": round(self.enrichment_spend_total_usd, 6),
                },
            }


_METRICS = MetricsRegistry()


async def _tiered_event_callback(event: dict) -> None:
    event_type = event.get("type")
    if event_type == "cache_lookup":
        _METRICS.record_cache_lookup(bool(event.get("hit")))
        return
    if event_type == "verification_result":
        _METRICS.record_enrichment_spend(float(event.get("enrichment_cost") or 0.0))
        smtp_code = int(event.get("smtp_code") or 0)
        reason = str(event.get("reason") or "")
        error = str(event.get("error") or "")
        reachability = str(event.get("reachability") or "")
        failure_reason = None
        if smtp_code == 0 and reachability == Reachability.unknown.value:
            failure_reason = "smtp_connection_or_timeout"
        elif 400 <= smtp_code < 500:
            failure_reason = "smtp_4xx_temp_failure"
        elif 500 <= smtp_code < 600:
            failure_reason = "smtp_5xx_rejection"
        elif error.startswith("syntax:"):
            failure_reason = "syntax_invalid"
        elif "no MX" in error:
            failure_reason = "dns_no_mx"
        elif reason == "role_account_filtered":
            failure_reason = "role_account_filtered"
        _METRICS.record_smtp_failure_reason(failure_reason)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        latency_ms = (time.perf_counter() - started) * 1000
        _METRICS.record_http(request.url.path, 500, latency_ms)
        raise

    latency_ms = (time.perf_counter() - started) * 1000
    _METRICS.record_http(request.url.path, response.status_code, latency_ms)
    return response


async def _run_tiered_verification(email: str) -> tuple[VerificationResult, int, str, float]:
    started = time.perf_counter()
    result, tier, reason = await verify_email_tiered(
        email=email,
        cache_lookup_fn=_cache_lookup,
        cache_update_fn=_cache_update,
        event_callback=_tiered_event_callback,
        helo_domain=HELO_DOMAIN,
        from_address=FROM_ADDRESS,
    )
    latency_ms = (time.perf_counter() - started) * 1000
    _METRICS.record_tier_latency(tier, latency_ms)
    return result, tier, reason, latency_ms

# --- DuckDB Cache for Tiered Verification ---

_cache_db = None
_cache_db_lock = threading.Lock()
_cache_redis = None
_supabase_client = None
_supabase_client_lock = threading.Lock()


def _get_supabase_client():
    """Create (lazily) a Supabase PostgREST client for verified email storage."""
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return None
    with _supabase_client_lock:
        if _supabase_client is not None:
            return _supabase_client
        from store.supabase_io import SupabaseRestClient

        _supabase_client = SupabaseRestClient(
            SUPABASE_URL,
            SUPABASE_SERVICE_ROLE_KEY,
            table=SUPABASE_TABLE,
            timeout_seconds=SUPABASE_TIMEOUT_SECONDS,
        )
        return _supabase_client

def _get_cache_db():
    """Get or create DuckDB connection for cache."""
    global _cache_db
    if _cache_db is None:
        try:
            import duckdb
            cache_path = Path(__file__).parent / "verified.duckdb"
            _cache_db = duckdb.connect(str(cache_path))

            # Ensure table exists
            _cache_db.execute("""
                CREATE TABLE IF NOT EXISTS verified_emails (
                    email VARCHAR PRIMARY KEY,
                    normalized VARCHAR,
                    reachability VARCHAR,
                    is_deliverable BOOLEAN,
                    is_catch_all BOOLEAN,
                    is_disposable BOOLEAN,
                    is_role BOOLEAN,
                    is_free BOOLEAN,
                    mx_host VARCHAR,
                    smtp_code INTEGER,
                    smtp_message VARCHAR,
                    provider VARCHAR,
                    domain VARCHAR,
                    verified_at TIMESTAMP,
                    error VARCHAR
                )
            """)
            logger.info(f"Cache DB connected: {cache_path}")
        except Exception as e:
            logger.error(f"Failed to initialize cache DB: {e}")
            _cache_db = None
    return _cache_db


async def _get_cache_redis():
    global _cache_redis
    if _cache_redis is not None:
        return _cache_redis
    if not CACHE_REDIS_URL:
        return None
    try:
        import redis.asyncio as redis

        _cache_redis = redis.from_url(
            CACHE_REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
        )
        return _cache_redis
    except Exception as e:
        logger.error("Failed to initialize Redis cache backend: %s", e)
        return None


def _deserialize_result(data: dict) -> VerificationResult:
    payload = dict(data)
    provider = payload.get("provider")
    if isinstance(provider, str):
        payload["provider"] = provider
    reachability = payload.get("reachability")
    if isinstance(reachability, str):
        payload["reachability"] = reachability
    return VerificationResult.model_validate(payload)


async def _cache_lookup_duckdb(email: str) -> Optional[VerificationResult]:
    try:
        db = _get_cache_db()
        if not db:
            return None

        with _cache_db_lock:
            result = db.execute(
                "SELECT * FROM verified_emails WHERE email = ?",
                [email]
            ).fetchone()

        if not result:
            return None

        return VerificationResult(
            email=result[0],
            normalized=result[1],
            reachability=Reachability(result[2]),
            is_deliverable=result[3],
            is_catch_all=result[4],
            is_disposable=result[5],
            is_role=result[6],
            is_free=result[7],
            mx_host=result[8],
            smtp_code=result[9],
            smtp_message=result[10],
            provider=result[11],
            domain=result[12],
            verified_at=result[13],
            error=result[14],
        )
    except Exception as e:
        logger.error(f"Cache lookup error for {email}: {e}")
        return None


_cache_update_count = 0

async def _cache_update_duckdb(result: VerificationResult):
    """Update DuckDB cache with verification result (single-writer lock)."""
    global _cache_update_count
    try:
        db = _get_cache_db()
        if not db:
            return

        with _cache_db_lock:
            db.execute("""
                INSERT OR REPLACE INTO verified_emails VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                result.email,
                result.normalized,
                result.reachability.value,
                result.is_deliverable,
                result.is_catch_all,
                result.is_disposable,
                result.is_role,
                result.is_free,
                result.mx_host,
                result.smtp_code,
                result.smtp_message,
                result.provider.value,
                result.domain,
                result.verified_at,
                result.error,
            ])
            db.commit()

            _cache_update_count += 1
            if _cache_update_count % 100 == 0:
                db.execute("CHECKPOINT")
                logger.info(f"Checkpointed database after {_cache_update_count} updates")
    except Exception as e:
        logger.error(f"Cache update error for {result.email}: {e}")


async def _cache_lookup_redis(email: str) -> Optional[VerificationResult]:
    client = await _get_cache_redis()
    if client is None:
        return None
    try:
        key = f"kadenverify:cache:{email}"
        payload = await client.get(key)
        if not payload:
            return None
        data = json.loads(payload)
        return _deserialize_result(data)
    except Exception as e:
        logger.error("Redis cache lookup failed for %s: %s", email, e)
        return None


async def _cache_update_redis(result: VerificationResult) -> None:
    client = await _get_cache_redis()
    if client is None:
        return
    try:
        key = f"kadenverify:cache:{result.email}"
        payload = result.model_dump(mode="json")
        await client.set(key, json.dumps(payload), ex=CACHE_TTL_SECONDS)
    except Exception as e:
        logger.error("Redis cache update failed for %s: %s", result.email, e)


async def _cache_lookup(email: str) -> Optional[VerificationResult]:
    if CACHE_BACKEND == "supabase":
        client = _get_supabase_client()
        if client is None:
            return None
        try:
            return await asyncio.to_thread(client.get_by_email, email)
        except Exception as e:
            logger.error("Supabase cache lookup failed for %s: %s", email, e)
            return None
    if CACHE_BACKEND == "redis":
        result = await _cache_lookup_redis(email)
        if result is not None:
            return result
    return await _cache_lookup_duckdb(email)


async def _cache_update(result: VerificationResult):
    if CACHE_BACKEND == "supabase":
        client = _get_supabase_client()
        if client is None:
            return
        try:
            await asyncio.to_thread(client.upsert_result, result)
        except Exception as e:
            logger.error("Supabase cache update failed for %s: %s", result.email, e)
        return
    if CACHE_BACKEND == "redis":
        await _cache_update_redis(result)
        return
    await _cache_update_duckdb(result)


# --- Auth ---

async def verify_api_key(request: Request):
    """Verify API key from X-API-Key header."""
    if not API_KEY:
        return  # No auth configured
    key = request.headers.get("X-API-Key", "")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# --- Rate limiting ---

_rate_limit_store: dict[str, list[float]] = {}
RATE_LIMIT_WINDOW = int(os.environ.get("KADENVERIFY_RATE_LIMIT_WINDOW", "60"))
RATE_LIMIT_MAX = int(os.environ.get("KADENVERIFY_RATE_LIMIT_MAX", "100"))
RATE_LIMIT_BACKEND = os.environ.get("KADENVERIFY_RATE_LIMIT_BACKEND", "memory").lower()
RATE_LIMIT_REDIS_URL = os.environ.get("KADENVERIFY_REDIS_URL", "")
_rate_limit_redis = None


def _extract_presented_api_key(request: Request) -> str:
    """Read API key from compatibility headers for limiter identity."""
    key = (
        request.headers.get("X-API-Key", "")
        or request.headers.get("x-api-key", "")
    )
    if key:
        return key

    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]

    return ""


def _rate_limit_identity(request: Request) -> str:
    """Build a rate-limit identity from client IP and presented API key fingerprint."""
    client_ip = request.client.host if request.client else "unknown"
    raw_key = _extract_presented_api_key(request) or "anonymous"
    key_fingerprint = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:16]
    return f"{client_ip}:{key_fingerprint}"


def _prune_rate_limit_store(now: float) -> None:
    """Remove stale timestamps and drop empty identity buckets."""
    stale_ips: list[str] = []
    for ip, timestamps in _rate_limit_store.items():
        fresh = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
        if fresh:
            _rate_limit_store[ip] = fresh
        else:
            stale_ips.append(ip)

    for ip in stale_ips:
        _rate_limit_store.pop(ip, None)


async def _get_redis_rate_limiter_client():
    global _rate_limit_redis
    if _rate_limit_redis is not None:
        return _rate_limit_redis
    if not RATE_LIMIT_REDIS_URL:
        return None
    try:
        import redis.asyncio as redis

        _rate_limit_redis = redis.from_url(
            RATE_LIMIT_REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
        )
        return _rate_limit_redis
    except Exception as e:
        logger.error("Failed to initialize Redis rate limiter: %s", e)
        return None


async def _allow_request_memory(identity: str, now: float) -> bool:
    _prune_rate_limit_store(now)
    if identity not in _rate_limit_store:
        _rate_limit_store[identity] = []
    if len(_rate_limit_store[identity]) >= RATE_LIMIT_MAX:
        return False
    _rate_limit_store[identity].append(now)
    return True


async def _allow_request_redis(identity: str, now: float) -> bool:
    client = await _get_redis_rate_limiter_client()
    if client is None:
        return await _allow_request_memory(identity, now)

    window_bucket = int(now // RATE_LIMIT_WINDOW)
    key = f"kadenverify:rate_limit:{window_bucket}:{identity}"
    try:
        count = await client.incr(key)
        if count == 1:
            await client.expire(key, RATE_LIMIT_WINDOW + 2)
        return count <= RATE_LIMIT_MAX
    except Exception as e:
        logger.error("Redis rate limiter error, falling back to memory: %s", e)
        return await _allow_request_memory(identity, now)


async def check_rate_limit(request: Request):
    """Rate limiting keyed by API key + IP, with Redis or memory backend."""
    import time
    now = time.time()
    identity = _rate_limit_identity(request)

    if RATE_LIMIT_BACKEND == "redis":
        allow = await _allow_request_redis(identity, now)
    else:
        allow = await _allow_request_memory(identity, now)

    if not allow:
        _METRICS.record_rate_limit_429()
        raise HTTPException(status_code=429, detail="Rate limit exceeded")


# --- Auth (also accept Bearer token and x-api-key for OmniVerifier compat) ---

async def verify_api_key_compat(request: Request):
    """Verify API key from X-API-Key, x-api-key, or Authorization: Bearer header."""
    if not API_KEY:
        return
    key = (
        request.headers.get("X-API-Key", "")
        or request.headers.get("x-api-key", "")
    )
    if not key:
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            key = auth[7:]
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# --- Request models ---

class SingleVerifyRequest(BaseModel):
    email: str

class BatchRequest(BaseModel):
    emails: list[str]


# --- Endpoints ---

@app.get("/verify", dependencies=[Depends(verify_api_key), Depends(check_rate_limit)])
async def verify_single(email: str = Query(..., description="Email address to verify")):
    """Verify a single email address.

    Returns OmniVerifier-compatible response with status: valid|invalid|catch_all|unknown.
    Uses tiered verification for OmniVerifier-level speed (<50ms for cached results).
    """
    if ENABLE_TIERED:
        result, tier, reason, _latency_ms = await _run_tiered_verification(email)
        response = result.to_omniverifier()
        response["_kadenverify_tier"] = tier
        response["_kadenverify_reason"] = reason
        return response
    else:
        result = await verify_email(
            email=email,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
        return result.to_omniverifier()


@app.post("/verify/batch", dependencies=[Depends(verify_api_key), Depends(check_rate_limit)])
async def verify_batch_endpoint(request: BatchRequest):
    """Verify a batch of email addresses (max 1000).

    Returns list of OmniVerifier-compatible results.
    """
    if len(request.emails) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Batch size exceeds maximum of {MAX_BATCH_SIZE}",
        )

    if not request.emails:
        return []

    results = await verify_batch(
        emails=request.emails,
        concurrency=CONCURRENCY,
        helo_domain=HELO_DOMAIN,
        from_address=FROM_ADDRESS,
    )
    for result in results:
        if result.reachability == Reachability.safe:
            continue
        if result.smtp_code == 0 and result.reachability == Reachability.unknown:
            _METRICS.record_smtp_failure_reason("smtp_connection_or_timeout")
        elif 400 <= result.smtp_code < 500:
            _METRICS.record_smtp_failure_reason("smtp_4xx_temp_failure")
        elif 500 <= result.smtp_code < 600:
            _METRICS.record_smtp_failure_reason("smtp_5xx_rejection")
        elif result.error and result.error.startswith("syntax:"):
            _METRICS.record_smtp_failure_reason("syntax_invalid")
        elif result.error and "no MX" in result.error:
            _METRICS.record_smtp_failure_reason("dns_no_mx")
    return [r.to_omniverifier() for r in results]


@app.post("/verify", dependencies=[Depends(verify_api_key), Depends(check_rate_limit)])
async def verify_single_post(request: SingleVerifyRequest):
    """Verify a single email (POST with JSON body).

    Uses tiered verification for OmniVerifier-level speed.
    """
    if ENABLE_TIERED:
        result, tier, reason, _latency_ms = await _run_tiered_verification(request.email)
        response = result.to_omniverifier()
        response["_kadenverify_tier"] = tier
        response["_kadenverify_reason"] = reason
        return response
    else:
        result = await verify_email(
            email=request.email,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
        return result.to_omniverifier()


# --- OmniVerifier-compatible routes ---
# NOTE: /v1/validate/credits MUST be registered before /v1/validate/{email}

@app.get("/v1/validate/credits", dependencies=[Depends(verify_api_key_compat)])
async def omni_credits():
    """OmniVerifier-compatible credits endpoint (always returns unlimited)."""
    return {"credits": 999999, "remaining": 999999}


@app.get("/v1/validate/{email}", dependencies=[Depends(verify_api_key_compat), Depends(check_rate_limit)])
async def omni_validate_get(email: str):
    """OmniVerifier-compatible GET endpoint (investor-outreach-platform).

    Uses tiered verification for instant results (<50ms for cached).
    """
    if ENABLE_TIERED:
        result, tier, reason, _latency_ms = await _run_tiered_verification(email)
        return result.to_omniverifier()
    else:
        result = await verify_email(
            email=email,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
        return result.to_omniverifier()


@app.post("/v1/verify", dependencies=[Depends(verify_api_key_compat), Depends(check_rate_limit)])
async def omni_verify_post(request: SingleVerifyRequest):
    """OmniVerifier-compatible POST endpoint (kadenwood-ui).

    Uses tiered verification for instant results (<50ms for cached).
    """
    if ENABLE_TIERED:
        result, tier, reason, _latency_ms = await _run_tiered_verification(request.email)
        return result.to_omniverifier()
    else:
        result = await verify_email(
            email=request.email,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
        return result.to_omniverifier()


async def _readiness_check_cache() -> dict:
    if CACHE_BACKEND == "supabase":
        client = _get_supabase_client()
        if client is None:
            return {"ok": False, "detail": "supabase not configured"}
        try:
            await asyncio.to_thread(client.get_by_email, "readiness-check@example.com")
            return {"ok": True, "detail": "supabase query ok"}
        except Exception as e:
            return {"ok": False, "detail": f"supabase error: {e}"}

    if CACHE_BACKEND == "redis":
        client = await _get_cache_redis()
        if client is None:
            return {"ok": False, "detail": "redis client unavailable"}
        try:
            pong = await client.ping()
            return {"ok": bool(pong), "detail": "redis ping ok" if pong else "redis ping failed"}
        except Exception as e:
            return {"ok": False, "detail": f"redis error: {e}"}

    db = _get_cache_db()
    if db is None:
        return {"ok": False, "detail": "duckdb unavailable"}
    try:
        with _cache_db_lock:
            db.execute("SELECT 1").fetchone()
        return {"ok": True, "detail": "duckdb query ok"}
    except Exception as e:
        return {"ok": False, "detail": f"duckdb error: {e}"}


async def _readiness_check_dns() -> dict:
    loop = asyncio.get_running_loop()
    try:
        await asyncio.wait_for(
            loop.getaddrinfo(READINESS_DNS_HOST, None, type=socket.SOCK_STREAM),
            timeout=READINESS_TIMEOUT_SECONDS,
        )
        return {"ok": True, "detail": f"resolved {READINESS_DNS_HOST}"}
    except Exception as e:
        return {"ok": False, "detail": f"dns resolution failed: {e}"}


async def _readiness_check_smtp() -> dict:
    writer = None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(READINESS_SMTP_HOST, READINESS_SMTP_PORT),
            timeout=READINESS_TIMEOUT_SECONDS,
        )
        return {"ok": True, "detail": f"connected to {READINESS_SMTP_HOST}:{READINESS_SMTP_PORT}"}
    except Exception as e:
        return {"ok": False, "detail": f"smtp connectivity failed: {e}"}
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


@app.get("/ready")
async def ready():
    """Readiness check with dependency-level validation."""
    cache_check, dns_check, smtp_check = await asyncio.gather(
        _readiness_check_cache(),
        _readiness_check_dns(),
        _readiness_check_smtp(),
    )
    checks = {
        "cache": cache_check,
        "dns": dns_check,
        "smtp_outbound": smtp_check,
    }
    is_ready = all(c["ok"] for c in checks.values())
    return {
        "status": "ready" if is_ready else "degraded",
        "checks": checks,
        "cache_backend": CACHE_BACKEND,
    }


@app.get("/metrics", dependencies=[Depends(verify_api_key)])
async def metrics_endpoint():
    """Operational metrics snapshot for latency, tiers, cache, and failures."""
    return _METRICS.snapshot()


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "kadenverify",
        "version": "0.1.0",
    }


@app.get("/stats", dependencies=[Depends(verify_api_key)])
async def stats_endpoint():
    """Get verification statistics from the verified email store."""
    try:
        if CACHE_BACKEND == "supabase":
            client = _get_supabase_client()
            if client is None:
                return {"error": "supabase not configured", "total": 0}
            return await asyncio.to_thread(client.get_stats)

        from store.duckdb_io import init_verified_db, get_stats
        conn = init_verified_db()
        s = get_stats(conn)
        conn.close()
        return s
    except Exception as e:
        return {"error": str(e), "total": 0}
