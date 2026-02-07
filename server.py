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
import logging
import os
import sys
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

app = FastAPI(
    title="KadenVerify",
    description="Self-hosted email verification API",
    version="0.1.0",
)

# --- DuckDB Cache for Tiered Verification ---

_cache_db = None

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


def _cache_lookup(email: str) -> Optional[VerificationResult]:
    """Look up email in cache."""
    try:
        db = _get_cache_db()
        if not db:
            return None

        result = db.execute(
            "SELECT * FROM verified_emails WHERE email = ?",
            [email]
        ).fetchone()

        if not result:
            return None

        # Convert to VerificationResult
        from datetime import datetime
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

def _cache_update(result: VerificationResult):
    """Update cache with verification result."""
    global _cache_update_count
    try:
        db = _get_cache_db()
        if not db:
            return

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
        # Explicitly commit the transaction
        db.commit()

        # Checkpoint WAL every 100 updates to keep WAL file size manageable
        _cache_update_count += 1
        if _cache_update_count % 100 == 0:
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            logger.info(f"Checkpointed WAL after {_cache_update_count} updates")
    except Exception as e:
        logger.error(f"Cache update error for {result.email}: {e}")


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
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 100  # requests per window


async def check_rate_limit(request: Request):
    """Simple in-memory rate limiter."""
    client_ip = request.client.host if request.client else "unknown"
    import time
    now = time.time()

    if client_ip not in _rate_limit_store:
        _rate_limit_store[client_ip] = []

    # Clean old entries
    _rate_limit_store[client_ip] = [
        t for t in _rate_limit_store[client_ip]
        if now - t < RATE_LIMIT_WINDOW
    ]

    if len(_rate_limit_store[client_ip]) >= RATE_LIMIT_MAX:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    _rate_limit_store[client_ip].append(now)


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
        result, tier, reason = await verify_email_tiered(
            email=email,
            cache_lookup_fn=_cache_lookup,
            cache_update_fn=_cache_update,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
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
    return [r.to_omniverifier() for r in results]


@app.post("/verify", dependencies=[Depends(verify_api_key), Depends(check_rate_limit)])
async def verify_single_post(request: SingleVerifyRequest):
    """Verify a single email (POST with JSON body).

    Uses tiered verification for OmniVerifier-level speed.
    """
    if ENABLE_TIERED:
        result, tier, reason = await verify_email_tiered(
            email=request.email,
            cache_lookup_fn=_cache_lookup,
            cache_update_fn=_cache_update,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
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
        result, tier, reason = await verify_email_tiered(
            email=email,
            cache_lookup_fn=_cache_lookup,
            cache_update_fn=_cache_update,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
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
        result, tier, reason = await verify_email_tiered(
            email=request.email,
            cache_lookup_fn=_cache_lookup,
            cache_update_fn=_cache_update,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
        return result.to_omniverifier()
    else:
        result = await verify_email(
            email=request.email,
            helo_domain=HELO_DOMAIN,
            from_address=FROM_ADDRESS,
        )
        return result.to_omniverifier()


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
    """Get verification statistics from verified.duckdb."""
    try:
        from store.duckdb_io import init_verified_db, get_stats
        conn = init_verified_db()
        s = get_stats(conn)
        conn.close()
        return s
    except Exception as e:
        return {"error": str(e), "total": 0}
