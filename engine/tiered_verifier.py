"""Tiered email verification with enrichment (Tier 4-5).

Tier 1: Cached results (instant, <50ms)
Tier 2: Fast validation (100-500ms) 
Tier 3: SMTP verification (2-5s)
Tier 4-5: Enrichment for unknowns (1-3s, $0-0.10)
"""

import asyncio
import inspect
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .models import DnsInfo, Provider, Reachability, VerificationResult
from .syntax import validate_syntax
from .metadata import classify as classify_metadata
from .dns import lookup_mx
from .verifier import verify_email as full_verify_email

logger = logging.getLogger("kadenverify.tiered")

# Load enrichment config
try:
    config_path = Path(__file__).parent.parent / "config.json"
    with open(config_path) as f:
        CONFIG = json.load(f)
        ENRICHMENT_ENABLED = CONFIG.get("enrichment", {}).get("enabled", False)
        EXA_API_KEY = CONFIG.get("enrichment", {}).get("exa_api_key")
        APOLLO_API_KEY = CONFIG.get("enrichment", {}).get("apollo_api_key")
except Exception as e:
    logger.warning(f"Could not load enrichment config: {e}")
    CONFIG = {}
    ENRICHMENT_ENABLED = False
    EXA_API_KEY = None
    APOLLO_API_KEY = None

# Configurable heuristics and queue settings
_tiered_config = CONFIG.get("tiered_verification", {})
if not isinstance(_tiered_config, dict):
    _tiered_config = {}

CACHE_TTL_DAYS = int(os.environ.get("KADENVERIFY_CACHE_TTL_DAYS", "30"))
FAST_TIER_CONFIDENCE = float(
    os.environ.get(
        "KADENVERIFY_FAST_TIER_CONFIDENCE",
        str(
            _tiered_config.get(
                "fast_tier_confidence_threshold",
                CONFIG.get("tiered_threshold", 0.85),
            )
        ),
    )
)
FILTER_ROLE_ACCOUNTS = os.environ.get(
    "KADENVERIFY_FILTER_ROLE_ACCOUNTS",
    str(_tiered_config.get("filter_role_accounts", True)),
).lower() == "true"
TIER3_BG_QUEUE_SIZE = int(os.environ.get("KADENVERIFY_TIER3_BG_QUEUE_SIZE", "500"))
TIER3_BG_WORKERS = int(os.environ.get("KADENVERIFY_TIER3_BG_WORKERS", "8"))


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


async def _cache_lookup(cache_lookup_fn, email: str) -> Optional[VerificationResult]:
    if not cache_lookup_fn:
        return None
    return await _maybe_await(cache_lookup_fn(email))


async def _cache_update(cache_update_fn, result: VerificationResult) -> None:
    if not cache_update_fn:
        return
    await _maybe_await(cache_update_fn(result))


async def _emit_event(event_callback, event: dict) -> None:
    if not event_callback:
        return
    try:
        await _maybe_await(event_callback(event))
    except Exception as e:
        logger.debug("Event callback failed: %s", e)


class Tier3BackgroundScheduler:
    """Bounded background queue for Tier-3 SMTP backfill work."""

    def __init__(self, max_queue_size: int, workers: int):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._workers = max(0, workers)
        self._started = False
        self._worker_tasks: list[asyncio.Task] = []
        self._start_lock = asyncio.Lock()

    async def _ensure_started(self) -> None:
        if self._started or self._workers == 0:
            return
        async with self._start_lock:
            if self._started:
                return
            for idx in range(self._workers):
                task = asyncio.create_task(self._worker_loop(idx))
                self._worker_tasks.append(task)
            self._started = True

    async def enqueue(self, item: tuple[str, str, str, object]) -> bool:
        await self._ensure_started()
        try:
            self._queue.put_nowait(item)
            return True
        except asyncio.QueueFull:
            return False

    def queue_size(self) -> int:
        return self._queue.qsize()

    async def _worker_loop(self, worker_id: int) -> None:
        while True:
            email, helo_domain, from_address, cache_update_fn = await self._queue.get()
            try:
                await _tier3_background(email, helo_domain, from_address, cache_update_fn)
            except Exception as e:
                logger.error("Tier-3 background worker %s failed for %s: %s", worker_id, email, e)
            finally:
                self._queue.task_done()


_tier3_scheduler = Tier3BackgroundScheduler(
    max_queue_size=TIER3_BG_QUEUE_SIZE,
    workers=TIER3_BG_WORKERS,
)


async def verify_email_tiered(
    email: str,
    cache_lookup_fn=None,
    cache_update_fn=None,
    event_callback=None,
    force_tier: Optional[int] = None,
    helo_domain: str = "verify.kadenwood.com",
    from_address: str = "verify@kadenwood.com",
) -> tuple[VerificationResult, int, str]:
    """Verify email using tiered approach with enrichment."""
    email = email.strip().lower()

    # Normalize email for cache key (Gmail dot/plus stripping).
    # All cache operations and downstream verification use the normalized form
    # so that john.doe@gmail.com and johndoe@gmail.com share one cache entry.
    syntax_pre = validate_syntax(email)
    if syntax_pre.is_valid:
        email = syntax_pre.normalized

    # Tier 1: Cached Results
    if force_tier != 2 and force_tier != 3 and cache_lookup_fn:
        cached = await _tier1_cached(email, cache_lookup_fn)
        if cached:
            await _emit_event(event_callback, {"type": "cache_lookup", "hit": True})
            await _emit_event(
                event_callback,
                {
                    "type": "verification_result",
                    "tier": 1,
                    "reason": "cached_result",
                    "enrichment_cost": 0.0,
                    "reachability": cached.reachability.value,
                    "smtp_code": cached.smtp_code,
                    "error": cached.error,
                },
            )
            return cached, 1, "cached_result"
        await _emit_event(event_callback, {"type": "cache_lookup", "hit": False})

    # Tier 2: Fast Validation
    if force_tier != 3:
        fast_result = await _tier2_fast(email)
        if fast_result:
            result, confidence = fast_result
            if confidence >= FAST_TIER_CONFIDENCE or force_tier == 2:
                if cache_update_fn:
                    enqueued = await _tier3_scheduler.enqueue(
                        (email, helo_domain, from_address, cache_update_fn)
                    )
                    if not enqueued:
                        logger.warning(
                            "Tier-3 background queue full (%s). Skipping async backfill for %s",
                            _tier3_scheduler.queue_size(),
                            email,
                        )
                await _emit_event(
                    event_callback,
                    {
                        "type": "verification_result",
                        "tier": 2,
                        "reason": f"fast_validation_confidence_{confidence:.2f}",
                        "enrichment_cost": 0.0,
                        "reachability": result.reachability.value,
                        "smtp_code": result.smtp_code,
                        "error": result.error,
                    },
                )
                return result, 2, f"fast_validation_confidence_{confidence:.2f}"

    # Tier 3: Full SMTP Verification
    result = await full_verify_email(email, helo_domain, from_address)

    # Filter out role accounts completely
    if FILTER_ROLE_ACCOUNTS and result.is_role:
        result.reachability = Reachability.invalid
        result.is_deliverable = False
        result.error = "role account filtered"
        if cache_update_fn:
            await _cache_update(cache_update_fn, result)
        await _emit_event(
            event_callback,
            {
                "type": "verification_result",
                "tier": 3,
                "reason": "role_account_filtered",
                "enrichment_cost": 0.0,
                "reachability": result.reachability.value,
                "smtp_code": result.smtp_code,
                "error": result.error,
            },
        )
        return result, 3, "role_account_filtered"

    # Tier 4-5: Enrichment for unknowns AND catch-all domains
    needs_enrichment = (
        (result.reachability == Reachability.unknown or result.is_catch_all)
        and ENRICHMENT_ENABLED
    )

    if needs_enrichment:
        try:
            from .enrichment import enrich_unknown

            status, confidence, reason, cost = await enrich_unknown(
                email=email,
                exa_api_key=EXA_API_KEY,
                apollo_api_key=APOLLO_API_KEY,
                is_catchall=result.is_catch_all or False,
            )

            # Update result
            if status == 'valid':
                result.reachability = Reachability.safe
                result.is_deliverable = True
            elif status == 'risky':
                result.reachability = Reachability.risky
                result.is_deliverable = False

            result.error = reason

            logger.info(f"Enriched {email}: {status} (tier: {reason}, cost: ${cost:.4f})")

            # Update cache with enriched result
            if cache_update_fn:
                try:
                    await _cache_update(cache_update_fn, result)
                except Exception as e:
                    logger.error(f"Cache update failed: {e}")

            tier_num = 4 if 'tier4' in reason else 5
            await _emit_event(
                event_callback,
                {
                    "type": "verification_result",
                    "tier": tier_num,
                    "reason": reason,
                    "enrichment_cost": cost,
                    "reachability": result.reachability.value,
                    "smtp_code": result.smtp_code,
                    "error": result.error,
                },
            )
            return result, tier_num, reason

        except Exception as e:
            logger.error(f"Enrichment failed for {email}: {e}")

    # Update cache with SMTP result
    if cache_update_fn:
        try:
            await _cache_update(cache_update_fn, result)
        except Exception as e:
            logger.error(f"Cache update failed: {e}")

    await _emit_event(
        event_callback,
        {
            "type": "verification_result",
            "tier": 3,
            "reason": "full_smtp_verification",
            "enrichment_cost": 0.0,
            "reachability": result.reachability.value,
            "smtp_code": result.smtp_code,
            "error": result.error,
        },
    )

    return result, 3, "full_smtp_verification"


async def _tier1_cached(email: str, cache_lookup_fn) -> Optional[VerificationResult]:
    """Tier 1: Return cached result if fresh."""
    try:
        cached = await _cache_lookup(cache_lookup_fn, email)
        if not cached:
            return None

        verified_at = cached.verified_at
        if verified_at.tzinfo is None:
            # DuckDB TIMESTAMP values are commonly naive UTC datetimes.
            verified_at = verified_at.replace(tzinfo=timezone.utc)
        else:
            verified_at = verified_at.astimezone(timezone.utc)

        age = datetime.now(timezone.utc) - verified_at
        if age > timedelta(days=CACHE_TTL_DAYS):
            logger.debug(f"Cache expired for {email}")
            return None

        logger.debug(f"Cache hit for {email}")
        return cached

    except Exception as e:
        logger.error(f"Cache lookup error: {e}")
        return None


async def _tier2_fast(email: str) -> Optional[tuple[VerificationResult, float]]:
    """Tier 2: Fast validation using syntax + DNS + metadata."""
    syntax = validate_syntax(email)
    if not syntax.is_valid:
        result = VerificationResult(
            email=email,
            normalized=email,
            reachability=Reachability.invalid,
            is_deliverable=False,
            error=f"syntax: {syntax.reason}",
        )
        return result, 1.0

    domain = syntax.domain
    local_part = syntax.local_part
    normalized = syntax.normalized

    meta = classify_metadata(local_part, domain)
    dns_info = await lookup_mx(domain)

    if not dns_info.has_mx:
        result = VerificationResult(
            email=email,
            normalized=normalized,
            reachability=Reachability.invalid,
            is_deliverable=False,
            is_disposable=meta["is_disposable"],
            is_role=meta["is_role"],
            is_free=meta["is_free"],
            provider=dns_info.provider,
            domain=domain,
            error="no MX or A records found",
        )
        return result, 1.0

    confidence = _compute_fast_tier_confidence(meta, dns_info)

    if confidence >= FAST_TIER_CONFIDENCE:
        reachability = _infer_reachability(meta, dns_info)
        is_deliverable = reachability == Reachability.safe

        result = VerificationResult(
            email=email,
            normalized=normalized,
            reachability=reachability,
            is_deliverable=is_deliverable,
            is_disposable=meta["is_disposable"],
            is_role=meta["is_role"],
            is_free=meta["is_free"],
            mx_host=dns_info.mx_hosts[0] if dns_info.mx_hosts else "",
            provider=dns_info.provider,
            domain=domain,
            smtp_code=0,
            error="fast_tier_probabilistic",
        )
        return result, confidence

    return None


def _compute_fast_tier_confidence(meta: dict, dns_info: DnsInfo) -> float:
    """Compute confidence score for fast tier validation."""
    confidence = 0.5

    if dns_info.provider in [Provider.gmail, Provider.google_workspace]:
        confidence += 0.3
    if dns_info.provider == Provider.microsoft365:
        confidence += 0.2
    if meta["is_free"]:
        confidence += 0.1
    if not meta["is_disposable"] and not meta["is_role"]:
        confidence += 0.1
    if meta["is_disposable"]:
        confidence -= 0.2
    if dns_info.provider == Provider.generic:
        confidence -= 0.1

    return max(0.0, min(1.0, confidence))


def _infer_reachability(meta: dict, dns_info: DnsInfo) -> Reachability:
    """Infer likely reachability without SMTP check.

    Returns risky (not safe) for known providers because Tier 2 has no SMTP
    confirmation. The Tier 3 background backfill will upgrade to safe after
    SMTP verification succeeds.
    """
    if meta["is_disposable"]:
        return Reachability.risky
    if meta["is_role"]:
        return Reachability.risky
    if dns_info.provider in [Provider.gmail, Provider.google_workspace]:
        return Reachability.risky
    if dns_info.provider == Provider.microsoft365:
        return Reachability.risky
    if meta["is_free"]:
        return Reachability.risky

    return Reachability.unknown


async def _tier3_background(email: str, helo_domain: str, from_address: str, cache_update_fn):
    """Background SMTP verification after fast tier."""
    try:
        logger.info(f"Background SMTP verification for {email}")
        result = await full_verify_email(email, helo_domain, from_address)
        await _cache_update(cache_update_fn, result)
        logger.info(f"Background verification complete: {email}")
    except Exception as e:
        logger.error(f"Background verification failed for {email}: {e}")
