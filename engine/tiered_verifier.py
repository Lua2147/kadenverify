"""Tiered email verification with enrichment (Tier 4-5).

Tier 1: Cached results (instant, <50ms)
Tier 2: Fast validation (100-500ms) 
Tier 3: SMTP verification (2-5s)
Tier 4-5: Enrichment for unknowns (1-3s, $0-0.10)
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .models import DnsInfo, Provider, Reachability, VerificationResult
from .syntax import validate_syntax
from .metadata import classify as classify_metadata
from .dns import lookup_mx
from .verifier import verify_email as full_verify_email

logger = logging.getLogger("kadenverify.tiered")

# Cache TTL settings
CACHE_TTL_DAYS = 30
FAST_TIER_CONFIDENCE = 0.85

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
    ENRICHMENT_ENABLED = False
    EXA_API_KEY = None
    APOLLO_API_KEY = None


async def verify_email_tiered(
    email: str,
    cache_lookup_fn=None,
    cache_update_fn=None,
    force_tier: Optional[int] = None,
    helo_domain: str = "verify.kadenwood.com",
    from_address: str = "verify@kadenwood.com",
) -> tuple[VerificationResult, int, str]:
    """Verify email using tiered approach with enrichment."""
    email = email.strip().lower()

    # Tier 1: Cached Results
    if force_tier != 2 and force_tier != 3 and cache_lookup_fn:
        cached = await _tier1_cached(email, cache_lookup_fn)
        if cached:
            return cached, 1, "cached_result"

    # Tier 2: Fast Validation
    if force_tier != 3:
        fast_result = await _tier2_fast(email)
        if fast_result:
            result, confidence = fast_result
            if confidence >= FAST_TIER_CONFIDENCE or force_tier == 2:
                if cache_update_fn:
                    asyncio.create_task(_tier3_background(email, helo_domain, from_address, cache_update_fn))
                return result, 2, f"fast_validation_confidence_{confidence:.2f}"

    # Tier 3: Full SMTP Verification
    result = await full_verify_email(email, helo_domain, from_address)

    # Filter out role accounts completely
    if result.is_role:
        result.reachability = Reachability.invalid
        result.is_deliverable = False
        result.error = "role account filtered"
        if cache_update_fn:
            cache_update_fn(result)
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
                    cache_update_fn(result)
                except Exception as e:
                    logger.error(f"Cache update failed: {e}")

            tier_num = 4 if 'tier4' in reason else 5
            return result, tier_num, reason

        except Exception as e:
            logger.error(f"Enrichment failed for {email}: {e}")

    # Update cache with SMTP result
    if cache_update_fn:
        try:
            cache_update_fn(result)
        except Exception as e:
            logger.error(f"Cache update failed: {e}")

    return result, 3, "full_smtp_verification"


async def _tier1_cached(email: str, cache_lookup_fn) -> Optional[VerificationResult]:
    """Tier 1: Return cached result if fresh."""
    try:
        cached = cache_lookup_fn(email)
        if not cached:
            return None

        age = datetime.now(timezone.utc) - cached.verified_at
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
    """Infer likely reachability without SMTP check."""
    if meta["is_disposable"]:
        return Reachability.risky
    if meta["is_role"]:
        return Reachability.risky
    if dns_info.provider in [Provider.gmail, Provider.google_workspace]:
        return Reachability.safe
    if dns_info.provider == Provider.microsoft365:
        return Reachability.safe
    if meta["is_free"]:
        return Reachability.safe

    return Reachability.unknown


async def _tier3_background(email: str, helo_domain: str, from_address: str, cache_update_fn):
    """Background SMTP verification after fast tier."""
    try:
        logger.info(f"Background SMTP verification for {email}")
        result = await full_verify_email(email, helo_domain, from_address)
        cache_update_fn(result)
        logger.info(f"Background verification complete: {email}")
    except Exception as e:
        logger.error(f"Background verification failed for {email}: {e}")
