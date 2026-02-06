"""Tiered email verification for OmniVerifier-level speed.

Tier 1: Cached results (instant, <50ms) - Return verified.duckdb results
Tier 2: Fast validation (100-500ms) - Syntax + DNS + metadata scoring
Tier 3: SMTP verification (2-5s) - Full verification, update cache

This achieves OmniVerifier-level speed after initial verification pass.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from .models import DnsInfo, Provider, Reachability, VerificationResult
from .syntax import validate_syntax
from .metadata import classify as classify_metadata
from .dns import lookup_mx
from .verifier import verify_email as full_verify_email

logger = logging.getLogger("kadenverify.tiered")

# Cache TTL settings
CACHE_TTL_DAYS = 30  # Re-verify after 30 days
FAST_TIER_CONFIDENCE = 0.85  # 85% confidence threshold for fast tier


async def verify_email_tiered(
    email: str,
    cache_lookup_fn=None,
    cache_update_fn=None,
    force_tier: Optional[int] = None,
    helo_domain: str = "verify.kadenwood.com",
    from_address: str = "verify@kadenwood.com",
) -> tuple[VerificationResult, int, str]:
    """Verify email using tiered approach for speed.

    Args:
        email: Email address to verify
        cache_lookup_fn: Optional function(email) -> VerificationResult | None
        cache_update_fn: Optional function(result) -> None
        force_tier: Force specific tier (1, 2, or 3) - for testing
        helo_domain: EHLO domain
        from_address: MAIL FROM address

    Returns:
        Tuple of (result, tier_used, reason)
        - result: VerificationResult
        - tier_used: 1 (cached), 2 (fast), or 3 (smtp)
        - reason: Why this tier was used
    """
    email = email.strip().lower()

    # Tier 1: Cached Results (instant)
    if force_tier != 2 and force_tier != 3 and cache_lookup_fn:
        cached = await _tier1_cached(email, cache_lookup_fn)
        if cached:
            return cached, 1, "cached_result"

    # Tier 2: Fast Validation (DNS + metadata)
    if force_tier != 3:
        fast_result = await _tier2_fast(email)
        if fast_result:
            result, confidence = fast_result
            if confidence >= FAST_TIER_CONFIDENCE or force_tier == 2:
                # Queue for background SMTP verification
                if cache_update_fn:
                    asyncio.create_task(_tier3_background(email, helo_domain, from_address, cache_update_fn))
                return result, 2, f"fast_validation_confidence_{confidence:.2f}"

    # Tier 3: Full SMTP Verification
    result = await full_verify_email(email, helo_domain, from_address)

    # Update cache
    if cache_update_fn:
        try:
            cache_update_fn(result)
        except Exception as e:
            logger.error(f"Cache update failed: {e}")

    return result, 3, "full_smtp_verification"


async def _tier1_cached(
    email: str,
    cache_lookup_fn,
) -> Optional[VerificationResult]:
    """Tier 1: Return cached result if fresh.

    Returns:
        VerificationResult if cached and fresh, None otherwise
    """
    try:
        cached = cache_lookup_fn(email)
        if not cached:
            return None

        # Check if cache is fresh
        age = datetime.now(timezone.utc) - cached.verified_at
        if age > timedelta(days=CACHE_TTL_DAYS):
            logger.debug(f"Cache expired for {email} (age: {age.days} days)")
            return None

        logger.debug(f"Cache hit for {email} (age: {age.days} days)")
        return cached

    except Exception as e:
        logger.error(f"Cache lookup error: {e}")
        return None


async def _tier2_fast(
    email: str,
) -> Optional[tuple[VerificationResult, float]]:
    """Tier 2: Fast validation using syntax + DNS + metadata.

    Returns:
        Tuple of (VerificationResult, confidence_score) or None
        confidence_score: 0.0-1.0, higher = more confident
    """
    # Step 1: Syntax validation
    syntax = validate_syntax(email)
    if not syntax.is_valid:
        result = VerificationResult(
            email=email,
            normalized=email,
            reachability=Reachability.invalid,
            is_deliverable=False,
            error=f"syntax: {syntax.reason}",
        )
        return result, 1.0  # 100% confident - syntax is definitive

    domain = syntax.domain
    local_part = syntax.local_part
    normalized = syntax.normalized

    # Step 2: Metadata classification
    meta = classify_metadata(local_part, domain)

    # Step 3: DNS lookup
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
        return result, 1.0  # 100% confident - no MX is definitive

    # Step 4: Compute confidence score
    confidence = _compute_fast_tier_confidence(meta, dns_info)

    # Step 5: Return probabilistic result
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
            smtp_code=0,  # Fast tier doesn't do SMTP
            error="fast_tier_probabilistic",
        )
        return result, confidence

    return None  # Not confident enough, escalate to Tier 3


def _compute_fast_tier_confidence(meta: dict, dns_info: DnsInfo) -> float:
    """Compute confidence score for fast tier validation.

    Returns:
        Confidence score 0.0-1.0
    """
    confidence = 0.5  # Base confidence

    # Positive signals (increase confidence)
    if dns_info.provider in [Provider.gmail, Provider.google_workspace]:
        confidence += 0.3  # Google domains are reliable

    if dns_info.provider == Provider.microsoft365:
        confidence += 0.2  # Microsoft domains are reliable

    if meta["is_free"]:
        confidence += 0.1  # Free providers are well-known

    if not meta["is_disposable"] and not meta["is_role"]:
        confidence += 0.1  # Not risky = more confident

    # Negative signals (decrease confidence)
    if meta["is_disposable"]:
        confidence -= 0.2  # Disposable domains are sketchy

    if dns_info.provider == Provider.generic:
        confidence -= 0.1  # Unknown providers need SMTP verification

    # Clamp to 0.0-1.0
    return max(0.0, min(1.0, confidence))


def _infer_reachability(meta: dict, dns_info: DnsInfo) -> Reachability:
    """Infer likely reachability without SMTP check.

    Returns:
        Reachability enum (safe, risky, invalid, unknown)
    """
    # Disposable = risky
    if meta["is_disposable"]:
        return Reachability.risky

    # Role account = risky
    if meta["is_role"]:
        return Reachability.risky

    # Gmail/Google Workspace = likely safe
    if dns_info.provider in [Provider.gmail, Provider.google_workspace]:
        return Reachability.safe

    # Microsoft 365 = likely safe
    if dns_info.provider == Provider.microsoft365:
        return Reachability.safe

    # Yahoo = likely safe
    if dns_info.provider == Provider.yahoo:
        return Reachability.safe

    # Free provider = likely safe
    if meta["is_free"]:
        return Reachability.safe

    # Generic domain = unknown (needs SMTP)
    return Reachability.unknown


async def _tier3_background(
    email: str,
    helo_domain: str,
    from_address: str,
    cache_update_fn,
):
    """Background task to verify email via SMTP and update cache.

    This runs asynchronously after Tier 2 returns fast result.
    """
    try:
        logger.info(f"Background SMTP verification for {email}")
        result = await full_verify_email(email, helo_domain, from_address)
        cache_update_fn(result)
        logger.info(f"Background verification complete: {email} -> {result.reachability.value}")
    except Exception as e:
        logger.error(f"Background verification failed for {email}: {e}")
