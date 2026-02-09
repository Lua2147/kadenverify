"""Main email verification orchestrator.

Pipeline: syntax -> metadata -> DNS -> provider_route -> SMTP -> score
"""

import asyncio
import logging
from typing import Optional

from .models import (
    DnsInfo,
    Provider,
    Reachability,
    SmtpResponse,
    VerificationResult,
)
from .syntax import validate_syntax
from .metadata import classify as classify_metadata
from .dns import lookup_mx
from .providers import get_config
from .smtp import smtp_check, check_catch_all

logger = logging.getLogger("kadenverify.verifier")

# Default concurrency for batch operations
DEFAULT_CONCURRENCY = 5


def _score(
    smtp_result: Optional[SmtpResponse],
    is_catch_all: Optional[bool],
    is_disposable: bool,
    is_role: bool,
    provider: Provider,
    provider_mark_risky: bool,
) -> tuple[Reachability, Optional[bool]]:
    """Compute reachability score and deliverability from verification results.

    Returns (reachability, is_deliverable).

    Scoring rules:
    - safe: SMTP 250 + not catch-all + not disposable
    - risky: catch-all domain OR greylisted OR provider marked risky (Hotmail B2C)
    - invalid: syntax fail OR no MX OR SMTP 550 with invalid pattern
    - unknown: timeout OR connection refused OR blacklisted
    """
    # Provider auto-marked risky (e.g., Hotmail B2C)
    if provider_mark_risky:
        return Reachability.risky, None

    # No SMTP result means we couldn't check
    if smtp_result is None:
        return Reachability.unknown, None

    # Blacklisted -- we can't trust the result
    if smtp_result.is_blacklisted:
        return Reachability.unknown, None

    # Connection failure (code 0)
    if smtp_result.code == 0:
        return Reachability.unknown, None

    # Invalid mailbox (5xx with invalid pattern)
    if smtp_result.is_invalid:
        return Reachability.invalid, False

    # Disabled account
    if smtp_result.is_disabled:
        return Reachability.invalid, False

    # Greylisted -- temporary, can't be sure
    if smtp_result.is_greylisted:
        return Reachability.risky, None

    # Full inbox -- user exists but can't receive
    if smtp_result.is_full_inbox:
        return Reachability.risky, True

    # SMTP accepted (2xx)
    if 200 <= smtp_result.code < 300:
        # Catch-all domain -- accepted but might not be real
        if is_catch_all:
            return Reachability.risky, None

        # Disposable -- technically deliverable but risky
        if is_disposable:
            return Reachability.risky, True

        # Role account -- deliverable but risky for outreach
        if is_role:
            return Reachability.risky, True

        # Clean, verified, deliverable
        return Reachability.safe, True

    # 5xx without recognized pattern
    if 500 <= smtp_result.code < 600:
        return Reachability.invalid, False

    # 4xx not caught as greylist
    if 400 <= smtp_result.code < 500:
        return Reachability.risky, None

    return Reachability.unknown, None


async def verify_email(
    email: str,
    helo_domain: str = "verify.kadenwood.com",
    from_address: str = "verify@kadenwood.com",
    dns_cache: Optional[dict] = None,
    catch_all_cache: Optional[dict] = None,
) -> VerificationResult:
    """Verify a single email address through the full pipeline.

    Args:
        email: Email address to verify.
        helo_domain: Domain to use in EHLO command.
        from_address: Address to use in MAIL FROM command.
        dns_cache: Optional dict to cache DNS results by domain.
        catch_all_cache: Optional dict to cache catch-all results by domain.

    Returns:
        VerificationResult with all verification data.
    """
    # Step 1: Syntax validation
    syntax = validate_syntax(email)
    if not syntax.is_valid:
        return VerificationResult(
            email=email,
            normalized=email.strip().lower(),
            reachability=Reachability.invalid,
            is_deliverable=False,
            error=f"syntax: {syntax.reason}",
        )

    domain = syntax.domain
    local_part = syntax.local_part
    normalized = syntax.normalized

    # Step 2: Metadata classification
    meta = classify_metadata(local_part, domain)

    # Step 3: DNS lookup (with optional cache)
    if dns_cache is not None and domain in dns_cache:
        dns_info = dns_cache[domain]
    else:
        dns_info = await lookup_mx(domain)
        if dns_cache is not None:
            dns_cache[domain] = dns_info

    if not dns_info.has_mx:
        return VerificationResult(
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

    mx_host = dns_info.mx_hosts[0]
    provider = dns_info.provider

    # Step 4: Provider-specific routing
    config = get_config(provider)

    smtp_result: Optional[SmtpResponse] = None
    is_catch_all: Optional[bool] = None

    # Step 5: SMTP handshake (if provider config allows)
    if config.do_smtp:
        smtp_result = await smtp_check(
            email=normalized,
            mx_host=mx_host,
            helo_domain=helo_domain,
            from_address=from_address,
        )

        # Step 6: Catch-all check (if provider config allows and SMTP succeeded)
        if config.do_catch_all and smtp_result.code >= 200:
            if catch_all_cache is not None and domain in catch_all_cache:
                is_catch_all = catch_all_cache[domain]
            else:
                is_catch_all = await check_catch_all(
                    domain=domain,
                    mx_host=mx_host,
                    helo_domain=helo_domain,
                    from_address=from_address,
                )
                if catch_all_cache is not None:
                    catch_all_cache[domain] = is_catch_all

    # Step 7: Score
    reachability, is_deliverable = _score(
        smtp_result=smtp_result,
        is_catch_all=is_catch_all,
        is_disposable=meta["is_disposable"],
        is_role=meta["is_role"],
        provider=provider,
        provider_mark_risky=config.mark_risky,
    )

    return VerificationResult(
        email=email,
        normalized=normalized,
        reachability=reachability,
        is_deliverable=is_deliverable,
        is_catch_all=is_catch_all,
        is_disposable=meta["is_disposable"],
        is_role=meta["is_role"],
        is_free=meta["is_free"],
        mx_host=mx_host,
        smtp_code=smtp_result.code if smtp_result else 0,
        smtp_message=smtp_result.message if smtp_result else "",
        provider=provider,
        domain=domain,
    )


async def verify_batch(
    emails: list[str],
    concurrency: int = DEFAULT_CONCURRENCY,
    helo_domain: str = "verify.kadenwood.com",
    from_address: str = "verify@kadenwood.com",
    progress_callback=None,
) -> list[VerificationResult]:
    """Verify a batch of emails with domain-first optimization.

    Groups emails by domain to reuse DNS and catch-all results.
    Uses a semaphore to limit concurrent SMTP connections.

    Args:
        emails: List of email addresses to verify.
        concurrency: Max concurrent SMTP connections.
        helo_domain: Domain for EHLO command.
        from_address: Address for MAIL FROM.
        progress_callback: Optional callable(result) called after each verification.

    Returns:
        List of VerificationResult in same order as input.
    """
    semaphore = asyncio.Semaphore(concurrency)
    dns_cache: dict[str, DnsInfo] = {}
    catch_all_cache: dict[str, Optional[bool]] = {}

    # Pre-warm DNS cache: resolve all unique domains in parallel
    unique_domains = {email.split("@")[-1].lower() for email in emails if "@" in email}
    logger.info(f"Pre-warming DNS cache for {len(unique_domains)} unique domains...")

    async def _resolve_domain(domain: str):
        dns_cache[domain] = await lookup_mx(domain)

    await asyncio.gather(*[_resolve_domain(d) for d in unique_domains], return_exceptions=True)

    # Domain-level lock to prevent simultaneous connections to same MX
    domain_locks: dict[str, asyncio.Lock] = {}

    async def _verify_with_limit(email: str) -> VerificationResult:
        # Extract domain for domain-level locking
        parts = email.strip().split("@")
        domain = parts[-1].lower() if len(parts) == 2 else ""

        # Get or create domain lock
        if domain not in domain_locks:
            domain_locks[domain] = asyncio.Lock()

        async with semaphore:
            async with domain_locks[domain]:
                try:
                    result = await verify_email(
                        email=email,
                        helo_domain=helo_domain,
                        from_address=from_address,
                        dns_cache=dns_cache,
                        catch_all_cache=catch_all_cache,
                    )
                except Exception:
                    logger.exception("Verification failed for %s", email)
                    result = VerificationResult(
                        email=email,
                        normalized=email.strip().lower(),
                        reachability=Reachability.unknown,
                        is_deliverable=None,
                        domain=domain,
                        error="internal verification error",
                    )
                if progress_callback:
                    progress_callback(result)
                return result

    # Sort emails by domain for better cache utilization
    indexed_emails = list(enumerate(emails))
    indexed_emails.sort(key=lambda x: x[1].split("@")[-1] if "@" in x[1] else "")

    # Run all verifications
    tasks = [_verify_with_limit(email) for _, email in indexed_emails]
    results = await asyncio.gather(*tasks)

    # Restore original order
    ordered_results = [None] * len(emails)
    for (orig_idx, _), result in zip(indexed_emails, results):
        ordered_results[orig_idx] = result

    return ordered_results
