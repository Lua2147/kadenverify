"""Tests for P0-P3 engine correctness fixes.

P0: Cache key uses normalized email (Gmail dot/plus dedup)
P1: to_omniverifier() maps risky correctly (not "unknown")
P2: _detect_provider() distinguishes gmail from google_workspace
P3: Tier 2 _infer_reachability() returns risky, not safe, without SMTP
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from engine.dns import _detect_provider, lookup_mx
from engine.models import DnsInfo, Provider, Reachability, VerificationResult
from engine.tiered_verifier import (
    _compute_fast_tier_confidence,
    _infer_reachability,
    verify_email_tiered,
)


# ---------------------------------------------------------------------------
# P1: to_omniverifier() risky mapping
# ---------------------------------------------------------------------------


def test_omni_risky_non_catchall():
    """Risky + non-catch-all should map to result='risky', status='risky'."""
    r = VerificationResult(
        email="user@tempmail.com",
        normalized="user@tempmail.com",
        reachability=Reachability.risky,
        is_deliverable=True,
        is_disposable=True,
        is_catch_all=False,
    )
    omni = r.to_omniverifier()
    assert omni["result"] == "risky"
    assert omni["status"] == "risky"


def test_omni_risky_catchall():
    """Risky + catch-all should map to result='accept_all', status='catch_all'."""
    r = VerificationResult(
        email="user@catchall.com",
        normalized="user@catchall.com",
        reachability=Reachability.risky,
        is_deliverable=None,
        is_catch_all=True,
    )
    omni = r.to_omniverifier()
    assert omni["result"] == "accept_all"
    assert omni["status"] == "catch_all"


def test_omni_risky_role_account():
    """Risky role account (not catch-all) should map to 'risky', not 'unknown'."""
    r = VerificationResult(
        email="info@company.com",
        normalized="info@company.com",
        reachability=Reachability.risky,
        is_deliverable=True,
        is_role=True,
        is_catch_all=False,
    )
    omni = r.to_omniverifier()
    assert omni["result"] == "risky"
    assert omni["status"] == "risky"


def test_omni_safe_still_deliverable():
    """Safe should still map to deliverable/valid (regression check)."""
    r = VerificationResult(
        email="real@gmail.com",
        normalized="real@gmail.com",
        reachability=Reachability.safe,
        is_deliverable=True,
    )
    omni = r.to_omniverifier()
    assert omni["result"] == "deliverable"
    assert omni["status"] == "valid"


def test_omni_invalid_still_undeliverable():
    """Invalid should still map to undeliverable/invalid (regression check)."""
    r = VerificationResult(
        email="bad@nowhere.xyz",
        normalized="bad@nowhere.xyz",
        reachability=Reachability.invalid,
        is_deliverable=False,
    )
    omni = r.to_omniverifier()
    assert omni["result"] == "undeliverable"
    assert omni["status"] == "invalid"


def test_omni_unknown_still_unknown():
    """Unknown (not risky, not catch-all) should still map to unknown."""
    r = VerificationResult(
        email="timeout@example.com",
        normalized="timeout@example.com",
        reachability=Reachability.unknown,
        is_deliverable=None,
        is_catch_all=False,
    )
    omni = r.to_omniverifier()
    assert omni["result"] == "unknown"
    assert omni["status"] == "unknown"


# ---------------------------------------------------------------------------
# P2: _detect_provider() Gmail vs Google Workspace
# ---------------------------------------------------------------------------


def test_detect_provider_gmail_personal():
    """gmail.com MX should return Provider.gmail, not google_workspace."""
    mx = ["gmail-smtp-in.l.google.com"]
    assert _detect_provider(mx, "gmail.com") == Provider.gmail


def test_detect_provider_googlemail_alias():
    """googlemail.com MX should return Provider.gmail."""
    mx = ["gmail-smtp-in.l.google.com"]
    assert _detect_provider(mx, "googlemail.com") == Provider.gmail


def test_detect_provider_google_workspace():
    """Custom domain with Google MX should return Provider.google_workspace."""
    mx = ["aspmx.l.google.com"]
    assert _detect_provider(mx, "kadenwood.com") == Provider.google_workspace


def test_detect_provider_microsoft365():
    """M365 MX should still return Provider.microsoft365."""
    mx = ["company-com.mail.protection.outlook.com"]
    assert _detect_provider(mx, "company.com") == Provider.microsoft365


def test_detect_provider_hotmail():
    """Hotmail/Outlook B2C MX should still return Provider.hotmail."""
    mx = ["mx1.hotmail.com"]
    assert _detect_provider(mx, "hotmail.com") == Provider.hotmail


def test_detect_provider_hotmail_olc():
    """Outlook.com OLC protection MX → hotmail (B2C)."""
    mx = ["outlook-com.olc.protection.outlook.com"]
    assert _detect_provider(mx, "outlook.com") == Provider.hotmail


def test_detect_provider_generic():
    """Unknown MX should return Provider.generic."""
    mx = ["mail.custom-server.net"]
    assert _detect_provider(mx, "custom-server.net") == Provider.generic


# ---------------------------------------------------------------------------
# P3: _infer_reachability() — no false safe at Tier 2
# ---------------------------------------------------------------------------


def test_infer_gmail_returns_risky():
    """Gmail at Tier 2 should be risky (not safe) without SMTP confirmation."""
    meta = {"is_disposable": False, "is_role": False, "is_free": True}
    dns = DnsInfo(mx_hosts=["gmail-smtp-in.l.google.com"], has_mx=True, provider=Provider.gmail)
    assert _infer_reachability(meta, dns) == Reachability.risky


def test_infer_google_workspace_returns_risky():
    """Google Workspace at Tier 2 should be risky without SMTP."""
    meta = {"is_disposable": False, "is_role": False, "is_free": False}
    dns = DnsInfo(mx_hosts=["aspmx.l.google.com"], has_mx=True, provider=Provider.google_workspace)
    assert _infer_reachability(meta, dns) == Reachability.risky


def test_infer_m365_returns_risky():
    """Microsoft 365 at Tier 2 should be risky without SMTP."""
    meta = {"is_disposable": False, "is_role": False, "is_free": False}
    dns = DnsInfo(
        mx_hosts=["company.mail.protection.outlook.com"],
        has_mx=True,
        provider=Provider.microsoft365,
    )
    assert _infer_reachability(meta, dns) == Reachability.risky


def test_infer_free_provider_returns_risky():
    """Free provider at Tier 2 should be risky without SMTP."""
    meta = {"is_disposable": False, "is_role": False, "is_free": True}
    dns = DnsInfo(mx_hosts=["mx.zoho.com"], has_mx=True, provider=Provider.generic)
    assert _infer_reachability(meta, dns) == Reachability.risky


def test_infer_disposable_still_risky():
    """Disposable should still be risky (regression check)."""
    meta = {"is_disposable": True, "is_role": False, "is_free": False}
    dns = DnsInfo(mx_hosts=["mx.tempmail.com"], has_mx=True, provider=Provider.generic)
    assert _infer_reachability(meta, dns) == Reachability.risky


def test_infer_generic_unknown():
    """Generic non-free provider should be unknown at Tier 2."""
    meta = {"is_disposable": False, "is_role": False, "is_free": False}
    dns = DnsInfo(mx_hosts=["mx.corporate.com"], has_mx=True, provider=Provider.generic)
    assert _infer_reachability(meta, dns) == Reachability.unknown


# ---------------------------------------------------------------------------
# P0: Cache key normalization — Gmail dot/plus variants share one entry
# ---------------------------------------------------------------------------


def test_cache_key_gmail_dot_variants():
    """Gmail dot variants should hit the same cache entry."""
    cache_store = {}

    async def cache_lookup(email):
        return cache_store.get(email)

    async def cache_update(result):
        cache_store[result.email] = result

    async def run():
        # First verification: john.doe@gmail.com
        result1, tier1, _ = await verify_email_tiered(
            "john.doe@gmail.com",
            cache_lookup_fn=cache_lookup,
            cache_update_fn=cache_update,
            force_tier=3,
        )

        # Cache should store under normalized key (johndoe@gmail.com)
        assert "johndoe@gmail.com" in cache_store
        assert "john.doe@gmail.com" not in cache_store

        # Second verification: johndoe@gmail.com — should hit cache
        result2, tier2, reason2 = await verify_email_tiered(
            "johndoe@gmail.com",
            cache_lookup_fn=cache_lookup,
            cache_update_fn=cache_update,
        )
        assert tier2 == 1, f"Expected tier 1 cache hit, got tier {tier2}"
        assert reason2 == "cached_result"

    asyncio.run(run())


def test_cache_key_gmail_plus_addressing():
    """Gmail plus-addressing should normalize to base address."""
    cache_store = {}

    async def cache_lookup(email):
        return cache_store.get(email)

    async def cache_update(result):
        cache_store[result.email] = result

    async def run():
        # Verify with plus-address
        result1, tier1, _ = await verify_email_tiered(
            "user+tag@gmail.com",
            cache_lookup_fn=cache_lookup,
            cache_update_fn=cache_update,
            force_tier=3,
        )

        # Should store under normalized (no plus, no dots)
        assert "user@gmail.com" in cache_store
        assert "user+tag@gmail.com" not in cache_store

    asyncio.run(run())


def test_cache_key_non_gmail_unchanged():
    """Non-Gmail addresses should cache under lowercased email as-is."""
    cache_store = {}

    async def cache_lookup(email):
        return cache_store.get(email)

    async def cache_update(result):
        cache_store[result.email] = result

    async def run():
        result1, tier1, _ = await verify_email_tiered(
            "John.Doe@Company.com",
            cache_lookup_fn=cache_lookup,
            cache_update_fn=cache_update,
            force_tier=3,
        )

        # Non-Gmail: lowercased but dots preserved
        assert "john.doe@company.com" in cache_store

    asyncio.run(run())
