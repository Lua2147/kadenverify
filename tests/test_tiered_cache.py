import asyncio
from datetime import datetime, timedelta, timezone

from engine.models import Reachability, VerificationResult
from engine.tiered_verifier import _tier1_cached


def _build_cached_result(email: str, verified_at: datetime) -> VerificationResult:
    return VerificationResult(
        email=email,
        normalized=email,
        reachability=Reachability.safe,
        is_deliverable=True,
        verified_at=verified_at,
    )


def test_tier1_cache_accepts_fresh_naive_timestamp() -> None:
    """DuckDB returns naive UTC timestamps; cache hits should still work."""
    email = "cached@example.com"
    naive_utc = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=1)

    def cache_lookup(_: str) -> VerificationResult:
        return _build_cached_result(email, naive_utc)

    cached = asyncio.run(_tier1_cached(email, cache_lookup))

    assert cached is not None
    assert cached.email == email


def test_tier1_cache_rejects_expired_naive_timestamp() -> None:
    email = "stale@example.com"
    naive_utc_stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=31)

    def cache_lookup(_: str) -> VerificationResult:
        return _build_cached_result(email, naive_utc_stale)

    cached = asyncio.run(_tier1_cached(email, cache_lookup))

    assert cached is None
