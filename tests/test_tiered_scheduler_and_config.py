import asyncio
from datetime import datetime, timezone

from engine.models import Provider, Reachability, VerificationResult
from engine.tiered_verifier import Tier3BackgroundScheduler, verify_email_tiered


def test_tier3_scheduler_is_bounded() -> None:
    scheduler = Tier3BackgroundScheduler(max_queue_size=1, workers=0)

    async def run():
        first = await scheduler.enqueue(("a@example.com", "helo", "from", None))
        second = await scheduler.enqueue(("b@example.com", "helo", "from", None))
        return first, second

    first, second = asyncio.run(run())
    assert first is True
    assert second is False


def test_role_filter_can_be_disabled(monkeypatch) -> None:
    async def fake_full_verify_email(email: str, helo_domain: str, from_address: str):
        return VerificationResult(
            email=email,
            normalized=email,
            reachability=Reachability.risky,
            is_deliverable=True,
            is_role=True,
            provider=Provider.generic,
            domain=email.split("@")[-1],
            smtp_code=250,
            verified_at=datetime.now(timezone.utc),
        )

    monkeypatch.setattr("engine.tiered_verifier.full_verify_email", fake_full_verify_email)
    monkeypatch.setattr("engine.tiered_verifier.FILTER_ROLE_ACCOUNTS", False)

    result, tier, reason = asyncio.run(
        verify_email_tiered(
            email="admin@example.com",
            force_tier=3,
            cache_lookup_fn=None,
            cache_update_fn=None,
        )
    )

    assert result.reachability == Reachability.risky
    assert result.is_deliverable is True
    assert tier == 3
    assert reason == "full_smtp_verification"
