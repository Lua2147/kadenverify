import asyncio

from engine.models import DnsInfo, Provider, Reachability, VerificationResult
from engine.verifier import verify_batch


def _result(email: str) -> VerificationResult:
    return VerificationResult(
        email=email,
        normalized=email,
        reachability=Reachability.safe,
        is_deliverable=True,
        provider=Provider.generic,
        domain=email.split("@")[-1],
    )


def test_verify_batch_survives_single_email_crash(monkeypatch) -> None:
    emails = ["ok1@example.com", "boom@example.com", "ok2@example.com"]

    async def fake_lookup_mx(domain: str):
        return DnsInfo(
            mx_hosts=["mx.example.com"],
            has_mx=True,
            provider=Provider.generic,
            domain=domain,
        )

    async def fake_verify_email(email: str, **kwargs):
        if email == "boom@example.com":
            raise RuntimeError("simulated verifier crash")
        return _result(email)

    monkeypatch.setattr("engine.verifier.lookup_mx", fake_lookup_mx)
    monkeypatch.setattr("engine.verifier.verify_email", fake_verify_email)

    results = asyncio.run(verify_batch(emails, concurrency=3))

    assert len(results) == 3
    assert results[0].reachability == Reachability.safe
    assert results[1].reachability == Reachability.unknown
    assert results[1].error == "internal verification error"
    assert results[2].reachability == Reachability.safe
