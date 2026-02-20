from fastapi.testclient import TestClient

import server
from engine.models import Provider, Reachability, VerificationResult


def _stub_result(email: str, reachability: Reachability = Reachability.safe) -> VerificationResult:
    return VerificationResult(
        email=email,
        normalized=email,
        reachability=reachability,
        is_deliverable=reachability == Reachability.safe,
        provider=Provider.generic,
        domain=email.split("@")[-1],
        error="internal verification error" if reachability == Reachability.unknown else None,
    )


def test_auth_header_compatibility(monkeypatch) -> None:
    async def fake_verify_email_tiered(**kwargs):
        return _stub_result(kwargs["email"]), 1, "cached_result"

    monkeypatch.setattr(server, "API_KEY", "test-secret")
    monkeypatch.setattr(server, "verify_email_tiered", fake_verify_email_tiered)
    server._rate_limit_store.clear()

    client = TestClient(server.app)

    assert client.get("/v1/validate/test@example.com", headers={"X-API-Key": "test-secret"}).status_code == 200
    assert client.get("/v1/validate/test@example.com", headers={"x-api-key": "test-secret"}).status_code == 200
    assert (
        client.get(
            "/v1/validate/test@example.com",
            headers={"Authorization": "Bearer test-secret"},
        ).status_code
        == 200
    )
    assert client.get("/v1/validate/test@example.com", headers={"X-API-Key": "wrong"}).status_code == 401


def test_rate_limit_returns_429(monkeypatch) -> None:
    async def fake_verify_email_tiered(**kwargs):
        return _stub_result(kwargs["email"]), 1, "cached_result"

    monkeypatch.setattr(server, "API_KEY", "test-secret")
    monkeypatch.setattr(server, "verify_email_tiered", fake_verify_email_tiered)
    monkeypatch.setattr(server, "RATE_LIMIT_BACKEND", "memory")
    monkeypatch.setattr(server, "RATE_LIMIT_MAX", 1)
    server._rate_limit_store.clear()

    client = TestClient(server.app)
    headers = {"X-API-Key": "test-secret"}

    assert client.get("/verify", params={"email": "one@example.com"}, headers=headers).status_code == 200
    assert client.get("/verify", params={"email": "two@example.com"}, headers=headers).status_code == 429


def test_verify_batch_partial_failure_shape(monkeypatch) -> None:
    async def fake_verify_batch(**kwargs):
        emails = kwargs["emails"]
        return [
            _stub_result(emails[0], Reachability.safe),
            _stub_result(emails[1], Reachability.unknown),
            _stub_result(emails[2], Reachability.safe),
        ]

    monkeypatch.setattr(server, "API_KEY", "test-secret")
    monkeypatch.setattr(server, "verify_batch", fake_verify_batch)
    monkeypatch.setattr(server, "CACHE_BACKEND", "supabase")
    monkeypatch.setattr(server, "_get_supabase_client", lambda: None)
    monkeypatch.setattr(server, "RATE_LIMIT_BACKEND", "memory")
    monkeypatch.setattr(server, "RATE_LIMIT_MAX", 100)
    server._rate_limit_store.clear()

    client = TestClient(server.app)
    headers = {"X-API-Key": "test-secret"}
    payload = {"emails": ["ok1@example.com", "boom@example.com", "ok2@example.com"]}
    resp = client.post("/verify/batch", json=payload, headers=headers)

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 3
    assert body[1]["status"] == "unknown"
    assert body[1]["reason"] == "internal verification error"


def test_verify_batch_persists_to_supabase(monkeypatch) -> None:
    class FakeSupabase:
        def __init__(self):
            self.calls = []

        def upsert_results_batch(self, results, batch_size=500) -> int:
            self.calls.append((list(results), batch_size))
            return len(results)

    fake = FakeSupabase()

    async def fake_verify_batch(**kwargs):
        emails = kwargs["emails"]
        return [_stub_result(emails[0], Reachability.safe)]

    monkeypatch.setattr(server, "API_KEY", "test-secret")
    monkeypatch.setattr(server, "CACHE_BACKEND", "supabase")
    monkeypatch.setattr(server, "_get_supabase_client", lambda: fake)
    monkeypatch.setattr(server, "verify_batch", fake_verify_batch)
    monkeypatch.setattr(server, "RATE_LIMIT_BACKEND", "memory")
    monkeypatch.setattr(server, "RATE_LIMIT_MAX", 100)
    server._rate_limit_store.clear()

    client = TestClient(server.app)
    headers = {"X-API-Key": "test-secret"}
    payload = {"emails": ["ok@example.com"]}
    resp = client.post("/verify/batch", json=payload, headers=headers)

    assert resp.status_code == 200
    assert len(fake.calls) == 1
    called_results, batch_size = fake.calls[0]
    assert batch_size == 500
    assert len(called_results) == 1
    assert called_results[0].email == "ok@example.com"


def test_readiness_endpoint_contract() -> None:
    client = TestClient(server.app)
    resp = client.get("/ready")

    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert "checks" in body


def test_metrics_endpoint_contract(monkeypatch) -> None:
    monkeypatch.setattr(server, "API_KEY", "test-secret")
    client = TestClient(server.app)
    resp = client.get("/metrics", headers={"X-API-Key": "test-secret"})

    assert resp.status_code == 200
    body = resp.json()
    assert "tier_latency_ms" in body
    assert "cache" in body
    assert "rate_limited_429" in body
