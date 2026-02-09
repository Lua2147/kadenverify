import asyncio

import server
from engine.models import Reachability, VerificationResult, Provider
from fastapi.testclient import TestClient


def _stub_result(email: str) -> VerificationResult:
    return VerificationResult(
        email=email,
        normalized=email,
        reachability=Reachability.safe,
        is_deliverable=True,
        provider=Provider.generic,
        domain=email.split("@")[-1],
    )


def test_cache_lookup_uses_supabase_backend(monkeypatch) -> None:
    class FakeSupabaseClient:
        def get_by_email(self, email: str):
            return _stub_result(email)

    monkeypatch.setattr(server, "CACHE_BACKEND", "supabase")
    monkeypatch.setattr(server, "_get_supabase_client", lambda: FakeSupabaseClient())

    result = asyncio.run(server._cache_lookup("cached@example.com"))

    assert result is not None
    assert result.email == "cached@example.com"


def test_stats_endpoint_uses_supabase_backend(monkeypatch) -> None:
    class FakeSupabaseClient:
        def get_stats(self):
            return {
                "total": 10,
                "by_reachability": {"safe": 7, "risky": 2, "invalid": 1, "unknown": 0},
                "catch_all": 3,
                "disposable": 4,
                "top_domains": [],
            }

    monkeypatch.setattr(server, "API_KEY", "test-secret")
    monkeypatch.setattr(server, "CACHE_BACKEND", "supabase")
    monkeypatch.setattr(server, "_get_supabase_client", lambda: FakeSupabaseClient())

    client = TestClient(server.app)
    resp = client.get("/stats", headers={"X-API-Key": "test-secret"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 10


def test_readiness_cache_check_uses_supabase_backend(monkeypatch) -> None:
    class FakeSupabaseClient:
        def get_by_email(self, email: str):
            return None

    monkeypatch.setattr(server, "CACHE_BACKEND", "supabase")
    monkeypatch.setattr(server, "_get_supabase_client", lambda: FakeSupabaseClient())

    result = asyncio.run(server._readiness_check_cache())

    assert result["ok"] is True
