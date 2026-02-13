from __future__ import annotations

from datetime import datetime, timezone

from engine.models import Provider, Reachability, VerificationResult


class _FakeResponse:
    def __init__(self, status_code: int, payload, headers: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


def test_supabase_get_by_email_returns_none_for_missing_row() -> None:
    from store.supabase_io import SupabaseRestClient

    calls: list[dict] = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers or {}),
                "params": dict(params or {}),
                "json": json,
            }
        )
        return _FakeResponse(200, [])

    client = SupabaseRestClient("https://example.supabase.co", "test-key", request_fn=fake_request)

    assert client.get_by_email("missing@example.com") is None
    assert calls[0]["method"] == "GET"
    assert "verified_emails" in calls[0]["url"]
    assert calls[0]["params"]["email"] == "eq.missing@example.com"


def test_supabase_get_by_email_parses_verification_result() -> None:
    from store.supabase_io import SupabaseRestClient

    row = {
        "email": "cached@example.com",
        "normalized": "cached@example.com",
        "reachability": "safe",
        "is_deliverable": True,
        "is_catch_all": False,
        "is_disposable": False,
        "is_role": False,
        "is_free": True,
        "mx_host": "mx.example.com",
        "smtp_code": 250,
        "smtp_message": "OK",
        "provider": "generic",
        "domain": "example.com",
        "verified_at": "2026-02-09T00:00:00Z",
        "error": None,
    }

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        return _FakeResponse(200, [row])

    client = SupabaseRestClient("https://example.supabase.co", "test-key", request_fn=fake_request)
    result = client.get_by_email("cached@example.com")

    assert result is not None
    assert result.email == "cached@example.com"
    assert result.reachability == Reachability.safe
    assert result.provider == Provider.generic
    assert result.verified_at.tzinfo is not None


def test_supabase_upsert_results_batches_and_sets_headers() -> None:
    from store.supabase_io import SupabaseRestClient

    calls: list[dict] = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers or {}),
                "params": dict(params or {}),
                "json": json,
            }
        )
        return _FakeResponse(201, [])

    client = SupabaseRestClient("https://example.supabase.co", "test-key", request_fn=fake_request)

    now = datetime.now(timezone.utc)
    results = [
        VerificationResult(email="a@example.com", normalized="a@example.com", verified_at=now),
        VerificationResult(email="b@example.com", normalized="b@example.com", verified_at=now),
        VerificationResult(email="c@example.com", normalized="c@example.com", verified_at=now),
    ]

    written = client.upsert_results_batch(results, batch_size=2)

    assert written == 3
    assert len(calls) == 2
    assert all(call["method"] == "POST" for call in calls)
    assert all("resolution=merge-duplicates" in call["headers"].get("Prefer", "") for call in calls)
    assert all(call["params"].get("on_conflict") == "email" for call in calls)


def test_supabase_get_stats_fallback_uses_count_queries() -> None:
    from store.supabase_io import SupabaseRestClient

    def _count_response(total: int):
        # Supabase/PostgREST uses Content-Range for totals when Prefer: count=exact is set.
        return _FakeResponse(200, [], headers={"content-range": f"0-0/{total}"})

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        assert method == "GET"
        params = params or {}
        reachability = params.get("reachability")
        if reachability == "eq.safe":
            return _count_response(7)
        if reachability == "eq.risky":
            return _count_response(2)
        if reachability == "eq.invalid":
            return _count_response(1)
        if reachability == "eq.unknown":
            return _count_response(0)
        if params.get("is_catch_all") == "is.true":
            return _count_response(3)
        if params.get("is_disposable") == "is.true":
            return _count_response(4)
        return _count_response(10)

    client = SupabaseRestClient("https://example.supabase.co", "test-key", request_fn=fake_request)
    stats = client.get_stats()

    assert stats["total"] == 10
    assert stats["by_reachability"]["safe"] == 7
    assert stats["by_reachability"]["risky"] == 2
    assert stats["by_reachability"]["invalid"] == 1
    assert stats["by_reachability"]["unknown"] == 0
    assert stats["catch_all"] == 3
    assert stats["disposable"] == 4
    assert stats["top_domains"] == []


def test_supabase_client_from_env_requires_url_and_key(monkeypatch) -> None:
    from store.supabase_io import supabase_client_from_env

    monkeypatch.delenv("KADENVERIFY_SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("KADENVERIFY_SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)

    assert supabase_client_from_env() is None

    monkeypatch.setenv("KADENVERIFY_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("KADENVERIFY_SUPABASE_SERVICE_ROLE_KEY", "test-key")

    client = supabase_client_from_env()
    assert client is not None


def test_supabase_query_rows_passes_filters_and_order() -> None:
    from store.supabase_io import SupabaseRestClient

    calls: list[dict] = []

    def fake_request(method, url, headers=None, params=None, json=None, timeout=None):
        calls.append(
            {
                "method": method,
                "url": url,
                "params": dict(params or {}),
            }
        )
        return _FakeResponse(200, [{"email": "a@example.com"}])

    client = SupabaseRestClient("https://example.supabase.co", "test-key", request_fn=fake_request)
    rows = client.query_rows(
        select="email",
        filters={"reachability": "eq.safe"},
        order="verified_at.desc",
        limit=123,
    )

    assert rows[0]["email"] == "a@example.com"
    assert calls[0]["method"] == "GET"
    assert calls[0]["params"]["select"] == "email"
    assert calls[0]["params"]["reachability"] == "eq.safe"
    assert calls[0]["params"]["order"] == "verified_at.desc"
    assert calls[0]["params"]["limit"] == "123"
