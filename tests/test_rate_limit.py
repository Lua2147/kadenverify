import asyncio

from starlette.requests import Request

import server


def _request_for_ip(ip: str, headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "path": "/verify",
        "raw_path": b"/verify",
        "query_string": b"",
        "headers": headers or [],
        "client": (ip, 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


def test_rate_limiter_prunes_stale_ips(monkeypatch) -> None:
    now = 1_000_000.0
    server._rate_limit_store.clear()
    server._rate_limit_store["198.51.100.10"] = [now - server.RATE_LIMIT_WINDOW - 10]
    server._rate_limit_store["198.51.100.11"] = [now - server.RATE_LIMIT_WINDOW - 20]

    monkeypatch.setattr("time.time", lambda: now)

    asyncio.run(server.check_rate_limit(_request_for_ip("203.0.113.5")))

    assert "198.51.100.10" not in server._rate_limit_store
    assert "198.51.100.11" not in server._rate_limit_store


def test_rate_limit_key_isolated_by_api_key(monkeypatch) -> None:
    now = 2_000_000.0
    server._rate_limit_store.clear()
    monkeypatch.setattr("time.time", lambda: now)

    monkeypatch.setattr(server, "RATE_LIMIT_MAX", 1)

    req_key_a = _request_for_ip(
        "203.0.113.20",
        headers=[(b"x-api-key", b"key-a")],
    )
    req_key_b = _request_for_ip(
        "203.0.113.20",
        headers=[(b"x-api-key", b"key-b")],
    )

    asyncio.run(server.check_rate_limit(req_key_a))
    # Same IP but different key should have independent budget.
    asyncio.run(server.check_rate_limit(req_key_b))
