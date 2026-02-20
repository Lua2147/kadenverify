from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import requests

from engine.models import VerificationResult

logger = logging.getLogger("kadenverify.supabase")


class SupabaseRestError(RuntimeError):
    """Raised when Supabase PostgREST returns a non-2xx response."""

    def __init__(self, status_code: int, message: str):
        super().__init__(f"supabase_rest_error status={status_code} {message}")
        self.status_code = status_code


class SupabaseRestClient:
    """Minimal Supabase PostgREST client for the verified_emails table.

    Uses a service role key (or other privileged key) to bypass RLS for this backend.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        table: str = "verified_emails",
        timeout_seconds: float = 5.0,
        request_fn=None,
    ):
        self._rest_url = base_url.rstrip("/") + "/rest/v1"
        self._table = table
        self._timeout_seconds = timeout_seconds
        self._request_fn = request_fn or requests.request
        self._base_headers = {
            "apikey": api_key,
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _parse_content_range_total(content_range: Optional[str]) -> Optional[int]:
        # Expected formats:
        # - "0-0/123"
        # - "*/0"
        if not content_range:
            return None
        if "/" not in content_range:
            return None
        try:
            total_part = content_range.split("/", 1)[1].strip()
            return int(total_part)
        except Exception:
            return None

    def _request(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, str]] = None,
        json_body: Any = None,
    ):
        merged_headers = dict(self._base_headers)
        if headers:
            merged_headers.update(headers)

        url = f"{self._rest_url}{path}"
        resp = self._request_fn(
            method,
            url,
            headers=merged_headers,
            params=params,
            json=json_body,
            timeout=self._timeout_seconds,
        )
        if not (200 <= resp.status_code < 300):
            # Never include headers (apikey) in error messages.
            text = getattr(resp, "text", "")
            raise SupabaseRestError(resp.status_code, text[:500])
        return resp

    def count(self, *, filters: Optional[dict[str, str]] = None) -> int:
        """Return an exact row count for the table (optionally filtered)."""
        params = {"select": "email"}
        if filters:
            params.update(filters)

        resp = self._request(
            "GET",
            f"/{self._table}",
            headers={
                "Prefer": "count=exact",
                "Range": "0-0",
            },
            params=params,
        )
        total = self._parse_content_range_total(
            resp.headers.get("content-range") or resp.headers.get("Content-Range")
        )
        if total is not None:
            return total

        # Fallback (shouldn't happen with count=exact): infer from payload.
        payload = resp.json()
        if isinstance(payload, list):
            return len(payload)
        return 1 if payload else 0

    def get_by_email(self, email: str) -> Optional[VerificationResult]:
        """Fetch a verified email row by primary key."""
        resp = self._request(
            "GET",
            f"/{self._table}",
            params={
                "select": "*",
                "email": f"eq.{email}",
                "limit": "1",
            },
        )
        payload = resp.json()
        if not payload:
            return None
        if isinstance(payload, list):
            row = payload[0]
        else:
            row = payload
        return VerificationResult.model_validate(row)

    def get_stats(self) -> dict:
        """Compute stats compatible with store.duckdb_io.get_stats()."""
        total = self.count()
        reachabilities = ["safe", "risky", "invalid", "unknown"]
        by_reachability = {
            reach: self.count(filters={"reachability": f"eq.{reach}"})
            for reach in reachabilities
        }
        catch_all = self.count(filters={"is_catch_all": "is.true"})
        disposable = self.count(filters={"is_disposable": "is.true"})

        # Top domains requires aggregation; keep empty unless an RPC is added later.
        return {
            "total": total,
            "by_reachability": by_reachability,
            "catch_all": catch_all,
            "disposable": disposable,
            "top_domains": [],
        }

    def query_rows(
        self,
        *,
        select: str = "*",
        filters: Optional[dict[str, str]] = None,
        order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[dict]:
        """Query rows from the verified_emails table via PostgREST."""
        params: dict[str, str] = {"select": select}
        if filters:
            params.update(filters)
        if order:
            params["order"] = order
        if limit is not None:
            params["limit"] = str(limit)
        if offset is not None:
            params["offset"] = str(offset)

        resp = self._request(
            "GET",
            f"/{self._table}",
            params=params,
        )
        payload = resp.json()
        if payload is None:
            return []
        if isinstance(payload, list):
            return payload
        return [payload]

    def upsert_result(self, result: VerificationResult) -> None:
        self.upsert_results_batch([result], batch_size=1)

    @staticmethod
    def _normalize_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def upsert_results_batch(self, results: list[VerificationResult], batch_size: int = 500) -> int:
        """Upsert verification results by email primary key.

        Returns the number of rows submitted for upsert (not necessarily changed).
        """
        if not results:
            return 0

        prefer = "resolution=merge-duplicates,return=minimal"
        written = 0
        for i in range(0, len(results), batch_size):
            chunk = results[i : i + batch_size]
            payload = []
            for r in chunk:
                verified_at = self._normalize_utc(r.verified_at)
                if verified_at is not r.verified_at:
                    r = r.model_copy(update={"verified_at": verified_at})
                payload.append(r.model_dump(mode="json"))
            self._request(
                "POST",
                f"/{self._table}",
                headers={"Prefer": prefer},
                params={"on_conflict": "email"},
                json_body=payload,
            )
            written += len(chunk)
        return written


def supabase_client_from_env() -> Optional[SupabaseRestClient]:
    url = os.environ.get("KADENVERIFY_SUPABASE_URL") or os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("KADENVERIFY_SUPABASE_SERVICE_ROLE_KEY") or os.environ.get(
        "SUPABASE_SERVICE_ROLE_KEY",
        "",
    )
    table = os.environ.get("KADENVERIFY_SUPABASE_TABLE", "verified_emails")
    timeout_seconds = float(os.environ.get("KADENVERIFY_SUPABASE_TIMEOUT_SECONDS", "5.0"))
    if not url or not key:
        return None
    return SupabaseRestClient(
        url,
        key,
        table=table,
        timeout_seconds=timeout_seconds,
    )
