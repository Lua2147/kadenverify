#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import time
from pathlib import Path
from urllib.parse import quote

import requests

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def clean(v: str | None) -> str:
    return (v or "").strip()


def clean_email(v: str | None) -> str:
    s = clean(v).lower()
    return s if EMAIL_RE.match(s) else ""


def get_google_token(token_file: Path) -> str:
    raw = token_file.read_text(encoding="utf-8")
    keys: dict[str, str] = {}

    for k in (
        "client_id",
        "client_secret",
        "refresh_token",
        "token_uri",
        "access_token",
        "token",
    ):
        m = re.search(r'"%s"\s*:\s*"([^"]+)"' % re.escape(k), raw)
        if m:
            keys[k] = m.group(1)

    tok = keys.get("access_token") or keys.get("token")

    if keys.get("refresh_token") and keys.get("client_id") and keys.get("client_secret"):
        rt = requests.post(
            keys.get("token_uri", "https://oauth2.googleapis.com/token"),
            data={
                "client_id": keys["client_id"],
                "client_secret": keys["client_secret"],
                "refresh_token": keys["refresh_token"],
                "grant_type": "refresh_token",
            },
            timeout=30,
        )
        if rt.status_code == 200:
            tok = rt.json()["access_token"]

    if not tok:
        raise RuntimeError("No usable Google token")

    return tok


class GSheets:
    def __init__(self, sheet_id: str, token_file: Path):
        self.sheet_id = sheet_id
        self.token_file = token_file
        self.token = ""
        self.headers: dict[str, str] = {}
        self.refresh()

    def refresh(self) -> None:
        self.token = get_google_token(self.token_file)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def _request(self, method: str, url: str, retries: int = 8, **kwargs):
        headers = kwargs.pop("headers", self.headers)
        last = None

        for attempt in range(1, retries + 1):
            r = requests.request(method, url, headers=headers, **kwargs)
            last = r

            if r.status_code == 401:
                self.refresh()
                headers = self.headers
                continue

            if r.status_code == 429:
                time.sleep(min(10 * attempt, 80))
                continue

            return r

        return last

    def tabs(self) -> list[str]:
        r = self._request(
            "GET",
            f"https://sheets.googleapis.com/v4/spreadsheets/{self.sheet_id}",
            params={"fields": "sheets.properties.title"},
            timeout=60,
        )
        if r.status_code != 200:
            raise RuntimeError(f"meta failed {r.status_code}: {r.text[:300]}")
        return [
            (s.get("properties") or {}).get("title", "")
            for s in r.json().get("sheets", [])
            if (s.get("properties") or {}).get("title")
        ]

    def get_col(self, tab: str, col_range: str) -> list[list[str]]:
        safe_tab = tab.replace("'", "''")
        rng = f"'{safe_tab}'!{col_range}"
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{self.sheet_id}/values/{quote(rng)}"
        r = self._request("GET", url, timeout=120)
        if r.status_code != 200:
            return []
        return r.json().get("values", [])


def load_sendable(path: Path) -> set[str]:
    out: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            e = clean_email(row.get("email"))
            if e:
                out.add(e)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--sendable-csv", required=True)
    ap.add_argument("--token-file", default="/opt/mundi-princeps/config/token.json")
    args = ap.parse_args()

    gs = GSheets(args.sheet_id, Path(args.token_file))
    tabs = gs.tabs()

    existing: set[str] = set()
    for t in tabs:
        rows = gs.get_col(t, "G2:G")
        for row in rows:
            e = clean_email(row[0] if row else "")
            if e:
                existing.add(e)

    sendable = load_sendable(Path(args.sendable_csv))

    print(f"tabs={len(tabs)}")
    print(f"sendable={len(sendable)}")
    print(f"existing={len(existing)}")
    print(f"needed_net_new={len(sendable - existing)}")


if __name__ == "__main__":
    main()
