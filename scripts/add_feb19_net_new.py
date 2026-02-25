#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import csv
import re
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote

import requests

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def clean(v: Optional[str]) -> str:
    return (v or "").strip()


def clean_email(v: Optional[str]) -> str:
    s = clean(v).lower()
    return s if EMAIL_RE.match(s) else ""


def clean_industry(v: Optional[str]) -> str:
    s = clean(v)
    if not s:
        return ""

    if s.startswith("[") and s.endswith("]"):
        try:
            obj = ast.literal_eval(s)
        except Exception:
            obj = None

        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, str) and clean(item):
                    return clean(item).lower()
            return ""

        if isinstance(obj, str):
            return clean(obj).lower()

        inner = s[1:-1].strip()
        if "," in inner:
            inner = inner.split(",", 1)[0]
        inner = inner.strip().strip('"').strip("'").strip()
        return inner.lower()

    return s.lower()


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
    def __init__(self, sheet_id: str, token_file: str):
        self.sheet_id = sheet_id
        self.token_file = Path(token_file)
        self.token = ""
        self.headers: dict[str, str] = {}
        self.refresh()

    def refresh(self) -> None:
        self.token = get_google_token(self.token_file)
        self.headers = {"Authorization": "Bearer %s" % self.token}

    def _request(self, method: str, url: str, **kwargs):
        headers = kwargs.pop("headers", self.headers)
        r = requests.request(method, url, headers=headers, **kwargs)
        if r.status_code == 401:
            self.refresh()
            r = requests.request(method, url, headers=self.headers, **kwargs)
        return r

    def meta(self) -> dict:
        r = self._request(
            "GET",
            "https://sheets.googleapis.com/v4/spreadsheets/%s" % self.sheet_id,
            timeout=60,
        )
        if r.status_code != 200:
            raise RuntimeError("meta failed %s: %s" % (r.status_code, r.text[:300]))
        return r.json()

    def get_values(self, rng: str) -> list:
        url = "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s" % (self.sheet_id, quote(rng))
        r = self._request("GET", url, timeout=180)
        if r.status_code != 200:
            raise RuntimeError("GET failed %s %s: %s" % (rng, r.status_code, r.text[:300]))
        return r.json().get("values", [])

    def update_values(self, rng: str, rows: list[list[str]]) -> None:
        url = "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s" % (self.sheet_id, quote(rng))
        r = self._request(
            "PUT",
            url,
            headers={**self.headers, "Content-Type": "application/json"},
            params={"valueInputOption": "USER_ENTERED"},
            json={"values": rows},
            timeout=180,
        )
        if r.status_code != 200:
            raise RuntimeError("update failed %s: %s" % (r.status_code, r.text[:300]))

    def batch_update(self, reqs: list) -> dict:
        url = "https://sheets.googleapis.com/v4/spreadsheets/%s:batchUpdate" % self.sheet_id
        r = self._request(
            "POST",
            url,
            headers={**self.headers, "Content-Type": "application/json"},
            json={"requests": reqs},
            timeout=180,
        )
        if r.status_code != 200:
            raise RuntimeError("batchUpdate failed %s: %s" % (r.status_code, r.text[:300]))
        return r.json()

    def append_rows(self, tab: str, rows: list[list[str]]) -> int:
        rng = "%s!A:AD" % tab
        url = "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s:append" % (
            self.sheet_id,
            quote(rng),
        )
        params = {"valueInputOption": "USER_ENTERED", "insertDataOption": "INSERT_ROWS"}
        r = self._request(
            "POST",
            url,
            headers={**self.headers, "Content-Type": "application/json"},
            params=params,
            json={"values": rows},
            timeout=180,
        )
        if r.status_code != 200:
            raise RuntimeError("append failed %s: %s" % (r.status_code, r.text[:300]))
        updates = r.json().get("updates", {})
        return int(updates.get("updatedRows", len(rows)) or len(rows))


def row_score(row: dict) -> int:
    score = 0
    for k, v in row.items():
        if k == "email":
            continue
        if clean(v):
            score += 1
    return score


def find_tab(meta: dict, title: str) -> bool:
    for s in meta.get("sheets", []):
        p = s.get("properties", {})
        if p.get("title") == title:
            return True
    return False


def ensure_tab(gs: GSheets, tab: str, header: list[str]) -> None:
    meta = gs.meta()
    if find_tab(meta, tab):
        return

    gs.batch_update(
        [
            {
                "addSheet": {
                    "properties": {
                        "title": tab,
                        "gridProperties": {"rowCount": 2000, "columnCount": max(30, len(header))},
                    }
                }
            }
        ]
    )
    gs.update_values("%s!A1:AD1" % tab, [header])
    print("[sheet] created tab and wrote header:", tab, flush=True)


def normalize_row(master: dict, header: list[str]) -> list[str]:
    out: Dict[str, str] = {}
    for h in header:
        out[h] = clean(master.get(h))

    out["email"] = clean(out.get("email")).lower()
    out["industry"] = clean_industry(out.get("industry"))
    out["org_industry"] = clean_industry(out.get("org_industry"))

    tier_label = clean(master.get("tier_label"))
    out["tier_label"] = tier_label
    out["tier"] = clean(master.get("tier")) or tier_label or "tier_unknown"

    return [out.get(h, "") for h in header]


def scan_existing_emails(gs: GSheets, tabs: list[str]) -> set[str]:
    existing: set[str] = set()
    for t in tabs:
        rows = gs.get_values("%s!A2:AD" % t)
        added = 0
        for row in rows:
            found = ""
            for cell in row:
                e = clean_email(cell)
                if e:
                    found = e
                    break
            if found and found not in existing:
                existing.add(found)
                added += 1
        print("[sheet] tab=%s rows=%s unique_added=%s total_unique=%s" % (t, len(rows), added, len(existing)), flush=True)
    return existing


def load_sendable_set(sendable_csv: Path) -> set[str]:
    out: set[str] = set()
    with sendable_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            e = clean_email(row.get("email"))
            if e:
                out.add(e)
    return out


def build_master_map(master_csv: Path, needed: set[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not needed:
        return out

    with master_csv.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            e = clean_email(row.get("email"))
            if not e or e not in needed:
                continue

            prev = out.get(e)
            if prev is None or row_score(row) > row_score(prev):
                out[e] = row

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--token-file", default="/opt/mundi-princeps/config/token.json")
    ap.add_argument("--template-tab", default="✅quick_wins_net_new_valid_catchall_2026-02-18")
    ap.add_argument("--target-tab", default="✅quick_wins_net_new_valid_catchall_2026-02-19")
    ap.add_argument("--sendable-csv", required=True)
    ap.add_argument("--master-csv", required=True)
    ap.add_argument("--chunk", type=int, default=1000)
    args = ap.parse_args()

    gs = GSheets(args.sheet_id, args.token_file)
    meta = gs.meta()
    tabs = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    tabs = [t for t in tabs if t]
    print("[sheet] tabs:", tabs, flush=True)

    hdr_vals = gs.get_values("%s!A1:AD1" % args.template_tab)
    if not hdr_vals or not hdr_vals[0]:
        raise RuntimeError("Template tab header missing: %s" % args.template_tab)
    header = [clean(h) for h in hdr_vals[0]]
    print("[sheet] header_len=%s" % len(header), flush=True)

    ensure_tab(gs, args.target_tab, header)

    meta2 = gs.meta()
    tabs2 = [s.get("properties", {}).get("title") for s in meta2.get("sheets", [])]
    tabs2 = [t for t in tabs2 if t]

    existing = scan_existing_emails(gs, tabs2)
    sendable = load_sendable_set(Path(args.sendable_csv))

    needed = sendable - existing
    print("[counts] sendable=%s existing=%s needed_net_new=%s" % (len(sendable), len(existing), len(needed)), flush=True)

    if not needed:
        print("[done] nothing to append", flush=True)
        return

    master_map = build_master_map(Path(args.master_csv), needed)
    print("[counts] matched_in_master=%s missing_in_master=%s" % (len(master_map), len(needed - set(master_map.keys()))), flush=True)

    rows_to_append: List[List[str]] = []
    missing_master = 0
    for e in sorted(needed):
        row = master_map.get(e)
        if not row:
            missing_master += 1
            continue
        rows_to_append.append(normalize_row(row, header))

    print("[counts] rows_ready=%s skipped_missing_master=%s" % (len(rows_to_append), missing_master), flush=True)
    if not rows_to_append:
        print("[done] no rows to append after mapping", flush=True)
        return

    appended = 0
    for i in range(0, len(rows_to_append), args.chunk):
        part = rows_to_append[i : i + args.chunk]
        n = gs.append_rows(args.target_tab, part)
        appended += n
        print("[sheet] appended_chunk=%s total_appended=%s/%s" % (n, appended, len(rows_to_append)), flush=True)

    print("[done] appended_total=%s" % appended, flush=True)


if __name__ == "__main__":
    main()
