#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import asyncio
import csv
import re
import time
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote

import aiohttp
import requests

GOOD = {"deliverable", "accept_all"}


def clean(v: str | None) -> str:
    return (v or "").strip()


def is_email(v: str | None) -> bool:
    s = clean(v).lower()
    return ("@" in s) and ("." in s.split("@")[-1])


def clean_industry(v: str | None) -> str:
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
        inner = inner.strip().strip(chr(34)).strip(chr(39)).strip()
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
        m = re.search(r"\"%s\"\s*:\s*\"([^\"]+)\"" % re.escape(k), raw)
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
            raise RuntimeError("meta failed %s: %s" % (r.status_code, r.text[:200]))
        return r.json()

    def get_values(self, rng: str) -> list:
        url = "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s" % (self.sheet_id, quote(rng))
        r = self._request("GET", url, timeout=180)
        if r.status_code != 200:
            raise RuntimeError("GET failed %s %s: %s" % (rng, r.status_code, r.text[:200]))
        return r.json().get("values", [])

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


def find_gid(meta: dict, title: str) -> int:
    for s in meta.get("sheets", []):
        p = s.get("properties", {})
        if p.get("title") == title:
            return int(p.get("sheetId"))
    raise RuntimeError("tab not found: %s" % title)


def ensure_row_capacity(gs: GSheets, tab_title: str, needed_rows: int, cols: int = 30) -> None:
    meta = gs.meta()
    gid = find_gid(meta, tab_title)

    cur = 0
    for s in meta.get("sheets", []):
        p = s.get("properties", {})
        if int(p.get("sheetId")) != gid:
            continue
        gp = p.get("gridProperties", {})
        cur = int(gp.get("rowCount", 0) or 0)
        break

    if cur >= needed_rows:
        return

    gs.batch_update(
        [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": gid,
                        "gridProperties": {"rowCount": needed_rows, "columnCount": cols},
                    },
                    "fields": "gridProperties(rowCount,columnCount)",
                }
            }
        ]
    )
    print("[sheet] expanded rows %s -> %s" % (cur, needed_rows), flush=True)


async def verify_block(
    api_url: str,
    api_key: str,
    emails: list[str],
    batch_size: int,
    concurrency: int,
) -> Dict[str, str]:
    headers = {"Content-Type": "application/json", "X-API-Key": api_key}
    batches = [emails[i : i + batch_size] for i in range(0, len(emails), batch_size)]
    sem = asyncio.Semaphore(concurrency)
    results: Dict[str, str] = {}
    done = 0

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
        async def do_batch(i: int, ems: list[str]) -> None:
            nonlocal done
            async with sem:
                for attempt in range(1, 7):
                    try:
                        async with session.post(
                            api_url + "/verify/batch",
                            headers=headers,
                            json={"emails": ems},
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                for item in data:
                                    e = clean(item.get("email")).lower()
                                    if e:
                                        results[e] = clean(item.get("result") or "unknown").lower() or "unknown"
                                done += 1
                                if done % 20 == 0 or done == len(batches):
                                    print("[verify] batches %s/%s" % (done, len(batches)), flush=True)
                                return
                            if resp.status == 429:
                                await asyncio.sleep(attempt * 2)
                                continue
                            body = (await resp.text())[:200]
                            print("[verify] batch %s status=%s body=%s" % (i, resp.status, body), flush=True)
                            await asyncio.sleep(attempt)
                    except Exception as e:
                        print("[verify] batch %s error=%s" % (i, e), flush=True)
                        await asyncio.sleep(attempt)

        await asyncio.gather(*[do_batch(i + 1, b) for i, b in enumerate(batches)])

    return results


def row_score(row: dict) -> int:
    score = 0
    for k, v in row.items():
        if k == "email":
            continue
        if clean(v):
            score += 1
    return score


def normalize_row_for_sheet(raw: dict, header: list[str]) -> list[str]:
    out: dict[str, str] = {}

    for h in header:
        if h == "tier":
            continue
        out[h] = clean(raw.get(h))

    out["email"] = clean(raw.get("email")).lower()

    tier_label = clean(raw.get("tier_label"))
    out["tier_label"] = tier_label
    out["tier"] = tier_label or clean(raw.get("tier")) or "tier_unknown"

    out["industry"] = clean_industry(out.get("industry"))
    out["org_industry"] = clean_industry(out.get("org_industry"))

    return [out.get(h, "") for h in header]


async def process_block(
    api_url: str,
    api_key: str,
    emails: List[str],
    rows_map: Dict[str, dict],
    header: List[str],
    batch_size: int,
    concurrency: int,
) -> List[List[str]]:
    if not emails:
        return []

    t0 = time.time()
    results = await verify_block(
        api_url,
        api_key,
        emails,
        batch_size=batch_size,
        concurrency=concurrency,
    )
    dt = time.time() - t0

    sendable_rows: List[List[str]] = []
    for e in emails:
        r = results.get(e, "unknown")
        if r in GOOD:
            raw = rows_map.get(e) or {}
            out_row = normalize_row_for_sheet(raw, header)
            sendable_rows.append(out_row)

    print("[block] verified=%s sendable=%s secs=%.1f" % (len(emails), len(sendable_rows), dt), flush=True)
    return sendable_rows


async def amain() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet-id", required=True)
    ap.add_argument("--tab", required=True)
    ap.add_argument("--token-file", default="/opt/mundi-princeps/config/token.json")
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--api-url", default="http://127.0.0.1:8025")
    ap.add_argument("--api-key", default="kadenwood_verify_2026")
    ap.add_argument("--target-total", type=int, default=50000)
    ap.add_argument("--verify-batch-size", type=int, default=500)
    ap.add_argument("--verify-concurrency", type=int, default=64)
    ap.add_argument("--block-emails", type=int, default=32000)
    ap.add_argument("--sheet-chunk", type=int, default=1000)
    ap.add_argument("--out-csv", default="")
    args = ap.parse_args()

    gs = GSheets(args.sheet_id, args.token_file)

    meta = gs.meta()
    tabs = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    print("[sheet] tabs=%s" % tabs, flush=True)

    hdr_vals = gs.get_values("%s!A1:AD1" % args.tab)
    if not hdr_vals or not hdr_vals[0]:
        raise RuntimeError("missing header in tab %s" % args.tab)
    header = [clean(h) for h in hdr_vals[0]]
    print("[sheet] header_len=%s" % len(header), flush=True)

    all_existing: set[str] = set()
    for t in tabs:
        if not t:
            continue
        vals = gs.get_values("%s!G2:G" % t)
        for row in vals:
            e = clean(row[0] if row else "").lower()
            if is_email(e):
                all_existing.add(e)
        print("[sheet] loaded=%s total_unique_emails=%s" % (t, len(all_existing)), flush=True)

    seen: set[str] = set(all_existing)

    colA = gs.get_values("%s!A:A" % args.tab)
    existing_rows = max(0, len(colA) - 1)
    print("[sheet] existing_rows_in_tab=%s" % existing_rows, flush=True)

    remaining_needed = max(0, args.target_total - existing_rows)
    if remaining_needed == 0:
        print("[done] already at target_total=%s" % args.target_total, flush=True)
        return

    print("[target] target_total=%s existing=%s need_additional=%s" % (args.target_total, existing_rows, remaining_needed), flush=True)

    ensure_row_capacity(gs, args.tab, needed_rows=args.target_total + 2000, cols=30)

    out_writer = None
    out_fh = None
    if args.out_csv:
        out_path = Path(args.out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_fh = out_path.open("a", encoding="utf-8", newline="")
        out_writer = csv.writer(out_fh)
        if out_path.stat().st_size == 0:
            out_writer.writerow(header)
            out_fh.flush()

    processed = 0
    queued = 0
    added = 0
    start = time.time()

    block_map: Dict[str, dict] = {}
    block_emails: List[str] = []

    def flush_sheet_rows(rows: List[List[str]]) -> None:
        nonlocal existing_rows
        if not rows:
            return
        updated = gs.append_rows(args.tab, rows)
        existing_rows += updated
        print("[sheet] appended=%s total_rows_in_tab=%s" % (updated, existing_rows), flush=True)

    with open(args.input_csv, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            processed += 1
            if added >= remaining_needed:
                break

            e = clean(row.get("email")).lower()
            if not is_email(e):
                continue

            if e in seen:
                continue

            if e in block_map:
                if row_score(row) > row_score(block_map[e]):
                    block_map[e] = row
                continue

            seen.add(e)
            block_map[e] = row
            block_emails.append(e)
            queued += 1

            if len(block_emails) >= args.block_emails:
                sendable_rows = await process_block(
                    args.api_url,
                    args.api_key,
                    block_emails,
                    block_map,
                    header,
                    batch_size=args.verify_batch_size,
                    concurrency=args.verify_concurrency,
                )

                for out_row in sendable_rows:
                    e2 = clean(out_row[6]).lower()
                    if is_email(e2):
                        all_existing.add(e2)

                if out_writer:
                    out_writer.writerows(sendable_rows)
                    out_fh.flush()

                for i in range(0, len(sendable_rows), args.sheet_chunk):
                    part = sendable_rows[i : i + args.sheet_chunk]
                    flush_sheet_rows(part)
                    added += len(part)
                    if added >= remaining_needed:
                        break

                block_map = {}
                block_emails = []

                elapsed = time.time() - start
                rate = processed / elapsed if elapsed > 0 else 0.0
                print("[progress] processed=%s queued=%s added=%s need_left=%s rate_rows_per_sec=%.1f" % (processed, queued, added, max(0, remaining_needed - added), rate), flush=True)

        if added < remaining_needed and block_emails:
            sendable_rows = await process_block(
                args.api_url,
                args.api_key,
                block_emails,
                block_map,
                header,
                batch_size=args.verify_batch_size,
                concurrency=args.verify_concurrency,
            )

            for out_row in sendable_rows:
                e2 = clean(out_row[6]).lower()
                if is_email(e2):
                    all_existing.add(e2)

            if out_writer:
                out_writer.writerows(sendable_rows)
                out_fh.flush()

            for i in range(0, len(sendable_rows), args.sheet_chunk):
                part = sendable_rows[i : i + args.sheet_chunk]
                flush_sheet_rows(part)
                added += len(part)
                if added >= remaining_needed:
                    break

    if out_fh:
        out_fh.close()

    elapsed = time.time() - start
    print("[done] processed=%s queued=%s added=%s secs=%.1f" % (processed, queued, added, elapsed), flush=True)


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
