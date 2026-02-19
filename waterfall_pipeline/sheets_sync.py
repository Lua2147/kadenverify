#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path
from urllib.parse import quote

import duckdb
import requests

try:
    from .qa import qa_assert_zero_overlap, write_qa_report
    from .schema import (
        PERSON_HEADERS_30,
        PLACEHOLDER_TOKENS,
        apply_row_defaults,
        clean,
        count_token_hits,
        detect_email_column,
        is_email,
    )
except ImportError:  # pragma: no cover
    from qa import qa_assert_zero_overlap, write_qa_report
    from schema import (
        PERSON_HEADERS_30,
        PLACEHOLDER_TOKENS,
        apply_row_defaults,
        clean,
        count_token_hits,
        detect_email_column,
        is_email,
    )


def parse_token(token_file: Path) -> str:
    raw = token_file.read_text(encoding="utf-8")
    keys = {}
    for k in ("client_id", "client_secret", "refresh_token", "token_uri", "access_token", "token"):
        m = re.search(rf'"{k}"\s*:\s*"([^"]+)"', raw)
        if m:
            keys[k] = m.group(1)

    tok = keys.get("access_token") or keys.get("token")
    if keys.get("refresh_token") and keys.get("client_id") and keys.get("client_secret"):
        rt = requests.post(
            keys.get("token_uri", "https://oauth2.googleapis.com/token"),
            data={
                "client_id": keys.get("client_id", ""),
                "client_secret": keys.get("client_secret", ""),
                "refresh_token": keys.get("refresh_token", ""),
                "grant_type": "refresh_token",
            },
            timeout=30,
        )
        if rt.status_code == 200:
            tok = rt.json()["access_token"]
    if not tok:
        raise RuntimeError("No usable Google access token")
    return tok


def get_sheet_meta(sheet_id: str, headers: dict) -> dict:
    r = requests.get(f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}", headers=headers, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"metadata failed {r.status_code}: {r.text[:300]}")
    return r.json()


def get_values(sheet_id: str, headers: dict, tab: str, rng: str):
    r = requests.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{quote(tab)}!{rng}",
        headers=headers,
        timeout=180,
    )
    if r.status_code != 200:
        raise RuntimeError(f"GET {tab}!{rng} failed {r.status_code}: {r.text[:300]}")
    return r.json().get("values", [])


def batch_update(sheet_id: str, headers: dict, reqs: list[dict]):
    r = requests.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}:batchUpdate",
        headers={**headers, "Content-Type": "application/json"},
        json={"requests": reqs},
        timeout=180,
    )
    if r.status_code != 200:
        raise RuntimeError(f"batchUpdate failed {r.status_code}: {r.text[:400]}")
    return r.json()


def ensure_tab(sheet_id: str, headers: dict, title: str, rows: int, cols: int = 30) -> int:
    meta = get_sheet_meta(sheet_id, headers)
    by_title = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    if title in by_title:
        sid = by_title[title]
        batch_update(
            sheet_id,
            headers,
            [
                {"updateCells": {"range": {"sheetId": sid}, "fields": "*"}},
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sid, "gridProperties": {"rowCount": max(rows, 1000), "columnCount": cols}},
                        "fields": "gridProperties(rowCount,columnCount)",
                    }
                },
            ],
        )
        return sid

    out = batch_update(sheet_id, headers, [{"addSheet": {"properties": {"title": title}}}])
    sid = out["replies"][0]["addSheet"]["properties"]["sheetId"]
    batch_update(
        sheet_id,
        headers,
        [
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sid, "gridProperties": {"rowCount": max(rows, 1000), "columnCount": cols}},
                    "fields": "gridProperties(rowCount,columnCount)",
                }
            }
        ],
    )
    return sid


def write_rows(sheet_id: str, headers: dict, tab: str, rows: list[list[str]], chunk: int = 1000) -> None:
    for i in range(0, len(rows), chunk):
        part = rows[i : i + chunk]
        start = i + 1
        end = i + len(part)
        rng = f"{tab}!A{start}:AD{end}"
        r = requests.put(
            f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{quote(rng)}",
            headers={**headers, "Content-Type": "application/json"},
            params={"valueInputOption": "USER_ENTERED"},
            json={"range": rng, "majorDimension": "ROWS", "values": part},
            timeout=180,
        )
        if r.status_code != 200:
            raise RuntimeError(f"write failed {rng}: {r.status_code} {r.text[:300]}")
        print(f"wrote_rows={start}-{end}")


def find_tier1_tab(titles: list[str], preferred: str) -> str:
    if preferred in titles:
        return preferred
    for t in titles:
        tl = t.lower()
        if "tier one first batch" in tl:
            return t
        if "tier1" in tl and "batch" in tl:
            return t
    raise RuntimeError("Could not find tier1 first-batch tab")


def tab_email_set(values: list[list[str]], header: list[str]) -> set[str]:
    rows = values
    declared = header.index("email") if "email" in header else 6
    email_idx = detect_email_column(rows, declared_index=declared)
    out = set()
    for row in rows:
        e = clean(row[email_idx] if email_idx < len(row) else "").lower()
        if is_email(e):
            out.add(e)
    return out


def headers_index(header: list[str]) -> dict[str, int]:
    return {h: i for i, h in enumerate(header)}


def map_row_to_contract(row: list[str], idx: dict[str, int]) -> dict[str, str]:
    out = {}
    for h in PERSON_HEADERS_30:
        i = idx.get(h, -1)
        out[h] = clean(row[i] if i >= 0 and i < len(row) else "")
    return out


def source_files(server_people_dir: Path, drive_src_dir: Path, run_dir: Path) -> list[Path]:
    files: list[Path] = []
    for p in [
        run_dir / "sheet_stage1_usable_enriched_30col.csv",
        run_dir / "provider_loop" / "provider_reverify_additional_usable.csv",
        run_dir / "provider_loop" / "provider_additional_usable.csv",
    ]:
        if p.exists():
            files.append(p)

    if server_people_dir.exists():
        for p in sorted(server_people_dir.glob("*.csv")):
            if "summary" in p.name:
                continue
            files.append(p)

    if drive_src_dir.exists():
        for p in sorted(drive_src_dir.rglob("*.csv")):
            files.append(p)

    seen = set()
    uniq: list[Path] = []
    for p in files:
        k = str(p)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(p)
    return uniq


def csv_header(path: Path) -> set[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return set(next(csv.reader(f)))


def build_query(path: Path, fields: list[str], cols: set[str]) -> str:
    exprs = ["lower(trim(s.email)) as email"]
    for f in fields:
        if f in cols:
            exprs.append(f"any_value(NULLIF(trim(CAST(s.{f} AS VARCHAR)), '')) as {f}")
        else:
            exprs.append(f"'' as {f}")
    return f"""
        SELECT {', '.join(exprs)}
        FROM read_csv_auto('{str(path)}', SAMPLE_SIZE=-1, IGNORE_ERRORS=true) s
        JOIN need n ON lower(trim(s.email)) = n.email
        GROUP BY 1
    """


def enrich_rows_from_sources(rows_by_email: dict[str, dict], files: list[Path]) -> Counter:
    fill_fields = [h for h in PERSON_HEADERS_30 if h != "email"]
    fills: Counter[str] = Counter()
    con = duckdb.connect()

    for p in files:
        try:
            cols = csv_header(p)
        except Exception:
            continue
        if "email" not in cols:
            continue

        need = [e for e, r in rows_by_email.items() if any(clean(r.get(f, "")).lower() in PLACEHOLDER_TOKENS for f in fill_fields)]
        if not need:
            break

        con.execute("DROP TABLE IF EXISTS need")
        con.execute("CREATE TEMP TABLE need(email VARCHAR)")
        con.executemany("INSERT INTO need VALUES (?)", [(e,) for e in need])

        try:
            out = con.execute(build_query(p, fill_fields, cols)).fetchall()
        except Exception:
            continue

        local = 0
        for rec in out:
            e = rec[0]
            cur = rows_by_email.get(e)
            if not cur:
                continue
            changed = False
            for j, f in enumerate(fill_fields, start=1):
                nv = clean(rec[j] if rec[j] is not None else "")
                if cur.get(f, "").strip().lower() in PLACEHOLDER_TOKENS and nv and nv.lower() not in PLACEHOLDER_TOKENS:
                    cur[f] = nv
                    fills[f] += 1
                    changed = True
            if changed:
                local += 1
        if local:
            print(f"source_applied={p.name} rows_changed={local}")

    con.close()
    return fills


def run(args: argparse.Namespace) -> None:
    token = parse_token(Path(args.token_file))
    headers = {"Authorization": f"Bearer {token}"}

    meta = get_sheet_meta(args.sheet_id, headers)
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]

    old_tab = args.old_tab
    run_tab = args.run_tab
    tier1_tab = find_tier1_tab(titles, args.tier1_tab)

    old_vals = get_values(args.sheet_id, headers, old_tab, "A1:AD")
    tier1_vals = get_values(args.sheet_id, headers, tier1_tab, "A1:AD")
    run_vals = get_values(args.sheet_id, headers, run_tab, "A1:AD")

    if not run_vals:
        raise RuntimeError(f"run tab empty: {run_tab}")

    old_header = old_vals[0] if old_vals else []
    tier1_header = tier1_vals[0] if tier1_vals else []
    run_header = run_vals[0]

    old_set = tab_email_set(old_vals[1:] if len(old_vals) > 1 else [], old_header)
    tier1_set = tab_email_set(tier1_vals[1:] if len(tier1_vals) > 1 else [], tier1_header)

    run_rows = run_vals[1:]
    run_idx = headers_index(run_header)

    rows_by_email: dict[str, dict] = {}
    for raw in run_rows:
        mapped = map_row_to_contract(raw, run_idx)
        e = mapped["email"].lower()
        if not is_email(e):
            continue
        if e in old_set or e in tier1_set:
            continue
        mapped["email"] = e
        if e in rows_by_email:
            # keep richer row
            score_cur = sum(1 for v in rows_by_email[e].values() if clean(v))
            score_new = sum(1 for v in mapped.values() if clean(v))
            if score_new > score_cur:
                rows_by_email[e] = mapped
        else:
            rows_by_email[e] = mapped

    print(f"run_rows_total={len(run_rows)}")
    print(f"old_unique_emails={len(old_set)}")
    print(f"tier1_tab={tier1_tab}")
    print(f"tier1_unique_emails={len(tier1_set)}")
    print(f"net_new_initial={len(rows_by_email)}")

    if args.enrich_from_sources:
        files = source_files(Path(args.server_people_dir), Path(args.drive_src_dir), Path(args.run_dir))
        print(f"source_files={len(files)}")
        fills = enrich_rows_from_sources(rows_by_email, files)
        print("fills_by_field")
        for k, v in fills.most_common(20):
            print(f"  {k}={v}")

    # finalize + write prep
    out_rows_dict = []
    for _, row in rows_by_email.items():
        out_rows_dict.append(apply_row_defaults(row))

    out_rows_dict.sort(key=lambda r: r["email"])

    out_values = [PERSON_HEADERS_30] + [[r.get(h, "") for h in PERSON_HEADERS_30] for r in out_rows_dict]

    sid = ensure_tab(args.sheet_id, headers, args.output_tab, rows=len(out_values) + 50, cols=30)
    write_rows(args.sheet_id, headers, args.output_tab, out_values)

    # QA gates before optional deletion
    new_set = set(rows_by_email.keys())
    qa_assert_zero_overlap(new_set, old_set, "net_new_vs_old")
    qa_assert_zero_overlap(new_set, tier1_set, "net_new_vs_tier1")
    hits = count_token_hits(out_values[1:], PERSON_HEADERS_30, {"unknown", "other"})

    deleted = 0
    if args.delete_redundant:
        meta2 = get_sheet_meta(args.sheet_id, headers)
        by_title = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta2.get("sheets", [])}
        reqs = []
        for t in args.delete_tab:
            sid_del = by_title.get(t)
            if sid_del is not None:
                reqs.append({"deleteSheet": {"sheetId": sid_del}})
        if reqs:
            batch_update(args.sheet_id, headers, reqs)
            deleted = len(reqs)
            print(f"deleted_tabs={deleted}")

    meta3 = get_sheet_meta(args.sheet_id, headers)
    final_tabs = [(s["properties"]["title"], s["properties"]["sheetId"]) for s in meta3.get("sheets", [])]

    report = {
        "old_tab": old_tab,
        "tier1_tab": tier1_tab,
        "run_tab": run_tab,
        "output_tab": args.output_tab,
        "output_sheet_id": sid,
        "rows_written": len(out_rows_dict),
        "old_unique": len(old_set),
        "tier1_unique": len(tier1_set),
        "net_new_unique": len(new_set),
        "overlap_with_old": len(new_set & old_set),
        "overlap_with_tier1": len(new_set & tier1_set),
        "unknown_other_total": sum(hits.values()),
        "deleted_tabs": deleted,
        "final_tabs": [{"title": t, "gid": gid} for t, gid in final_tabs],
    }
    write_qa_report(Path(args.qa_report), report)

    print(f"output_tab={args.output_tab}")
    print(f"output_sheet_id={sid}")
    print(f"rows_written={len(out_rows_dict)}")
    print(f"overlap_old={len(new_set & old_set)}")
    print(f"overlap_tier1={len(new_set & tier1_set)}")
    print(f"unknown_or_other_total={sum(hits.values())}")
    print("final_tabs")
    for t, gid in final_tabs:
        print(f"  {t}\t{gid}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Net-new sheet sync with 3-tab lifecycle and QA gates")
    p.add_argument("--sheet-id", required=True)
    p.add_argument("--token-file", default="/opt/mundi-princeps/config/token.json")
    p.add_argument("--old-tab", default="quick_wins_old_2026-02-11")
    p.add_argument("--tier1-tab", default="Sheet1")
    p.add_argument("--run-tab", default="quick_wins_run_valid_catchall_2026-02-16")
    p.add_argument("--output-tab", default="quick_wins_net_new_valid_catchall_2026-02-16")
    p.add_argument("--run-dir", default="/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211")
    p.add_argument("--server-people-dir", default="/data/local-machine-backup/20260211/people_contacts_exports")
    p.add_argument("--drive-src-dir", default="/tmp/drive_people_sources")
    p.add_argument("--enrich-from-sources", action="store_true")
    p.add_argument("--delete-redundant", action="store_true")
    p.add_argument(
        "--delete-tab",
        action="append",
        default=[
            "quick_wins_new_2026-02-15_reverify",
            "quick_wins_consolidated_2026-02-15",
            "quick_wins_new_only_valid_catchall_2026-02-16",
            "quick_wins_run_valid_catchall_2026-02-16",
        ],
    )
    p.add_argument("--qa-report", default="/tmp/waterfall_sheet_sync_qa.json")
    return p.parse_args()


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
