#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import time
from collections import Counter
from pathlib import Path
from typing import Callable, Awaitable

import aiohttp
import duckdb

try:
    from .qa import qa_assert_file, write_qa_report
    from .schema import SchemaValidationError, clean, is_email, read_csv_rows
except ImportError:  # pragma: no cover
    from qa import qa_assert_file, write_qa_report
    from schema import SchemaValidationError, clean, is_email, read_csv_rows

# local helper to avoid importing optional symbol from schema

def _extract_domain(value: str) -> str:
    v = (value or "").strip().lower()
    if not v:
        return ""
    if "@" in v:
        return v.split("@", 1)[1].strip()
    v = v.replace("https://", "").replace("http://", "").replace("www.", "")
    return v.split("/", 1)[0].split(":", 1)[0].strip()


GOOD_VERIFY_RESULTS = {"deliverable", "accept_all"}
GOOD_ALEADS_QUALITY = {"good", "ok", "verified"}


def _valid_name(name: str) -> bool:
    return len((name or "").strip()) >= 2


def load_candidates(input_csv: Path, dedup_csv: Path) -> tuple[dict[str, dict], list[str]]:
    qa_assert_file(input_csv, "waterfall_input")
    headers, rows = read_csv_rows(input_csv)
    if not headers:
        raise SchemaValidationError("Input CSV has no headers")

    rows_by_key: dict[str, dict] = {}
    for row in rows:
        first = clean(row.get("first_name"))
        last = clean(row.get("last_name"))
        domain = _extract_domain(row.get("domain", ""))
        if not domain:
            domain = _extract_domain(row.get("email", ""))
        if not (_valid_name(first) and _valid_name(last) and domain):
            continue

        key = f"{first.lower()}|{last.lower()}|{domain.lower()}"
        if key in rows_by_key:
            continue

        out = dict(row)
        out["first_name"] = first
        out["last_name"] = last
        out["domain"] = domain.lower()
        out["contact_key"] = key
        rows_by_key[key] = out

    dedup_csv.parent.mkdir(parents=True, exist_ok=True)
    with dedup_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["contact_key", "first_name", "last_name", "domain"])
        w.writeheader()
        for row in rows_by_key.values():
            w.writerow(
                {
                    "contact_key": row["contact_key"],
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "domain": row["domain"],
                }
            )

    return rows_by_key, headers


def apollo_local_lookup(dedup_csv: Path) -> dict[str, dict]:
    con = duckdb.connect()
    con.execute("ATTACH '/data/people-warehouse/apollo.duckdb' AS ap (READ_ONLY)")

    q = f"""
    CREATE OR REPLACE TEMP TABLE input_candidates AS
    SELECT
      contact_key,
      lower(trim(first_name)) AS first_name,
      lower(trim(last_name)) AS last_name,
      lower(trim(domain)) AS domain
    FROM read_csv_auto('{str(dedup_csv).replace("'", "''")}', header=true, all_varchar=true);

    CREATE OR REPLACE TEMP TABLE apollo_matches AS
    SELECT
      i.contact_key,
      lower(trim(p.email)) AS email
    FROM input_candidates i
    JOIN ap.persons p
      ON lower(trim(p.first_name)) = i.first_name
     AND lower(trim(p.last_name)) = i.last_name
     AND lower(trim(p.org_domain)) = i.domain
    WHERE p.email IS NOT NULL
      AND trim(p.email) <> '';
    """
    con.execute(q)

    rows = con.execute(
        """
        SELECT contact_key, min(email) AS email
        FROM apollo_matches
        GROUP BY 1
        """
    ).fetchall()
    con.close()

    out: dict[str, dict] = {}
    for key, email in rows:
        if is_email(email):
            out[key] = {"email": email.strip().lower(), "source": "apollo_local"}
    return out



def warehouse_org_domain_lookup(dedup_csv: Path, db_path: str, source: str) -> dict[str, dict]:
    con = duckdb.connect()
    con.execute(f"ATTACH '{db_path}' AS wh (READ_ONLY)")

    q = f"""
    CREATE OR REPLACE TEMP TABLE input_candidates AS
    SELECT
      contact_key,
      lower(trim(first_name)) AS first_name,
      lower(trim(last_name)) AS last_name,
      lower(trim(domain)) AS domain
    FROM read_csv_auto('{str(dedup_csv).replace("'", "''")}', header=true, all_varchar=true);

    CREATE OR REPLACE TEMP TABLE wh_matches AS
    SELECT
      i.contact_key,
      lower(trim(p.email)) AS email
    FROM input_candidates i
    JOIN wh.persons p
      ON lower(trim(p.first_name)) = i.first_name
     AND lower(trim(p.last_name)) = i.last_name
     AND lower(trim(p.org_domain)) = i.domain
    WHERE p.email IS NOT NULL
      AND trim(p.email) <> '';
    """
    con.execute(q)

    rows = con.execute(
        """
        SELECT contact_key, min(email) AS email
        FROM wh_matches
        GROUP BY 1
        """
    ).fetchall()
    con.close()

    out: dict[str, dict] = {}
    for key, email in rows:
        if is_email(email):
            out[key] = {"email": email.strip().lower(), "source": source}
    return out


def warehouse_ldpd_lookup(dedup_csv: Path) -> dict[str, dict]:
    con = duckdb.connect()
    con.execute("ATTACH '/data/people-warehouse/ldpd.duckdb' AS ldpd (READ_ONLY)")

    q = f"""
    CREATE OR REPLACE TEMP TABLE input_candidates AS
    SELECT
      contact_key,
      lower(trim(first_name)) AS first_name,
      lower(trim(last_name)) AS last_name,
      lower(trim(domain)) AS domain
    FROM read_csv_auto('{str(dedup_csv).replace("'", "''")}', header=true, all_varchar=true);

    CREATE OR REPLACE TEMP TABLE ldpd_matches AS
    SELECT DISTINCT
      i.contact_key,
      lower(trim(email)) AS email
    FROM input_candidates i
    JOIN ldpd.persons p
      ON lower(trim(p.first_name)) = i.first_name
     AND lower(trim(p.last_name)) = i.last_name
    CROSS JOIN UNNEST(p.emails) AS t(email)
    WHERE email IS NOT NULL
      AND trim(email) <> ''
      AND lower(split_part(email, '@', 2)) = i.domain;
    """
    con.execute(q)

    rows = con.execute(
        """
        SELECT contact_key, min(email) AS email
        FROM ldpd_matches
        GROUP BY 1
        """
    ).fetchall()
    con.close()

    out: dict[str, dict] = {}
    for key, email in rows:
        if is_email(email):
            out[key] = {"email": email.strip().lower(), "source": "warehouse_ldpd"}
    return out


async def run_provider(
    name: str,
    contacts: list[dict],
    worker: Callable[[aiohttp.ClientSession, dict], Awaitable[dict | None]],
    concurrency: int,
    progress_every: int = 1000,
) -> tuple[dict[str, dict], dict]:
    results: dict[str, dict] = {}
    q: asyncio.Queue = asyncio.Queue()
    for c in contacts:
        q.put_nowait(c)

    processed = 0
    found = 0
    errors = 0
    start = time.time()
    lock = asyncio.Lock()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async def worker_task() -> None:
            nonlocal processed, found, errors
            while True:
                try:
                    c = q.get_nowait()
                except asyncio.QueueEmpty:
                    break

                try:
                    res = await worker(session, c)
                    async with lock:
                        processed += 1
                        if res:
                            results[c["contact_key"]] = res
                            found += 1
                except Exception:
                    async with lock:
                        processed += 1
                        errors += 1
                finally:
                    q.task_done()

                if processed and processed % progress_every == 0:
                    elapsed = max(time.time() - start, 1)
                    rate = processed / elapsed * 60
                    print(f"[{name}] {processed}/{len(contacts)} found={found} errors={errors} ({rate:.0f}/min)", flush=True)

        tasks = [asyncio.create_task(worker_task()) for _ in range(max(concurrency, 1))]
        await asyncio.gather(*tasks)

    elapsed = max(time.time() - start, 1)
    metrics = {
        "queried": len(contacts),
        "processed": processed,
        "found": found,
        "errors": errors,
        "minutes": round(elapsed / 60, 2),
        "yield": (found / len(contacts)) if contacts else 0.0,
    }
    print(f"[{name}] done processed={processed} found={found} errors={errors} mins={elapsed/60:.1f}", flush=True)
    return results, metrics


async def verify_candidates(api_url: str, api_key: str, emails: list[str]) -> dict[str, str]:
    unique = sorted({e.strip().lower() for e in emails if is_email(e)})
    if not unique:
        return {}

    headers = {"Content-Type": "application/json", "X-API-Key": api_key}
    batch_size = 500
    concurrency = 10
    batches = [unique[i:i + batch_size] for i in range(0, len(unique), batch_size)]
    sem = asyncio.Semaphore(concurrency)
    results: dict[str, str] = {}
    done = 0

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180)) as session:
        async def do_batch(idx: int, ems: list[str]) -> None:
            nonlocal done
            async with sem:
                for attempt in range(1, 5):
                    try:
                        async with session.post(
                            f"{api_url}/verify/batch",
                            headers=headers,
                            json={"emails": ems},
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                for r in data:
                                    e = clean(r.get("email", "")).lower()
                                    if e:
                                        results[e] = clean(r.get("result", "unknown")).lower() or "unknown"
                                done += 1
                                if done % 20 == 0 or done == len(batches):
                                    print(f"[verify] batches {done}/{len(batches)}", flush=True)
                                return
                            if resp.status == 429:
                                await asyncio.sleep(attempt * 2)
                                continue
                            txt = (await resp.text())[:180]
                            print(f"[verify] batch={idx} status={resp.status} body={txt}", flush=True)
                            await asyncio.sleep(attempt)
                    except Exception as e:
                        print(f"[verify] batch={idx} error={e}", flush=True)
                        await asyncio.sleep(attempt)

        await asyncio.gather(*(do_batch(i + 1, b) for i, b in enumerate(batches)))

    return results


def load_json(path: Path) -> dict:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_found(path: Path) -> dict[str, dict]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    _, rows = read_csv_rows(path)
    out: dict[str, dict] = {}
    for row in rows:
        key = clean(row.get("contact_key"))
        email = clean(row.get("email")).lower()
        if key and is_email(email):
            out[key] = {"email": email, "source": clean(row.get("source"))}
    return out


def write_found(path: Path, found: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["contact_key", "email", "source"])
        w.writeheader()
        for k, v in found.items():
            w.writerow({"contact_key": k, "email": v.get("email", ""), "source": v.get("source", "")})


def load_verified(path: Path) -> dict[str, str]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    _, rows = read_csv_rows(path)
    out: dict[str, str] = {}
    for row in rows:
        e = clean(row.get("email")).lower()
        if is_email(e):
            out[e] = clean(row.get("verify_result", "unknown")).lower() or "unknown"
    return out


def write_verified(path: Path, found: dict[str, dict], verify_map: dict[str, str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["contact_key", "email", "source", "verify_result"])
        w.writeheader()
        for k, v in found.items():
            e = clean(v.get("email")).lower()
            w.writerow(
                {
                    "contact_key": k,
                    "email": e,
                    "source": clean(v.get("source")),
                    "verify_result": verify_map.get(e, "unknown"),
                }
            )


def update_yield_stats(path: Path, stage_metrics: dict[str, dict]) -> None:
    data = load_json(path)
    now = int(time.time())
    for stage, m in stage_metrics.items():
        rec = data.get(stage, {})
        q_total = int(rec.get("queried_total", 0)) + int(m.get("queried", 0))
        f_total = int(rec.get("found_total", 0)) + int(m.get("found", 0))
        data[stage] = {
            "queried_total": q_total,
            "found_total": f_total,
            "global_yield": (f_total / q_total) if q_total else 0.0,
            "last_yield": float(m.get("yield", 0.0)),
            "last_queried": int(m.get("queried", 0)),
            "last_found": int(m.get("found", 0)),
            "last_run_epoch": now,
        }
    save_json(path, data)


def rank_paid_stages(stats: dict, default_order: list[str]) -> list[str]:
    def key(stage: str):
        rec = stats.get(stage, {})
        # Favor recent last_yield first, then global yield, then default order.
        return (
            float(rec.get("last_yield", 0.0)),
            float(rec.get("global_yield", 0.0)),
            -default_order.index(stage),
        )

    ranked = sorted(default_order, key=key, reverse=True)
    return ranked


async def run(args: argparse.Namespace) -> None:
    input_csv = Path(args.input_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    dedup_csv = out_dir / "provider_candidates_dedup.csv"
    all_found_csv = out_dir / "provider_candidates_all.csv"
    verified_csv = out_dir / "provider_candidates_verified.csv"
    additional_usable_csv = out_dir / "provider_additional_usable.csv"
    summary_txt = out_dir / "provider_loop_summary.txt"
    state_json = Path(args.state_file) if args.state_file else (out_dir / "provider_loop_state.json")
    qa_report = out_dir / "provider_loop_qa.json"
    yield_stats_json = Path(args.yield_stats_file) if args.yield_stats_file else (out_dir / "provider_stage_yield.json")

    rows_by_key, input_fields = load_candidates(input_csv, dedup_csv)
    keys_all = set(rows_by_key.keys())
    print(f"Dedup candidates: {len(keys_all)}", flush=True)

    config = json.load(open(args.api_keys_path, "r", encoding="utf-8"))
    aleads_key = clean(config.get("a_leads", {}).get("api_key", ""))
    prospeo_key = clean(config.get("prospeo", {}).get("api_key", ""))
    apollo_key = clean(config.get("apollo", {}).get("api_key", ""))
    pdl_key = clean(config.get("peopledatalabs", {}).get("api_key", ""))

    state = load_json(state_json)
    if args.force_restart:
        state = {}

    completed = set(state.get("completed_stages", []))
    source_counts = Counter(state.get("source_counts", {}))
    stage_metrics = state.get("stage_metrics", {})

    found_by_key = load_found(all_found_csv)
    # Keep only keys still present in current input universe.
    found_by_key = {k: v for k, v in found_by_key.items() if k in keys_all}

    def remaining_rows() -> list[dict]:
        return [rows_by_key[k] for k in keys_all if k not in found_by_key]

    def persist_state() -> None:
        payload = {
            "input_csv": str(input_csv),
            "out_dir": str(out_dir),
            "completed_stages": sorted(completed),
            "source_counts": dict(source_counts),
            "stage_metrics": stage_metrics,
            "found_count": len(found_by_key),
            "updated_epoch": int(time.time()),
        }
        save_json(state_json, payload)
        write_found(all_found_csv, found_by_key)

    print(f"Resume state: completed={sorted(completed)} found={len(found_by_key)}", flush=True)

    # Stage 1: Apollo local
    if "apollo_local" not in completed:
        print("Stage 1: Apollo local DB", flush=True)
        t0 = time.time()
        ap_local = apollo_local_lookup(dedup_csv)
        for k, v in ap_local.items():
            if k in keys_all and k not in found_by_key:
                found_by_key[k] = v
        source_counts["apollo_local"] = source_counts.get("apollo_local", 0) + len(ap_local)
        metrics = {
            "queried": len(keys_all),
            "found": len(ap_local),
            "errors": 0,
            "minutes": round((time.time() - t0) / 60, 2),
            "yield": (len(ap_local) / len(keys_all)) if keys_all else 0.0,
        }
        stage_metrics["apollo_local"] = metrics
        completed.add("apollo_local")
        persist_state()

    

    # Stage 1b: Qualified local DB
    if "warehouse_qualified" not in completed:
        print("Stage 1b: Qualified local DB", flush=True)
        t0 = time.time()
        wh = warehouse_org_domain_lookup(
            dedup_csv,
            "/data/people-warehouse/qualified.duckdb",
            source="warehouse_qualified",
        )
        added = 0
        for k, v in wh.items():
            if k in keys_all and k not in found_by_key:
                found_by_key[k] = v
                added += 1
        source_counts["warehouse_qualified"] = source_counts.get("warehouse_qualified", 0) + added
        stage_metrics["warehouse_qualified"] = {
            "queried": len(keys_all),
            "found": added,
            "errors": 0,
            "minutes": round((time.time() - t0) / 60, 2),
            "yield": (added / len(keys_all)) if keys_all else 0.0,
        }
        completed.add("warehouse_qualified")
        persist_state()

    # Stage 1c: 82m local DB
    if "warehouse_82m" not in completed:
        print("Stage 1c: 82m local DB", flush=True)
        t0 = time.time()
        wh = warehouse_org_domain_lookup(
            dedup_csv,
            "/data/people-warehouse/82m.duckdb",
            source="warehouse_82m",
        )
        added = 0
        for k, v in wh.items():
            if k in keys_all and k not in found_by_key:
                found_by_key[k] = v
                added += 1
        source_counts["warehouse_82m"] = source_counts.get("warehouse_82m", 0) + added
        stage_metrics["warehouse_82m"] = {
            "queried": len(keys_all),
            "found": added,
            "errors": 0,
            "minutes": round((time.time() - t0) / 60, 2),
            "yield": (added / len(keys_all)) if keys_all else 0.0,
        }
        completed.add("warehouse_82m")
        persist_state()

    # Stage 1d: L-Series local DB
    if "warehouse_l_series" not in completed:
        print("Stage 1d: L-Series local DB", flush=True)
        t0 = time.time()
        wh = warehouse_org_domain_lookup(
            dedup_csv,
            "/data/people-warehouse/l_series.duckdb",
            source="warehouse_l_series",
        )
        added = 0
        for k, v in wh.items():
            if k in keys_all and k not in found_by_key:
                found_by_key[k] = v
                added += 1
        source_counts["warehouse_l_series"] = source_counts.get("warehouse_l_series", 0) + added
        stage_metrics["warehouse_l_series"] = {
            "queried": len(keys_all),
            "found": added,
            "errors": 0,
            "minutes": round((time.time() - t0) / 60, 2),
            "yield": (added / len(keys_all)) if keys_all else 0.0,
        }
        completed.add("warehouse_l_series")
        persist_state()

    # Stage 1e: LDPD local DB
    if "warehouse_ldpd" not in completed:
        print("Stage 1e: LDPD local DB", flush=True)
        t0 = time.time()
        wh = warehouse_ldpd_lookup(dedup_csv)
        added = 0
        for k, v in wh.items():
            if k in keys_all and k not in found_by_key:
                found_by_key[k] = v
                added += 1
        source_counts["warehouse_ldpd"] = source_counts.get("warehouse_ldpd", 0) + added
        stage_metrics["warehouse_ldpd"] = {
            "queried": len(keys_all),
            "found": added,
            "errors": 0,
            "minutes": round((time.time() - t0) / 60, 2),
            "yield": (added / len(keys_all)) if keys_all else 0.0,
        }
        completed.add("warehouse_ldpd")
        persist_state()
    print(f"Remaining after Stage1: {len(remaining_rows())}", flush=True)

    async def worker_aleads(session: aiohttp.ClientSession, c: dict) -> dict | None:
        if not aleads_key:
            return None
        payload = {"data": {"first_name": c["first_name"], "last_name": c["last_name"], "website": c["domain"]}}
        for attempt in range(1, 3):
            async with session.post(
                "https://api.a-leads.co/gateway/v1/search/find-email",
                headers={"x-api-key": aleads_key, "Content-Type": "application/json"},
                json=payload,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    info = data.get("data") or {}
                    email = clean(info.get("email", "")).lower()
                    quality = clean(info.get("quality", "")).lower()
                    if is_email(email) and quality in GOOD_ALEADS_QUALITY:
                        return {"email": email, "source": "a_leads", "quality": quality}
                    return None
                if resp.status == 429:
                    await asyncio.sleep(2 * attempt)
                    continue
                return None
        return None

    async def worker_prospeo(session: aiohttp.ClientSession, c: dict) -> dict | None:
        if not prospeo_key:
            return None
        payload = {
            "only_verified_email": True,
            "data": {"full_name": f"{c['first_name']} {c['last_name']}", "company_website": c["domain"]},
        }
        for attempt in range(1, 3):
            async with session.post(
                "https://api.prospeo.io/enrich-person",
                headers={"X-KEY": prospeo_key, "Content-Type": "application/json"},
                json=payload,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    person = data.get("person") or {}
                    email_info = person.get("email") or {}
                    email = clean(email_info.get("email", "")).lower()
                    status = clean(email_info.get("status", "")).upper()
                    if is_email(email) and status == "VERIFIED":
                        return {"email": email, "source": "prospeo", "status": status}
                    return None
                if resp.status == 429:
                    await asyncio.sleep(2 * attempt)
                    continue
                return None
        return None

    async def worker_apollo_api(session: aiohttp.ClientSession, c: dict) -> dict | None:
        if not apollo_key:
            return None
        payload = {
            "first_name": c["first_name"],
            "last_name": c["last_name"],
            "domain": c["domain"],
            "organization_name": c["domain"].split(".")[0],
            "reveal_personal_emails": False,
        }
        for attempt in range(1, 3):
            async with session.post(
                "https://api.apollo.io/v1/people/match",
                headers={"X-Api-Key": apollo_key, "Content-Type": "application/json", "Cache-Control": "no-cache"},
                json=payload,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    person = data.get("person") or {}
                    email = clean(person.get("email", "")).lower()
                    if is_email(email):
                        return {"email": email, "source": "apollo_api"}
                    return None
                if resp.status == 429:
                    await asyncio.sleep(5 * attempt)
                    continue
                return None
        return None


    async def worker_peopledatalabs(session: aiohttp.ClientSession, c: dict) -> dict | None:
        if not pdl_key:
            return None

        company = c.get("domain") or c.get("company") or ""
        if not company:
            return None

        url = "https://api.peopledatalabs.com/v5/person/enrich"
        headers = {"X-Api-Key": pdl_key}
        params = {
            "first_name": c.get("first_name", ""),
            "last_name": c.get("last_name", ""),
            "company": company,
            "required": "work_email",
            "include_if_matched": "true",
            "pretty": "false",
            "titlecase": "false",
            "data_include": "work_email",
        }

        for attempt in range(1, 4):
            try:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 200:
                        payload = await resp.json()
                        matched = payload.get("matched") or []
                        if matched and "company" not in {str(x) for x in matched}:
                            return None

                        person = payload.get("data") or {}
                        email = clean(person.get("work_email", "")).lower()
                        if is_email(email):
                            return {"email": email, "source": "peopledatalabs"}
                        return None

                    if resp.status in (400, 404):
                        return None
                    if resp.status == 429:
                        await asyncio.sleep(2 * attempt)
                        continue
                    return None
            except Exception:
                await asyncio.sleep(attempt)

        return None

    stage_workers: dict[str, tuple[Callable, int, int]] = {
        "a_leads": (worker_aleads, 8, 1000),
        "prospeo": (worker_prospeo, 10, 1000),
        "apollo_api": (worker_apollo_api, 4, 500),
        "peopledatalabs": (worker_peopledatalabs, 4, 500),
    }

    paid_default = ["a_leads", "prospeo", "apollo_api", "peopledatalabs"]
    yield_stats = load_json(yield_stats_json)
    paid_ranked = rank_paid_stages(yield_stats, paid_default)
    print(f"Paid stage order (yield-ranked): {paid_ranked}", flush=True)

    for stage in paid_ranked:
        if stage in completed:
            continue

        remaining = remaining_rows()
        if not remaining:
            break

        prev_yield = float((yield_stats.get(stage) or {}).get("global_yield", 0.0))
        if args.skip_low_yield and prev_yield < args.min_stage_yield:
            print(f"Skipping stage={stage} due to low historical yield={prev_yield:.4f}", flush=True)
            completed.add(stage)
            stage_metrics[stage] = {
                "queried": len(remaining),
                "found": 0,
                "errors": 0,
                "minutes": 0.0,
                "yield": 0.0,
                "skipped": True,
                "skip_reason": f"historical_yield<{args.min_stage_yield}",
            }
            persist_state()
            continue

        worker, conc, progress_every = stage_workers[stage]
        print(f"Stage: {stage} remaining={len(remaining)}", flush=True)
        stage_found, metrics = await run_provider(stage, remaining, worker, concurrency=conc, progress_every=progress_every)
        for k, v in stage_found.items():
            if k in keys_all and k not in found_by_key:
                found_by_key[k] = v
        source_counts[stage] = source_counts.get(stage, 0) + len(stage_found)
        stage_metrics[stage] = metrics
        completed.add(stage)
        persist_state()

    print(f"Total provider-found candidates: {len(found_by_key)}", flush=True)

    # Verify only missing emails if this is a resume run.
    verify_map = load_verified(verified_csv)
    need_verify = [v.get("email", "") for v in found_by_key.values() if v.get("email", "").lower() not in verify_map]
    if need_verify:
        print(f"Verifying provider candidates missing={len(need_verify)}", flush=True)
        new_verify = await verify_candidates(args.verify_api_url, args.kadenverify_api_key, need_verify)
        verify_map.update(new_verify)
    write_verified(verified_csv, found_by_key, verify_map)

    add_deliverable = 0
    add_catch_all = 0
    extra_fields = ["new_email", "new_email_source", "new_email_verify_result"]
    output_fields = list(input_fields)
    if "contact_key" not in output_fields:
        output_fields.append("contact_key")

    with additional_usable_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=output_fields + extra_fields, extrasaction="ignore")
        w.writeheader()
        for k, v in found_by_key.items():
            e = clean(v.get("email", "")).lower()
            result = verify_map.get(e, "unknown")
            if result in GOOD_VERIFY_RESULTS:
                base = rows_by_key[k]
                row = {name: base.get(name, "") for name in output_fields}
                row["contact_key"] = k
                row["new_email"] = e
                row["new_email_source"] = clean(v.get("source", ""))
                row["new_email_verify_result"] = result
                w.writerow(row)
                if result == "deliverable":
                    add_deliverable += 1
                else:
                    add_catch_all += 1

    vr = Counter(verify_map.values())
    summary_lines = [
        f"input={input_csv}",
        f"dedup_candidates={len(keys_all)}",
        f"provider_found_total={len(found_by_key)}",
        f"additional_usable_total={add_deliverable + add_catch_all}",
        f"additional_deliverable={add_deliverable}",
        f"additional_catch_all={add_catch_all}",
        f"state_file={state_json}",
        f"yield_stats_file={yield_stats_json}",
        f"paid_stage_order={','.join(paid_ranked)}",
        "found_by_source:",
    ]
    for s, c in source_counts.items():
        summary_lines.append(f"  - {s}: {c}")

    summary_lines.append("stage_metrics:")
    for s in ["apollo_local"] + paid_ranked:
        if s in stage_metrics:
            m = stage_metrics[s]
            summary_lines.append(
                f"  - {s}: queried={m.get('queried', 0)} found={m.get('found', 0)} yield={m.get('yield', 0):.6f} mins={m.get('minutes', 0)}"
            )

    summary_lines.append("verification_results:")
    for s, c in vr.most_common():
        summary_lines.append(f"  - {s}: {c}")

    summary_txt.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    # Persist final state and global yield stats for stage routing on future runs.
    persist_state()
    update_yield_stats(yield_stats_json, {k: v for k, v in stage_metrics.items() if not v.get("skipped")})

    write_qa_report(
        qa_report,
        {
            "dedup_candidates": len(keys_all),
            "provider_found_total": len(found_by_key),
            "additional_deliverable": add_deliverable,
            "additional_catch_all": add_catch_all,
            "verification_counts": dict(vr),
            "completed_stages": sorted(completed),
        },
    )

    print(f"Wrote: {all_found_csv}")
    print(f"Wrote: {verified_csv}")
    print(f"Wrote: {additional_usable_csv}")
    print(f"Wrote: {summary_txt}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Resumable full provider waterfall loop")
    p.add_argument("input_csv")
    p.add_argument("out_dir")
    p.add_argument("kadenverify_api_key")
    p.add_argument("--verify-api-url", default="http://127.0.0.1:8025")
    p.add_argument("--api-keys-path", default="/opt/mundi-princeps/config/api_keys.json")
    p.add_argument("--state-file", default="")
    p.add_argument("--yield-stats-file", default="")
    p.add_argument("--force-restart", action="store_true")
    p.add_argument("--skip-low-yield", action="store_true")
    p.add_argument("--min-stage-yield", type=float, default=0.0005)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
