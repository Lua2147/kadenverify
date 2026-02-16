#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import time
from collections import Counter
from pathlib import Path

import aiohttp

from .qa import qa_assert_file, write_qa_report
from .schema import clean

GOOD = {"deliverable", "accept_all"}


def extract_domain(value: str) -> str:
    v = clean(value).lower()
    if not v:
        return ""
    if "@" in v:
        return v.split("@", 1)[1].strip()
    v = v.replace("https://", "").replace("http://", "").replace("www.", "")
    return v.split("/", 1)[0].split(":", 1)[0].strip()


def valid_name(name: str) -> bool:
    return len(clean(name)) >= 2


def make_contact_key(row: dict) -> str:
    first = clean(row.get("first_name"))
    last = clean(row.get("last_name"))
    domain = extract_domain(row.get("domain") or row.get("email"))
    if not (valid_name(first) and valid_name(last) and domain):
        return ""
    return f"{first.lower()}|{last.lower()}|{domain.lower()}"


def load_waterfall_rows(path: Path) -> tuple[dict[str, dict], list[str]]:
    rows_by_key = {}
    fieldnames: list[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        fieldnames = list(r.fieldnames or [])
        for row in r:
            key = make_contact_key(row)
            if key and key not in rows_by_key:
                rows_by_key[key] = dict(row)
    return rows_by_key, fieldnames


def load_unresolved_from_verified(path: Path) -> tuple[dict[str, dict], list[str]]:
    unresolved = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            key = clean(row.get("contact_key"))
            email = clean(row.get("email")).lower()
            source = clean(row.get("source"))
            prev = clean(row.get("verify_result")).lower()
            if not key or "@" not in email:
                continue
            if prev in GOOD:
                continue
            unresolved[key] = {
                "contact_key": key,
                "email": email,
                "source": source,
                "prev_result": prev or "unknown",
                "current_result": prev or "unknown",
                "resolved_iter": "",
            }
    return unresolved, []


def load_unresolved_from_state(path: Path) -> tuple[dict[str, dict], list[str]]:
    unresolved = {}
    iter_cols: list[str] = []
    if not path.exists() or path.stat().st_size == 0:
        return unresolved, iter_cols

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        fieldnames = list(r.fieldnames or [])
        iter_cols = [c for c in fieldnames if c.startswith("iter_") and c.endswith("_result")]
        for row in r:
            key = clean(row.get("contact_key"))
            email = clean(row.get("email")).lower()
            source = clean(row.get("source"))
            if not key or "@" not in email:
                continue

            prev = clean(row.get("prev_result") or row.get("verify_result")).lower()
            cur = clean(row.get("current_result") or prev or "unknown").lower()
            out = {
                "contact_key": key,
                "email": email,
                "source": source,
                "prev_result": prev or "unknown",
                "current_result": cur or "unknown",
                "resolved_iter": clean(row.get("resolved_iter")),
            }
            for c in iter_cols:
                out[c] = clean(row.get(c)).lower()
            unresolved[key] = out

    return unresolved, iter_cols


async def verify_emails(api_url: str, api_key: str, emails: list[str], batch_size: int, concurrency: int) -> dict[str, str]:
    headers = {"Content-Type": "application/json", "X-API-Key": api_key}
    batches = [emails[i:i + batch_size] for i in range(0, len(emails), batch_size)]
    sem = asyncio.Semaphore(concurrency)
    results = {}
    done = 0

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180)) as session:
        async def do_batch(i: int, ems: list[str]) -> None:
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
                                for item in data:
                                    e = clean(item.get("email")).lower()
                                    if e:
                                        results[e] = clean(item.get("result") or "unknown").lower() or "unknown"
                                done += 1
                                if done % 20 == 0 or done == len(batches):
                                    print(f"[reverify] batches {done}/{len(batches)}", flush=True)
                                return
                            if resp.status == 429:
                                await asyncio.sleep(attempt * 2)
                                continue
                            text = (await resp.text())[:160]
                            print(f"[reverify] batch {i} status={resp.status} body={text}", flush=True)
                            await asyncio.sleep(attempt)
                    except Exception as e:
                        print(f"[reverify] batch {i} error={e}", flush=True)
                        await asyncio.sleep(attempt)

        await asyncio.gather(*(do_batch(i + 1, b) for i, b in enumerate(batches)))

    return results


def write_state(path: Path, rows: dict[str, dict], iter_cols: list[str]) -> None:
    fields = ["contact_key", "email", "source", "prev_result"] + iter_cols + ["current_result", "resolved_iter"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for key in sorted(rows):
            w.writerow(rows[key])


def write_usable(path: Path, resolved_rows: list[dict], base_fields: list[str]) -> None:
    fields = list(base_fields)
    if "contact_key" not in fields:
        fields.append("contact_key")
    fields += ["new_email", "new_email_source", "new_email_verify_result"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in resolved_rows:
            w.writerow({k: row.get(k, "") for k in fields})


def write_summary(
    path: Path,
    input_verified: Path,
    input_waterfall: Path,
    state_out: Path,
    usable_out: Path,
    total: int,
    remaining: int,
    iter_counts: list[dict],
    final_counts: Counter,
    usable_rows: int,
    stop_reason: str,
) -> None:
    lines = [
        f"input_verified={input_verified}",
        f"input_waterfall={input_waterfall}",
        f"state_out={state_out}",
        f"usable_out={usable_out}",
        f"total_reverify_candidates={total}",
        f"remaining_unresolved={remaining}",
        f"usable_rows={usable_rows}",
        f"stop_reason={stop_reason}",
        "iteration_gains:",
    ]
    for s in iter_counts:
        lines.append(
            f"  - iter={s['iter']} queried={s['queried']} newly_deliverable={s['deliverable']} newly_catch_all={s['catch_all']} gains={s['gains']} gain_rate={s['gain_rate']:.6f} remaining={s['remaining']}"
        )
    lines.append("final_results:")
    for k, v in final_counts.most_common():
        lines.append(f"  - {k}: {v}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def run(args: argparse.Namespace) -> None:
    input_verified = Path(args.input_verified)
    input_waterfall = Path(args.input_waterfall)
    out_state = Path(args.out_state)
    out_usable = Path(args.out_usable)
    out_summary = Path(args.out_summary)
    qa_report = Path(args.qa_report)

    qa_assert_file(input_verified, "provider_candidates_verified")
    qa_assert_file(input_waterfall, "waterfall_unknown_undeliverable")

    rows_by_key, base_fields = load_waterfall_rows(input_waterfall)
    unresolved, iter_cols = load_unresolved_from_state(out_state)
    if unresolved:
        print(f"[reverify] resuming from state rows={len(unresolved)}", flush=True)
    else:
        unresolved, iter_cols = load_unresolved_from_verified(input_verified)

    total = len(unresolved)
    print(f"[reverify] loaded unresolved={total}", flush=True)

    if total == 0:
        write_state(out_state, unresolved, iter_cols)
        write_usable(out_usable, [], base_fields)
        write_summary(
            out_summary,
            input_verified,
            input_waterfall,
            out_state,
            out_usable,
            0,
            0,
            [],
            Counter(),
            0,
            "empty",
        )
        write_qa_report(qa_report, {"total": 0, "remaining": 0, "usable": 0, "stop_reason": "empty"})
        print("[reverify] nothing to do", flush=True)
        return

    iter_stats = []
    next_iter = len(iter_cols) + 1
    low_gain_streak = 0
    stop_reason = "max_iters"

    for offset in range(args.max_iters):
        i = next_iter + offset
        pending_keys = [k for k, r in unresolved.items() if r.get("current_result") not in GOOD]
        if not pending_keys:
            stop_reason = "resolved_all"
            break

        pending_emails = sorted({unresolved[k]["email"] for k in pending_keys if "@" in unresolved[k]["email"]})
        if not pending_emails:
            stop_reason = "no_pending_emails"
            break

        iter_col = f"iter_{i}_result"
        iter_cols.append(iter_col)

        t0 = time.time()
        verify_map = await verify_emails(
            api_url=args.api_url,
            api_key=args.api_key,
            emails=pending_emails,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
        )

        new_deliverable = 0
        new_catch_all = 0

        for k in pending_keys:
            row = unresolved[k]
            result = verify_map.get(row["email"], "unknown")
            row[iter_col] = result
            row["current_result"] = result
            if result in GOOD and not row.get("resolved_iter"):
                row["resolved_iter"] = str(i)
                if result == "deliverable":
                    new_deliverable += 1
                else:
                    new_catch_all += 1

        remaining = sum(1 for r in unresolved.values() if r.get("current_result") not in GOOD)
        gains = new_deliverable + new_catch_all
        gain_rate = gains / len(pending_keys) if pending_keys else 0.0
        dt = time.time() - t0

        print(
            f"[reverify] iter={i} pending={len(pending_keys)} emails={len(pending_emails)} "
            f"new_deliverable={new_deliverable} new_catch_all={new_catch_all} gains={gains} "
            f"gain_rate={gain_rate:.6f} remaining={remaining} mins={dt/60:.1f}",
            flush=True,
        )

        iter_stats.append(
            {
                "iter": i,
                "queried": len(pending_emails),
                "deliverable": new_deliverable,
                "catch_all": new_catch_all,
                "gains": gains,
                "gain_rate": gain_rate,
                "remaining": remaining,
            }
        )

        write_state(out_state, unresolved, iter_cols)

        if remaining == 0:
            stop_reason = "resolved_all"
            break

        # Delta stop logic: stop when gains flatten repeatedly.
        if gains <= args.gain_stop_abs or gain_rate < args.gain_stop_rate:
            low_gain_streak += 1
        else:
            low_gain_streak = 0

        if (
            len(pending_keys) >= args.min_pending_for_stop
            and low_gain_streak >= args.gain_stop_streak
        ):
            stop_reason = f"low_gain_streak_{low_gain_streak}"
            print(
                f"[reverify] stopping early due to low gains streak={low_gain_streak} "
                f"(abs<={args.gain_stop_abs} or rate<{args.gain_stop_rate})",
                flush=True,
            )
            break

        if offset < args.max_iters - 1:
            await asyncio.sleep(args.cooldown_seconds)

    resolved_rows = []
    final_counts: Counter[str] = Counter()
    for key, info in unresolved.items():
        result = info.get("current_result") or "unknown"
        final_counts[result] += 1
        if result not in GOOD:
            continue

        base = dict(rows_by_key.get(key, {}))
        if not base:
            first, last, domain = key.split("|", 2)
            base = {
                "first_name": first,
                "last_name": last,
                "full_name": f"{first} {last}".strip(),
                "domain": domain,
                "email": "",
                "result": "",
            }

        base["contact_key"] = key
        base["new_email"] = info["email"]
        base["new_email_source"] = f"provider_reverify:{info.get('source', 'provider')}"
        base["new_email_verify_result"] = result
        resolved_rows.append(base)

    write_state(out_state, unresolved, iter_cols)
    write_usable(out_usable, resolved_rows, base_fields)

    remaining = sum(1 for r in unresolved.values() if r.get("current_result") not in GOOD)
    write_summary(
        out_summary,
        input_verified,
        input_waterfall,
        out_state,
        out_usable,
        total,
        remaining,
        iter_stats,
        final_counts,
        len(resolved_rows),
        stop_reason,
    )

    usable_del = sum(1 for r in resolved_rows if r.get("new_email_verify_result") == "deliverable")
    usable_ca = sum(1 for r in resolved_rows if r.get("new_email_verify_result") == "accept_all")

    write_qa_report(
        qa_report,
        {
            "total_candidates": total,
            "remaining": remaining,
            "usable_total": len(resolved_rows),
            "usable_deliverable": usable_del,
            "usable_catch_all": usable_ca,
            "stop_reason": stop_reason,
            "iterations": len(iter_stats),
        },
    )

    print(
        f"[reverify] complete usable_total={len(resolved_rows)} "
        f"deliverable={usable_del} catch_all={usable_ca} remaining={remaining}",
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Resumable reverify loop with delta stop logic")
    p.add_argument("input_verified")
    p.add_argument("input_waterfall")
    p.add_argument("out_state")
    p.add_argument("out_usable")
    p.add_argument("out_summary")
    p.add_argument("api_key")
    p.add_argument("--api-url", default="http://127.0.0.1:8025")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--concurrency", type=int, default=12)
    p.add_argument("--max-iters", type=int, default=6)
    p.add_argument("--cooldown-seconds", type=int, default=20)
    p.add_argument("--gain-stop-abs", type=int, default=50)
    p.add_argument("--gain-stop-rate", type=float, default=0.0002)
    p.add_argument("--gain-stop-streak", type=int, default=2)
    p.add_argument("--min-pending-for-stop", type=int, default=50000)
    p.add_argument("--qa-report", default="")
    args = p.parse_args()
    if not args.qa_report:
        args.qa_report = str(Path(args.out_summary).with_name("provider_reverify_qa.json"))
    return args


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
