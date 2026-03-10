#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import time
from collections import Counter
from pathlib import Path

import aiohttp

try:
    from .qa import qa_assert_file, write_qa_report
    from .schema import clean
except ImportError:  # pragma: no cover
    from qa import qa_assert_file, write_qa_report
    from schema import clean

GOOD = {"deliverable", "accept_all"}


def shard_for_key(contact_key: str, shard_count: int) -> int:
    digest = hashlib.blake2b(contact_key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % max(1, shard_count)


def in_shard(contact_key: str, shard_count: int, shard_index: int) -> bool:
    if shard_count <= 1:
        return True
    return shard_for_key(contact_key, shard_count) == shard_index


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


def parse_int(value: str | int | None, default: int = 0) -> int:
    try:
        return int(clean(str(value or "")))
    except Exception:
        return default


def parse_csv_tokens(value: str) -> list[str]:
    if not value:
        return []
    return [clean(v).lower() for v in value.split(",") if clean(v)]


def normalize_domain_tokens(values: list[str]) -> tuple[str, ...]:
    out: set[str] = set()
    for value in values:
        token = clean(value).lower().lstrip(".")
        if token:
            out.add(token)
    return tuple(sorted(out))


def load_domain_tokens_file(path_value: str) -> list[str]:
    path_str = clean(path_value)
    if not path_str:
        return []
    path = Path(path_str)
    if not path.exists() or path.stat().st_size == 0:
        return []
    out: list[str] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip().lower().lstrip(".")
            if line:
                out.append(line)
    return out


def email_domain(value: str) -> str:
    email = clean(value).lower()
    if "@" not in email:
        return ""
    return email.split("@", 1)[1].strip()


def domain_matches_suffix(domain: str, suffixes: tuple[str, ...]) -> bool:
    if not domain or not suffixes:
        return False
    for suffix in suffixes:
        if domain == suffix or domain.endswith("." + suffix):
            return True
    return False


def has_prior_touch(row: dict, iter_cols: list[str]) -> bool:
    if parse_int(row.get("unknown_streak"), 0) > 0:
        return True
    for col in iter_cols:
        if clean(row.get(col)):
            return True
    return False


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
                "unknown_streak": "0",
                "next_retry_iter": "",
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
        has_unknown_streak = "unknown_streak" in fieldnames
        has_next_retry_iter = "next_retry_iter" in fieldnames
        for row in r:
            key = clean(row.get("contact_key"))
            email = clean(row.get("email")).lower()
            source = clean(row.get("source"))
            if not key or "@" not in email:
                continue

            prev = clean(row.get("prev_result") or row.get("verify_result")).lower()
            cur = clean(row.get("current_result") or prev or "unknown").lower()

            unknown_streak = max(0, parse_int(row.get("unknown_streak"), 0))
            next_retry_iter = max(0, parse_int(row.get("next_retry_iter"), 0))

            if not has_unknown_streak:
                streak = 0
                for c in reversed(iter_cols):
                    value = clean(row.get(c)).lower()
                    if value == "unknown":
                        streak += 1
                        continue
                    if value:
                        break
                unknown_streak = streak

            if not has_next_retry_iter:
                next_retry_iter = 0

            out = {
                "contact_key": key,
                "email": email,
                "source": source,
                "prev_result": prev or "unknown",
                "current_result": cur or "unknown",
                "resolved_iter": clean(row.get("resolved_iter")),
                "unknown_streak": str(unknown_streak),
                "next_retry_iter": str(next_retry_iter) if next_retry_iter > 0 else "",
            }
            for c in iter_cols:
                out[c] = clean(row.get(c)).lower()
            unresolved[key] = out

    return unresolved, iter_cols


async def verify_emails(
    api_url: str,
    api_key: str,
    emails: list[str],
    batch_size: int,
    concurrency: int,
    request_timeout_seconds: int,
    batch_max_attempts: int,
) -> dict[str, str]:
    headers = {"Content-Type": "application/json", "X-API-Key": api_key}
    batches = [emails[i:i + batch_size] for i in range(0, len(emails), batch_size)]
    sem = asyncio.Semaphore(concurrency)
    results = {}
    done = 0
    post_kwargs = {}
    if api_url.lower().startswith("http://"):
        # Avoid SSL handshakes against plain HTTP endpoints.
        post_kwargs["ssl"] = False

    timeout = max(30, int(request_timeout_seconds))
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:

        async def do_batch(i: int, ems: list[str]) -> None:
            nonlocal done
            async with sem:
                for attempt in range(1, max(1, int(batch_max_attempts)) + 1):
                    try:
                        async with session.post(
                            f"{api_url}/verify/batch",
                            headers=headers,
                            json={"emails": ems},
                            **post_kwargs,
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
                            if attempt < max(1, int(batch_max_attempts)):
                                await asyncio.sleep(attempt)
                    except Exception as e:
                        print(f"[reverify] batch {i} error={e!r}", flush=True)
                        if attempt < max(1, int(batch_max_attempts)):
                            await asyncio.sleep(attempt)

        await asyncio.gather(*(do_batch(i + 1, b) for i, b in enumerate(batches)))

    return results


def write_state(path: Path, rows: dict[str, dict], iter_cols: list[str]) -> None:
    fields = ["contact_key", "email", "source", "prev_result"] + iter_cols + [
        "current_result",
        "resolved_iter",
        "unknown_streak",
        "next_retry_iter",
    ]
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
            f"  - iter={s['iter']} pending={s.get('pending', s['queried'])} eligible={s.get('eligible', s['queried'])} "
            f"queried={s['queried']} verify_miss={s.get('verify_miss', 0)} backoff_skipped={s.get('backoff_skipped', 0)} "
            f"first_touch_skipped={s.get('first_touch_skipped', 0)} dead_domain_skipped={s.get('dead_domain_skipped', 0)} "
            f"hot_eligible={s.get('hot_eligible', 0)} hot_selected={s.get('hot_selected', 0)} "
            f"cold_eligible={s.get('cold_eligible', 0)} cold_selected={s.get('cold_selected', 0)} "
            f"deferred_hot={s.get('deferred_hot', 0)} deferred_cold={s.get('deferred_cold', 0)} "
            f"newly_deliverable={s['deliverable']} newly_catch_all={s['catch_all']} "
            f"gains={s['gains']} gain_rate={s['gain_rate']:.6f} "
            f"gains_per_10k={s.get('gains_per_10k', 0.0):.2f} remaining={s['remaining']}"
        )
    lines.append("final_results:")
    for k, v in final_counts.most_common():
        lines.append(f"  - {k}: {v}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def run(args: argparse.Namespace) -> None:
    global GOOD
    parsed_good = set(parse_csv_tokens(args.good_results))
    GOOD = parsed_good or {"deliverable", "accept_all"}

    input_verified = Path(args.input_verified)
    input_waterfall = Path(args.input_waterfall)
    out_state = Path(args.out_state)
    out_usable = Path(args.out_usable)
    out_summary = Path(args.out_summary)
    qa_report = Path(args.qa_report)

    qa_assert_file(input_verified, "provider_candidates_verified")
    qa_assert_file(input_waterfall, "waterfall_unknown_undeliverable")

    rows_by_key, base_fields = load_waterfall_rows(input_waterfall)
    if args.force_load_from_verified:
        unresolved, iter_cols = load_unresolved_from_verified(input_verified)
        print(f"[reverify] force_load_from_verified rows={len(unresolved)}", flush=True)
    else:
        resume_state = Path(args.resume_state) if args.resume_state else out_state
        unresolved, iter_cols = load_unresolved_from_state(resume_state)
        if unresolved:
            print(f"[reverify] resuming from state rows={len(unresolved)}", flush=True)
        else:
            unresolved, iter_cols = load_unresolved_from_verified(input_verified)

    owns_key = lambda key: in_shard(key, args.shard_count, args.shard_index)
    total = sum(1 for key in unresolved if owns_key(key))
    print(f"[reverify] loaded unresolved={total}", flush=True)
    if args.shard_count > 1:
        print(
            f"[reverify] shard_index={args.shard_index} shard_count={args.shard_count} "
            f"owned_rows={total} total_rows={len(unresolved)}",
            flush=True,
        )

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
    hot_source_prefixes = tuple(parse_csv_tokens(args.hot_source_prefixes))
    hot_domain_suffixes = normalize_domain_tokens(parse_csv_tokens(args.hot_domain_suffixes))
    skip_domain_suffixes = normalize_domain_tokens(
        parse_csv_tokens(args.skip_domain_suffixes) + load_domain_tokens_file(args.skip_domain_file)
    )

    if hot_source_prefixes or hot_domain_suffixes:
        print(
            f"[reverify] hot_mode strict_hot_only={int(args.strict_hot_only)} "
            f"first_touch_only={int(args.first_touch_only)} "
            f"hot_source_prefixes={len(hot_source_prefixes)} hot_domain_suffixes={len(hot_domain_suffixes)}",
            flush=True,
        )
    if skip_domain_suffixes:
        print(
            f"[reverify] skip_domains enabled={len(skip_domain_suffixes)} "
            f"source_file={clean(args.skip_domain_file) or 'none'}",
            flush=True,
        )

    if args.unknown_streak_lock > 0:
        retry_gap = max(args.unknown_retry_gap_iters, 1)
        primed = 0
        for key, row in unresolved.items():
            if not owns_key(key):
                continue
            if row.get("current_result") in GOOD:
                continue
            unknown_streak = parse_int(row.get("unknown_streak"), 0)
            next_retry_iter = parse_int(row.get("next_retry_iter"), 0)
            if unknown_streak >= args.unknown_streak_lock and next_retry_iter <= 0:
                row["next_retry_iter"] = str(next_iter + retry_gap)
                primed += 1
        if primed:
            print(
                f"[reverify] primed_backoff_rows={primed} "
                f"unknown_streak_lock={args.unknown_streak_lock} retry_gap_iters={retry_gap}",
                flush=True,
            )

    for offset in range(args.max_iters):
        i = next_iter + offset
        all_pending_keys = [k for k, r in unresolved.items() if r.get("current_result") not in GOOD and owns_key(k)]
        if not all_pending_keys:
            stop_reason = "resolved_all"
            break

        pending_hot_keys = []
        pending_cold_keys = []
        backoff_skipped = 0
        first_touch_skipped = 0
        dead_domain_skipped = 0
        for key in all_pending_keys:
            row = unresolved[key]
            next_retry_iter = parse_int(row.get("next_retry_iter"), 0)
            if next_retry_iter > i:
                backoff_skipped += 1
                continue
            if args.first_touch_only and has_prior_touch(row, iter_cols):
                first_touch_skipped += 1
                continue

            domain = email_domain(row.get("email"))
            if skip_domain_suffixes and domain_matches_suffix(domain, skip_domain_suffixes):
                dead_domain_skipped += 1
                continue

            source = clean(row.get("source")).lower()
            unknown_streak = parse_int(row.get("unknown_streak"), 0)
            is_hot = False
            if hot_source_prefixes and any(source.startswith(pfx) for pfx in hot_source_prefixes):
                is_hot = True
            elif hot_domain_suffixes and domain_matches_suffix(domain, hot_domain_suffixes):
                is_hot = True
            elif args.fresh_unknown_streak_max >= 0 and unknown_streak <= args.fresh_unknown_streak_max:
                is_hot = True

            if is_hot:
                pending_hot_keys.append(key)
            else:
                pending_cold_keys.append(key)

        pending_hot_keys.sort(
            key=lambda k: (
                parse_int(unresolved[k].get("unknown_streak"), 0),
                unresolved[k].get("contact_key", ""),
            )
        )
        pending_cold_keys.sort(
            key=lambda k: (
                parse_int(unresolved[k].get("unknown_streak"), 0),
                unresolved[k].get("contact_key", ""),
            )
        )

        hot_eligible = len(pending_hot_keys)
        cold_eligible = len(pending_cold_keys)
        hot_selected = hot_eligible
        cold_selected = cold_eligible
        deferred_hot = 0
        deferred_cold = 0

        if args.max_pending_per_iter > 0:
            max_rows = args.max_pending_per_iter
            hot_quota = args.hot_priority_quota if args.hot_priority_quota > 0 else max_rows
            if args.strict_hot_only:
                chosen_hot = pending_hot_keys[:max_rows]
                chosen_cold = []
            else:
                chosen_hot = pending_hot_keys[: min(hot_quota, max_rows)]
                remaining_slots = max_rows - len(chosen_hot)

                if remaining_slots > 0 and len(chosen_hot) < len(pending_hot_keys):
                    extra_hot = pending_hot_keys[len(chosen_hot) : len(chosen_hot) + remaining_slots]
                    chosen_hot.extend(extra_hot)
                    remaining_slots = max_rows - len(chosen_hot)

                chosen_cold = pending_cold_keys[: max(0, remaining_slots)]

            hot_selected = len(chosen_hot)
            cold_selected = len(chosen_cold)
            deferred_hot = hot_eligible - hot_selected
            deferred_cold = cold_eligible - cold_selected
            pending_keys = chosen_hot + chosen_cold
        else:
            pending_keys = pending_hot_keys if args.strict_hot_only else (pending_hot_keys + pending_cold_keys)
            hot_selected = len(pending_hot_keys)
            cold_selected = 0 if args.strict_hot_only else len(pending_cold_keys)
            deferred_hot = 0
            deferred_cold = cold_eligible - cold_selected

        deferred_pending = deferred_hot + deferred_cold

        iter_col = f"iter_{i}_result"
        iter_cols.append(iter_col)

        if not pending_keys:
            remaining = len(all_pending_keys)
            print(
                f"[reverify] iter={i} pending={len(all_pending_keys)} eligible=0 backoff_skipped={backoff_skipped} "
                f"first_touch_skipped={first_touch_skipped} dead_domain_skipped={dead_domain_skipped} "
                f"hot_eligible={hot_eligible} cold_eligible={cold_eligible} action=skip_no_eligible",
                flush=True,
            )
            iter_stats.append(
                {
                    "iter": i,
                    "pending": len(all_pending_keys),
                    "eligible": 0,
                    "queried": 0,
                    "verify_miss": 0,
                    "backoff_skipped": backoff_skipped,
                    "first_touch_skipped": first_touch_skipped,
                    "dead_domain_skipped": dead_domain_skipped,
                    "hot_eligible": hot_eligible,
                    "hot_selected": 0,
                    "cold_eligible": cold_eligible,
                    "cold_selected": 0,
                    "deferred_hot": deferred_hot,
                    "deferred_cold": deferred_cold,
                    "deliverable": 0,
                    "catch_all": 0,
                    "gains": 0,
                    "gain_rate": 0.0,
                    "gains_per_10k": 0.0,
                    "remaining": remaining,
                }
            )
            write_state(out_state, unresolved, iter_cols)
            if offset < args.max_iters - 1:
                await asyncio.sleep(max(args.cooldown_seconds, 1))
            continue

        pending_emails = sorted({unresolved[k]["email"] for k in pending_keys if "@" in unresolved[k]["email"]})
        if not pending_emails:
            stop_reason = "no_pending_emails"
            break

        t0 = time.time()
        verify_map = await verify_emails(
            api_url=args.api_url,
            api_key=args.api_key,
            emails=pending_emails,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            request_timeout_seconds=args.request_timeout_seconds,
            batch_max_attempts=args.batch_max_attempts,
        )

        new_deliverable = 0
        new_catch_all = 0
        verify_miss = 0

        for k in pending_keys:
            row = unresolved[k]
            result = verify_map.get(row["email"])
            if result is None:
                # API miss/disconnect: keep existing state and don't punish unknown streak.
                verify_miss += 1
                row[iter_col] = clean(row.get("current_result") or "unknown").lower() or "unknown"
                continue

            row[iter_col] = result
            row["current_result"] = result
            if result in GOOD and not row.get("resolved_iter"):
                row["resolved_iter"] = str(i)
                if result == "deliverable":
                    new_deliverable += 1
                else:
                    new_catch_all += 1

            if result in GOOD:
                row["unknown_streak"] = "0"
                row["next_retry_iter"] = ""
            elif result == "unknown":
                unknown_streak = parse_int(row.get("unknown_streak"), 0) + 1
                row["unknown_streak"] = str(unknown_streak)
                if args.unknown_streak_lock > 0 and unknown_streak >= args.unknown_streak_lock:
                    retry_gap = max(args.unknown_retry_gap_iters, 1)
                    row["next_retry_iter"] = str(i + retry_gap)
                else:
                    row["next_retry_iter"] = ""
            else:
                row["unknown_streak"] = "0"
                row["next_retry_iter"] = ""

        remaining = sum(1 for k, r in unresolved.items() if owns_key(k) and r.get("current_result") not in GOOD)
        gains = new_deliverable + new_catch_all
        gain_rate = gains / len(pending_keys) if pending_keys else 0.0
        gains_per_10k = gain_rate * 10000.0
        dt = time.time() - t0

        print(
            f"[reverify] iter={i} pending={len(all_pending_keys)} eligible={len(pending_keys)} "
            f"emails={len(pending_emails)} verify_miss={verify_miss} backoff_skipped={backoff_skipped} "
            f"first_touch_skipped={first_touch_skipped} dead_domain_skipped={dead_domain_skipped} "
            f"hot_eligible={hot_eligible} hot_selected={hot_selected} "
            f"cold_eligible={cold_eligible} cold_selected={cold_selected} "
            f"deferred_hot={deferred_hot} deferred_cold={deferred_cold} deferred_pending={deferred_pending} "
            f"new_deliverable={new_deliverable} new_catch_all={new_catch_all} gains={gains} "
            f"gain_rate={gain_rate:.6f} gains_per_10k={gains_per_10k:.2f} "
            f"remaining={remaining} mins={dt/60:.1f}",
            flush=True,
        )

        iter_stats.append(
            {
                "iter": i,
                "pending": len(all_pending_keys),
                "eligible": len(pending_keys),
                "queried": len(pending_emails),
                "verify_miss": verify_miss,
                "backoff_skipped": backoff_skipped,
                "first_touch_skipped": first_touch_skipped,
                "dead_domain_skipped": dead_domain_skipped,
                "hot_eligible": hot_eligible,
                "hot_selected": hot_selected,
                "cold_eligible": cold_eligible,
                "cold_selected": cold_selected,
                "deferred_hot": deferred_hot,
                "deferred_cold": deferred_cold,
                "deliverable": new_deliverable,
                "catch_all": new_catch_all,
                "gains": gains,
                "gain_rate": gain_rate,
                "gains_per_10k": gains_per_10k,
                "remaining": remaining,
            }
        )

        write_state(out_state, unresolved, iter_cols)

        if remaining == 0:
            stop_reason = "resolved_all"
            break

        # Delta stop logic: stop when gains flatten repeatedly.
        low_gain = gains <= args.gain_stop_abs or gain_rate < args.gain_stop_rate
        if args.gain_stop_per_10k >= 0 and gains_per_10k < args.gain_stop_per_10k:
            low_gain = True

        if low_gain:
            low_gain_streak += 1
        else:
            low_gain_streak = 0

        if (
            len(all_pending_keys) >= args.min_pending_for_stop
            and low_gain_streak >= args.gain_stop_streak
        ):
            stop_reason = f"low_gain_streak_{low_gain_streak}"
            print(
                f"[reverify] stopping early due to low gains streak={low_gain_streak} "
                f"(abs<={args.gain_stop_abs} or rate<{args.gain_stop_rate} or "
                f"per10k<{args.gain_stop_per_10k if args.gain_stop_per_10k >= 0 else 'disabled'})",
                flush=True,
            )
            break

        if offset < args.max_iters - 1:
            await asyncio.sleep(args.cooldown_seconds)

    resolved_rows = []
    final_counts: Counter[str] = Counter()
    for key, info in unresolved.items():
        if not owns_key(key):
            continue
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

    remaining = sum(1 for k, r in unresolved.items() if owns_key(k) and r.get("current_result") not in GOOD)
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
    p.add_argument("--resume-state", default="")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--concurrency", type=int, default=64)
    p.add_argument("--request-timeout-seconds", type=int, default=240)
    p.add_argument("--max-iters", type=int, default=6)
    p.add_argument("--cooldown-seconds", type=int, default=0)
    p.add_argument("--gain-stop-abs", type=int, default=40)
    p.add_argument("--gain-stop-rate", type=float, default=0.00015)
    p.add_argument("--gain-stop-per-10k", type=float, default=-1.0)
    p.add_argument("--gain-stop-streak", type=int, default=2)
    p.add_argument("--min-pending-for-stop", type=int, default=50000)
    p.add_argument("--unknown-streak-lock", type=int, default=3)
    p.add_argument("--unknown-retry-gap-iters", type=int, default=12)
    p.add_argument("--max-pending-per-iter", type=int, default=250000)
    p.add_argument("--batch-max-attempts", type=int, default=2)
    p.add_argument("--good-results", default="deliverable,accept_all")
    p.add_argument("--hot-source-prefixes", default="drive_parallel_hold")
    p.add_argument("--hot-domain-suffixes", default="")
    p.add_argument("--hot-priority-quota", type=int, default=80000)
    p.add_argument("--strict-hot-only", action="store_true")
    p.add_argument("--first-touch-only", action="store_true")
    p.add_argument("--skip-domain-suffixes", default="")
    p.add_argument("--skip-domain-file", default="")
    p.add_argument("--fresh-unknown-streak-max", type=int, default=0)
    p.add_argument("--shard-count", type=int, default=1)
    p.add_argument("--shard-index", type=int, default=0)
    p.add_argument("--force-load-from-verified", action="store_true")
    p.add_argument("--qa-report", default="")
    args = p.parse_args()
    if args.shard_count < 1:
        p.error("--shard-count must be >= 1")
    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        p.error("--shard-index must be in [0, shard-count)")
    if not args.qa_report:
        args.qa_report = str(Path(args.out_summary).with_name("provider_reverify_qa.json"))
    return args


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
