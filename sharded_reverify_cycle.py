#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


def shard_for_key(contact_key: str, shard_count: int) -> int:
    digest = hashlib.blake2b(contact_key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % max(1, shard_count)


def parse_last_iter_metrics(summary_path: Path) -> tuple[dict[str, float], str]:
    metrics = {
        "iter": 0.0,
        "pending": 0.0,
        "eligible": 0.0,
        "queried": 0.0,
        "verify_miss": 0.0,
        "backoff_skipped": 0.0,
        "hot_eligible": 0.0,
        "hot_selected": 0.0,
        "cold_eligible": 0.0,
        "cold_selected": 0.0,
        "deferred_hot": 0.0,
        "deferred_cold": 0.0,
        "deliverable": 0.0,
        "catch_all": 0.0,
        "gains": 0.0,
        "gain_rate": 0.0,
        "remaining": 0.0,
    }
    stop_reason = ""
    if not summary_path.exists() or summary_path.stat().st_size == 0:
        return metrics, stop_reason

    text = summary_path.read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        if line.startswith("stop_reason="):
            stop_reason = line.split("=", 1)[1].strip()

    iter_lines = [line.strip() for line in text.splitlines() if line.strip().startswith("- iter=")]
    if not iter_lines:
        return metrics, stop_reason

    kv = dict(re.findall(r"([a-z_]+)=([0-9.]+)", iter_lines[-1]))
    for key in metrics:
        if key in kv:
            try:
                metrics[key] = float(kv[key])
            except Exception:
                pass
    return metrics, stop_reason


def load_state_rows(path: Path) -> tuple[list[str], dict[str, dict]]:
    if not path.exists() or path.stat().st_size == 0:
        return [], {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        fields = list(r.fieldnames or [])
        rows = {}
        for row in r:
            key = (row.get("contact_key") or "").strip().lower()
            if key:
                rows[key] = dict(row)
    return fields, rows


def write_state_rows(path: Path, fields: list[str], rows: dict[str, dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for key in sorted(rows):
            w.writerow(rows[key])


def merge_state_outputs(
    out_state: Path,
    shard_state_paths: list[Path],
    resume_state: Path,
    shard_count: int,
) -> None:
    base_fields, merged_rows = load_state_rows(resume_state)

    if not base_fields:
        for p in shard_state_paths:
            if not p.exists() or p.stat().st_size == 0:
                continue
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                r = csv.DictReader(f)
                base_fields = list(r.fieldnames or [])
            if base_fields:
                break

    for shard_index, p in enumerate(shard_state_paths):
        if not p.exists() or p.stat().st_size == 0:
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                key = (row.get("contact_key") or "").strip().lower()
                if not key:
                    continue
                if shard_for_key(key, shard_count) == shard_index:
                    merged_rows[key] = dict(row)

    write_state_rows(out_state, base_fields, merged_rows)


def merge_usable_outputs(out_usable: Path, shard_usable_paths: list[Path]) -> int:
    fields: list[str] = []
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()

    for p in shard_usable_paths:
        if not p.exists() or p.stat().st_size == 0:
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            this_fields = list(r.fieldnames or [])
            if not fields:
                fields = this_fields
            for row in r:
                key = (
                    (row.get("contact_key") or "").strip().lower(),
                    (row.get("new_email") or "").strip().lower(),
                )
                if not key[0] or not key[1]:
                    continue
                if key in seen:
                    continue
                seen.add(key)
                rows.append(dict(row))

    with out_usable.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if fields:
            w.writeheader()
            for row in rows:
                w.writerow(row)
    return len(rows)


def merge_qa_reports(out_qa: Path, shard_qa_paths: list[Path]) -> dict:
    agg = {
        "total_candidates": 0,
        "remaining": 0,
        "usable_total": 0,
        "usable_deliverable": 0,
        "usable_catch_all": 0,
        "stop_reason": "",
        "iterations": 0,
    }
    stop_reasons = []
    for p in shard_qa_paths:
        if not p.exists() or p.stat().st_size == 0:
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        agg["total_candidates"] += int(obj.get("total_candidates", 0) or 0)
        agg["remaining"] += int(obj.get("remaining", 0) or 0)
        agg["usable_total"] += int(obj.get("usable_total", 0) or 0)
        agg["usable_deliverable"] += int(obj.get("usable_deliverable", 0) or 0)
        agg["usable_catch_all"] += int(obj.get("usable_catch_all", 0) or 0)
        agg["iterations"] = max(agg["iterations"], int(obj.get("iterations", 0) or 0))
        reason = str(obj.get("stop_reason", "") or "").strip()
        if reason:
            stop_reasons.append(reason)

    uniq = sorted(set(stop_reasons))
    if len(uniq) == 1:
        agg["stop_reason"] = uniq[0]
    elif len(uniq) > 1:
        agg["stop_reason"] = "mixed:" + ",".join(uniq)

    out_qa.write_text(json.dumps(agg, separators=(",", ":")), encoding="utf-8")
    return agg


def write_merged_summary(
    out_summary: Path,
    input_verified: Path,
    input_waterfall: Path,
    out_state: Path,
    out_usable: Path,
    qa_agg: dict,
    shard_summary_paths: list[Path],
) -> dict[str, float]:
    agg = {
        "iter": 0.0,
        "pending": 0.0,
        "eligible": 0.0,
        "queried": 0.0,
        "verify_miss": 0.0,
        "backoff_skipped": 0.0,
        "hot_eligible": 0.0,
        "hot_selected": 0.0,
        "cold_eligible": 0.0,
        "cold_selected": 0.0,
        "deferred_hot": 0.0,
        "deferred_cold": 0.0,
        "deliverable": 0.0,
        "catch_all": 0.0,
        "gains": 0.0,
        "gain_rate": 0.0,
        "remaining": float(int(qa_agg.get("remaining", 0) or 0)),
    }
    stop_reasons = []

    for p in shard_summary_paths:
        metrics, stop_reason = parse_last_iter_metrics(p)
        agg["iter"] = max(agg["iter"], metrics["iter"])
        for k in (
            "pending",
            "eligible",
            "queried",
            "verify_miss",
            "backoff_skipped",
            "hot_eligible",
            "hot_selected",
            "cold_eligible",
            "cold_selected",
            "deferred_hot",
            "deferred_cold",
            "deliverable",
            "catch_all",
            "gains",
        ):
            agg[k] += metrics[k]
        if stop_reason:
            stop_reasons.append(stop_reason)

    queried = int(agg["queried"])
    gains = int(agg["gains"])
    agg["gain_rate"] = (gains / queried) if queried > 0 else 0.0
    if not stop_reasons and qa_agg.get("stop_reason"):
        stop_reasons = [str(qa_agg.get("stop_reason"))]
    uniq = sorted(set(stop_reasons))
    stop_reason = uniq[0] if len(uniq) == 1 else ("mixed:" + ",".join(uniq) if uniq else "")

    lines = [
        f"input_verified={input_verified}",
        f"input_waterfall={input_waterfall}",
        f"state_out={out_state}",
        f"usable_out={out_usable}",
        f"total_reverify_candidates={int(qa_agg.get('total_candidates', 0) or 0)}",
        f"remaining_unresolved={int(qa_agg.get('remaining', 0) or 0)}",
        f"usable_rows={int(qa_agg.get('usable_total', 0) or 0)}",
        f"stop_reason={stop_reason}",
        "iteration_gains:",
        (
            "  - iter={iter} pending={pending} eligible={eligible} queried={queried} "
            "verify_miss={verify_miss} backoff_skipped={backoff_skipped} "
            "hot_eligible={hot_eligible} hot_selected={hot_selected} "
            "cold_eligible={cold_eligible} cold_selected={cold_selected} "
            "deferred_hot={deferred_hot} deferred_cold={deferred_cold} "
            "newly_deliverable={deliverable} newly_catch_all={catch_all} "
            "gains={gains} gain_rate={gain_rate:.6f} remaining={remaining}"
        ).format(
            iter=int(agg["iter"]),
            pending=int(agg["pending"]),
            eligible=int(agg["eligible"]),
            queried=int(agg["queried"]),
            verify_miss=int(agg["verify_miss"]),
            backoff_skipped=int(agg["backoff_skipped"]),
            hot_eligible=int(agg["hot_eligible"]),
            hot_selected=int(agg["hot_selected"]),
            cold_eligible=int(agg["cold_eligible"]),
            cold_selected=int(agg["cold_selected"]),
            deferred_hot=int(agg["deferred_hot"]),
            deferred_cold=int(agg["deferred_cold"]),
            deliverable=int(agg["deliverable"]),
            catch_all=int(agg["catch_all"]),
            gains=int(agg["gains"]),
            gain_rate=float(agg["gain_rate"]),
            remaining=int(qa_agg.get("remaining", 0) or 0),
        ),
        "final_results:",
        f"  - deliverable: {int(qa_agg.get('usable_deliverable', 0) or 0)}",
        f"  - accept_all: {int(qa_agg.get('usable_catch_all', 0) or 0)}",
        f"  - unresolved: {int(qa_agg.get('remaining', 0) or 0)}",
    ]
    out_summary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return agg


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run sharded parallel reverify cycle and merge outputs.")
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
    p.add_argument("--gain-stop-streak", type=int, default=2)
    p.add_argument("--min-pending-for-stop", type=int, default=50000)
    p.add_argument("--unknown-streak-lock", type=int, default=3)
    p.add_argument("--unknown-retry-gap-iters", type=int, default=12)
    p.add_argument("--max-pending-per-iter", type=int, default=250000)
    p.add_argument("--batch-max-attempts", type=int, default=2)
    p.add_argument("--good-results", default="deliverable,accept_all")
    p.add_argument("--hot-source-prefixes", default="drive_parallel_hold")
    p.add_argument("--hot-priority-quota", type=int, default=80000)
    p.add_argument("--fresh-unknown-streak-max", type=int, default=0)
    p.add_argument("--qa-report", default="")
    p.add_argument("--shard-count", type=int, default=2)
    p.add_argument("--force-load-from-verified", action="store_true")
    args = p.parse_args()
    if args.shard_count < 2:
        p.error("--shard-count must be >= 2 for sharded mode")
    if not args.qa_report:
        args.qa_report = str(Path(args.out_summary).with_name("provider_reverify_qa.json"))
    return args


def main() -> None:
    args = parse_args()

    input_verified = Path(args.input_verified)
    input_waterfall = Path(args.input_waterfall)
    out_state = Path(args.out_state)
    out_usable = Path(args.out_usable)
    out_summary = Path(args.out_summary)
    out_qa = Path(args.qa_report)
    resume_state = Path(args.resume_state) if args.resume_state else out_state

    shard_state_paths = [Path(f"{out_state}.shard{i}") for i in range(args.shard_count)]
    shard_usable_paths = [Path(f"{out_usable}.shard{i}") for i in range(args.shard_count)]
    shard_summary_paths = [Path(f"{out_summary}.shard{i}") for i in range(args.shard_count)]
    shard_qa_paths = [Path(f"{out_qa}.shard{i}") for i in range(args.shard_count)]

    cmds: list[list[str]] = []
    for i in range(args.shard_count):
        cmd = [
            sys.executable,
            "-u",
            "-m",
            "waterfall_pipeline.reverify_loop",
            str(input_verified),
            str(input_waterfall),
            str(shard_state_paths[i]),
            str(shard_usable_paths[i]),
            str(shard_summary_paths[i]),
            args.api_key,
            "--api-url",
            args.api_url,
            "--resume-state",
            str(resume_state),
            "--batch-size",
            str(args.batch_size),
            "--concurrency",
            str(args.concurrency),
            "--request-timeout-seconds",
            str(args.request_timeout_seconds),
            "--batch-max-attempts",
            str(args.batch_max_attempts),
            "--max-pending-per-iter",
            str(args.max_pending_per_iter),
            "--max-iters",
            str(args.max_iters),
            "--cooldown-seconds",
            str(args.cooldown_seconds),
            "--gain-stop-abs",
            str(args.gain_stop_abs),
            "--gain-stop-rate",
            str(args.gain_stop_rate),
            "--gain-stop-streak",
            str(args.gain_stop_streak),
            "--min-pending-for-stop",
            str(args.min_pending_for_stop),
            "--unknown-streak-lock",
            str(args.unknown_streak_lock),
            "--unknown-retry-gap-iters",
            str(args.unknown_retry_gap_iters),
            "--good-results",
            args.good_results,
            "--hot-source-prefixes",
            args.hot_source_prefixes,
            "--hot-priority-quota",
            str(args.hot_priority_quota),
            "--fresh-unknown-streak-max",
            str(args.fresh_unknown_streak_max),
            "--qa-report",
            str(shard_qa_paths[i]),
            "--shard-count",
            str(args.shard_count),
            "--shard-index",
            str(i),
        ]
        if args.force_load_from_verified:
            cmd.append("--force-load-from-verified")
        cmds.append(cmd)

    print(f"[reverify-sharded] launching shards={args.shard_count}", flush=True)
    procs = [subprocess.Popen(cmd) for cmd in cmds]
    exit_codes: list[int] = []
    for i, proc in enumerate(procs):
        rc = proc.wait()
        exit_codes.append(rc)
        print(f"[reverify-sharded] shard={i} exit={rc}", flush=True)

    if any(rc != 0 for rc in exit_codes):
        bad = [str(i) for i, rc in enumerate(exit_codes) if rc != 0]
        print(f"[reverify-sharded] failed shards={','.join(bad)}", flush=True)
        raise SystemExit(1)

    merge_state_outputs(out_state, shard_state_paths, resume_state, args.shard_count)
    merged_usable_rows = merge_usable_outputs(out_usable, shard_usable_paths)
    qa_agg = merge_qa_reports(out_qa, shard_qa_paths)
    agg = write_merged_summary(
        out_summary,
        input_verified,
        input_waterfall,
        out_state,
        out_usable,
        qa_agg,
        shard_summary_paths,
    )
    print(
        f"[reverify-sharded] complete shards={args.shard_count} "
        f"queried={int(agg['queried'])} gains={int(agg['gains'])} "
        f"usable_rows={merged_usable_rows} remaining={int(qa_agg.get('remaining', 0) or 0)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
