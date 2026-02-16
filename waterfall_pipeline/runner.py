#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
import time
from pathlib import Path

try:
    from .merge_final import run as run_merge
    from .provider_full_loop import run as run_provider
    from .reverify_loop import run as run_reverify
    from .round2_input import run as run_round2_input
    from .split_stage1 import run as run_split_stage1
except ImportError:  # pragma: no cover
    from merge_final import run as run_merge
    from provider_full_loop import run as run_provider
    from reverify_loop import run as run_reverify
    from round2_input import run as run_round2_input
    from split_stage1 import run as run_split_stage1


def read_pid(path: Path) -> int:
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def wait_for_pid(pid: int, label: str, poll_seconds: int) -> None:
    if not pid_running(pid):
        return
    print(f"[runner] waiting for {label} pid={pid}", flush=True)
    while pid_running(pid):
        time.sleep(max(poll_seconds, 1))
        print(f"[runner] still waiting for {label} pid={pid}", flush=True)
    print(f"[runner] {label} pid={pid} exited", flush=True)


def count_data_rows(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    rows = -1  # discount header
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for _ in f:
            rows += 1
    return max(rows, 0)


def write_pid_file(path: Path, pid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{pid}\n", encoding="utf-8")


async def run_orchestrate(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    orchestrator_pid_file = run_dir / "orchestrator.pid"
    provider_pid_file = run_dir / "provider_loop.pid"
    reverify_pid_file = run_dir / "run_reverify.pid"
    write_pid_file(orchestrator_pid_file, os.getpid())

    try:
        stage1_pid_file = run_dir / args.stage1_pid_file
        stage1_state = run_dir / args.stage1_state_file
        quick_csv = run_dir / args.quick_csv
        waterfall_csv = run_dir / args.waterfall_csv
        review_csv = run_dir / args.review_csv
        segment_summary = run_dir / args.segment_summary

        provider_dir = run_dir / args.provider_dir
        provider_verified = provider_dir / "provider_candidates_verified.csv"
        provider_extra = provider_dir / "provider_additional_usable.csv"

        reverify_state = provider_dir / "provider_reverify_state.csv"
        reverify_extra = provider_dir / "provider_reverify_additional_usable.csv"
        reverify_summary = provider_dir / "provider_reverify_summary.txt"

        output_csv = run_dir / args.output_csv
        output_summary = run_dir / args.output_summary

        if args.wait_stage1 and stage1_pid_file.exists():
            wait_for_pid(read_pid(stage1_pid_file), "stage1", args.poll_seconds)

        if not stage1_state.exists():
            raise RuntimeError(f"Missing stage1 state CSV: {stage1_state}")

        run_split_stage1(
            argparse.Namespace(
                state_csv=str(stage1_state),
                quick_csv=str(quick_csv),
                waterfall_csv=str(waterfall_csv),
                review_csv=str(review_csv),
                summary_txt=str(segment_summary),
                qa_report=str(run_dir / "stage1_split_qa.json"),
            )
        )

        write_pid_file(provider_pid_file, os.getpid())
        await run_provider(
            argparse.Namespace(
                input_csv=str(waterfall_csv),
                out_dir=str(provider_dir),
                kadenverify_api_key=args.kadenverify_api_key,
                verify_api_url=args.verify_api_url,
                api_keys_path=args.api_keys_path,
                state_file=str(provider_dir / "provider_loop_state.json"),
                yield_stats_file=str(provider_dir / "provider_stage_yield.json"),
                force_restart=args.force_provider_restart,
                skip_low_yield=args.skip_low_yield,
                min_stage_yield=args.min_stage_yield,
            )
        )
        write_pid_file(provider_pid_file, 0)

        write_pid_file(reverify_pid_file, os.getpid())
        await run_reverify(
            argparse.Namespace(
                input_verified=str(provider_verified),
                input_waterfall=str(waterfall_csv),
                out_state=str(reverify_state),
                out_usable=str(reverify_extra),
                out_summary=str(reverify_summary),
                api_key=args.kadenverify_api_key,
                api_url=args.verify_api_url,
                batch_size=args.reverify_batch_size,
                concurrency=args.reverify_concurrency,
                max_iters=args.reverify_max_iters,
                cooldown_seconds=args.reverify_cooldown_seconds,
                gain_stop_abs=args.gain_stop_abs,
                gain_stop_rate=args.gain_stop_rate,
                gain_stop_streak=args.gain_stop_streak,
                min_pending_for_stop=args.min_pending_for_stop,
                qa_report=str(provider_dir / "provider_reverify_qa.json"),
            )
        )
        write_pid_file(reverify_pid_file, 0)

        run_merge(
            argparse.Namespace(
                stage1_state=str(stage1_state),
                provider_extra=str(provider_extra),
                reverify_extra=str(reverify_extra),
                output=str(output_csv),
                summary=str(output_summary),
                qa_report=str(run_dir / "quick_wins_merge_qa.json"),
            )
        )

        print("[runner] full orchestrate complete", flush=True)
        print(f"[runner] output_csv={output_csv}", flush=True)
        print(f"[runner] output_rows={count_data_rows(output_csv)}", flush=True)
    finally:
        write_pid_file(provider_pid_file, 0)
        write_pid_file(reverify_pid_file, 0)
        write_pid_file(orchestrator_pid_file, 0)


async def run_queue_round2(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir)
    provider_dir = run_dir / args.provider_dir
    round2_pid_file = run_dir / "run_provider_round2.pid"

    try:
        pid_candidates = [
            run_dir / args.reverify_pid_file,
            run_dir / "run_reverify.pid",
            run_dir / "provider_reverify.pid",
        ]
        reverify_pid = 0
        for p in pid_candidates:
            pid = read_pid(p)
            if pid_running(pid):
                reverify_pid = pid
                break
        if reverify_pid and args.wait_reverify:
            wait_for_pid(reverify_pid, "reverify", args.poll_seconds)

        round2_input = run_dir / args.round2_input_csv
        run_round2_input(
            argparse.Namespace(
                state_csv=str(provider_dir / "provider_reverify_state.csv"),
                waterfall_csv=str(run_dir / args.waterfall_csv),
                output_csv=str(round2_input),
                summary_txt=str(run_dir / "round2_input_summary.txt"),
                qa_report=str(run_dir / "round2_input_qa.json"),
            )
        )

        rows = count_data_rows(round2_input)
        if rows == 0:
            print(f"[runner] round2 input empty: {round2_input}", flush=True)
            return

        write_pid_file(round2_pid_file, os.getpid())
        await run_provider(
            argparse.Namespace(
                input_csv=str(round2_input),
                out_dir=str(run_dir / args.round2_provider_dir),
                kadenverify_api_key=args.kadenverify_api_key,
                verify_api_url=args.verify_api_url,
                api_keys_path=args.api_keys_path,
                state_file=str(run_dir / args.round2_provider_dir / "provider_loop_state.json"),
                yield_stats_file=str(provider_dir / "provider_stage_yield.json"),
                force_restart=args.force_provider_restart,
                skip_low_yield=args.skip_low_yield,
                min_stage_yield=args.min_stage_yield,
            )
        )

        print("[runner] queue-round2 complete", flush=True)
    finally:
        write_pid_file(round2_pid_file, 0)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Waterfall pipeline runner/orchestrator")
    sub = p.add_subparsers(dest="cmd", required=True)

    orch = sub.add_parser("orchestrate", help="Run split -> provider -> reverify -> merge")
    orch.add_argument("--run-dir", required=True)
    orch.add_argument("--kadenverify-api-key", required=True)
    orch.add_argument("--verify-api-url", default="http://127.0.0.1:8025")
    orch.add_argument("--api-keys-path", default="/opt/mundi-princeps/config/api_keys.json")
    orch.add_argument("--wait-stage1", action="store_true")
    orch.add_argument("--stage1-pid-file", default="run_stage1.pid")
    orch.add_argument("--stage1-state-file", default="state.csv")
    orch.add_argument("--quick-csv", default="quick_wins_deliverable.csv")
    orch.add_argument("--waterfall-csv", default="waterfall_unknown_undeliverable.csv")
    orch.add_argument("--review-csv", default="review_accept_all_risky.csv")
    orch.add_argument("--segment-summary", default="segmentation_summary.txt")
    orch.add_argument("--provider-dir", default="provider_loop")
    orch.add_argument("--force-provider-restart", action="store_true")
    orch.add_argument("--skip-low-yield", action="store_true")
    orch.add_argument("--min-stage-yield", type=float, default=0.0005)
    orch.add_argument("--reverify-batch-size", type=int, default=500)
    orch.add_argument("--reverify-concurrency", type=int, default=12)
    orch.add_argument("--reverify-max-iters", type=int, default=6)
    orch.add_argument("--reverify-cooldown-seconds", type=int, default=20)
    orch.add_argument("--gain-stop-abs", type=int, default=50)
    orch.add_argument("--gain-stop-rate", type=float, default=0.0002)
    orch.add_argument("--gain-stop-streak", type=int, default=2)
    orch.add_argument("--min-pending-for-stop", type=int, default=50000)
    orch.add_argument("--output-csv", default="quick_wins_plus_catchall_fullloop.csv")
    orch.add_argument("--output-summary", default="quick_wins_plus_catchall_fullloop_summary.txt")
    orch.add_argument("--poll-seconds", type=int, default=60)

    q2 = sub.add_parser("queue-round2", help="Wait for reverify, build unresolved input, run provider round2")
    q2.add_argument("--run-dir", required=True)
    q2.add_argument("--kadenverify-api-key", required=True)
    q2.add_argument("--verify-api-url", default="http://127.0.0.1:8025")
    q2.add_argument("--api-keys-path", default="/opt/mundi-princeps/config/api_keys.json")
    q2.add_argument("--wait-reverify", action="store_true")
    q2.add_argument("--reverify-pid-file", default="provider_reverify.pid")
    q2.add_argument("--provider-dir", default="provider_loop")
    q2.add_argument("--waterfall-csv", default="waterfall_unknown_undeliverable.csv")
    q2.add_argument("--round2-input-csv", default="waterfall_unknown_unresolved_round2.csv")
    q2.add_argument("--round2-provider-dir", default="provider_loop_round2")
    q2.add_argument("--force-provider-restart", action="store_true")
    q2.add_argument("--skip-low-yield", action="store_true")
    q2.add_argument("--min-stage-yield", type=float, default=0.0005)
    q2.add_argument("--poll-seconds", type=int, default=60)
    return p


async def _amain() -> None:
    args = build_parser().parse_args()
    if args.cmd == "orchestrate":
        await run_orchestrate(args)
    elif args.cmd == "queue-round2":
        await run_queue_round2(args)
    else:
        raise RuntimeError(f"Unknown command: {args.cmd}")


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
