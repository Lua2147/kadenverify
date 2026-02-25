#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

FILE_TOKEN_FILTER = (
    "reverify",
    "provider_reverify",
    "waterfall_unknown_unresolved_round2",
    "quick_wins_plus_catchall_fullloop",
    "provider_candidates_verified",
)

EXCLUDED_DIRS = {
    ".git",
    ".venv",
    "node_modules",
    "__pycache__",
    ".next",
    ".cache",
}

CSV_SUFFIXES = {".csv"}
LOG_SUFFIXES = {".log", ".txt"}

COMPLETE_RE = re.compile(
    r"\[reverify\] complete usable_total=(\d+) deliverable=(\d+) catch_all=(\d+) remaining=(\d+)"
)
LOADED_RE = re.compile(r"\[reverify\] loaded unresolved=(\d+)")
RESUME_RE = re.compile(r"\[reverify\] resuming from state rows=(\d+)")
QUEUE_UNRESOLVED_RE = re.compile(r"unresolved_keys=(\d+)")


@dataclass
class CandidateFile:
    path: str
    size_bytes: int
    mtime_utc: str


def utc_mtime(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def should_include_file(name: str) -> bool:
    lower = name.lower()
    return any(token in lower for token in FILE_TOKEN_FILTER)


def walk_candidates(roots: Iterable[Path], max_files: int) -> list[CandidateFile]:
    out: list[CandidateFile] = []
    seen: set[str] = set()

    for root in roots:
        if not root.exists():
            continue

        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in EXCLUDED_DIRS]
            for filename in filenames:
                if not should_include_file(filename):
                    continue
                path = Path(dirpath) / filename
                try:
                    stat = path.stat()
                except OSError:
                    continue

                key = str(path.resolve())
                if key in seen:
                    continue
                seen.add(key)
                out.append(
                    CandidateFile(
                        path=key,
                        size_bytes=stat.st_size,
                        mtime_utc=utc_mtime(stat.st_mtime),
                    )
                )
                if len(out) >= max_files:
                    return sorted(out, key=lambda x: x.mtime_utc, reverse=True)

    return sorted(out, key=lambda x: x.mtime_utc, reverse=True)


def csv_row_count(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    rows = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        next(r, None)
        for _ in r:
            rows += 1
    return rows


def summarize_reverify_state(path: Path) -> dict:
    current = Counter()
    prev = Counter()
    rows = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows += 1
            current[(row.get("current_result") or "").strip().lower()] += 1
            prev[(row.get("prev_result") or "").strip().lower()] += 1
    return {
        "path": str(path),
        "rows": rows,
        "current_result_counts": dict(current),
        "prev_result_counts": dict(prev),
    }


def summarize_reverify_usable(path: Path) -> dict:
    verify = Counter()
    source = Counter()
    rows = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows += 1
            verify[(row.get("new_email_verify_result") or "").strip().lower()] += 1
            source[(row.get("new_email_source") or "").strip()] += 1
    return {
        "path": str(path),
        "rows": rows,
        "new_email_verify_result_counts": dict(verify),
        "top_new_email_sources": source.most_common(10),
    }


def analyze_reverify_log(path: Path, min_drop_ratio: float, min_drop_rows: int) -> dict:
    max_usable = None
    max_usable_line = 0
    last_complete = None
    last_complete_line = 0
    loaded_drops = []
    resume_drops = []
    prev_loaded = None
    prev_loaded_line = 0
    prev_resume = None
    prev_resume_line = 0

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, start=1):
            c = COMPLETE_RE.search(line)
            if c:
                usable_total = int(c.group(1))
                payload = {
                    "usable_total": usable_total,
                    "deliverable": int(c.group(2)),
                    "catch_all": int(c.group(3)),
                    "remaining": int(c.group(4)),
                    "line": i,
                }
                last_complete = payload
                last_complete_line = i
                if max_usable is None or usable_total > max_usable["usable_total"]:
                    max_usable = payload
                    max_usable_line = i

            l = LOADED_RE.search(line)
            if l:
                current = int(l.group(1))
                if prev_loaded is not None and prev_loaded > 0:
                    drop = prev_loaded - current
                    ratio = current / prev_loaded
                    if drop >= min_drop_rows and ratio < min_drop_ratio:
                        loaded_drops.append(
                            {
                                "prev": prev_loaded,
                                "prev_line": prev_loaded_line,
                                "current": current,
                                "line": i,
                                "drop": drop,
                                "ratio": ratio,
                            }
                        )
                prev_loaded = current
                prev_loaded_line = i

            r = RESUME_RE.search(line)
            if r:
                current = int(r.group(1))
                if prev_resume is not None and prev_resume > 0:
                    drop = prev_resume - current
                    ratio = current / prev_resume
                    if drop >= min_drop_rows and ratio < min_drop_ratio:
                        resume_drops.append(
                            {
                                "prev": prev_resume,
                                "prev_line": prev_resume_line,
                                "current": current,
                                "line": i,
                                "drop": drop,
                                "ratio": ratio,
                            }
                        )
                prev_resume = current
                prev_resume_line = i

    return {
        "path": str(path),
        "max_usable": max_usable,
        "max_usable_line": max_usable_line,
        "last_complete": last_complete,
        "last_complete_line": last_complete_line,
        "loaded_unresolved_drops": loaded_drops[-20:],
        "resume_rows_drops": resume_drops[-20:],
    }


def analyze_queue_log(path: Path, min_drop_ratio: float, min_drop_rows: int) -> dict:
    prev = None
    prev_line = 0
    drops = []
    last = None
    last_line = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, start=1):
            m = QUEUE_UNRESOLVED_RE.search(line)
            if not m:
                continue
            current = int(m.group(1))
            last = current
            last_line = i
            if prev is not None and prev > 0:
                drop = prev - current
                ratio = current / prev
                if drop >= min_drop_rows and ratio < min_drop_ratio:
                    drops.append(
                        {
                            "prev": prev,
                            "prev_line": prev_line,
                            "current": current,
                            "line": i,
                            "drop": drop,
                            "ratio": ratio,
                        }
                    )
            prev = current
            prev_line = i
    return {
        "path": str(path),
        "last_unresolved_keys": last,
        "last_unresolved_line": last_line,
        "unresolved_drops": drops[-20:],
    }


def default_roots() -> list[Path]:
    roots = [
        Path.cwd(),
        Path("/data/local-machine-backup"),
        Path("/opt/mundi-princeps"),
        Path("/tmp"),
        Path.home(),
    ]
    unique = []
    seen = set()
    for r in roots:
        k = str(r)
        if k in seen:
            continue
        seen.add(k)
        unique.append(r)
    return unique


def print_report(report: dict) -> None:
    print("=== Reverify Forensics Scan ===")
    print(f"scan_utc={report['scan_utc']}")
    print(f"roots={report['roots']}")
    print(f"candidate_files={len(report['candidate_files'])}")

    print("\n=== Top Candidate Files (Newest 40) ===")
    for item in report["candidate_files"][:40]:
        print(f"{item['mtime_utc']}  {item['size_bytes']:>12}  {item['path']}")

    print("\n=== CSV Stats ===")
    for stat in report["csv_stats"]:
        print(f"{stat['path']}")
        for k, v in stat.items():
            if k == "path":
                continue
            print(f"  {k}: {v}")

    print("\n=== Reverify Log Analysis ===")
    for item in report["reverify_log_analysis"]:
        print(f"{item['path']}")
        print(f"  max_usable: {item.get('max_usable')}")
        print(f"  last_complete: {item.get('last_complete')}")
        drops = item.get("loaded_unresolved_drops") or []
        resume_drops = item.get("resume_rows_drops") or []
        print(f"  loaded_unresolved_drops: {len(drops)}")
        for d in drops[-5:]:
            print(f"    prev={d['prev']}@L{d['prev_line']} -> current={d['current']}@L{d['line']} ratio={d['ratio']:.4f}")
        print(f"  resume_rows_drops: {len(resume_drops)}")
        for d in resume_drops[-5:]:
            print(f"    prev={d['prev']}@L{d['prev_line']} -> current={d['current']}@L{d['line']} ratio={d['ratio']:.4f}")

    print("\n=== Queue Log Analysis ===")
    for item in report["queue_log_analysis"]:
        print(f"{item['path']}")
        print(f"  last_unresolved_keys: {item.get('last_unresolved_keys')}")
        drops = item.get("unresolved_drops") or []
        print(f"  unresolved_drops: {len(drops)}")
        for d in drops[-5:]:
            print(f"    prev={d['prev']}@L{d['prev_line']} -> current={d['current']}@L{d['line']} ratio={d['ratio']:.4f}")


def build_report(args: argparse.Namespace) -> dict:
    roots = [Path(r).expanduser() for r in (args.roots or [])] or default_roots()
    candidates = walk_candidates(roots, args.max_files)

    csv_stats: list[dict] = []
    reverify_logs: list[Path] = []
    queue_logs: list[Path] = []
    for c in candidates:
        p = Path(c.path)
        suffix = p.suffix.lower()
        name = p.name.lower()

        if suffix in CSV_SUFFIXES:
            if "provider_reverify_state" in name:
                try:
                    csv_stats.append(summarize_reverify_state(p))
                except Exception as e:  # pragma: no cover
                    csv_stats.append({"path": str(p), "error": str(e)})
            elif "provider_reverify_additional_usable" in name:
                try:
                    csv_stats.append(summarize_reverify_usable(p))
                except Exception as e:  # pragma: no cover
                    csv_stats.append({"path": str(p), "error": str(e)})
            else:
                try:
                    csv_stats.append({"path": str(p), "rows": csv_row_count(p)})
                except Exception as e:  # pragma: no cover
                    csv_stats.append({"path": str(p), "error": str(e)})

        if suffix in LOG_SUFFIXES and "provider_reverify" in name:
            reverify_logs.append(p)
        if suffix in LOG_SUFFIXES and "queue_after_reverify" in name:
            queue_logs.append(p)

    reverify_analysis = []
    for p in sorted(set(reverify_logs), key=lambda x: str(x)):
        try:
            reverify_analysis.append(
                analyze_reverify_log(p, min_drop_ratio=args.min_drop_ratio, min_drop_rows=args.min_drop_rows)
            )
        except Exception as e:  # pragma: no cover
            reverify_analysis.append({"path": str(p), "error": str(e)})

    queue_analysis = []
    for p in sorted(set(queue_logs), key=lambda x: str(x)):
        try:
            queue_analysis.append(
                analyze_queue_log(p, min_drop_ratio=args.min_drop_ratio, min_drop_rows=args.min_drop_rows)
            )
        except Exception as e:  # pragma: no cover
            queue_analysis.append({"path": str(p), "error": str(e)})

    report = {
        "scan_utc": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "roots": [str(r) for r in roots],
        "candidate_files": [asdict(c) for c in candidates],
        "csv_stats": csv_stats,
        "reverify_log_analysis": reverify_analysis,
        "queue_log_analysis": queue_analysis,
    }
    return report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Find and summarize potential lost reverify artifacts.")
    p.add_argument("--roots", nargs="*", default=[], help="Directories to scan. Defaults to common locations.")
    p.add_argument("--max-files", type=int, default=1500, help="Maximum candidate files to collect.")
    p.add_argument("--min-drop-ratio", type=float, default=0.80, help="Drop ratio threshold (current/previous).")
    p.add_argument("--min-drop-rows", type=int, default=5000, help="Minimum absolute row drop for anomaly.")
    p.add_argument("--json-out", default="", help="Optional path to save full JSON report.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    print_report(report)

    if args.json_out:
        out = Path(args.json_out).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"\nSaved JSON report: {out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
