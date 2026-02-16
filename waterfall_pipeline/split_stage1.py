#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    from .qa import qa_assert_file, write_qa_report
except ImportError:  # pragma: no cover
    from qa import qa_assert_file, write_qa_report

QUICK_RESULTS = {"deliverable"}
WATERFALL_RESULTS = {"unknown", "undeliverable"}


def run(args: argparse.Namespace) -> None:
    state_csv = Path(args.state_csv)
    quick_csv = Path(args.quick_csv)
    waterfall_csv = Path(args.waterfall_csv)
    review_csv = Path(args.review_csv)
    summary_txt = Path(args.summary_txt)
    qa_report = Path(args.qa_report)

    qa_assert_file(state_csv, "stage1_state")

    quick_csv.parent.mkdir(parents=True, exist_ok=True)
    waterfall_csv.parent.mkdir(parents=True, exist_ok=True)
    review_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_txt.parent.mkdir(parents=True, exist_ok=True)

    counts = Counter()
    segment_counts = Counter()
    rows_total = 0

    with state_csv.open("r", encoding="utf-8-sig", newline="") as f_in:
        reader = csv.DictReader(f_in)
        if not reader.fieldnames:
            raise RuntimeError(f"State CSV has no headers: {state_csv}")
        fieldnames = list(reader.fieldnames)

        with (
            quick_csv.open("w", encoding="utf-8", newline="") as f_quick,
            waterfall_csv.open("w", encoding="utf-8", newline="") as f_waterfall,
            review_csv.open("w", encoding="utf-8", newline="") as f_review,
        ):
            quick_writer = csv.DictWriter(f_quick, fieldnames=fieldnames)
            waterfall_writer = csv.DictWriter(f_waterfall, fieldnames=fieldnames)
            review_writer = csv.DictWriter(f_review, fieldnames=fieldnames)
            quick_writer.writeheader()
            waterfall_writer.writeheader()
            review_writer.writeheader()

            for row in reader:
                rows_total += 1
                result = (row.get("result") or "").strip().lower()
                counts[result] += 1

                if result in QUICK_RESULTS:
                    quick_writer.writerow(row)
                    segment_counts["quick_wins"] += 1
                elif result in WATERFALL_RESULTS:
                    waterfall_writer.writerow(row)
                    segment_counts["waterfall"] += 1
                else:
                    review_writer.writerow(row)
                    segment_counts["review"] += 1

    summary_lines = [
        f"generated_utc={datetime.now(timezone.utc).isoformat()}",
        f"state_csv={state_csv}",
        f"quick_csv={quick_csv}",
        f"waterfall_csv={waterfall_csv}",
        f"review_csv={review_csv}",
        f"total_rows={rows_total}",
        f"quick_wins_deliverable={segment_counts['quick_wins']}",
        f"waterfall_unknown_undeliverable={segment_counts['waterfall']}",
        f"review_other={segment_counts['review']}",
        "result_breakdown:",
    ]
    for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        summary_lines.append(f"  - {(k or '<blank>')}: {v}")
    summary_txt.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    write_qa_report(
        qa_report,
        {
            "state_csv": str(state_csv),
            "total_rows": rows_total,
            "quick_wins_deliverable": segment_counts["quick_wins"],
            "waterfall_unknown_undeliverable": segment_counts["waterfall"],
            "review_other": segment_counts["review"],
            "result_breakdown": dict(counts),
        },
    )

    print(f"wrote_quick={quick_csv} rows={segment_counts['quick_wins']}")
    print(f"wrote_waterfall={waterfall_csv} rows={segment_counts['waterfall']}")
    print(f"wrote_review={review_csv} rows={segment_counts['review']}")
    print(f"wrote_summary={summary_txt}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Split stage1 state into quick/waterfall/review segments")
    p.add_argument("state_csv")
    p.add_argument("quick_csv")
    p.add_argument("waterfall_csv")
    p.add_argument("review_csv")
    p.add_argument("summary_txt")
    p.add_argument("--qa-report", default="")
    args = p.parse_args()
    if not args.qa_report:
        args.qa_report = str(Path(args.summary_txt).with_name("stage1_split_qa.json"))
    return args


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
