#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

try:
    from .qa import qa_assert_file, qa_validate_person_csv, write_qa_report
    from .schema import clean, is_email, union_field_order, write_csv_rows
except ImportError:  # pragma: no cover
    from qa import qa_assert_file, qa_validate_person_csv, write_qa_report
    from schema import clean, is_email, union_field_order, write_csv_rows

GOOD = {"deliverable", "accept_all"}


def norm(v: str) -> str:
    return clean(v).lower()


def merge_rows(stage1_rows: list[dict], extra_provider: list[dict], extra_reverify: list[dict]) -> tuple[list[dict], dict]:
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()

    base_deliverable = 0
    base_catch_all = 0
    added_provider_deliverable = 0
    added_provider_catch_all = 0
    added_reverify_deliverable = 0
    added_reverify_catch_all = 0

    for row in stage1_rows:
        result = norm(row.get("result"))
        if result not in GOOD:
            continue
        email = norm(row.get("email"))
        full_name = norm(row.get("full_name"))
        key = (email, full_name)
        if not is_email(email) or key in seen:
            continue
        seen.add(key)
        rows.append(dict(row))
        if result == "deliverable":
            base_deliverable += 1
        else:
            base_catch_all += 1

    def add_extra(extra_rows: list[dict], source_label: str) -> None:
        nonlocal added_provider_deliverable, added_provider_catch_all, added_reverify_deliverable, added_reverify_catch_all
        for row in extra_rows:
            new_email = norm(row.get("new_email"))
            verify_result = norm(row.get("new_email_verify_result"))
            if not is_email(new_email) or verify_result not in GOOD:
                continue

            row2 = dict(row)
            row2["email"] = new_email
            row2["email_source"] = clean(row.get("new_email_source")) or row2.get("email_source") or source_label
            row2["result"] = verify_result

            full_name = norm(row2.get("full_name"))
            key = (new_email, full_name)
            if key in seen:
                continue

            seen.add(key)
            row2.pop("new_email", None)
            row2.pop("new_email_source", None)
            row2.pop("new_email_verify_result", None)
            rows.append(row2)

            if source_label == "provider_loop":
                if verify_result == "deliverable":
                    added_provider_deliverable += 1
                else:
                    added_provider_catch_all += 1
            else:
                if verify_result == "deliverable":
                    added_reverify_deliverable += 1
                else:
                    added_reverify_catch_all += 1

    add_extra(extra_provider, "provider_loop")
    add_extra(extra_reverify, "provider_reverify")

    summary = {
        "base_deliverable": base_deliverable,
        "base_catch_all": base_catch_all,
        "added_provider_deliverable": added_provider_deliverable,
        "added_provider_catch_all": added_provider_catch_all,
        "added_reverify_deliverable": added_reverify_deliverable,
        "added_reverify_catch_all": added_reverify_catch_all,
        "total_output_rows": len(rows),
    }
    return rows, summary


def read_rows_or_empty(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    import csv

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        return [dict(row) for row in r]


def run(args: argparse.Namespace) -> None:
    stage1_state = Path(args.stage1_state)
    provider_extra = Path(args.provider_extra)
    reverify_extra = Path(args.reverify_extra)
    output_csv = Path(args.output)
    summary_txt = Path(args.summary)
    qa_report = Path(args.qa_report)

    qa_assert_file(stage1_state, "stage1_state")

    stage1_rows = read_rows_or_empty(stage1_state)
    provider_rows = read_rows_or_empty(provider_extra)
    reverify_rows = read_rows_or_empty(reverify_extra)

    merged, summary = merge_rows(stage1_rows, provider_rows, reverify_rows)
    field_order: list[str] = []
    if merged:
        field_order = union_field_order(merged)
    else:
        for source_rows in (stage1_rows, provider_rows, reverify_rows):
            if source_rows:
                field_order = union_field_order(source_rows)
                break
    write_csv_rows(output_csv, merged, field_order=field_order)

    summary_lines = [
        f"stage1_state={stage1_state}",
        f"provider_extra={provider_extra}",
        f"reverify_extra={reverify_extra}",
        f"output={output_csv}",
    ]
    for k, v in summary.items():
        summary_lines.append(f"{k}={v}")
    summary_txt.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    metrics = qa_validate_person_csv(output_csv, "merged_output") if output_csv.exists() and output_csv.stat().st_size > 0 else None
    write_qa_report(
        qa_report,
        {
            "rows": summary["total_output_rows"],
            "metrics": {
                "rows": metrics.rows,
                "unique_emails": metrics.unique_emails,
                "unknown_or_other_total": metrics.unknown_or_other_total,
            }
            if metrics
            else None,
        },
    )

    print(
        f"wrote {output_csv} rows={summary['total_output_rows']} "
        f"base(d={summary['base_deliverable']},ca={summary['base_catch_all']}) "
        f"provider_add(d={summary['added_provider_deliverable']},ca={summary['added_provider_catch_all']}) "
        f"reverify_add(d={summary['added_reverify_deliverable']},ca={summary['added_reverify_catch_all']})"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Schema-safe final merge")
    p.add_argument("stage1_state")
    p.add_argument("provider_extra")
    p.add_argument("reverify_extra")
    p.add_argument("output")
    p.add_argument("summary")
    p.add_argument("--qa-report", default="")
    args = p.parse_args()
    if not args.qa_report:
        args.qa_report = str(Path(args.summary).with_name("quick_wins_merge_qa.json"))
    return args


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
