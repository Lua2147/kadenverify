#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

try:
    from .qa import qa_assert_file, write_qa_report
    from .schema import clean
except ImportError:  # pragma: no cover
    from qa import qa_assert_file, write_qa_report
    from schema import clean

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


def unresolved_keys_from_state(path: Path) -> set[str]:
    keys: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            key = clean(row.get("contact_key"))
            cur = clean(row.get("current_result") or row.get("verify_result")).lower()
            if key and cur not in GOOD:
                keys.add(key)
    return keys


def write_round2_input(state_csv: Path, waterfall_csv: Path, out_csv: Path) -> tuple[int, int]:
    unresolved = unresolved_keys_from_state(state_csv)
    rows = []
    fieldnames: list[str] = []

    with waterfall_csv.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        fieldnames = list(r.fieldnames or [])
        for row in r:
            key = make_contact_key(row)
            if key and key in unresolved:
                rows.append(row)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if not fieldnames:
        fieldnames = [
            "first_name",
            "last_name",
            "full_name",
            "email",
            "company",
            "domain",
            "position",
            "website",
            "phone",
            "linkedin",
            "location",
            "profile_url",
            "source_file",
            "email_source",
            "result",
            "find_method",
            "find_confidence",
        ]

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return len(unresolved), len(rows)


def run(args: argparse.Namespace) -> None:
    state_csv = Path(args.state_csv)
    waterfall_csv = Path(args.waterfall_csv)
    out_csv = Path(args.output_csv)
    summary_txt = Path(args.summary_txt)
    qa_report = Path(args.qa_report)

    qa_assert_file(state_csv, "reverify_state")
    qa_assert_file(waterfall_csv, "waterfall_unknown_undeliverable")

    unresolved, written = write_round2_input(state_csv, waterfall_csv, out_csv)
    summary_lines = [
        f"state_csv={state_csv}",
        f"waterfall_csv={waterfall_csv}",
        f"output_csv={out_csv}",
        f"unresolved_keys={unresolved}",
        f"round2_rows={written}",
    ]
    summary_txt.parent.mkdir(parents=True, exist_ok=True)
    summary_txt.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    write_qa_report(
        qa_report,
        {
            "state_csv": str(state_csv),
            "waterfall_csv": str(waterfall_csv),
            "output_csv": str(out_csv),
            "unresolved_keys": unresolved,
            "round2_rows": written,
        },
    )

    print(f"unresolved_keys={unresolved}")
    print(f"round2_rows={written}")
    print(f"round2_input={out_csv}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build round2 waterfall input from unresolved reverify state")
    p.add_argument("state_csv")
    p.add_argument("waterfall_csv")
    p.add_argument("output_csv")
    p.add_argument("--summary-txt", default="")
    p.add_argument("--qa-report", default="")
    args = p.parse_args()
    if not args.summary_txt:
        args.summary_txt = str(Path(args.output_csv).with_name("round2_input_summary.txt"))
    if not args.qa_report:
        args.qa_report = str(Path(args.output_csv).with_name("round2_input_qa.json"))
    return args


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
