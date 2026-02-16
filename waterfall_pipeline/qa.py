from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .schema import (
    PERSON_HEADERS_30,
    SchemaValidationError,
    count_token_hits,
    detect_email_column,
    ensure_required_headers,
    is_email,
    read_csv_rows,
)


@dataclass
class QAMetrics:
    rows: int
    unique_emails: int
    unknown_or_other_total: int


def qa_assert_file(path: Path, label: str) -> None:
    if not path.exists() or path.stat().st_size == 0:
        raise SchemaValidationError(f"Missing or empty {label}: {path}")


def qa_validate_person_csv(path: Path, label: str) -> QAMetrics:
    qa_assert_file(path, label)
    headers, rows = read_csv_rows(path)
    ensure_required_headers(headers, ["email"], label=label)

    value_rows = [[row.get(h, "") for h in headers] for row in rows]
    email_idx = headers.index("email")
    detected = detect_email_column(value_rows, declared_index=email_idx)

    emails = set()
    for row in value_rows:
        e = row[detected] if detected < len(row) else ""
        if is_email(e):
            emails.add(e.strip().lower())

    hits = count_token_hits(value_rows, headers, {"unknown", "other"})
    return QAMetrics(
        rows=len(rows),
        unique_emails=len(emails),
        unknown_or_other_total=sum(hits.values()),
    )


def qa_assert_zero_overlap(candidate_emails: set[str], excluded_emails: set[str], label: str) -> None:
    overlap = len(candidate_emails & excluded_emails)
    if overlap != 0:
        raise SchemaValidationError(f"{label} overlap must be 0, found {overlap}")


def qa_assert_required_headers(headers: list[str], allow_shifted: bool = False) -> None:
    # Strict contract for output tab rows.
    ensure_required_headers(headers, PERSON_HEADERS_30, label="output_csv")
    if not allow_shifted and headers != PERSON_HEADERS_30:
        missing = [h for h in PERSON_HEADERS_30 if h not in headers]
        extra = [h for h in headers if h not in PERSON_HEADERS_30]
        if missing or extra:
            raise SchemaValidationError(f"Header contract mismatch missing={missing} extra={extra}")


def write_qa_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
