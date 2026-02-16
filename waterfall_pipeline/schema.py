from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Iterable

PERSON_HEADERS_30 = [
    "db_name",
    "source_databases",
    "duplicate_records",
    "full_name",
    "first_name",
    "last_name",
    "email",
    "phone",
    "linkedin_url",
    "job_title",
    "seniority",
    "industry",
    "org_name",
    "org_domain",
    "org_website",
    "org_industry",
    "org_num_employees",
    "org_revenue_k",
    "org_hq_city",
    "org_hq_state",
    "org_hq_country",
    "source",
    "source_file",
    "linkedin_connections",
    "tier_label",
    "tier",
    "contact_key",
    "has_email",
    "has_phone",
    "has_linkedin",
]

PLACEHOLDER_TOKENS = {"", "unknown", "other", "n/a", "na", "none", "null", "-", "--"}


class SchemaValidationError(RuntimeError):
    pass


def clean(v: str | None) -> str:
    return (v or "").strip()


def is_email(v: str | None) -> bool:
    s = clean(v).lower()
    return "@" in s and "." in s.split("@")[-1]


def is_placeholder(v: str | None) -> bool:
    return clean(v).lower() in PLACEHOLDER_TOKENS


def normalize_email(v: str | None) -> str:
    return clean(v).lower()


def cap_first(v: str | None) -> str:
    s = clean(v)
    if not s:
        return ""
    return s[:1].upper() + s[1:].lower()


def normalize_industry(v: str | None) -> str:
    s = clean(v)
    if not s:
        return ""
    words = []
    for tok in s.split():
        if tok.isupper() and len(tok) <= 4:
            words.append(tok)
        else:
            words.append(tok[:1].upper() + tok[1:].lower())
    return " ".join(words)


def ensure_required_headers(headers: Iterable[str], required: Iterable[str], label: str = "csv") -> None:
    hs = set(headers)
    missing = [h for h in required if h not in hs]
    if missing:
        raise SchemaValidationError(f"{label} missing required headers: {missing}")


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        headers = list(r.fieldnames or [])
        rows = [{k: clean(v) for k, v in row.items()} for row in r]
    return headers, rows


def write_csv_rows(path: Path, rows: list[dict[str, str]], field_order: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        field_order = field_order or []
    else:
        if not field_order:
            field_order = union_field_order(rows)

    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=field_order or [], extrasaction="ignore")
        w.writeheader()
        for row in rows:
            if field_order:
                w.writerow({k: row.get(k, "") for k in field_order})
            else:
                w.writerow({})


def union_field_order(rows: list[dict[str, str]]) -> list[str]:
    out: list[str] = []
    for row in rows:
        for k in row.keys():
            if k not in out:
                out.append(k)
    return out


def detect_email_column(rows: list[list[str]], declared_index: int = 6, sample_limit: int = 2000) -> int:
    counts: Counter[int] = Counter()
    for row in rows[:sample_limit]:
        for i, v in enumerate(row):
            if is_email(v):
                counts[i] += 1

    if not counts:
        return declared_index

    best_idx, best_count = counts.most_common(1)[0]
    declared_count = counts.get(declared_index, 0)
    if best_count > max(declared_count * 2, 10):
        return best_idx
    return declared_index


def rows_to_email_set(values_rows: list[list[str]], email_idx: int) -> set[str]:
    out = set()
    for row in values_rows:
        e = normalize_email(row[email_idx] if email_idx < len(row) else "")
        if is_email(e):
            out.add(e)
    return out


def count_token_hits(values_rows: list[list[str]], headers: list[str], tokens: set[str]) -> Counter[str]:
    hits: Counter[str] = Counter()
    low_tokens = {t.lower() for t in tokens}
    for row in values_rows:
        for i, col in enumerate(headers):
            v = clean(row[i] if i < len(row) else "").lower()
            if v in low_tokens:
                hits[col] += 1
    return hits


def apply_row_defaults(row: dict[str, str]) -> dict[str, str]:
    out = {k: clean(v) for k, v in row.items()}
    out["email"] = normalize_email(out.get("email"))
    out["first_name"] = cap_first(out.get("first_name"))
    out["last_name"] = cap_first(out.get("last_name"))
    if is_placeholder(out.get("full_name")):
        out["full_name"] = (out["first_name"] + " " + out["last_name"]).strip()

    if not is_placeholder(out.get("industry")):
        out["industry"] = normalize_industry(out.get("industry"))
    if not is_placeholder(out.get("org_industry")):
        out["org_industry"] = normalize_industry(out.get("org_industry"))

    if is_placeholder(out.get("db_name")):
        out["db_name"] = "qualified"
    if is_placeholder(out.get("duplicate_records")):
        out["duplicate_records"] = "0"
    if is_placeholder(out.get("source")):
        out["source"] = "original"
    if is_placeholder(out.get("tier_label")):
        out["tier_label"] = "tier_unknown"
    if is_placeholder(out.get("tier")):
        out["tier"] = "tier_unknown"
    if is_placeholder(out.get("linkedin_connections")):
        out["linkedin_connections"] = "0"
    if is_placeholder(out.get("contact_key")):
        out["contact_key"] = out.get("email", "")

    out["has_email"] = "1" if is_email(out.get("email")) else "0"
    out["has_phone"] = "0" if is_placeholder(out.get("phone")) else "1"
    out["has_linkedin"] = "0" if is_placeholder(out.get("linkedin_url")) else "1"
    return out
