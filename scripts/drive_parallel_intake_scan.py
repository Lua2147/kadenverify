#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
import re
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import requests


ROOT_FOLDER_ID = "1LMsC6GCXl0PlWkqImvew1EpTb4ROkNPU"
RUN_DIR = Path("/data/local-machine-backup/20260211/email-verifier-runs/tier1_tier2_v2_fullloop_20260211")
OUT_BASE = RUN_DIR / "drive_parallel_intake"
TOKEN_FILE = Path("/opt/mundi-princeps/config/token.json")
DRIVE_API = "https://www.googleapis.com/drive/v3"
REQUIRE_ASSET_HEAVY_SECTOR = False

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
US_CA_RE = re.compile(r"\b(united states|usa|u\.s\.?a?\.?|canada)\b", re.I)

ALLOWED_TITLE_TOKENS = [
    "cfo",
    "chief financial officer",
    "vp finance",
    "vice president finance",
    "svp finance",
    "senior vice president finance",
    "treasurer",
    "assistant treasurer",
    "corporate treasurer",
    "vp corporate finance",
    "director of finance",
    "finance director",
    "vp treasury",
    "head of treasury",
    "ceo",
    "chief executive officer",
    "president",
    "owner",
    "co-owner",
    "chief operating officer",
    "coo",
    "vp corporate development",
    "director",
    "controller",
]

ALLOWED_NAICS_PREFIXES = {
    "331",
    "332",
    "333",
    "335",
    "339",
    "336",
    "325",
    "326",
    "322",
    "327",
    "321",
    "311",
    "312",
    "236",
    "237",
    "238",
    "484",
    "482",
    "486",
    "488",
    "483",
    "481",
    "493",
    "211",
    "213",
    "324",
    "212",
    "221",
    "562",
}

ASSET_HEAVY_KEYWORDS = [
    "manufactur",
    "industrial",
    "metal",
    "machinery",
    "electrical",
    "automotive",
    "aerospace",
    "chemical",
    "plastic",
    "paper",
    "building",
    "food",
    "beverage",
    "construction",
    "civil",
    "trucking",
    "freight",
    "rail",
    "pipeline",
    "logistics",
    "warehouse",
    "3pl",
    "cold chain",
    "energy",
    "oil",
    "gas",
    "mining",
    "utility",
    "waste",
    "environmental",
    "equipment",
    "fleet",
    "capex",
    "abl",
    "asset-based",
]

EXCLUDE_KEYWORDS = [
    "reit",
    "real estate",
    "property management",
    "multifamily",
    "property development",
    "brokerage",
    "chapter 11",
    "bankruptcy",
    "liquidation",
    "saas",
    "influencer",
]

EXCLUDE_FINANCE_KEYWORDS = [
    "bank",
    "credit union",
    "financial services",
    "finance company",
    "lender",
    "loan servicing",
    "mortgage",
    "insurance",
    "wealth management",
    "asset management",
    "investment management",
    "hedge fund",
    "private equity",
    "venture capital",
    "broker-dealer",
    "broker dealer",
    "capital markets",
    "fintech",
    "payments",
]

EXCLUDE_GOV_KEYWORDS = [
    "government",
    "federal",
    "state of",
    "county",
    "city of",
    "municipal",
    "public sector",
    "department of",
    "ministry",
    "agency",
    "authority",
    "army",
    "navy",
    "air force",
    "defense",
]

EXCLUDE_NONPROFIT_KEYWORDS = [
    "nonprofit",
    "non-profit",
    "not-for-profit",
    "not for profit",
    "ngo",
    "charity",
    "charitable",
    "foundation",
    "association",
    "public charity",
]

ALLOWED_COMPANY_TYPES = {
    "privately held",
    "public company",
    "partnership",
    "self-owned",
    "self owned",
}

CONTACT_EXTS = {".csv", ".tsv", ".txt", ".xlsx", ".xls"}
CONTACT_MIMES = {
    "text/csv",
    "application/csv",
    "text/plain",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.google-apps.spreadsheet",
}

EMAIL_COLUMNS = [
    "Email Address",
    "Email",
    "email",
    "email_address",
    "Work Email",
    "Contact Primary E-mail Address",
    "E-mail Address",
    "Personal Email",
    "personal_email",
]
NAME_COLUMNS = ["Full Name", "full_name", "Name", "name", "Contact Name", "Contact Full Name", "member_full_name"]
FIRST_COLUMNS = ["First Name", "first_name", "FirstName", "Contact First Name", "member_name_first"]
LAST_COLUMNS = ["Last Name", "last_name", "LastName", "Contact Last Name", "member_name_last"]
COMPANY_COLUMNS = ["Primary Company", "Company", "company", "Organization", "Company Name", "company_name"]
WEBSITE_COLUMNS = ["Primary Company Website", "Website", "website", "Domain", "Company Website", "domain"]
TITLE_COLUMNS = ["Primary Title", "Title", "Position", "Job Title", "title", "position", "job_title", "Contact Title"]
LOCATION_COLUMNS = [
    "Location",
    "location",
    "City",
    "Geography",
    "Contact Location",
    "member_location_raw_address",
    "Country",
    "company_country",
]
INDUSTRY_COLUMNS = [
    "industry",
    "Industry",
    "org_industry",
    "sector",
    "categories_and_keywords",
    "Company Keywords",
    "NAICS",
    "naics",
    "naics_codes",
]
REVENUE_COLUMNS = ["revenue", "Revenue", "annual_revenue", "min_revenue_annual", "Company Revenue", "org_revenue_k"]
SIZE_COLUMNS = [
    "size_range",
    "company_size",
    "Company Size",
    "Headcount range (LinkedIn)",
    "mapped_company_size",
    "org_num_employees",
    "Employees Count (LinkedIn)",
]
TYPE_COLUMNS = ["company_type", "Company Type", "Ownership Type", "Company Ownership Type"]
COUNTRY_COLUMNS = ["Country", "country", "Company Country", "org_hq_country"]
NAICS_COLUMNS = ["naics", "NAICS", "naics_codes", "NAICS Code", "Industry (NAICS/SIC)"]


def clean(v: object) -> str:
    return str(v or "").strip()


def lclean(v: object) -> str:
    return clean(v).lower()


def find_col(headers: list[str], candidates: list[str]) -> str | None:
    exact = {h.strip(): h.strip() for h in headers}
    for c in candidates:
        if c in exact:
            return exact[c]
    lower = {h.lower().strip(): h.strip() for h in headers}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def safe_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", name or "file")[:180]


def parse_revenue(v: str) -> float | None:
    s = lclean(v).replace(",", "").replace("$", "").strip()
    if not s:
        return None
    mult = 1
    if s.endswith("k"):
        mult = 1_000
        s = s[:-1]
    elif s.endswith("m"):
        mult = 1_000_000
        s = s[:-1]
    elif s.endswith("b"):
        mult = 1_000_000_000
        s = s[:-1]
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1)) * mult
    except Exception:
        return None


def parse_size_range(v: str) -> tuple[int, int] | None:
    s = lclean(v)
    if not s:
        return None
    m = re.search(r"(\d{1,6})\s*[-–]\s*(\d{1,6})", s)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m2 = re.search(r"(\d{1,6})", s)
    if m2:
        n = int(m2.group(1))
        return (n, n)
    return None


def title_matches(title: str) -> bool:
    t = lclean(title)
    return bool(t) and any(tok in t for tok in ALLOWED_TITLE_TOKENS)


def geo_matches(*vals: str) -> bool:
    txt = " | ".join([lclean(v) for v in vals if lclean(v)])
    return bool(txt) and bool(US_CA_RE.search(txt))


def has_exclude_keywords(*vals: str) -> bool:
    txt = " | ".join([lclean(v) for v in vals if lclean(v)])
    return any(k in txt for k in EXCLUDE_KEYWORDS)


def is_excluded_vertical(company: str, industry: str, naics_text: str, company_type: str, domain: str, source_name: str) -> bool:
    blob = " | ".join(
        [
            lclean(company),
            lclean(industry),
            lclean(naics_text),
            lclean(company_type),
            lclean(domain),
            lclean(source_name),
        ]
    )

    # Hard domain exclusions for government/military.
    if domain:
        d = lclean(domain)
        if d.endswith(".gov") or d.endswith(".mil"):
            return True

    if any(k in blob for k in EXCLUDE_FINANCE_KEYWORDS):
        return True
    if any(k in blob for k in EXCLUDE_GOV_KEYWORDS):
        return True
    if any(k in blob for k in EXCLUDE_NONPROFIT_KEYWORDS):
        return True

    # NAICS vertical exclusions:
    # 52* = Finance/Insurance, 92* = Public Administration, 813* = Nonprofit/Civic orgs.
    naics_codes = re.findall(r"\d{3,6}", lclean(naics_text))
    for code in naics_codes:
        if code.startswith("52") or code.startswith("92") or code.startswith("813"):
            return True

    return False


def asset_heavy_match(industry_text: str, naics_text: str, source_name: str) -> bool:
    i = lclean(industry_text)
    n = lclean(naics_text)
    s = lclean(source_name)
    if n:
        codes = re.findall(r"\d{3}", n)
        if any(c in ALLOWED_NAICS_PREFIXES for c in codes):
            return True
    blob = " | ".join([i, n, s])
    if any(k in blob for k in ASSET_HEAVY_KEYWORDS):
        return True
    return any(k in s for k in ["debt", "naics", "tier", "mandate", "asset-heavy", "asset_heavy"])


def guess_domain(company: str) -> str:
    c = lclean(company)
    if not c:
        return ""
    c = re.sub(
        r"\b(inc|llc|ltd|corp|co|group|holdings|partners|capital|management|ventures|advisors|consulting|international|services|solutions|technologies|global|the)\b",
        "",
        c,
    )
    c = re.sub(r"[^a-z0-9]", "", c).strip()
    return f"{c}.com" if c else ""


def extract_first_email(values: object) -> str:
    for v in values:
        m = EMAIL_RE.search(str(v or ""))
        if m:
            return m.group(0).lower()
    return ""


def load_token_data() -> dict:
    return json.loads(TOKEN_FILE.read_text(encoding="utf-8"))


def save_token_data(d: dict) -> None:
    TOKEN_FILE.write_text(json.dumps(d, indent=2), encoding="utf-8")


def refresh_access_token(d: dict) -> str:
    rt = d.get("refresh_token")
    cid = d.get("client_id")
    cs = d.get("client_secret")
    token_uri = d.get("token_uri") or "https://oauth2.googleapis.com/token"
    if not (rt and cid and cs):
        return d.get("access_token") or d.get("token") or ""
    resp = requests.post(
        token_uri,
        data={
            "client_id": cid,
            "client_secret": cs,
            "refresh_token": rt,
            "grant_type": "refresh_token",
        },
        timeout=45,
    )
    resp.raise_for_status()
    tok = resp.json().get("access_token", "")
    if tok:
        d["access_token"] = tok
        d["token"] = tok
        save_token_data(d)
    return tok


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    token_data = load_token_data()
    access_token = refresh_access_token(token_data)
    if not access_token:
        raise RuntimeError("No Google access token")

    session = requests.Session()

    def drive_get(endpoint: str, params: dict | None = None, stream: bool = False, export: bool = False, retry: int = 1):
        nonlocal access_token, token_data
        params = dict(params or {})
        if not export:
            params.setdefault("supportsAllDrives", "true")
            params.setdefault("includeItemsFromAllDrives", "true")
        url = endpoint if endpoint.startswith("https://") else f"{DRIVE_API}/{endpoint}"
        headers = {"Authorization": f"Bearer {access_token}"}
        resp = session.get(url, params=params, headers=headers, timeout=120, stream=stream)
        if resp.status_code == 401 and retry > 0:
            access_token = refresh_access_token(token_data)
            return drive_get(endpoint, params=params, stream=stream, export=export, retry=retry - 1)
        if resp.status_code >= 400:
            raise RuntimeError(f"Drive API error {resp.status_code}: {resp.text[:500]}")
        return resp.content if stream else resp.json()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = OUT_BASE / stamp
    dl_dir = out_dir / "downloads"
    out_dir.mkdir(parents=True, exist_ok=True)
    dl_dir.mkdir(parents=True, exist_ok=True)

    about = drive_get("about", {"fields": "user"})
    print("drive_user=", json.dumps(about.get("user", {}), ensure_ascii=False))

    folders_seen: set[str] = set()
    queue: deque[str] = deque([ROOT_FOLDER_ID])
    files: list[dict] = []

    while queue:
        folder_id = queue.popleft()
        if folder_id in folders_seen:
            continue
        folders_seen.add(folder_id)
        page = None
        while True:
            resp = drive_get(
                "files",
                {
                    "q": f"'{folder_id}' in parents and trashed=false",
                    "fields": "nextPageToken,files(id,name,mimeType,size,modifiedTime,parents)",
                    "pageSize": 1000,
                    **({"pageToken": page} if page else {}),
                },
            )
            for item in resp.get("files", []):
                if item.get("mimeType") == "application/vnd.google-apps.folder":
                    queue.append(item["id"])
                else:
                    files.append(item)
            page = resp.get("nextPageToken")
            if not page:
                break

    print(f"folders_scanned={len(folders_seen)} files_found={len(files)}")

    candidates: list[dict] = []
    for f in files:
        name = f.get("name", "")
        ext = Path(name).suffix.lower()
        mt = f.get("mimeType", "")
        lname = name.lower()
        looks_contact = any(
            k in lname
            for k in ["contact", "lead", "email", "prospect", "apollo", "list", "investor", "tier", "naics", "mandate", "debt"]
        )
        if ext in CONTACT_EXTS or mt in CONTACT_MIMES or looks_contact:
            candidates.append(f)

    print(f"candidate_files={len(candidates)}")

    downloaded: list[dict] = []
    failed: list[dict] = []

    for idx, f in enumerate(candidates, 1):
        fid = f["id"]
        name = f.get("name", "file")
        mt = f.get("mimeType", "")
        ext = Path(name).suffix.lower()
        try:
            if mt == "application/vnd.google-apps.spreadsheet":
                data = drive_get(
                    f"https://www.googleapis.com/drive/v3/files/{fid}/export",
                    {"mimeType": "text/csv"},
                    stream=True,
                    export=True,
                )
                out_ext = ".csv"
            else:
                data = drive_get(f"files/{fid}", {"alt": "media"}, stream=True)
                if ext:
                    out_ext = ext
                elif mt in ("text/csv", "application/csv"):
                    out_ext = ".csv"
                elif "spreadsheet" in mt or "excel" in mt:
                    out_ext = ".xlsx"
                else:
                    out_ext = ".bin"

            out = dl_dir / f"{safe_name(Path(name).stem)}_{fid}{out_ext}"
            out.write_bytes(data)
            downloaded.append(
                {
                    "id": fid,
                    "name": name,
                    "mimeType": mt,
                    "modifiedTime": f.get("modifiedTime", ""),
                    "size": f.get("size", ""),
                    "path": str(out),
                }
            )
        except Exception as e:
            failed.append({"id": fid, "name": name, "mimeType": mt, "error": str(e)})

        if idx % 50 == 0:
            print(f"download_progress={idx}/{len(candidates)} ok={len(downloaded)} failed={len(failed)}")

    print(f"download_complete ok={len(downloaded)} failed={len(failed)}")

    records: list[dict] = []
    parse_errors: list[dict] = []

    def append_from_map(row_map: dict[str, str], meta: dict) -> None:
        headers = list(row_map.keys())

        email_col = find_col(headers, EMAIL_COLUMNS)
        name_col = find_col(headers, NAME_COLUMNS)
        first_col = find_col(headers, FIRST_COLUMNS)
        last_col = find_col(headers, LAST_COLUMNS)
        comp_col = find_col(headers, COMPANY_COLUMNS)
        web_col = find_col(headers, WEBSITE_COLUMNS)
        title_col = find_col(headers, TITLE_COLUMNS)
        loc_col = find_col(headers, LOCATION_COLUMNS)
        ind_col = find_col(headers, INDUSTRY_COLUMNS)
        rev_col = find_col(headers, REVENUE_COLUMNS)
        size_col = find_col(headers, SIZE_COLUMNS)
        type_col = find_col(headers, TYPE_COLUMNS)
        country_col = find_col(headers, COUNTRY_COLUMNS)
        naics_col = find_col(headers, NAICS_COLUMNS)

        full_name = clean(row_map.get(name_col, "")) if name_col else ""
        first_name = clean(row_map.get(first_col, "")) if first_col else ""
        last_name = clean(row_map.get(last_col, "")) if last_col else ""
        if not full_name and (first_name or last_name):
            full_name = f"{first_name} {last_name}".strip()
        if full_name and not (first_name and last_name):
            pts = full_name.split()
            if not first_name and pts:
                first_name = pts[0]
            if not last_name and len(pts) >= 2:
                last_name = pts[-1]

        email = clean(row_map.get(email_col, "")).lower() if email_col else ""
        if not EMAIL_RE.fullmatch(email or ""):
            email = extract_first_email(row_map.values())

        company = clean(row_map.get(comp_col, "")) if comp_col else ""
        website = clean(row_map.get(web_col, "")) if web_col else ""
        title = clean(row_map.get(title_col, "")) if title_col else ""
        location = clean(row_map.get(loc_col, "")) if loc_col else ""
        industry = clean(row_map.get(ind_col, "")) if ind_col else ""
        company_type = clean(row_map.get(type_col, "")) if type_col else ""
        country = clean(row_map.get(country_col, "")) if country_col else ""
        naics = clean(row_map.get(naics_col, "")) if naics_col else ""
        revenue_raw = clean(row_map.get(rev_col, "")) if rev_col else ""
        size_raw = clean(row_map.get(size_col, "")) if size_col else ""

        domain = ""
        if email and "@" in email:
            domain = email.split("@", 1)[1].lower()
        elif website:
            domain = (
                website.lower()
                .replace("https://", "")
                .replace("http://", "")
                .replace("www.", "")
                .split("/", 1)[0]
                .split(":", 1)[0]
                .strip()
            )
        elif company:
            domain = guess_domain(company)

        if not (email or (first_name and last_name and domain)):
            return
        if not title_matches(title):
            return
        if not geo_matches(location, country, industry):
            return
        if has_exclude_keywords(company, industry, meta.get("name", "")):
            return
        if is_excluded_vertical(company, industry, naics, company_type, domain, meta.get("name", "")):
            return
        if REQUIRE_ASSET_HEAVY_SECTOR and not asset_heavy_match(industry, naics, meta.get("name", "")):
            return

        rev = parse_revenue(revenue_raw)
        if rev is not None and not (50_000_000 <= rev <= 1_000_000_000):
            return

        rng = parse_size_range(size_raw)
        if rng is not None:
            lo, hi = rng
            if hi < 201 or lo > 10000:
                return

        ctype = lclean(company_type)
        if ctype and all(t not in ctype for t in ALLOWED_COMPANY_TYPES):
            return

        records.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "full_name": full_name,
                "email": email,
                "company": company,
                "domain": domain,
                "position": title,
                "location": location,
                "industry": industry,
                "naics": naics,
                "revenue_raw": revenue_raw,
                "size_raw": size_raw,
                "company_type": company_type,
                "country": country,
                "source_file": meta.get("name", ""),
                "source_file_id": meta.get("id", ""),
                "source_mime": meta.get("mimeType", ""),
                "source_path": meta.get("path", ""),
            }
        )

    for f in downloaded:
        p = Path(f["path"])
        ext = p.suffix.lower()
        try:
            if ext in {".csv", ".tsv", ".txt"}:
                raw = p.read_text(encoding="utf-8", errors="ignore")
                sample = raw[:4096]
                delim = ","
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
                    delim = dialect.delimiter
                except Exception:
                    pass
                reader = csv.DictReader(io.StringIO(raw), delimiter=delim)
                for row in reader:
                    if row:
                        row_map = {str(k): clean(v) for k, v in row.items() if k is not None}
                        if row_map:
                            append_from_map(row_map, f)
            elif ext in {".xlsx", ".xls"}:
                import openpyxl

                wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
                for ws in wb.worksheets[:8]:
                    it = ws.iter_rows(min_row=1, values_only=True)
                    header = next(it, None)
                    if not header:
                        continue
                    headers = [str(h).strip() if h is not None else f"col_{i}" for i, h in enumerate(header)]
                    for vals in it:
                        row_map: dict[str, str] = {}
                        for i, h in enumerate(headers):
                            row_map[h] = clean(vals[i]) if i < len(vals) and vals[i] is not None else ""
                        append_from_map(row_map, f)
                wb.close()
        except Exception as e:
            parse_errors.append({"path": str(p), "error": str(e)})

    print(f"criteria_matched_records_raw={len(records)} parse_errors={len(parse_errors)}")

    dedup: dict[str, dict] = {}
    for r in records:
        e = lclean(r.get("email"))
        if e:
            key = f"e:{e}"
        else:
            key = f"k:{lclean(r.get('first_name'))}|{lclean(r.get('last_name'))}|{lclean(r.get('domain'))}"
        if key not in dedup:
            dedup[key] = r

    criteria_records = list(dedup.values())
    criteria_with_email = [r for r in criteria_records if lclean(r.get("email"))]
    criteria_no_email = [r for r in criteria_records if not lclean(r.get("email"))]
    print(f"criteria_unique={len(criteria_records)} with_email={len(criteria_with_email)} no_email={len(criteria_no_email)}")

    existing_files = [
        RUN_DIR / "state.csv",
        RUN_DIR / "quick_wins_plus_catchall_fullloop.csv",
        RUN_DIR / "bulk_net_new_consolidated_sendable_2026-02-18.csv",
        RUN_DIR / "provider_loop" / "provider_reverify_additional_usable.csv",
        RUN_DIR / "provider_loop" / "provider_reverify_state.csv",
    ]
    existing_emails: set[str] = set()
    for p in existing_files:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for c in ("email", "new_email"):
                    e = lclean(row.get(c, ""))
                    if EMAIL_RE.fullmatch(e or ""):
                        existing_emails.add(e)

    net_new = [r for r in criteria_with_email if lclean(r.get("email")) not in existing_emails]
    print(f"existing_email_universe={len(existing_emails)} criteria_net_new={len(net_new)}")

    write_csv(out_dir / "downloaded_files.csv", downloaded)
    write_csv(out_dir / "failed_downloads.csv", failed)
    write_csv(out_dir / "parse_errors.csv", parse_errors)
    write_csv(out_dir / "criteria_matched_contacts.csv", criteria_records)
    write_csv(out_dir / "criteria_matched_net_new_email_contacts.csv", net_new)
    write_csv(out_dir / "criteria_matched_no_email_contacts.csv", criteria_no_email)

    summary = {
        "timestamp_utc": stamp,
        "root_folder_id": ROOT_FOLDER_ID,
        "drive_user": about.get("user", {}),
        "folders_scanned": len(folders_seen),
        "files_found": len(files),
        "candidate_files": len(candidates),
        "downloaded_files": len(downloaded),
        "failed_downloads": len(failed),
        "criteria_matched_records_raw": len(records),
        "criteria_matched_unique": len(criteria_records),
        "criteria_matched_with_email": len(criteria_with_email),
        "criteria_matched_no_email": len(criteria_no_email),
        "existing_email_universe": len(existing_emails),
        "criteria_matched_net_new_email": len(net_new),
        "output_dir": str(out_dir),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
