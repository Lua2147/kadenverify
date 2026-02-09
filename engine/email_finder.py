"""Email Finder Module — find emails given (first_name, last_name, domain).

Waterfall:
  1. Domain intelligence (DNS, provider, catch-all) — cached per domain
  2. Generate candidate patterns ordered by corporate frequency
  3. SMTP batch verification (one connection, all RCPT TO)
  4. Enrichment waterfall (cheapest first):
     a. Apollo local DB (free)
     b. Exa web search ($0.0005)
     c. Prospeo enrich-person (1 credit, ~$0.006)
     d. Apollo API ($0.10)
     e. Pattern scoring fallback (free)
"""

import asyncio
import logging
import os
from typing import Callable, Optional

import aiohttp

from .dns import lookup_mx
from .models import (
    CandidateResult,
    DnsInfo,
    FinderResult,
    Provider,
    Reachability,
)
from .smtp import check_catch_all, smtp_check_batch

logger = logging.getLogger("kadenverify.finder")

_config_cache: Optional[dict] = None


def _load_key_from_config(service: str) -> Optional[str]:
    """Load an API key from config/api_keys.json (monorepo standard)."""
    global _config_cache
    if _config_cache is None:
        from pathlib import Path
        import json
        candidates = [
            Path(__file__).parent.parent.parent.parent / "config" / "api_keys.json",
            Path.home() / "Mundi Princeps" / "config" / "api_keys.json",
        ]
        for p in candidates:
            if p.exists():
                _config_cache = json.loads(p.read_text())
                break
        if _config_cache is None:
            _config_cache = {}
    entry = _config_cache.get(service, {})
    return entry.get("api_key") if isinstance(entry, dict) else None


# ---------------------------------------------------------------------------
# Pattern generation
# ---------------------------------------------------------------------------

PATTERNS: list[tuple[str, Callable[[str, str], str]]] = [
    ("first.last",  lambda f, l: f"{f}.{l}"),
    ("flast",       lambda f, l: f"{f[0]}{l}"),
    ("firstl",      lambda f, l: f"{f}{l[0]}"),
    ("first",       lambda f, l: f),
    ("first_last",  lambda f, l: f"{f}_{l}"),
    ("first-last",  lambda f, l: f"{f}-{l}"),
    ("f.last",      lambda f, l: f"{f[0]}.{l}"),
    ("lastf",       lambda f, l: f"{l}{f[0]}"),
    ("last.first",  lambda f, l: f"{l}.{f}"),
    ("firstlast",   lambda f, l: f"{f}{l}"),
]


def generate_candidates(first_name: str, last_name: str, domain: str) -> list[CandidateResult]:
    """Generate candidate emails ordered by corporate frequency."""
    first = first_name.lower().strip()
    last = last_name.lower().strip()
    seen: set[str] = set()
    candidates: list[CandidateResult] = []
    for pattern_name, fn in PATTERNS:
        local = fn(first, last)
        email = f"{local}@{domain}"
        if email not in seen:
            seen.add(email)
            candidates.append(CandidateResult(email=email, pattern=pattern_name))
    return candidates


# ---------------------------------------------------------------------------
# Domain intelligence cache
# ---------------------------------------------------------------------------

_domain_cache: dict[str, tuple[DnsInfo, Optional[bool]]] = {}


async def _get_domain_intel(domain: str) -> tuple[DnsInfo, Optional[bool]]:
    """Return (dns_info, is_catchall) for a domain, cached."""
    if domain in _domain_cache:
        return _domain_cache[domain]

    dns_info = await lookup_mx(domain)
    is_catchall: Optional[bool] = None
    if dns_info.has_mx:
        is_catchall = await check_catch_all(domain, dns_info.mx_hosts[0])

    _domain_cache[domain] = (dns_info, is_catchall)
    return dns_info, is_catchall


# ---------------------------------------------------------------------------
# Enrichment helpers (Phase 4)
# ---------------------------------------------------------------------------

def _lookup_apollo_local(
    first_name: str, last_name: str, domain: str
) -> Optional[CandidateResult]:
    """Query local Apollo DuckDB for a person at this domain (zero cost)."""
    try:
        import duckdb
        from pathlib import Path

        apollo_db_path = os.environ.get("APOLLO_DB_PATH")
        if not apollo_db_path:
            defaults = [
                "./apollo.duckdb",
                "./data/apollo.duckdb",
                str(Path.home() / "Mundi Princeps" / "apps" / "people-warehouse" / "etl" / "apollo.duckdb"),
            ]
            for p in defaults:
                if Path(p).exists():
                    apollo_db_path = p
                    break

        if not apollo_db_path or not Path(apollo_db_path).exists():
            return None

        conn = duckdb.connect(apollo_db_path, read_only=True)
        row = conn.execute(
            """
            SELECT email FROM persons
            WHERE LOWER(organization_domain) = LOWER(?)
              AND (LOWER(name) ILIKE ? OR (LOWER(first_name) ILIKE ? AND LOWER(last_name) ILIKE ?))
            LIMIT 1
            """,
            [domain, f"%{first_name}%{last_name}%", f"%{first_name}%", f"%{last_name}%"],
        ).fetchone()
        conn.close()

        if row and row[0]:
            return CandidateResult(
                email=row[0],
                pattern="apollo_local",
                confidence=0.90,
                source="apollo_local",
            )
    except Exception as e:
        logger.debug(f"Apollo local lookup failed: {e}")
    return None


async def _search_apollo_api(
    first_name: str, last_name: str, domain: str, api_key: str
) -> Optional[CandidateResult]:
    """Query Apollo API /v1/people/match (costs ~$0.10)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.apollo.io/v1/people/match",
                headers={"X-Api-Key": api_key},
                json={
                    "first_name": first_name,
                    "last_name": last_name,
                    "organization_name": domain.split(".")[0],
                    "domain": domain,
                },
                timeout=aiohttp.ClientTimeout(total=10),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    person = data.get("person")
                    if person and person.get("email"):
                        return CandidateResult(
                            email=person["email"],
                            pattern="apollo_api",
                            confidence=0.92,
                            source="apollo_api",
                        )
    except Exception as e:
        logger.debug(f"Apollo API failed: {e}")
    return None


async def _search_exa(
    first_name: str, last_name: str, domain: str, api_key: str
) -> Optional[CandidateResult]:
    """Search Exa for email mentions (costs ~$0.0005)."""
    import re

    queries = [
        f'"{first_name} {last_name}" email @{domain}',
        f'"{first_name} {last_name}" {domain.split(".")[0]} site:linkedin.com',
    ]
    try:
        async with aiohttp.ClientSession() as session:
            for query in queries:
                async with session.post(
                    "https://api.exa.ai/search",
                    headers={"x-api-key": api_key},
                    json={"query": query, "num_results": 3},
                    timeout=aiohttp.ClientTimeout(total=5),
                    ssl=False,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        text = " ".join(
                            r.get("text", "") for r in data.get("results", [])
                        ).lower()
                        # Look for an email at the target domain
                        found = re.findall(
                            rf"[\w.+-]+@{re.escape(domain)}", text
                        )
                        if found:
                            return CandidateResult(
                                email=found[0],
                                pattern="exa_search",
                                confidence=0.85,
                                source="exa",
                            )
    except Exception as e:
        logger.debug(f"Exa search failed: {e}")
    return None


# ---------------------------------------------------------------------------
# Prospeo enrich-person (1 credit per match, ~$0.006)
# ---------------------------------------------------------------------------

async def _search_prospeo(
    first_name: str, last_name: str, domain: str, api_key: str
) -> Optional[CandidateResult]:
    """Find email via Prospeo enrich-person API.

    POST https://api.prospeo.io/enrich-person
    1 credit per match (no charge on miss or duplicate).
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.prospeo.io/enrich-person",
                headers={"X-KEY": api_key, "Content-Type": "application/json"},
                json={
                    "data": {
                        "first_name": first_name,
                        "last_name": last_name,
                        "company_website": domain,
                    },
                    "only_verified_email": True,
                },
                timeout=aiohttp.ClientTimeout(total=15),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("error") is False:
                        person = data.get("person", {})
                        email_obj = person.get("email", {})
                        email = email_obj.get("email", "").strip() if isinstance(email_obj, dict) else ""
                        if email and "@" in email:
                            verified = email_obj.get("status") == "VERIFIED"
                            return CandidateResult(
                                email=email,
                                pattern="prospeo_enrich",
                                confidence=0.95 if verified else 0.85,
                                source="prospeo",
                            )
                elif resp.status == 429:
                    logger.warning("Prospeo rate limited")
                else:
                    logger.debug(f"Prospeo returned {resp.status}")
    except Exception as e:
        logger.debug(f"Prospeo enrichment failed: {e}")
    return None


# ---------------------------------------------------------------------------
# Core finder
# ---------------------------------------------------------------------------

async def find_email(
    first_name: str,
    last_name: str,
    domain: str,
    company_name: Optional[str] = None,
    apollo_api_key: Optional[str] = None,
    exa_api_key: Optional[str] = None,
    prospeo_api_key: Optional[str] = None,
    helo_domain: str = "verify.kadenwood.com",
    from_address: str = "verify@kadenwood.com",
) -> FinderResult:
    """Find an email address for a person at a domain.

    Waterfall: DNS -> pattern generation -> SMTP batch -> enrichment fallback.
    """
    domain = domain.lower().strip().lstrip("@")
    first_name = first_name.strip()
    last_name = last_name.strip()
    cost = 0.0

    # --- Phase 1: Domain intelligence ---
    dns_info, is_catchall = await _get_domain_intel(domain)

    if not dns_info.has_mx:
        return FinderResult(
            error=f"No MX records for {domain}",
            provider=dns_info.provider,
        )

    mx_host = dns_info.mx_hosts[0]

    # --- Phase 2: Generate candidates ---
    candidates = generate_candidates(first_name, last_name, domain)

    # --- Phase 3: SMTP batch verification (non-catch-all) ---
    if is_catchall is False:
        emails = [c.email for c in candidates]
        responses = await smtp_check_batch(
            emails, mx_host, helo_domain=helo_domain, from_address=from_address
        )

        for candidate, resp in zip(candidates, responses):
            candidate.smtp_code = resp.code
            if resp.code == 250:
                candidate.confidence = 0.95
                candidate.source = "smtp"

        # First 250 wins
        winner = next((c for c in candidates if c.smtp_code == 250), None)
        if winner:
            return FinderResult(
                email=winner.email,
                confidence=winner.confidence,
                method="smtp_verified",
                reachability=Reachability.safe,
                domain_is_catchall=False,
                provider=dns_info.provider,
                candidates_tried=len(candidates),
                candidates=candidates,
                cost=cost,
            )

        # All 550 = no valid pattern, go to enrichment
        all_rejected = all(500 <= c.smtp_code < 600 for c in candidates)
        if all_rejected:
            # Fall through to enrichment
            pass
        else:
            # Mixed/timeout - partial results, still try enrichment
            pass

    # --- Phase 4: Enrichment waterfall (cheapest first) ---

    # 4a: Apollo local DB (free — zero cost)
    apollo_local = _lookup_apollo_local(first_name, last_name, domain)
    if apollo_local:
        candidates.append(apollo_local)
        return FinderResult(
            email=apollo_local.email,
            confidence=apollo_local.confidence,
            method="apollo_local_db",
            reachability=Reachability.risky,
            domain_is_catchall=is_catchall,
            provider=dns_info.provider,
            candidates_tried=len(candidates),
            candidates=candidates,
            cost=cost,
        )

    # 4b: Exa web search ($0.0005)
    if not exa_api_key:
        exa_api_key = os.environ.get("EXA_API_KEY")
    if not exa_api_key:
        exa_api_key = _load_key_from_config("exa")
    if exa_api_key:
        exa_result = await _search_exa(first_name, last_name, domain, exa_api_key)
        cost += 0.0005
        if exa_result:
            candidates.append(exa_result)
            return FinderResult(
                email=exa_result.email,
                confidence=exa_result.confidence,
                method="exa_search",
                reachability=Reachability.risky,
                domain_is_catchall=is_catchall,
                provider=dns_info.provider,
                candidates_tried=len(candidates),
                candidates=candidates,
                cost=cost,
            )

    # 4c: Prospeo enrich-person (1 credit, ~$0.006)
    if not prospeo_api_key:
        prospeo_api_key = os.environ.get("PROSPEO_API_KEY")
    if not prospeo_api_key:
        prospeo_api_key = _load_key_from_config("prospeo")
    if prospeo_api_key:
        prospeo_result = await _search_prospeo(
            first_name, last_name, domain, prospeo_api_key
        )
        cost += 0.006
        if prospeo_result:
            candidates.append(prospeo_result)
            return FinderResult(
                email=prospeo_result.email,
                confidence=prospeo_result.confidence,
                method="prospeo_enrich",
                reachability=Reachability.risky,
                domain_is_catchall=is_catchall,
                provider=dns_info.provider,
                candidates_tried=len(candidates),
                candidates=candidates,
                cost=cost,
            )

    # 4d: Apollo API ($0.10 — most expensive, last)
    if not apollo_api_key:
        apollo_api_key = os.environ.get("APOLLO_API_KEY")
    if not apollo_api_key:
        apollo_api_key = _load_key_from_config("apollo")
    if apollo_api_key:
        apollo_result = await _search_apollo_api(
            first_name, last_name, domain, apollo_api_key
        )
        cost += 0.10
        if apollo_result:
            candidates.append(apollo_result)
            return FinderResult(
                email=apollo_result.email,
                confidence=apollo_result.confidence,
                method="apollo_api",
                reachability=Reachability.risky,
                domain_is_catchall=is_catchall,
                provider=dns_info.provider,
                candidates_tried=len(candidates),
                candidates=candidates,
                cost=cost,
            )

    # 4e: Pattern scoring fallback (catch-all domains)
    if is_catchall is True:
        from .catchall_validator import score_catchall_email

        best_candidate = candidates[0]  # first.last is most common
        score = score_catchall_email(
            email=best_candidate.email,
            first_name=first_name,
            last_name=last_name,
            company_name=company_name,
        )
        best_candidate.confidence = score.confidence
        best_candidate.source = "pattern_score"
        return FinderResult(
            email=best_candidate.email,
            confidence=score.confidence,
            method="pattern_score_catchall",
            reachability=Reachability.risky if score.confidence >= 0.50 else Reachability.unknown,
            domain_is_catchall=True,
            provider=dns_info.provider,
            candidates_tried=len(candidates),
            candidates=candidates,
            cost=cost,
        )

    # Nothing found
    return FinderResult(
        confidence=0.0,
        method="exhausted",
        domain_is_catchall=is_catchall,
        provider=dns_info.provider,
        candidates_tried=len(candidates),
        candidates=candidates,
        cost=cost,
    )


# ---------------------------------------------------------------------------
# Batch finder
# ---------------------------------------------------------------------------

async def find_emails_batch(
    contacts: list[dict],
    concurrency: int = 10,
    apollo_api_key: Optional[str] = None,
    exa_api_key: Optional[str] = None,
    prospeo_api_key: Optional[str] = None,
    helo_domain: str = "verify.kadenwood.com",
    from_address: str = "verify@kadenwood.com",
    progress_callback: Optional[Callable[[FinderResult], None]] = None,
) -> list[FinderResult]:
    """Find emails for many contacts, grouped by domain for efficiency.

    Each contact dict must have: first_name, last_name, domain.
    Optional: company_name.
    """
    from collections import defaultdict

    # Group by domain
    domain_groups: dict[str, list[tuple[int, dict]]] = defaultdict(list)
    for idx, contact in enumerate(contacts):
        d = contact.get("domain", "").lower().strip().lstrip("@")
        domain_groups[d].append((idx, contact))

    results: list[Optional[FinderResult]] = [None] * len(contacts)
    sem = asyncio.Semaphore(concurrency)

    async def process_domain(domain: str, group: list[tuple[int, dict]]):
        # Pre-warm domain intelligence (cached)
        await _get_domain_intel(domain)

        # Process contacts within same domain sequentially (SMTP connection reuse)
        for idx, contact in group:
            async with sem:
                result = await find_email(
                    first_name=contact["first_name"],
                    last_name=contact["last_name"],
                    domain=domain,
                    company_name=contact.get("company_name"),
                    apollo_api_key=apollo_api_key,
                    exa_api_key=exa_api_key,
                    prospeo_api_key=prospeo_api_key,
                    helo_domain=helo_domain,
                    from_address=from_address,
                )
                results[idx] = result
                if progress_callback:
                    progress_callback(result)

    # Process different domains concurrently
    tasks = [
        process_domain(domain, group)
        for domain, group in domain_groups.items()
    ]
    await asyncio.gather(*tasks)

    return results
