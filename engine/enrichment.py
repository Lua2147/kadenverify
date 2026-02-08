"""Tier 4-5 with SMTP Verification Loop

After Exa/Apollo finds a person, we re-verify via SMTP to confirm deliverability.
Only mark as VALID if both enrichment AND SMTP confirm.
"""

import asyncio
import logging
import re
from typing import Optional, Tuple
import aiohttp

logger = logging.getLogger("kadenverify.enrichment")

# Import SMTP verification
from .verifier import verify_email as smtp_verify_email

# Import catch-all validator for advanced scoring
from .catchall_validator import (
    score_catchall_email,
    check_apollo_local,
    check_apollo_match,
)

ROLE_KEYWORDS = [
    'info', 'admin', 'support', 'sales', 'contact', 'help', 'service',
    'team', 'hello', 'hi', 'mail', 'webmaster', 'noreply', 'no-reply',
]

CORPORATE_DOMAINS = {
    'apple.com': 0.92, 'microsoft.com': 0.92, 'google.com': 0.92,
    'amazon.com': 0.92, 'facebook.com': 0.92, 'meta.com': 0.92,
}


def extract_name_from_email(email: str) -> Tuple[Optional[str], Optional[str], float, str]:
    """Extract name and confidence from email pattern."""
    local = email.split('@')[0].lower()

    # Pattern 1: first.last@
    match = re.match(r'^([a-z]{2,})\.([a-z]{2,})$', local)
    if match:
        first, last = match.groups()
        if 2 <= len(first) <= 15 and 2 <= len(last) <= 20:
            return (first.capitalize(), last.capitalize(), 0.92, 'first.last')

    # Pattern 2: first.m.last@
    match = re.match(r'^([a-z]{2,})\.([a-z])\.([a-z]{2,})$', local)
    if match:
        first, middle, last = match.groups()
        return (first.capitalize(), last.capitalize(), 0.88, 'first.m.last')

    # Pattern 3: first_last or first-last@
    for sep in ['_', '-']:
        if sep in local:
            parts = local.split(sep)
            if len(parts) == 2 and all(2 <= len(p) <= 15 and p.isalpha() for p in parts):
                return (parts[0].capitalize(), parts[1].capitalize(), 0.86, f'first{sep}last')

    # Pattern 4: f.last@ or flast@
    match = re.match(r'^([a-z])\.([a-z]{2,})$', local)
    if match:
        first_initial, last = match.groups()
        return (first_initial.upper(), last.capitalize(), 0.78, 'f.last')

    match = re.match(r'^([a-z])([a-z]{3,})$', local)
    if match and len(local) <= 10:
        first_initial, last = match.groups()
        return (first_initial.upper(), last.capitalize(), 0.74, 'flast')

    return (None, None, 0.0, 'no_pattern')


def tier4_catchall_advanced(email: str, is_catchall: bool = False) -> Optional[Tuple[str, float, str]]:
    """Tier 4: ADVANCED catch-all validation using catch-all validator.

    Uses Apollo local DB, pattern matching, and heuristics.
    Returns None if not confident enough (falls back to tier4_free_pattern).
    """
    if not is_catchall:
        return None  # Only for catch-all domains

    # Extract name from email pattern
    first, last, _, _ = extract_name_from_email(email)

    # Check Apollo local database (zero cost, 90% accuracy)
    apollo_match = check_apollo_local(email)

    # Score using advanced catch-all validator
    score = score_catchall_email(
        email=email,
        first_name=first,
        last_name=last,
        apollo_match=apollo_match,
    )

    # High confidence → VALID
    if score.is_likely_real and score.confidence >= 0.80:
        reason_str = '_'.join(score.reasons[:2])  # Top 2 reasons
        return ('valid', score.confidence, f'catchall_validated_{reason_str}')

    # Medium confidence → RISKY
    elif score.confidence >= 0.50:
        reason_str = '_'.join(score.reasons[:2])
        return ('risky', score.confidence, f'catchall_medium_{reason_str}')

    # Low confidence → INVALID
    elif score.confidence < 0.30:
        reason_str = '_'.join(score.reasons[:2])
        return ('invalid', score.confidence, f'catchall_low_{reason_str}')

    return None  # Fall back to tier4_free_pattern


def tier4_free_pattern(email: str) -> Tuple[str, float, str]:
    """Tier 4: FREE pattern-based enrichment (fallback)."""
    domain = email.split('@')[1].lower()
    local = email.split('@')[0].lower()

    # Rule 1: Role accounts → RISKY
    for keyword in ROLE_KEYWORDS:
        if keyword in local:
            return ('risky', 0.90, f'role_account_{keyword}')

    # Rule 2: Extract name pattern
    first, last, name_conf, pattern = extract_name_from_email(email)

    # Rule 3: Known corporate domains + good pattern → VALID
    if domain in CORPORATE_DOMAINS and name_conf >= 0.70:
        combined_conf = (CORPORATE_DOMAINS[domain] + name_conf) / 2
        return ('valid', combined_conf, f'corporate_{pattern}')

    # Rule 4: Strong pattern → VALID
    if name_conf >= 0.88:
        return ('valid', name_conf, f'strong_pattern_{pattern}')

    # Rule 5: Medium pattern → RISKY
    if 0.70 <= name_conf < 0.88:
        return ('risky', name_conf, f'medium_pattern_{pattern}')

    return ('risky', 0.55, f'low_confidence_{pattern}')


async def tier5a_exa_search(email: str, exa_api_key: str) -> dict:
    """Tier 5A: Exa web search."""
    first, last, _, _ = extract_name_from_email(email)

    if not first or not last:
        return {'found': False, 'reason': 'no_name'}

    domain = email.split('@')[1]
    company = domain.split('.')[0].capitalize()

    queries = [
        f"{first} {last} {company} site:linkedin.com",
        f"{first} {last} site:{domain}",
    ]

    try:
        async with aiohttp.ClientSession() as session:
            for query in queries:
                async with session.post(
                    'https://api.exa.ai/search',
                    headers={'x-api-key': exa_api_key},
                    json={'query': query, 'num_results': 3},
                    timeout=aiohttp.ClientTimeout(total=5),
                    ssl=False,
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = data.get('results', [])

                        if results:
                            text = ' '.join([r.get('text', '') for r in results]).lower()

                            has_email = email.lower() in text
                            has_name = first.lower() in text and last.lower() in text
                            has_company = company.lower() in text

                            confidence = 0.50
                            if has_email:
                                confidence = 0.95
                            elif has_name and has_company:
                                confidence = 0.85

                            if confidence >= 0.85:
                                return {'found': True, 'confidence': confidence}

    except Exception as e:
        logger.debug(f"Exa error: {e}")

    return {'found': False}


async def tier5b_apollo_api(email: str, apollo_api_key: str) -> dict:
    """Tier 5B: Apollo API with quality filtering."""
    first, last, _, _ = extract_name_from_email(email)

    try:
        payload = {'email': email}
        if first:
            payload['first_name'] = first
        if last:
            payload['last_name'] = last

        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://api.apollo.io/v1/people/match',
                headers={'X-Api-Key': apollo_api_key},
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
                ssl=False,
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    person = data.get('person')

                    if person:
                        name = person.get('name', '').lower()
                        title = person.get('title')

                        # Quality check
                        if not title:
                            return {'found': False, 'reason': 'no_title'}

                        if first and last:
                            if first.lower() not in name and last.lower() not in name:
                                return {'found': False, 'reason': 'name_mismatch'}

                        return {
                            'found': True,
                            'confidence': 0.92,
                            'name': person.get('name'),
                            'title': title,
                        }

    except Exception as e:
        logger.debug(f"Apollo error: {e}")

    return {'found': False}


async def tier6_smtp_reverify(
    email: str,
    helo_domain: str,
    from_address: str
) -> Tuple[Optional[bool], int, str]:
    """Tier 6: Re-verify enriched email via SMTP.

    Returns: (is_valid, smtp_code, smtp_message)
    """
    try:
        result = await smtp_verify_email(email, helo_domain, from_address)

        # Check SMTP result
        if result.smtp_code == 250:
            return (True, 250, 'smtp_confirmed')
        elif result.smtp_code >= 500:
            return (False, result.smtp_code, 'smtp_rejected')
        else:
            # Inconclusive
            return (None, result.smtp_code or 0, 'smtp_inconclusive')

    except Exception as e:
        logger.error(f"SMTP re-verify failed: {e}")
        return (None, 0, 'smtp_error')


async def enrich_unknown(
    email: str,
    exa_api_key: Optional[str] = None,
    apollo_api_key: Optional[str] = None,
    helo_domain: str = "verify.kadenwood.com",
    from_address: str = "verify@kadenwood.com",
    is_catchall: bool = False,
) -> Tuple[str, float, str, float]:
    """Full enrichment with SMTP verification loop.

    Returns: (status, confidence, reason, cost)
    """
    total_cost = 0.0

    # Tier 4A: ADVANCED catch-all validation (if catch-all domain)
    if is_catchall:
        catchall_result = tier4_catchall_advanced(email, is_catchall=True)
        if catchall_result:
            status, confidence, reason = catchall_result
            # High confidence from catch-all validator → return immediately
            if confidence >= 0.75:
                return (status, confidence, f'tier4a_{reason}', 0.0)

    # Tier 4B: FREE pattern (fallback or non-catch-all)
    status, confidence, reason = tier4_free_pattern(email)

    if status == 'valid' and confidence >= 0.88:
        return (status, confidence, f'tier4_{reason}', 0.0)

    if status == 'risky' and 'role_account' in reason:
        return (status, confidence, f'tier4_{reason}', 0.0)

    # Tier 5: Enrichment
    enrichment_found = False
    enrichment_source = None
    enrichment_confidence = 0.0

    # Try Exa
    if exa_api_key:
        exa_result = await tier5a_exa_search(email, exa_api_key)
        total_cost += 0.0005

        if exa_result.get('found'):
            enrichment_found = True
            enrichment_source = 'exa'
            enrichment_confidence = exa_result.get('confidence', 0.85)

    # Try Apollo (if Exa failed)
    if not enrichment_found and apollo_api_key:
        apollo_result = await tier5b_apollo_api(email, apollo_api_key)
        total_cost += 0.10

        if apollo_result.get('found'):
            enrichment_found = True
            enrichment_source = 'apollo'
            enrichment_confidence = apollo_result.get('confidence', 0.92)

    # Tier 6: SMTP Re-verification
    if enrichment_found:
        logger.info(f"✅ {email}: Found via {enrichment_source}, re-verifying SMTP...")

        is_valid, smtp_code, smtp_msg = await tier6_smtp_reverify(
            email, helo_domain, from_address
        )

        if is_valid == True:
            # Both enrichment AND SMTP confirm
            logger.info(f"✅ {email}: SMTP confirmed (250)")
            return (
                'valid',
                0.95,
                f'tier6_{enrichment_source}_smtp_confirmed',
                total_cost
            )
        elif is_valid == False:
            # Enrichment found but SMTP rejects
            logger.warning(f"❌ {email}: SMTP rejected ({smtp_code}) despite {enrichment_source} match")
            return (
                'invalid',
                0.90,
                f'tier6_{enrichment_source}_smtp_rejected_{smtp_code}',
                total_cost
            )
        else:
            # SMTP inconclusive, trust enrichment
            logger.info(f"⚠️  {email}: SMTP inconclusive, trusting {enrichment_source}")
            return (
                'valid',
                enrichment_confidence * 0.9,
                f'tier6_{enrichment_source}_smtp_inconclusive',
                total_cost
            )

    # No enrichment found
    return ('risky', confidence, f'tier4_{reason}', total_cost)
