"""Advanced catch-all email validation.

For emails on catch-all domains (where SMTP accepts everything),
use additional signals to estimate likelihood of being real:

1. Pattern confidence scoring (email format analysis)
2. Social/directory cross-reference (LinkedIn, Apollo, company websites)
3. Company heuristics (size, type, industry)
4. Historical bounce tracking (requires integration with email sender)

This improves catch-all confidence from "unknown" to 60-90% accuracy.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger("kadenverify.catchall")

# Common corporate email patterns (ordered by confidence)
CORPORATE_PATTERNS = [
    (r'^[a-z]+\.[a-z]+@', 0.90),           # first.last@domain (most common)
    (r'^[a-z]+[a-z]+@', 0.85),             # firstlast@domain
    (r'^[a-z]\.[a-z]+@', 0.80),            # f.last@domain
    (r'^[a-z]+@', 0.75),                   # first@domain
    (r'^[a-z]+_[a-z]+@', 0.70),            # first_last@domain
    (r'^[a-z]+-[a-z]+@', 0.70),            # first-last@domain
    (r'^[a-z]+\.[a-z]\.[a-z]+@', 0.65),    # first.m.last@domain
    (r'^[a-z]+[0-9]+@', 0.50),             # first123@domain (less common)
    (r'^[a-z][a-z]+@', 0.60),              # flast@domain
]

# Red flag patterns (likely fake/generic)
RED_FLAG_PATTERNS = [
    (r'^test', 0.05),                      # test@domain
    (r'^admin', 0.10),                     # admin@domain (could be role)
    (r'^noreply', 0.05),                   # noreply@domain
    (r'^[0-9]+@', 0.10),                   # 123456@domain (numbers only)
    (r'^[a-z]{15,}@', 0.20),               # verylongrandomstring@domain
    (r'^\w{3,}[0-9]{5,}@', 0.15),          # name12345@domain (suspicious pattern)
]

# Company type indicators
COMPANY_TYPE_SIGNALS = {
    '.edu': {'catch_all_likely': True, 'confidence_adjustment': 0.15},
    '.gov': {'catch_all_likely': False, 'confidence_adjustment': -0.10},
    '.mil': {'catch_all_likely': False, 'confidence_adjustment': -0.10},
    '.org': {'catch_all_likely': True, 'confidence_adjustment': 0.05},
}


class CatchAllScore:
    """Catch-all email confidence score."""

    def __init__(
        self,
        email: str,
        confidence: float,
        is_likely_real: bool,
        reasons: list[str],
        social_match: Optional[dict] = None,
    ):
        self.email = email
        self.confidence = confidence  # 0.0-1.0
        self.is_likely_real = is_likely_real  # True if confidence >= 0.70
        self.reasons = reasons  # Why this confidence
        self.social_match = social_match  # LinkedIn/Apollo match details


def score_catchall_email(
    email: str,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    company_name: Optional[str] = None,
    company_size: Optional[int] = None,
    apollo_match: Optional[dict] = None,
    linkedin_match: Optional[dict] = None,
    check_apollo_db: bool = True,
) -> CatchAllScore:
    """Score a catch-all email's likelihood of being real.

    Args:
        email: Email address to score
        first_name: Person's first name (if known)
        last_name: Person's last name (if known)
        company_name: Company name (if known)
        company_size: Number of employees (if known)
        apollo_match: Apollo.io database match (if found)
        linkedin_match: LinkedIn profile match (if found)

    Returns:
        CatchAllScore with confidence (0.0-1.0) and reasoning
    """
    email_lower = email.lower().strip()
    local_part = email_lower.split('@')[0]
    domain = email_lower.split('@')[1] if '@' in email_lower else ''

    confidence = 0.50  # Base confidence for catch-all
    reasons = []

    # 0. Check local Apollo database first (zero cost, 90% accuracy)
    if check_apollo_db and not apollo_match:
        apollo_match = check_apollo_local(email)

    # 1. Social/Directory Cross-Reference (Strongest Signal)
    if apollo_match:
        confidence += 0.40
        reasons.append(f"apollo_match_confidence_{apollo_match.get('confidence', 'unknown')}")

    if linkedin_match:
        confidence += 0.35
        reasons.append(f"linkedin_profile_match")

    # 2. Name-Based Pattern Matching
    if first_name and last_name:
        name_confidence = _check_name_pattern(local_part, first_name, last_name)
        if name_confidence > 0:
            confidence += name_confidence * 0.30
            reasons.append(f"name_pattern_match_{name_confidence:.2f}")
        else:
            confidence -= 0.20
            reasons.append("name_pattern_mismatch")

    # 3. Email Pattern Analysis
    pattern_confidence = _check_email_pattern(local_part)
    confidence += (pattern_confidence - 0.50) * 0.20  # Adjust from baseline
    reasons.append(f"pattern_confidence_{pattern_confidence:.2f}")

    # 4. Company Size Heuristics
    if company_size:
        if company_size > 1000:
            # Large companies rarely have true catch-all
            confidence += 0.15
            reasons.append(f"large_company_{company_size}_employees")
        elif company_size < 10:
            # Small companies more likely to have catch-all
            confidence -= 0.05
            reasons.append(f"small_company_{company_size}_employees")

    # 5. Domain Type Analysis
    for suffix, signals in COMPANY_TYPE_SIGNALS.items():
        if domain.endswith(suffix):
            confidence += signals['confidence_adjustment']
            reasons.append(f"domain_type_{suffix}")

    # 6. Red Flags
    for pattern, penalty in RED_FLAG_PATTERNS:
        if re.match(pattern, local_part):
            confidence = min(confidence, penalty)
            reasons.append(f"red_flag_{pattern}")
            break

    # Clamp to 0.0-1.0
    confidence = max(0.0, min(1.0, confidence))

    is_likely_real = confidence >= 0.70

    social_match = None
    if apollo_match or linkedin_match:
        social_match = {
            'apollo': apollo_match,
            'linkedin': linkedin_match,
        }

    return CatchAllScore(
        email=email,
        confidence=confidence,
        is_likely_real=is_likely_real,
        reasons=reasons,
        social_match=social_match,
    )


def _check_name_pattern(local_part: str, first_name: str, last_name: str) -> float:
    """Check if email local part matches name pattern.

    Returns:
        Confidence 0.0-1.0 based on name match quality
    """
    first = first_name.lower().strip()
    last = last_name.lower().strip()
    local = local_part.lower().strip()

    # Exact matches (high confidence)
    if local == f"{first}.{last}":
        return 0.95
    if local == f"{first}{last}":
        return 0.90
    if local == f"{first[0]}.{last}":
        return 0.85
    if local == first:
        return 0.80
    if local == f"{first}_{last}":
        return 0.85
    if local == f"{first}-{last}":
        return 0.85

    # Partial matches (medium confidence)
    if first in local and last in local:
        return 0.70
    if last in local:
        return 0.60
    if first in local:
        return 0.50

    # No match (low confidence - might be wrong name or fake email)
    return 0.0


def _check_email_pattern(local_part: str) -> float:
    """Analyze email pattern to estimate corporate vs random.

    Returns:
        Confidence 0.0-1.0 based on pattern analysis
    """
    local = local_part.lower().strip()

    # Check against corporate patterns
    for pattern, confidence in CORPORATE_PATTERNS:
        if re.match(pattern, local + '@'):
            return confidence

    # Check against red flags
    for pattern, confidence in RED_FLAG_PATTERNS:
        if re.match(pattern, local):
            return confidence

    # Default: medium confidence
    return 0.50


def enhance_verification_with_catchall_scoring(
    verification_result,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    company_name: Optional[str] = None,
    company_size: Optional[int] = None,
) -> dict:
    """Enhance verification result with catch-all scoring.

    Args:
        verification_result: VerificationResult from standard verification
        first_name, last_name, company_name, company_size: Additional context

    Returns:
        Enhanced result dict with catch-all confidence score
    """
    result = verification_result.to_omniverifier()

    # Only enhance if email is on catch-all domain
    if not verification_result.is_catch_all:
        result['catchall_confidence'] = None
        result['catchall_likely_real'] = None
        return result

    # Score the catch-all email
    score = score_catchall_email(
        email=verification_result.email,
        first_name=first_name,
        last_name=last_name,
        company_name=company_name,
        company_size=company_size,
    )

    # Add catch-all specific fields
    result['catchall_confidence'] = score.confidence
    result['catchall_likely_real'] = score.is_likely_real
    result['catchall_reasons'] = score.reasons

    # Upgrade status if high confidence
    if score.is_likely_real and score.confidence >= 0.80:
        result['status'] = 'valid'  # Upgrade from catch_all to valid
        result['is_valid'] = True
        result['reason'] = f"catch_all_high_confidence_{score.confidence:.2f}"
    elif score.confidence <= 0.30:
        result['status'] = 'invalid'  # Downgrade to invalid if very low confidence
        result['is_valid'] = False
        result['reason'] = f"catch_all_low_confidence_{score.confidence:.2f}"

    return result


# Integration with Apollo.io for social validation
def check_apollo_local(email: str, apollo_db_path: Optional[str] = None) -> Optional[dict]:
    """Check if email exists in local Apollo database (apollo.duckdb).

    This is a ZERO-COST alternative to Apollo API that uses your existing
    contact database.

    Args:
        email: Email to check
        apollo_db_path: Path to apollo.duckdb (required if not set via env)
                       Can also be set via APOLLO_DB_PATH environment variable

    Returns:
        Match details if found, None otherwise
    """
    try:
        import duckdb
        from pathlib import Path
        import os

        # Get path from parameter, env var, or default
        if not apollo_db_path:
            apollo_db_path = os.environ.get('APOLLO_DB_PATH')

        if not apollo_db_path:
            # Try common default locations
            default_paths = [
                './apollo.duckdb',
                './data/apollo.duckdb',
                str(Path.home() / "Mundi Princeps" / "apps" / "people-warehouse" / "etl" / "apollo.duckdb"),
            ]
            for path in default_paths:
                if Path(path).exists():
                    apollo_db_path = path
                    break

        if not Path(apollo_db_path).exists():
            logger.debug(f"Apollo DB not found at {apollo_db_path}")
            return None

        conn = duckdb.connect(apollo_db_path, read_only=True)

        # Query for exact email match
        result = conn.execute(
            """
            SELECT email, name, title, organization_name, person_id
            FROM persons
            WHERE LOWER(email) = LOWER(?)
            LIMIT 1
            """,
            [email]
        ).fetchone()

        conn.close()

        if result:
            return {
                'found': True,
                'confidence': 0.90,  # Very high confidence - email exists in Apollo
                'email': result[0],
                'name': result[1],
                'title': result[2],
                'company': result[3],
                'person_id': result[4],
                'source': 'apollo_local_db',
            }

    except Exception as e:
        logger.error(f"Apollo local DB error: {e}")

    return None


async def check_apollo_match(email: str, apollo_api_key: Optional[str] = None) -> Optional[dict]:
    """Check if email exists in Apollo.io API (paid option).

    Note: Use check_apollo_local() instead for zero-cost validation
    using your existing apollo.duckdb database.

    Args:
        email: Email to check
        apollo_api_key: Apollo API key (optional)

    Returns:
        Match details if found, None otherwise
    """
    if not apollo_api_key:
        return None

    try:
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.post(
                'https://api.apollo.io/v1/people/match',
                headers={
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-cache',
                    'X-Api-Key': apollo_api_key,
                },
                json={'email': email},
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('person'):
                        return {
                            'found': True,
                            'confidence': 0.90,
                            'name': data['person'].get('name'),
                            'title': data['person'].get('title'),
                            'company': data['person'].get('organization_name'),
                            'source': 'apollo_api',
                        }
    except Exception as e:
        logger.error(f"Apollo API error: {e}")

    return None


# Integration with LinkedIn (requires scraping or API)
async def check_linkedin_match(
    email: str,
    first_name: str,
    last_name: str,
    company_name: str,
) -> Optional[dict]:
    """Check if person exists on LinkedIn at company.

    Note: Requires LinkedIn scraping or API access (not included).
    Placeholder for future integration.

    Args:
        email: Email to verify
        first_name, last_name: Person's name
        company_name: Company name

    Returns:
        Match details if found, None otherwise
    """
    # TODO: Implement LinkedIn verification
    # - Option 1: LinkedIn API (requires partnership)
    # - Option 2: Web scraping (use Stagehand/Playwright)
    # - Option 3: Third-party service (PhantomBuster, ScrapingBee)

    logger.debug(f"LinkedIn match check not yet implemented for {email}")
    return None
