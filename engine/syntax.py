"""RFC 5322 email syntax validation with normalization."""

import re
from .models import SyntaxResult

# Gmail alias: googlemail.com -> gmail.com
_GMAIL_ALIASES = {"googlemail.com": "gmail.com"}

# Valid local-part characters (simplified RFC 5322 — no quoted strings)
_LOCAL_PART_RE = re.compile(
    r"^[a-zA-Z0-9!#$%&'*+\-/=?^_`{|}~]"
    r"(?:[a-zA-Z0-9!#$%&'*+\-/=?^_`{|}~.]*[a-zA-Z0-9!#$%&'*+\-/=?^_`{|}~])?$"
)

# Domain label: alphanumeric and hyphens, no leading/trailing hyphens
_DOMAIN_LABEL_RE = re.compile(r"^[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$")


def validate_syntax(email: str) -> SyntaxResult:
    """Validate email syntax per RFC 5322 (simplified, no quoted strings).

    Checks:
    - Total length <= 254
    - Local part length <= 64
    - Domain length <= 255
    - No consecutive dots in local part
    - No leading/trailing dots in local part
    - Valid characters in local part
    - Valid domain labels
    - At least one dot in domain (TLD required)
    - Normalizes: lowercase, strip whitespace, googlemail->gmail
    """
    email = email.strip()

    if not email:
        return SyntaxResult(is_valid=False, reason="empty email")

    # Total length check
    if len(email) > 254:
        return SyntaxResult(is_valid=False, reason="total length exceeds 254")

    # Must have exactly one @
    if email.count("@") != 1:
        return SyntaxResult(is_valid=False, reason="must contain exactly one @")

    local_part, domain = email.rsplit("@", 1)

    # Normalize
    local_part = local_part.strip()
    domain = domain.strip().lower()

    # Apply gmail alias
    if domain in _GMAIL_ALIASES:
        domain = _GMAIL_ALIASES[domain]

    # For gmail, strip dots and plus-addressing from local part
    if domain == "gmail.com":
        # Remove dots (gmail ignores them)
        clean_local = local_part.replace(".", "")
        # Remove plus-addressing
        if "+" in clean_local:
            clean_local = clean_local.split("+")[0]
        normalized_local = clean_local.lower()
    else:
        normalized_local = local_part.lower()

    normalized = f"{normalized_local}@{domain}"

    # Local part length
    if not local_part:
        return SyntaxResult(is_valid=False, reason="empty local part")
    if len(local_part) > 64:
        return SyntaxResult(is_valid=False, reason="local part exceeds 64 characters")

    # Domain length
    if not domain:
        return SyntaxResult(is_valid=False, reason="empty domain")
    if len(domain) > 255:
        return SyntaxResult(is_valid=False, reason="domain exceeds 255 characters")

    # Consecutive dots in local part
    if ".." in local_part:
        return SyntaxResult(is_valid=False, reason="consecutive dots in local part")

    # Leading/trailing dots in local part
    if local_part.startswith(".") or local_part.endswith("."):
        return SyntaxResult(is_valid=False, reason="leading or trailing dot in local part")

    # Reject quoted strings
    if local_part.startswith('"') or local_part.endswith('"'):
        return SyntaxResult(is_valid=False, reason="quoted strings not supported")

    # Valid local-part characters
    if not _LOCAL_PART_RE.match(local_part):
        return SyntaxResult(is_valid=False, reason="invalid characters in local part")

    # Domain validation
    labels = domain.split(".")

    # Must have at least 2 labels (name + TLD)
    if len(labels) < 2:
        return SyntaxResult(is_valid=False, reason="domain must have at least one dot")

    # TLD must be at least 2 characters and all alpha
    tld = labels[-1]
    if len(tld) < 2:
        return SyntaxResult(is_valid=False, reason="TLD too short")
    if not tld.isalpha():
        return SyntaxResult(is_valid=False, reason="TLD must be alphabetic")

    for label in labels:
        if not label:
            return SyntaxResult(is_valid=False, reason="empty domain label")
        if len(label) > 63:
            return SyntaxResult(is_valid=False, reason="domain label exceeds 63 characters")
        if not _DOMAIN_LABEL_RE.match(label):
            return SyntaxResult(is_valid=False, reason=f"invalid domain label: {label}")

    return SyntaxResult(
        is_valid=True,
        local_part=local_part,
        domain=domain,
        normalized=normalized,
    )
