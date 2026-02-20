"""Email metadata detection: disposable, role, and free provider classification."""

import os
from functools import lru_cache
from pathlib import Path

_LISTS_DIR = Path(__file__).parent.parent / "lists"


@lru_cache(maxsize=8)
def _load_set(filename: str) -> frozenset[str]:
    """Load a newline-delimited text file into a frozenset."""
    filepath = _LISTS_DIR / filename
    if not filepath.exists():
        return frozenset()
    with open(filepath) as f:
        return frozenset(
            line.strip().lower()
            for line in f
            if line.strip() and not line.startswith("#")
        )


def disposable_domains() -> frozenset[str]:
    return _load_set("disposable.txt")


def free_providers() -> frozenset[str]:
    return _load_set("free_providers.txt")


def role_prefixes() -> frozenset[str]:
    return _load_set("role_accounts.txt")


def is_disposable(domain: str) -> bool:
    """Check if domain is a known disposable email provider.

    Checks both the full domain and the base domain (strips subdomains).
    """
    domain = domain.lower()
    domains = disposable_domains()
    if domain in domains:
        return True
    # Check base domain (e.g., sub.tempmail.com -> tempmail.com)
    parts = domain.split(".")
    if len(parts) > 2:
        base = ".".join(parts[-2:])
        if base in domains:
            return True
    return False


def is_role_account(local_part: str) -> bool:
    """Check if the local part is a known role account prefix."""
    return local_part.lower() in role_prefixes()


def is_free_provider(domain: str) -> bool:
    """Check if domain is a known free email provider.

    Checks both the full domain and the base domain.
    """
    domain = domain.lower()
    providers = free_providers()
    if domain in providers:
        return True
    parts = domain.split(".")
    if len(parts) > 2:
        base = ".".join(parts[-2:])
        if base in providers:
            return True
    return False


def classify(local_part: str, domain: str) -> dict:
    """Classify an email address by metadata.

    Returns dict with is_disposable, is_role, is_free flags.
    """
    return {
        "is_disposable": is_disposable(domain),
        "is_role": is_role_account(local_part),
        "is_free": is_free_provider(domain),
    }

# Warm list caches once at import for high-concurrency workloads
_ = disposable_domains(), free_providers(), role_prefixes()
