"""Domain-level cache for DNS and catch-all results.

Caches MX records (24hr TTL) and catch-all status (7-day TTL) to avoid
repeated lookups when processing batches of emails at the same domain.
"""

import time
from dataclasses import dataclass, field
from typing import Optional

from engine.models import DnsInfo, Provider


# TTLs in seconds
MX_TTL = 86400       # 24 hours
CATCH_ALL_TTL = 604800  # 7 days


@dataclass
class DomainCacheEntry:
    """Cached data for a single domain."""
    dns_info: Optional[DnsInfo] = None
    dns_cached_at: float = 0.0

    is_catch_all: Optional[bool] = None
    catch_all_cached_at: float = 0.0


class DomainCache:
    """In-memory domain-level cache with TTL expiration.

    Thread-safe for asyncio (single-threaded event loop).
    """

    def __init__(self, mx_ttl: float = MX_TTL, catch_all_ttl: float = CATCH_ALL_TTL):
        self._entries: dict[str, DomainCacheEntry] = {}
        self._mx_ttl = mx_ttl
        self._catch_all_ttl = catch_all_ttl

    def _get_or_create(self, domain: str) -> DomainCacheEntry:
        domain = domain.lower()
        if domain not in self._entries:
            self._entries[domain] = DomainCacheEntry()
        return self._entries[domain]

    def get_dns(self, domain: str) -> Optional[DnsInfo]:
        """Get cached DNS info if not expired."""
        domain = domain.lower()
        entry = self._entries.get(domain)
        if entry is None or entry.dns_info is None:
            return None
        if time.time() - entry.dns_cached_at > self._mx_ttl:
            entry.dns_info = None  # Expired
            return None
        return entry.dns_info

    def set_dns(self, domain: str, dns_info: DnsInfo) -> None:
        """Cache DNS info for a domain."""
        entry = self._get_or_create(domain)
        entry.dns_info = dns_info
        entry.dns_cached_at = time.time()

    def get_catch_all(self, domain: str) -> Optional[bool]:
        """Get cached catch-all status if not expired.

        Returns None if not cached or expired.
        Note: None could also mean "indeterminate" — use has_catch_all() to distinguish.
        """
        domain = domain.lower()
        entry = self._entries.get(domain)
        if entry is None or entry.catch_all_cached_at == 0.0:
            return None
        if time.time() - entry.catch_all_cached_at > self._catch_all_ttl:
            entry.catch_all_cached_at = 0.0  # Expired
            return None
        return entry.is_catch_all

    def has_catch_all(self, domain: str) -> bool:
        """Check if catch-all status is cached and not expired."""
        domain = domain.lower()
        entry = self._entries.get(domain)
        if entry is None or entry.catch_all_cached_at == 0.0:
            return False
        return time.time() - entry.catch_all_cached_at <= self._catch_all_ttl

    def set_catch_all(self, domain: str, is_catch_all: Optional[bool]) -> None:
        """Cache catch-all status for a domain."""
        entry = self._get_or_create(domain)
        entry.is_catch_all = is_catch_all
        entry.catch_all_cached_at = time.time()

    def stats(self) -> dict:
        """Return cache statistics."""
        now = time.time()
        total = len(self._entries)
        dns_valid = sum(
            1 for e in self._entries.values()
            if e.dns_info is not None and now - e.dns_cached_at <= self._mx_ttl
        )
        catch_all_valid = sum(
            1 for e in self._entries.values()
            if e.catch_all_cached_at > 0 and now - e.catch_all_cached_at <= self._catch_all_ttl
        )
        catch_all_true = sum(
            1 for e in self._entries.values()
            if e.is_catch_all is True and e.catch_all_cached_at > 0
            and now - e.catch_all_cached_at <= self._catch_all_ttl
        )

        return {
            "total_domains": total,
            "dns_cached": dns_valid,
            "catch_all_cached": catch_all_valid,
            "catch_all_domains": catch_all_true,
        }

    def clear(self) -> None:
        """Clear all cached entries."""
        self._entries.clear()

    def clear_expired(self) -> int:
        """Remove expired entries. Returns count of entries removed."""
        now = time.time()
        expired = []
        for domain, entry in self._entries.items():
            dns_expired = (
                entry.dns_info is None
                or now - entry.dns_cached_at > self._mx_ttl
            )
            catch_all_expired = (
                entry.catch_all_cached_at == 0.0
                or now - entry.catch_all_cached_at > self._catch_all_ttl
            )
            if dns_expired and catch_all_expired:
                expired.append(domain)

        for domain in expired:
            del self._entries[domain]

        return len(expired)
