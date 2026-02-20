"""Async DNS MX/A/AAAA lookup with provider detection."""

import asyncio
import logging
from typing import Optional

import dns.asyncresolver
import dns.resolver
import dns.name
import dns.rdatatype
import dns.exception

from .models import DnsInfo, Provider

logger = logging.getLogger("kadenverify.dns")

# DNS timeout in seconds
DNS_TIMEOUT = 10.0

# Reuse a single resolver instance and enable dnspython built-in cache.
# This reduces repeated MX/A/AAAA lookups across large verification runs.
_RESOLVER = dns.asyncresolver.Resolver()
_RESOLVER.timeout = DNS_TIMEOUT
_RESOLVER.lifetime = DNS_TIMEOUT
_RESOLVER.cache = dns.resolver.Cache()



def _detect_provider(mx_hosts: list[str], domain: str = "") -> Provider:
    """Detect email provider from MX hostnames.

    Checks the highest-priority (first) MX host against known patterns.
    Uses domain to distinguish Gmail personal from Google Workspace.
    """
    if not mx_hosts:
        return Provider.generic

    domain_lower = domain.lower().rstrip(".")

    for mx in mx_hosts:
        mx_lower = mx.lower().rstrip(".")

        # Google (Gmail / Google Workspace)
        if mx_lower.endswith(".google.com") or mx_lower.endswith(".googlemail.com"):
            if domain_lower in ("gmail.com", "googlemail.com"):
                return Provider.gmail
            return Provider.google_workspace

        # Yahoo
        if mx_lower.endswith(".yahoodns.net"):
            return Provider.yahoo

        # Microsoft — distinguish B2B (M365) from B2C (Hotmail/Outlook.com)
        if mx_lower.endswith(".protection.outlook.com"):
            if ".olc.protection.outlook.com" in mx_lower:
                return Provider.hotmail  # B2C (Hotmail, Outlook.com, Live.com)
            return Provider.microsoft365  # B2B (Microsoft 365)

        # Outlook.com direct MX
        if mx_lower.endswith(".hotmail.com") or mx_lower.endswith(".outlook.com"):
            return Provider.hotmail

    return Provider.generic


async def lookup_mx(domain: str, timeout: float = DNS_TIMEOUT) -> DnsInfo:
    """Look up MX records for a domain, falling back to A/AAAA.

    Returns DnsInfo with mx_hosts sorted by priority (lowest priority number first).
    """
    resolver = _RESOLVER
    if timeout != DNS_TIMEOUT:
        resolver = dns.asyncresolver.Resolver()
        resolver.timeout = timeout
        resolver.lifetime = timeout
        resolver.cache = _RESOLVER.cache

    mx_hosts: list[str] = []

    # Try MX records first
    try:
        mx_response = await resolver.resolve(domain, "MX")
        # Sort by priority (lower = higher priority)
        records = sorted(mx_response, key=lambda r: r.preference)
        mx_hosts = [str(r.exchange).rstrip(".") for r in records]
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.DNSException) as e:
        logger.debug(f"MX lookup failed for {domain}: {e}")

    # Fall back to A record if no MX
    if not mx_hosts:
        try:
            a_response = await resolver.resolve(domain, "A")
            mx_hosts = [str(rdata) for rdata in a_response]
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.DNSException) as e:
            logger.debug(f"A record lookup failed for {domain}: {e}")

    # Fall back to AAAA if no A record either
    if not mx_hosts:
        try:
            aaaa_response = await resolver.resolve(domain, "AAAA")
            mx_hosts = [str(rdata) for rdata in aaaa_response]
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.exception.DNSException) as e:
            logger.debug(f"AAAA lookup failed for {domain}: {e}")

    has_mx = len(mx_hosts) > 0
    provider = _detect_provider(mx_hosts, domain) if has_mx else Provider.generic

    return DnsInfo(
        mx_hosts=mx_hosts,
        has_mx=has_mx,
        provider=provider,
        domain=domain,
    )
