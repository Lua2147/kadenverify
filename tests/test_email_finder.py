"""Tests for email finder module."""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from engine.email_finder import (
    generate_candidates,
    find_email,
    find_emails_batch,
    _domain_cache,
)
from engine.models import (
    CandidateResult,
    DnsInfo,
    FinderResult,
    Provider,
    Reachability,
    SmtpResponse,
)


# ---------------------------------------------------------------------------
# Pattern generation tests
# ---------------------------------------------------------------------------

class TestGenerateCandidates:
    def test_generates_10_patterns(self):
        candidates = generate_candidates("John", "Smith", "company.com")
        assert len(candidates) == 10

    def test_first_is_first_dot_last(self):
        candidates = generate_candidates("John", "Smith", "company.com")
        assert candidates[0].email == "john.smith@company.com"
        assert candidates[0].pattern == "first.last"

    def test_correct_pattern_order(self):
        candidates = generate_candidates("John", "Smith", "company.com")
        expected_emails = [
            "john.smith@company.com",
            "jsmith@company.com",
            "johns@company.com",
            "john@company.com",
            "john_smith@company.com",
            "john-smith@company.com",
            "j.smith@company.com",
            "smithj@company.com",
            "smith.john@company.com",
            "johnsmith@company.com",
        ]
        actual_emails = [c.email for c in candidates]
        assert actual_emails == expected_emails

    def test_lowercases_names(self):
        candidates = generate_candidates("ALICE", "JONES", "example.com")
        assert candidates[0].email == "alice.jones@example.com"

    def test_strips_whitespace(self):
        candidates = generate_candidates("  Bob  ", "  Lee  ", "test.com")
        assert candidates[0].email == "bob.lee@test.com"

    def test_no_duplicate_emails(self):
        candidates = generate_candidates("A", "B", "x.com")
        emails = [c.email for c in candidates]
        assert len(emails) == len(set(emails))


# ---------------------------------------------------------------------------
# find_email tests (mocked SMTP/DNS)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_domain_cache():
    """Clear domain intelligence cache between tests."""
    _domain_cache.clear()
    yield
    _domain_cache.clear()


class TestFindEmailNoMX:
    def test_no_mx_returns_error(self):
        with patch("engine.email_finder.lookup_mx", new_callable=AsyncMock) as mock_mx:
            mock_mx.return_value = DnsInfo(mx_hosts=[], has_mx=False, domain="nope.com")
            with patch("engine.email_finder.check_catch_all", new_callable=AsyncMock):
                result = asyncio.run(find_email("John", "Smith", "nope.com"))
                assert result.email is None
                assert "No MX" in result.error


class TestFindEmailSMTP:
    def test_smtp_verified_winner(self):
        """Non-catch-all domain: first 250 response wins."""
        dns = DnsInfo(mx_hosts=["mx.company.com"], has_mx=True, provider=Provider.generic, domain="company.com")
        responses = [SmtpResponse(code=550, message="unknown")] * 10
        responses[0] = SmtpResponse(code=250, message="ok")

        with patch("engine.email_finder.lookup_mx", new_callable=AsyncMock, return_value=dns):
            with patch("engine.email_finder.check_catch_all", new_callable=AsyncMock, return_value=False):
                with patch("engine.email_finder.smtp_check_batch", new_callable=AsyncMock, return_value=responses):
                    result = asyncio.run(find_email("John", "Smith", "company.com"))
                    assert result.email == "john.smith@company.com"
                    assert result.confidence == 0.95
                    assert result.method == "smtp_verified"
                    assert result.reachability == Reachability.safe

    def test_all_rejected_falls_to_enrichment(self):
        """All 550: falls through to enrichment (no Apollo/Exa configured)."""
        dns = DnsInfo(mx_hosts=["mx.company.com"], has_mx=True, provider=Provider.generic, domain="company.com")
        responses = [SmtpResponse(code=550, message="user unknown")] * 10

        with patch("engine.email_finder.lookup_mx", new_callable=AsyncMock, return_value=dns):
            with patch("engine.email_finder.check_catch_all", new_callable=AsyncMock, return_value=False):
                with patch("engine.email_finder.smtp_check_batch", new_callable=AsyncMock, return_value=responses):
                    with patch("engine.email_finder._lookup_apollo_local", return_value=None):
                        result = asyncio.run(find_email("John", "Smith", "company.com"))
                        assert result.email is None
                        assert result.method == "exhausted"

    def test_second_pattern_wins(self):
        """flast@ pattern is the first 250."""
        dns = DnsInfo(mx_hosts=["mx.company.com"], has_mx=True, provider=Provider.generic, domain="company.com")
        responses = [SmtpResponse(code=550, message="no")] * 10
        responses[1] = SmtpResponse(code=250, message="ok")  # flast

        with patch("engine.email_finder.lookup_mx", new_callable=AsyncMock, return_value=dns):
            with patch("engine.email_finder.check_catch_all", new_callable=AsyncMock, return_value=False):
                with patch("engine.email_finder.smtp_check_batch", new_callable=AsyncMock, return_value=responses):
                    result = asyncio.run(find_email("John", "Smith", "company.com"))
                    assert result.email == "jsmith@company.com"
                    assert result.method == "smtp_verified"


class TestFindEmailCatchAll:
    def test_catchall_uses_pattern_score(self):
        """Catch-all domain: uses pattern scoring fallback."""
        dns = DnsInfo(mx_hosts=["mx.catch.com"], has_mx=True, provider=Provider.generic, domain="catch.com")

        with patch("engine.email_finder.lookup_mx", new_callable=AsyncMock, return_value=dns):
            with patch("engine.email_finder.check_catch_all", new_callable=AsyncMock, return_value=True):
                with patch("engine.email_finder._lookup_apollo_local", return_value=None):
                    result = asyncio.run(find_email("John", "Smith", "catch.com"))
                    assert result.email == "john.smith@catch.com"
                    assert result.method == "pattern_score_catchall"
                    assert result.domain_is_catchall is True


class TestFindEmailApolloLocal:
    def test_apollo_local_hit(self):
        """Apollo local DB returns a match."""
        dns = DnsInfo(mx_hosts=["mx.co.com"], has_mx=True, provider=Provider.generic, domain="co.com")
        apollo_result = CandidateResult(
            email="john.smith@co.com",
            pattern="apollo_local",
            confidence=0.90,
            source="apollo_local",
        )

        with patch("engine.email_finder.lookup_mx", new_callable=AsyncMock, return_value=dns):
            with patch("engine.email_finder.check_catch_all", new_callable=AsyncMock, return_value=True):
                with patch("engine.email_finder._lookup_apollo_local", return_value=apollo_result):
                    result = asyncio.run(find_email("John", "Smith", "co.com"))
                    assert result.email == "john.smith@co.com"
                    assert result.method == "apollo_local_db"
                    assert result.confidence == 0.90


# ---------------------------------------------------------------------------
# Batch processing tests
# ---------------------------------------------------------------------------

class TestFindEmailsBatch:
    def test_groups_by_domain(self):
        """Batch processes contacts grouped by domain."""
        contacts = [
            {"first_name": "Alice", "last_name": "A", "domain": "a.com"},
            {"first_name": "Bob", "last_name": "B", "domain": "b.com"},
            {"first_name": "Carol", "last_name": "C", "domain": "a.com"},
        ]

        dns_a = DnsInfo(mx_hosts=["mx.a.com"], has_mx=True, provider=Provider.generic, domain="a.com")
        dns_b = DnsInfo(mx_hosts=["mx.b.com"], has_mx=True, provider=Provider.generic, domain="b.com")

        call_count = {"a.com": 0, "b.com": 0}

        async def mock_lookup_mx(domain, timeout=10.0):
            if domain == "a.com":
                return dns_a
            return dns_b

        async def mock_catch_all(domain, mx_host, **kw):
            return True  # all catch-all

        original_find = find_email

        async def tracking_find(first_name, last_name, domain, **kw):
            call_count[domain] = call_count.get(domain, 0) + 1
            return FinderResult(
                email=f"{first_name.lower()}.{last_name.lower()}@{domain}",
                confidence=0.70,
                method="test",
                reachability=Reachability.risky,
            )

        with patch("engine.email_finder.lookup_mx", side_effect=mock_lookup_mx):
            with patch("engine.email_finder.check_catch_all", side_effect=mock_catch_all):
                with patch("engine.email_finder.find_email", side_effect=tracking_find):
                    results = asyncio.run(find_emails_batch(contacts, concurrency=5))

        assert len(results) == 3
        assert results[0].email == "alice.a@a.com"
        assert results[1].email == "bob.b@b.com"
        assert results[2].email == "carol.c@a.com"
        # a.com had 2 contacts, b.com had 1
        assert call_count["a.com"] == 2
        assert call_count["b.com"] == 1

    def test_preserves_order(self):
        """Results maintain original contact order."""
        contacts = [
            {"first_name": "Z", "last_name": "Z", "domain": "z.com"},
            {"first_name": "A", "last_name": "A", "domain": "a.com"},
        ]

        async def mock_find(first_name, last_name, domain, **kw):
            return FinderResult(
                email=f"{first_name.lower()}@{domain}",
                confidence=0.50,
                method="test",
            )

        with patch("engine.email_finder.find_email", side_effect=mock_find):
            with patch("engine.email_finder._get_domain_intel", new_callable=AsyncMock,
                       return_value=(DnsInfo(has_mx=True, mx_hosts=["mx"]), None)):
                results = asyncio.run(find_emails_batch(contacts, concurrency=5))

        assert results[0].email == "z@z.com"
        assert results[1].email == "a@a.com"
