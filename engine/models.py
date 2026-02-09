"""Data models for KadenVerify email verification engine."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Reachability(str, Enum):
    """Email reachability classification."""
    safe = "safe"
    risky = "risky"
    invalid = "invalid"
    unknown = "unknown"


class Provider(str, Enum):
    """Detected email provider type."""
    gmail = "gmail"
    google_workspace = "google_workspace"
    yahoo = "yahoo"
    microsoft365 = "microsoft365"
    hotmail = "hotmail"
    generic = "generic"


class SmtpResponse(BaseModel):
    """Raw SMTP response from server."""
    code: int = 0
    message: str = ""
    is_invalid: bool = False
    is_greylisted: bool = False
    is_blacklisted: bool = False
    is_full_inbox: bool = False
    is_disabled: bool = False


class DnsInfo(BaseModel):
    """DNS lookup results for a domain."""
    mx_hosts: list[str] = Field(default_factory=list)
    has_mx: bool = False
    provider: Provider = Provider.generic
    domain: str = ""


class EmailMetadata(BaseModel):
    """Metadata classification for an email address."""
    is_disposable: bool = False
    is_role: bool = False
    is_free: bool = False
    local_part: str = ""
    domain: str = ""
    normalized: str = ""


class SyntaxResult(BaseModel):
    """Result of syntax validation."""
    is_valid: bool = True
    reason: str = ""
    local_part: str = ""
    domain: str = ""
    normalized: str = ""


class CandidateResult(BaseModel):
    """A single email candidate tested during email finding."""
    email: str
    pattern: str = ""           # e.g. "first.last"
    smtp_code: int = 0
    confidence: float = 0.0
    source: str = ""            # "smtp", "apollo_local", "apollo_api", "exa", "pattern_score"


class FinderResult(BaseModel):
    """Result of email finding for a contact."""
    email: Optional[str] = None          # Best found email (None if not found)
    confidence: float = 0.0
    method: str = ""                     # How it was found
    reachability: Reachability = Reachability.unknown
    domain_is_catchall: Optional[bool] = None
    provider: Provider = Provider.generic
    candidates_tried: int = 0
    candidates: list[CandidateResult] = Field(default_factory=list)
    cost: float = 0.0                   # Total enrichment spend
    error: Optional[str] = None


class VerificationResult(BaseModel):
    """Complete email verification result."""
    email: str
    normalized: str = ""
    reachability: Reachability = Reachability.unknown
    is_deliverable: Optional[bool] = None
    is_catch_all: Optional[bool] = None
    is_disposable: bool = False
    is_role: bool = False
    is_free: bool = False
    mx_host: str = ""
    smtp_code: int = 0
    smtp_message: str = ""
    provider: Provider = Provider.generic
    domain: str = ""
    verified_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error: Optional[str] = None

    def to_omniverifier(self) -> dict:
        """Convert to OmniVerifier-compatible response format.

        Returns fields compatible with both kadenwood-ui and investor-outreach
        integration clients:
        - 'result' field maps: safe->deliverable, invalid->undeliverable,
          risky+catch_all->accept_all, risky->risky, unknown->unknown
        - Boolean fields use both naming conventions for compatibility
        """
        if self.reachability == Reachability.safe:
            result = "deliverable"
            status = "valid"
        elif self.reachability == Reachability.invalid:
            result = "undeliverable"
            status = "invalid"
        elif self.reachability == Reachability.risky:
            if self.is_catch_all:
                result = "accept_all"
                status = "catch_all"
            else:
                result = "risky"
                status = "risky"
        elif self.is_catch_all:
            result = "accept_all"
            status = "catch_all"
        else:
            result = "unknown"
            status = "unknown"

        return {
            "email": self.email,
            "result": result,
            "status": status,
            "reason": self.error or self.reachability.value,
            # kadenwood-ui field names
            "is_disposable": self.is_disposable,
            "is_role": self.is_role,
            "is_free": self.is_free,
            "mx_records": [self.mx_host] if self.mx_host else [],
            # investor-outreach field names
            "is_valid": self.reachability == Reachability.safe,
            "is_catchall": self.is_catch_all or False,
            "mx_found": bool(self.mx_host),
            "smtp_check": self.smtp_code > 0,
            # Additional aliases for compatibility
            "disposable": self.is_disposable,
            "role_account": self.is_role,
            "free_provider": self.is_free,
            # KadenVerify extended fields
            "is_deliverable": self.is_deliverable,
            "is_catch_all": self.is_catch_all,
            "provider": self.provider.value,
            "mx_host": self.mx_host,
            "smtp_code": self.smtp_code,
            "verified_at": self.verified_at.isoformat(),
        }
