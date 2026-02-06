"""Provider-specific email verification routing.

Different email providers have different SMTP behaviors:
- Gmail/Google Workspace: Always returns 550 for invalid addresses, no catch-all needed
- Yahoo: Standard SMTP, catch-all probe recommended
- Microsoft 365 (B2B): Standard SMTP, catch-all probe recommended (many M365 domains are catch-all)
- Hotmail/Outlook.com (B2C): SMTP unreliable for verification, mark as risky
- Generic: Full SMTP + catch-all probe
"""

from dataclasses import dataclass
from .models import Provider


@dataclass
class ProviderConfig:
    """Configuration for how to verify emails at a specific provider."""
    provider: Provider
    do_smtp: bool = True          # Whether to perform SMTP handshake
    do_catch_all: bool = True     # Whether to probe for catch-all
    mark_risky: bool = False      # Whether to automatically mark as risky
    notes: str = ""


# Provider-specific verification strategies
_PROVIDER_CONFIGS: dict[Provider, ProviderConfig] = {
    Provider.gmail: ProviderConfig(
        provider=Provider.gmail,
        do_smtp=True,
        do_catch_all=False,  # Google always returns 550 for bad addresses
        notes="Gmail always returns definitive 550 for nonexistent addresses",
    ),
    Provider.google_workspace: ProviderConfig(
        provider=Provider.google_workspace,
        do_smtp=True,
        do_catch_all=False,  # Google Workspace also returns 550 reliably
        notes="Google Workspace returns definitive 550 for nonexistent addresses",
    ),
    Provider.yahoo: ProviderConfig(
        provider=Provider.yahoo,
        do_smtp=True,
        do_catch_all=True,
        notes="Yahoo standard SMTP verification",
    ),
    Provider.microsoft365: ProviderConfig(
        provider=Provider.microsoft365,
        do_smtp=True,
        do_catch_all=True,  # Many M365 domains are catch-all
        notes="M365 B2B - many domains have catch-all enabled",
    ),
    Provider.hotmail: ProviderConfig(
        provider=Provider.hotmail,
        do_smtp=False,  # Hotmail B2C SMTP is unreliable for verification
        do_catch_all=False,
        mark_risky=True,
        notes="Hotmail/Outlook.com B2C: SMTP unreliable, auto-mark risky",
    ),
    Provider.generic: ProviderConfig(
        provider=Provider.generic,
        do_smtp=True,
        do_catch_all=True,
        notes="Generic provider: full SMTP + catch-all probe",
    ),
}


def get_config(provider: Provider) -> ProviderConfig:
    """Get verification configuration for a provider."""
    return _PROVIDER_CONFIGS.get(provider, _PROVIDER_CONFIGS[Provider.generic])
