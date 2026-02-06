"""SMTP error response parser.

Detects invalid mailboxes, greylisting, IP blacklists, full inboxes,
and disabled accounts from SMTP response codes and messages.
Supports patterns across English, French, German, Spanish, Italian, Polish, and Czech.
"""

import re
from .models import SmtpResponse


# --- Invalid mailbox patterns (user doesn't exist) ---
_INVALID_PATTERNS: list[re.Pattern] = [
    # English
    re.compile(r"user unknown", re.I),
    re.compile(r"unknown user", re.I),
    re.compile(r"user not found", re.I),
    re.compile(r"no such user", re.I),
    re.compile(r"mailbox not found", re.I),
    re.compile(r"mailbox unavailable", re.I),
    re.compile(r"recipient not found", re.I),
    re.compile(r"recipient rejected", re.I),
    re.compile(r"recipient unknown", re.I),
    re.compile(r"unknown recipient", re.I),
    re.compile(r"address rejected", re.I),
    re.compile(r"address unknown", re.I),
    re.compile(r"does not exist", re.I),
    re.compile(r"doesn't exist", re.I),
    re.compile(r"not exist", re.I),
    re.compile(r"no mailbox", re.I),
    re.compile(r"invalid address", re.I),
    re.compile(r"invalid recipient", re.I),
    re.compile(r"invalid mailbox", re.I),
    re.compile(r"undeliverable", re.I),
    re.compile(r"bad destination", re.I),
    re.compile(r"unknown address", re.I),
    re.compile(r"account .* not found", re.I),
    re.compile(r"no such account", re.I),
    re.compile(r"mailbox .* does not exist", re.I),
    re.compile(r"email address .* not found", re.I),
    re.compile(r"is not a valid mailbox", re.I),
    re.compile(r"relay not permitted", re.I),
    re.compile(r"relaying denied", re.I),
    re.compile(r"not our customer", re.I),
    re.compile(r"no such recipient", re.I),
    re.compile(r"verification failed", re.I),
    re.compile(r"account has been disabled", re.I),
    re.compile(r"account disabled", re.I),
    re.compile(r"this mailbox .* disabled", re.I),
    re.compile(r"mailbox disabled", re.I),
    re.compile(r"recipient address denied", re.I),
    # French
    re.compile(r"utilisateur inconnu", re.I),
    re.compile(r"adresse .* introuvable", re.I),
    re.compile(r"destinataire inconnu", re.I),
    re.compile(r"bo[iî]te .* introuvable", re.I),
    re.compile(r"n'existe pas", re.I),
    # German
    re.compile(r"postfach nicht gefunden", re.I),
    re.compile(r"benutzer nicht gefunden", re.I),
    re.compile(r"empf[aä]nger .* unbekannt", re.I),
    re.compile(r"unbekannter empf[aä]nger", re.I),
    re.compile(r"nicht gefunden", re.I),
    re.compile(r"existiert nicht", re.I),
    # Spanish
    re.compile(r"usuario desconocido", re.I),
    re.compile(r"destinatario desconocido", re.I),
    re.compile(r"buz[oó]n no encontrado", re.I),
    re.compile(r"no existe", re.I),
    re.compile(r"direcci[oó]n .* inv[aá]lida", re.I),
    # Italian
    re.compile(r"utente sconosciuto", re.I),
    re.compile(r"destinatario sconosciuto", re.I),
    re.compile(r"casella .* non trovata", re.I),
    re.compile(r"non esiste", re.I),
    # Polish
    re.compile(r"u[zż]ytkownik nieznany", re.I),
    re.compile(r"skrzynka .* nie istnieje", re.I),
    re.compile(r"odbiorca nieznany", re.I),
    re.compile(r"nie istnieje", re.I),
    # Czech
    re.compile(r"u[zž]ivatel nenalezen", re.I),
    re.compile(r"adresa nenalezena", re.I),
    re.compile(r"p[rř][ií]jemce nenalezen", re.I),
    re.compile(r"neexistuje", re.I),
]

# --- IP blacklist / blocklist patterns ---
_BLACKLIST_PATTERNS: list[re.Pattern] = [
    re.compile(r"spamhaus", re.I),
    re.compile(r"proofpoint", re.I),
    re.compile(r"cloudmark", re.I),
    re.compile(r"barracuda", re.I),
    re.compile(r"sorbs", re.I),
    re.compile(r"spamcop", re.I),
    re.compile(r"blocked.*ip", re.I),
    re.compile(r"ip.*blocked", re.I),
    re.compile(r"blacklist", re.I),
    re.compile(r"blocklist", re.I),
    re.compile(r"denied.*ip", re.I),
    re.compile(r"ip.*denied", re.I),
    re.compile(r"reject.*ip", re.I),
    re.compile(r"listed.*rbl", re.I),
    re.compile(r"rbl.*listed", re.I),
    re.compile(r"dnsbl", re.I),
    re.compile(r"your ip .* has been .* blocked", re.I),
    re.compile(r"connection .* refused", re.I),
    re.compile(r"access denied", re.I),
    re.compile(r"not allowed to send", re.I),
    re.compile(r"service refused", re.I),
]

# --- Greylisting patterns (temporary rejection, try again) ---
_GREYLIST_PATTERNS: list[re.Pattern] = [
    re.compile(r"try again later", re.I),
    re.compile(r"temporarily rejected", re.I),
    re.compile(r"please try again", re.I),
    re.compile(r"temporary.*failure", re.I),
    re.compile(r"temporary.*error", re.I),
    re.compile(r"greylisted", re.I),
    re.compile(r"greylist", re.I),
    re.compile(r"too many connections", re.I),
    re.compile(r"rate limit", re.I),
    re.compile(r"come back later", re.I),
    re.compile(r"defer.*connection", re.I),
    re.compile(r"resource temporarily unavailable", re.I),
    re.compile(r"service temporarily unavailable", re.I),
]

# --- Full inbox patterns ---
_FULL_INBOX_PATTERNS: list[re.Pattern] = [
    re.compile(r"mailbox full", re.I),
    re.compile(r"mailbox .* full", re.I),
    re.compile(r"over.*quota", re.I),
    re.compile(r"quota exceeded", re.I),
    re.compile(r"insufficient.*storage", re.I),
    re.compile(r"not enough space", re.I),
    re.compile(r"user .* over .* quota", re.I),
    re.compile(r"mail.*box.*storage", re.I),
    re.compile(r"exceeded.*storage", re.I),
    re.compile(r"bo[iî]te .* pleine", re.I),  # French
    re.compile(r"postfach .* voll", re.I),      # German
    re.compile(r"buz[oó]n .* lleno", re.I),     # Spanish
]

# --- Disabled account patterns ---
_DISABLED_PATTERNS: list[re.Pattern] = [
    re.compile(r"account .* disabled", re.I),
    re.compile(r"account .* suspended", re.I),
    re.compile(r"account .* deactivated", re.I),
    re.compile(r"account .* locked", re.I),
    re.compile(r"mailbox .* disabled", re.I),
    re.compile(r"mailbox .* inactive", re.I),
    re.compile(r"user .* disabled", re.I),
    re.compile(r"temporarily disabled", re.I),
]


def _match_any(message: str, patterns: list[re.Pattern]) -> bool:
    """Check if message matches any pattern in the list."""
    return any(p.search(message) for p in patterns)


def parse_smtp_response(code: int, message: str) -> SmtpResponse:
    """Parse an SMTP response code and message into a structured result.

    SMTP response code classes:
    - 2xx: Success (250 = accepted)
    - 4xx: Temporary failure (greylisting, rate limiting)
    - 5xx: Permanent failure (invalid mailbox, rejected)

    Returns SmtpResponse with boolean flags for each detection category.
    """
    result = SmtpResponse(code=code, message=message)

    # 2xx = success, no errors to parse
    if 200 <= code < 300:
        return result

    # Check for blacklisting (any code)
    if _match_any(message, _BLACKLIST_PATTERNS):
        result.is_blacklisted = True
        return result

    # 4xx = temporary failures
    if 400 <= code < 500:
        if _match_any(message, _GREYLIST_PATTERNS):
            result.is_greylisted = True
        elif _match_any(message, _FULL_INBOX_PATTERNS):
            result.is_full_inbox = True
        else:
            # Generic 4xx — treat as greylist by default
            result.is_greylisted = True
        return result

    # 5xx = permanent failures
    if 500 <= code < 600:
        # Check disabled first (more specific)
        if _match_any(message, _DISABLED_PATTERNS):
            result.is_disabled = True
            result.is_invalid = True
            return result

        # Check full inbox (user exists but can't receive)
        if _match_any(message, _FULL_INBOX_PATTERNS):
            result.is_full_inbox = True
            return result

        # Check invalid mailbox
        if _match_any(message, _INVALID_PATTERNS):
            result.is_invalid = True
            return result

        # Generic 550 without recognized pattern — still likely invalid
        if code in (550, 551, 552, 553):
            result.is_invalid = True
            return result

    return result
