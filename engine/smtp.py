"""Async SMTP handshake for email verification.

Connects to MX host on port 25, performs:
  EHLO -> MAIL FROM -> RCPT TO -> QUIT
Never sends DATA. Includes catch-all detection and greylisting retry.
"""

import asyncio
import logging
import random
import string
from typing import Optional

from .errors import parse_smtp_response
from .models import SmtpResponse

logger = logging.getLogger("kadenverify.smtp")

# Defaults
DEFAULT_HELO_DOMAIN = "198-23-249-137-host.colocrossing.com"
DEFAULT_FROM_ADDRESS = "postmaster@198-23-249-137-host.colocrossing.com"
CONNECT_TIMEOUT = 10
COMMAND_TIMEOUT = 10
TOTAL_TIMEOUT = 45
GREYLIST_DELAY = 35
GREYLIST_RETRIES = 2
SMTP_PORT = 25


def _random_address(domain: str, length: int = 15) -> str:
    """Generate a random email address for catch-all detection."""
    chars = string.ascii_lowercase + string.digits
    local = "".join(random.choices(chars, k=length))
    return f"{local}@{domain}"


async def _read_response(reader: asyncio.StreamReader, timeout: float = COMMAND_TIMEOUT) -> tuple[int, str]:
    """Read a complete SMTP response (may be multi-line).

    Returns (code, full_message).
    """
    lines = []
    while True:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        except asyncio.TimeoutError:
            if lines:
                break
            return 0, "read timeout"

        if not line:
            break

        decoded = line.decode("utf-8", errors="replace").rstrip("\r\n")
        lines.append(decoded)

        # SMTP multi-line: "250-..." continues, "250 ..." is final
        if len(decoded) >= 4 and decoded[3] == " ":
            break
        if len(decoded) < 4:
            break

    if not lines:
        return 0, "no response"

    full_message = "\n".join(lines)
    try:
        code = int(lines[-1][:3])
    except (ValueError, IndexError):
        code = 0

    return code, full_message


async def _send_command(
    writer: asyncio.StreamWriter,
    reader: asyncio.StreamReader,
    command: str,
    timeout: float = COMMAND_TIMEOUT,
) -> tuple[int, str]:
    """Send an SMTP command and read the response."""
    logger.debug(f">>> {command}")
    writer.write(f"{command}\r\n".encode())
    await writer.drain()
    code, message = await _read_response(reader, timeout)
    logger.debug(f"<<< {code} {message}")
    return code, message


async def smtp_check(
    email: str,
    mx_host: str,
    helo_domain: str = DEFAULT_HELO_DOMAIN,
    from_address: str = DEFAULT_FROM_ADDRESS,
    port: int = SMTP_PORT,
    connect_timeout: float = CONNECT_TIMEOUT,
    command_timeout: float = COMMAND_TIMEOUT,
    total_timeout: float = TOTAL_TIMEOUT,
) -> SmtpResponse:
    """Perform SMTP handshake to verify an email address.

    Flow: connect -> read banner -> EHLO -> MAIL FROM -> RCPT TO -> QUIT

    Does NOT send DATA (we're only checking if the mailbox exists).
    Handles greylisting with retries.
    """

    async def _attempt() -> SmtpResponse:
        reader: Optional[asyncio.StreamReader] = None
        writer: Optional[asyncio.StreamWriter] = None

        try:
            # Connect
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(mx_host, port),
                timeout=connect_timeout,
            )

            # Read banner
            code, message = await _read_response(reader, command_timeout)
            if code != 220:
                return parse_smtp_response(code, message)

            # EHLO
            code, message = await _send_command(writer, reader, f"EHLO {helo_domain}", command_timeout)
            if code != 250:
                # Try HELO as fallback
                code, message = await _send_command(writer, reader, f"HELO {helo_domain}", command_timeout)
                if code != 250:
                    return parse_smtp_response(code, message)

            # Try STARTTLS if available (best effort, don't fail if unavailable)
            if "STARTTLS" in message.upper():
                try:
                    tls_code, tls_msg = await _send_command(writer, reader, "STARTTLS", command_timeout)
                    if tls_code == 220:
                        import ssl
                        ssl_context = ssl.create_default_context()
                        ssl_context.check_hostname = False
                        ssl_context.verify_mode = ssl.CERT_NONE

                        transport = writer.transport
                        protocol = transport.get_protocol()
                        loop = asyncio.get_event_loop()
                        new_transport = await loop.start_tls(
                            transport, protocol, ssl_context,
                            server_hostname=mx_host,
                        )
                        writer._transport = new_transport

                        # Re-EHLO after STARTTLS
                        code, message = await _send_command(writer, reader, f"EHLO {helo_domain}", command_timeout)
                except Exception as e:
                    logger.debug(f"STARTTLS failed (continuing without): {e}")

            # MAIL FROM
            code, message = await _send_command(
                writer, reader, f"MAIL FROM:<{from_address}>", command_timeout
            )
            if code != 250:
                return parse_smtp_response(code, message)

            # RCPT TO (the actual verification)
            code, message = await _send_command(
                writer, reader, f"RCPT TO:<{email}>", command_timeout
            )

            # QUIT (always, regardless of result)
            try:
                await _send_command(writer, reader, "QUIT", 5)
            except Exception:
                pass

            return parse_smtp_response(code, message)

        except asyncio.TimeoutError:
            return SmtpResponse(code=0, message="connection timeout")
        except ConnectionRefusedError:
            return SmtpResponse(code=0, message="connection refused")
        except OSError as e:
            return SmtpResponse(code=0, message=f"connection error: {e}")
        finally:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    # Execute with total timeout and greylisting retries
    for attempt in range(GREYLIST_RETRIES + 1):
        try:
            result = await asyncio.wait_for(_attempt(), timeout=total_timeout)
        except asyncio.TimeoutError:
            return SmtpResponse(code=0, message="total timeout exceeded")

        # If greylisted and we have retries left, wait and retry
        if result.is_greylisted and attempt < GREYLIST_RETRIES:
            logger.info(f"Greylisted on attempt {attempt + 1}, retrying in {GREYLIST_DELAY}s...")
            await asyncio.sleep(GREYLIST_DELAY)
            continue

        return result

    return SmtpResponse(code=0, message="max retries exceeded")


async def check_catch_all(
    domain: str,
    mx_host: str,
    helo_domain: str = DEFAULT_HELO_DOMAIN,
    from_address: str = DEFAULT_FROM_ADDRESS,
    port: int = SMTP_PORT,
) -> Optional[bool]:
    """Check if a domain is catch-all by sending RCPT TO with a random address.

    Returns:
        True: Domain is catch-all (accepts everything)
        False: Domain is NOT catch-all (rejects unknown addresses)
        None: Could not determine (connection failed, timeout, etc.)
    """
    random_email = _random_address(domain)

    result = await smtp_check(
        email=random_email,
        mx_host=mx_host,
        helo_domain=helo_domain,
        from_address=from_address,
        port=port,
    )

    # 250 on random address = catch-all
    if result.code == 250:
        return True

    # 550 family on random address = NOT catch-all (rejects unknowns)
    if 500 <= result.code < 600:
        return False

    # Anything else = indeterminate
    return None


async def smtp_check_batch(
    emails: list[str],
    mx_host: str,
    helo_domain: str = DEFAULT_HELO_DOMAIN,
    from_address: str = DEFAULT_FROM_ADDRESS,
    port: int = SMTP_PORT,
    connect_timeout: float = CONNECT_TIMEOUT,
    command_timeout: float = COMMAND_TIMEOUT,
) -> list[SmtpResponse]:
    """Batch verify multiple emails to the same MX host using one connection.

    Opens one SMTP connection and sends multiple RCPT TO commands.
    This is 3-5x faster than opening separate connections per email.

    Args:
        emails: List of email addresses (should all be same domain)
        mx_host: Mail exchanger hostname
        helo_domain: Domain for EHLO command
        from_address: Address for MAIL FROM
        port: SMTP port (default 25)
        connect_timeout: Connection timeout in seconds
        command_timeout: Command timeout in seconds

    Returns:
        List of SmtpResponse objects in same order as emails
    """
    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None

    try:
        # Connect
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(mx_host, port),
            timeout=connect_timeout,
        )

        # Read banner
        code, message = await _read_response(reader, command_timeout)
        if code != 220:
            # Connection failed - fall back to individual checks
            return [await smtp_check(email, mx_host, helo_domain, from_address, port) for email in emails]

        # EHLO
        code, message = await _send_command(writer, reader, f"EHLO {helo_domain}", command_timeout)
        if code != 250:
            # Try HELO as fallback
            code, message = await _send_command(writer, reader, f"HELO {helo_domain}", command_timeout)
            if code != 250:
                return [await smtp_check(email, mx_host, helo_domain, from_address, port) for email in emails]

        # STARTTLS (best effort, skip if fails)
        if "STARTTLS" in message.upper():
            try:
                tls_code, tls_msg = await _send_command(writer, reader, "STARTTLS", command_timeout)
                if tls_code == 220:
                    import ssl
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE

                    transport = writer.transport
                    protocol = transport.get_protocol()
                    loop = asyncio.get_event_loop()
                    new_transport = await loop.start_tls(
                        transport, protocol, ssl_context,
                        server_hostname=mx_host,
                    )
                    writer._transport = new_transport

                    # Re-EHLO after STARTTLS
                    code, message = await _send_command(writer, reader, f"EHLO {helo_domain}", command_timeout)
            except Exception as e:
                logger.debug(f"STARTTLS failed (continuing): {e}")

        # MAIL FROM (one per batch)
        code, message = await _send_command(
            writer, reader, f"MAIL FROM:<{from_address}>", command_timeout
        )
        if code != 250:
            return [await smtp_check(email, mx_host, helo_domain, from_address, port) for email in emails]

        # RCPT TO for each email (reusing connection)
        results = []
        for email in emails:
            code, message = await _send_command(
                writer, reader, f"RCPT TO:<{email}>", command_timeout
            )
            results.append(parse_smtp_response(code, message))

        # QUIT
        try:
            await _send_command(writer, reader, "QUIT", 5)
        except Exception:
            pass

        return results

    except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
        # Connection failed - fall back to individual checks
        logger.debug(f"Batch connection failed, falling back to individual: {e}")
        return [await smtp_check(email, mx_host, helo_domain, from_address, port) for email in emails]
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
