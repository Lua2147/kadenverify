"""KadenVerify CLI — email verification from the command line.

Commands:
  verify       Verify a single email address
  verify-file  Verify emails from a text file
  pipeline     Run batch verification against a DuckDB source
  find-email   Find email for a single contact (name + domain)
  find-emails  Batch find emails from CSV
  stats        Show verification statistics from verified.duckdb
  migrate-duckdb-to-supabase  Upload verified_emails from DuckDB into Supabase
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

import click
from tqdm import tqdm

# Ensure the project root is on the path
sys.path.insert(0, str(Path(__file__).parent))

from engine.verifier import verify_email, verify_batch
from engine.tiered_verifier import _tier1_cached
from engine.models import Reachability, VerificationResult
from store.supabase_io import supabase_client_from_env


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
def main(verbose: bool):
    """KadenVerify — self-hosted email verification engine."""
    _setup_logging(verbose)


@main.command()
@click.argument("email")
@click.option("--helo", default="verify.kadenwood.com", help="EHLO domain")
@click.option("--from-addr", default="verify@kadenwood.com", help="MAIL FROM address")
@click.option("--json-output", is_flag=True, help="Output as JSON")
def verify(email: str, helo: str, from_addr: str, json_output: bool):
    """Verify a single email address."""
    result = asyncio.run(verify_email(email, helo_domain=helo, from_address=from_addr))

    if json_output:
        click.echo(json.dumps(result.to_omniverifier(), indent=2))
    else:
        _print_result(result)


@main.command("verify-file")
@click.argument("filepath", type=click.Path(exists=True))
@click.option("--output", "-o", type=click.Path(), help="Output file path")
@click.option("--format", "fmt", type=click.Choice(["json", "csv", "text"]), default="text")
@click.option("--concurrency", "-c", default=5, help="Max concurrent SMTP connections")
@click.option("--helo", default="verify.kadenwood.com", help="EHLO domain")
@click.option("--from-addr", default="verify@kadenwood.com", help="MAIL FROM address")
def verify_file(filepath: str, output: str, fmt: str, concurrency: int, helo: str, from_addr: str):
    """Verify emails from a text file (one email per line)."""
    emails = _read_email_file(filepath)
    if not emails:
        click.echo("No emails found in file.")
        return

    click.echo(f"Verifying {len(emails)} emails (concurrency={concurrency})...")

    pbar = tqdm(total=len(emails), desc="Verifying", unit="email")

    def on_progress(result):
        pbar.update(1)

    results = asyncio.run(
        verify_batch(
            emails,
            concurrency=concurrency,
            helo_domain=helo,
            from_address=from_addr,
            progress_callback=on_progress,
        )
    )
    pbar.close()

    _output_results(results, output, fmt)
    _print_summary(results)


@main.command()
@click.option("--source", required=True, help="Source DuckDB name (e.g., 'qualified', 'apollo')")
@click.option("--source-path", type=click.Path(), help="Full path to source .duckdb file")
@click.option("--table", default="contacts", help="Table name in source DB")
@click.option("--email-column", default="email", help="Email column name")
@click.option("--verified-db", type=click.Path(), help="Path to verified.duckdb")
@click.option("--concurrency", "-c", default=5, help="Max concurrent SMTP connections")
@click.option("--limit", type=int, help="Max emails to verify")
@click.option("--helo", default="verify.kadenwood.com", help="EHLO domain")
@click.option("--from-addr", default="verify@kadenwood.com", help="MAIL FROM address")
def pipeline(
    source: str,
    source_path: str,
    table: str,
    email_column: str,
    verified_db: str,
    concurrency: int,
    limit: int,
    helo: str,
    from_addr: str,
):
    """Run batch verification against a DuckDB source.

    Reads emails from a people-warehouse DuckDB database, verifies them,
    and writes results to verified.duckdb. Incremental: skips already-verified emails.
    """
    from store.duckdb_io import (
        init_verified_db,
        read_emails_from_source,
        write_results_batch,
        get_stats,
    )

    # Resolve paths
    if source_path:
        src = Path(source_path)
    else:
        # Look for source in standard locations
        candidates = [
            Path(__file__).parent / f"{source}.duckdb",
            Path(__file__).parent.parent / "people-warehouse" / "etl" / f"{source}.duckdb",
        ]
        src = next((p for p in candidates if p.exists()), None)
        if src is None:
            click.echo(f"Could not find {source}.duckdb. Use --source-path to specify.")
            sys.exit(1)

    supa = supabase_client_from_env()
    if supa is not None:
        click.echo(f"Reading emails from {src}...")
        emails = read_emails_from_source(
            source_path=src,
            table=table,
            email_column=email_column,
            limit=limit,
            exclude_verified_db=None,
        )

        if not emails:
            click.echo("No emails to verify.")
            stats = supa.get_stats()
            click.echo(f"Total verified: {stats['total']}")
            return

        click.echo(f"Verifying {len(emails)} emails (concurrency={concurrency}) using Supabase store...")
        pbar = tqdm(total=len(emails), desc="Verifying", unit="email")

        async def cache_lookup(email: str):
            return await asyncio.to_thread(supa.get_by_email, email)

        async def upsert_result(result):
            await asyncio.to_thread(supa.upsert_result, result)

        async def verify_one(email: str):
            cached = await _tier1_cached(email, cache_lookup)
            if cached is not None:
                return cached
            try:
                return await verify_email(email, helo_domain=helo, from_address=from_addr)
            except Exception:
                logging.getLogger("kadenverify.cli").exception("Verification failed for %s", email)
                from engine.models import VerificationResult as _VR

                return _VR(
                    email=email,
                    normalized=email.strip().lower(),
                    reachability=Reachability.unknown,
                    is_deliverable=None,
                    domain=email.split("@")[-1] if "@" in email else "",
                    error="internal verification error",
                )

        queue: asyncio.Queue = asyncio.Queue()
        for email in emails:
            queue.put_nowait(email)
        for _ in range(max(1, concurrency)):
            queue.put_nowait(None)

        results: list = []

        async def worker():
            while True:
                email = await queue.get()
                if email is None:
                    queue.task_done()
                    break
                result = await verify_one(email)
                await upsert_result(result)
                results.append(result)
                pbar.update(1)
                queue.task_done()

        async def run():
            workers = [asyncio.create_task(worker()) for _ in range(max(1, concurrency))]
            await queue.join()
            await asyncio.gather(*workers)
            return results

        results = asyncio.run(run())
        pbar.close()

        stats = supa.get_stats()
        click.echo(f"\nTotal verified: {stats['total']}")
        for reach, count in stats.get("by_reachability", {}).items():
            click.echo(f"  {reach}: {count}")
        click.echo(f"  catch-all domains: {stats.get('catch_all', 0)}")
        click.echo(f"  disposable: {stats.get('disposable', 0)}")
        return

    # DuckDB-backed pipeline (legacy)
    verified_path = Path(verified_db) if verified_db else None

    vconn = init_verified_db(verified_path)
    verified_path = verified_path or Path(__file__).parent / "verified.duckdb"

    click.echo(f"Reading emails from {src}...")
    emails = read_emails_from_source(
        source_path=src,
        table=table,
        email_column=email_column,
        limit=limit,
        exclude_verified_db=verified_path,
    )

    if not emails:
        click.echo("No new emails to verify.")
        stats = get_stats(vconn)
        click.echo(f"Total verified: {stats['total']}")
        vconn.close()
        return

    click.echo(f"Verifying {len(emails)} emails (concurrency={concurrency})...")

    pbar = tqdm(total=len(emails), desc="Verifying", unit="email")
    batch_buffer: list = []
    WRITE_BATCH_SIZE = 100

    def on_progress(result):
        pbar.update(1)
        batch_buffer.append(result)
        if len(batch_buffer) >= WRITE_BATCH_SIZE:
            write_results_batch(vconn, batch_buffer)
            batch_buffer.clear()

    results = asyncio.run(
        verify_batch(
            emails,
            concurrency=concurrency,
            helo_domain=helo,
            from_address=from_addr,
            progress_callback=on_progress,
        )
    )
    pbar.close()

    if batch_buffer:
        write_results_batch(vconn, batch_buffer)
        batch_buffer.clear()

    stats = get_stats(vconn)
    click.echo(f"\nTotal verified: {stats['total']}")
    for reach, count in stats.get("by_reachability", {}).items():
        click.echo(f"  {reach}: {count}")
    click.echo(f"  catch-all domains: {stats.get('catch_all', 0)}")
    click.echo(f"  disposable: {stats.get('disposable', 0)}")

    vconn.close()


@main.command()
@click.option("--verified-db", type=click.Path(), help="Path to verified.duckdb")
def stats(verified_db: str):
    """Show verification statistics from the configured store backend."""
    supa = supabase_client_from_env()
    if supa is not None:
        s = supa.get_stats()
        conn = None
    else:
        from store.duckdb_io import init_verified_db, get_stats

        verified_path = Path(verified_db) if verified_db else None
        conn = init_verified_db(verified_path)
        s = get_stats(conn)

    click.echo(f"Total verified emails: {s['total']}")
    click.echo("\nBy reachability:")
    for reach, count in s.get("by_reachability", {}).items():
        pct = (count / s["total"] * 100) if s["total"] > 0 else 0
        click.echo(f"  {reach}: {count} ({pct:.1f}%)")

    click.echo(f"\nCatch-all domains: {s.get('catch_all', 0)}")
    click.echo(f"Disposable: {s.get('disposable', 0)}")

    if s.get("top_domains"):
        click.echo("\nTop 20 domains:")
        for d in s["top_domains"]:
            click.echo(f"  {d['domain']}: {d['count']}")

    if conn is not None:
        conn.close()


@main.command("find-email")
@click.option("--first", required=True, help="First name")
@click.option("--last", required=True, help="Last name")
@click.option("--domain", required=True, help="Company domain")
@click.option("--company", default=None, help="Company name (optional)")
@click.option("--json-output", is_flag=True, help="Output as JSON")
@click.option("--helo", default="verify.kadenwood.com", help="EHLO domain")
@click.option("--from-addr", default="verify@kadenwood.com", help="MAIL FROM address")
def find_email_cmd(first: str, last: str, domain: str, company: str, json_output: bool, helo: str, from_addr: str):
    """Find email for a contact given name + domain."""
    from engine.email_finder import find_email

    result = asyncio.run(find_email(
        first_name=first,
        last_name=last,
        domain=domain,
        company_name=company,
        helo_domain=helo,
        from_address=from_addr,
    ))

    if json_output:
        click.echo(json.dumps(result.model_dump(), indent=2))
    else:
        if result.email:
            icon = {
                Reachability.safe: "✓",
                Reachability.risky: "~",
                Reachability.unknown: "?",
            }.get(result.reachability, "?")
            click.echo(f"\n{icon} {result.email}")
            click.echo(f"  Confidence:  {result.confidence:.2f}")
            click.echo(f"  Method:      {result.method}")
            click.echo(f"  Reachability: {result.reachability.value}")
            click.echo(f"  Provider:    {result.provider.value}")
            click.echo(f"  Catch-all:   {result.domain_is_catchall}")
            click.echo(f"  Candidates:  {result.candidates_tried}")
            if result.cost > 0:
                click.echo(f"  Cost:        ${result.cost:.4f}")
        else:
            click.echo(f"\n✗ No email found for {first} {last} @ {domain}")
            if result.error:
                click.echo(f"  Error: {result.error}")
            click.echo(f"  Candidates tried: {result.candidates_tried}")


@main.command("find-emails")
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--output", "-o", type=click.Path(), help="Output CSV path")
@click.option("--concurrency", "-c", default=10, help="Max concurrent domains")
@click.option("--helo", default="verify.kadenwood.com", help="EHLO domain")
@click.option("--from-addr", default="verify@kadenwood.com", help="MAIL FROM address")
def find_emails_cmd(input_file: str, output: str, concurrency: int, helo: str, from_addr: str):
    """Batch find emails from a CSV file.

    CSV must have columns: first_name, last_name, domain.
    Optional column: company_name.
    """
    import csv

    from engine.email_finder import find_emails_batch

    contacts = []
    with open(input_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "first_name" in row and "last_name" in row and "domain" in row:
                contacts.append({
                    "first_name": row["first_name"],
                    "last_name": row["last_name"],
                    "domain": row["domain"],
                    "company_name": row.get("company_name", ""),
                })

    if not contacts:
        click.echo("No valid contacts found in CSV (need first_name, last_name, domain columns).")
        return

    click.echo(f"Finding emails for {len(contacts)} contacts (concurrency={concurrency})...")
    pbar = tqdm(total=len(contacts), desc="Finding", unit="contact")

    def on_progress(result):
        pbar.update(1)

    results = asyncio.run(find_emails_batch(
        contacts=contacts,
        concurrency=concurrency,
        helo_domain=helo,
        from_address=from_addr,
        progress_callback=on_progress,
    ))
    pbar.close()

    # Output results
    found = sum(1 for r in results if r.email)
    click.echo(f"\nFound: {found}/{len(contacts)} emails")

    if output:
        with open(output, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "first_name", "last_name", "domain", "email", "confidence",
                "method", "reachability", "is_catchall", "cost",
            ])
            for contact, result in zip(contacts, results):
                writer.writerow([
                    contact["first_name"],
                    contact["last_name"],
                    contact["domain"],
                    result.email or "",
                    f"{result.confidence:.2f}",
                    result.method,
                    result.reachability.value,
                    result.domain_is_catchall,
                    f"{result.cost:.4f}",
                ])
        click.echo(f"Results written to {output}")
    else:
        for contact, result in zip(contacts, results):
            if result.email:
                click.echo(f"  ✓ {contact['first_name']} {contact['last_name']} → {result.email} ({result.confidence:.2f}, {result.method})")
            else:
                click.echo(f"  ✗ {contact['first_name']} {contact['last_name']} @ {contact['domain']} — not found")


@main.command("migrate-duckdb-to-supabase")
@click.option(
    "--duckdb-path",
    type=click.Path(exists=True),
    default=str(Path(__file__).parent / "verified.duckdb"),
    show_default=True,
    help="Path to the DuckDB file containing verified_emails.",
)
@click.option("--batch-size", type=int, default=500, show_default=True)
@click.option("--limit", type=int, default=None, help="Max rows to migrate (for testing).")
def migrate_duckdb_to_supabase(duckdb_path: str, batch_size: int, limit: int):
    """Migrate verified_emails rows from DuckDB into Supabase (upsert by email)."""
    supa = supabase_client_from_env()
    if supa is None:
        click.echo(
            "Supabase not configured. Set KADENVERIFY_SUPABASE_URL and "
            "KADENVERIFY_SUPABASE_SERVICE_ROLE_KEY."
        )
        sys.exit(1)

    import duckdb
    from datetime import datetime, timezone

    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info('verified_emails')").fetchall()]
        expected = [
            "email",
            "normalized",
            "reachability",
            "is_deliverable",
            "is_catch_all",
            "is_disposable",
            "is_role",
            "is_free",
            "mx_host",
            "smtp_code",
            "smtp_message",
            "provider",
            "domain",
            "verified_at",
            "error",
        ]
        select_cols = [c for c in expected if c in cols]
        if "email" not in select_cols:
            raise RuntimeError("DuckDB table verified_emails is missing required column: email")

        query = f"SELECT {', '.join(select_cols)} FROM verified_emails"
        if limit:
            query += f" LIMIT {int(limit)}"

        cursor = conn.execute(query)
        migrated = 0
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break

            results = []
            for row in batch:
                data = dict(zip(select_cols, row))
                email = str(data.get("email") or "").strip()
                if not email:
                    continue

                if not data.get("normalized"):
                    data["normalized"] = email
                if not data.get("reachability"):
                    data["reachability"] = "unknown"
                if not data.get("provider"):
                    data["provider"] = "generic"
                if not data.get("domain") and "@" in email:
                    data["domain"] = email.split("@")[-1]
                if not data.get("verified_at"):
                    data["verified_at"] = datetime.now(timezone.utc)

                results.append(VerificationResult.model_validate(data))

            supa.upsert_results_batch(results, batch_size=len(results))
            migrated += len(results)

            if migrated and migrated % (batch_size * 10) == 0:
                click.echo(f"Migrated {migrated} rows...")

        click.echo(f"Migration complete: {migrated} rows upserted.")
    finally:
        conn.close()


def _read_email_file(filepath: str) -> list[str]:
    """Read emails from a text file (one per line)."""
    with open(filepath) as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#") and "@" in line
        ]


def _print_result(result):
    """Pretty-print a single verification result."""
    icon = {
        Reachability.safe: "✓",
        Reachability.risky: "~",
        Reachability.invalid: "✗",
        Reachability.unknown: "?",
    }.get(result.reachability, "?")

    click.echo(f"\n{icon} {result.email}")
    click.echo(f"  Reachability: {result.reachability.value}")
    click.echo(f"  Deliverable:  {result.is_deliverable}")
    click.echo(f"  Provider:     {result.provider.value}")
    click.echo(f"  MX Host:      {result.mx_host}")
    click.echo(f"  SMTP Code:    {result.smtp_code}")

    flags = []
    if result.is_catch_all:
        flags.append("catch-all")
    if result.is_disposable:
        flags.append("disposable")
    if result.is_role:
        flags.append("role")
    if result.is_free:
        flags.append("free")
    if flags:
        click.echo(f"  Flags:        {', '.join(flags)}")
    if result.error:
        click.echo(f"  Error:        {result.error}")


def _output_results(results, output_path, fmt):
    """Write results to a file or stdout."""
    if fmt == "json":
        data = [r.to_omniverifier() for r in results]
        text = json.dumps(data, indent=2)
    elif fmt == "csv":
        import csv
        import io
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "email", "status", "reachability", "deliverable", "catch_all",
            "disposable", "role", "free", "provider", "mx_host", "smtp_code",
        ])
        for r in results:
            writer.writerow([
                r.email, r.to_omniverifier()["status"], r.reachability.value,
                r.is_deliverable, r.is_catch_all, r.is_disposable,
                r.is_role, r.is_free, r.provider.value, r.mx_host, r.smtp_code,
            ])
        text = buf.getvalue()
    else:
        lines = []
        for r in results:
            icon = {"safe": "✓", "risky": "~", "invalid": "✗", "unknown": "?"}.get(
                r.reachability.value, "?"
            )
            lines.append(f"{icon} {r.email} [{r.reachability.value}]")
        text = "\n".join(lines)

    if output_path:
        with open(output_path, "w") as f:
            f.write(text)
        click.echo(f"Results written to {output_path}")
    else:
        click.echo(text)


def _print_summary(results):
    """Print a summary of batch verification results."""
    total = len(results)
    counts = {}
    for r in results:
        counts[r.reachability.value] = counts.get(r.reachability.value, 0) + 1

    click.echo(f"\nSummary ({total} emails):")
    for reach in ["safe", "risky", "invalid", "unknown"]:
        count = counts.get(reach, 0)
        pct = (count / total * 100) if total > 0 else 0
        click.echo(f"  {reach}: {count} ({pct:.1f}%)")


if __name__ == "__main__":
    main()
