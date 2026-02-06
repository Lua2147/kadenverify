"""DuckDB I/O for People Warehouse integration.

Reads contacts from people-warehouse DuckDB databases (qualified.duckdb, etc.)
and writes verification results to a separate verified.duckdb file.
"""

import logging
from pathlib import Path
from typing import Optional

import duckdb

from engine.models import VerificationResult

logger = logging.getLogger("kadenverify.duckdb")

# Default path for verified results
DEFAULT_VERIFIED_DB = Path(__file__).parent.parent / "verified.duckdb"

# Schema for verified_emails table
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS verified_emails (
    email TEXT PRIMARY KEY,
    normalized TEXT,
    reachability TEXT,
    is_deliverable BOOLEAN,
    is_catch_all BOOLEAN,
    is_disposable BOOLEAN,
    is_role BOOLEAN,
    is_free BOOLEAN,
    mx_host TEXT,
    smtp_code INTEGER,
    smtp_message TEXT,
    provider TEXT,
    domain TEXT,
    verified_at TIMESTAMP
);
"""

_CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_ve_reachability ON verified_emails(reachability);",
    "CREATE INDEX IF NOT EXISTS idx_ve_domain ON verified_emails(domain);",
    "CREATE INDEX IF NOT EXISTS idx_ve_verified_at ON verified_emails(verified_at);",
]

_UPSERT_SQL = """
INSERT OR REPLACE INTO verified_emails (
    email, normalized, reachability, is_deliverable, is_catch_all,
    is_disposable, is_role, is_free, mx_host, smtp_code,
    smtp_message, provider, domain, verified_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def init_verified_db(db_path: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
    """Initialize the verified.duckdb database with schema.

    Creates the database file and verified_emails table if they don't exist.
    Returns a DuckDB connection.
    """
    db_path = db_path or DEFAULT_VERIFIED_DB
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    conn.execute(_CREATE_TABLE_SQL)
    for idx_sql in _CREATE_INDEXES_SQL:
        conn.execute(idx_sql)

    logger.info(f"Initialized verified DB at {db_path}")
    return conn


def write_result(conn: duckdb.DuckDBPyConnection, result: VerificationResult) -> None:
    """Write a single verification result to the database (upsert)."""
    conn.execute(_UPSERT_SQL, [
        result.email,
        result.normalized,
        result.reachability.value,
        result.is_deliverable,
        result.is_catch_all,
        result.is_disposable,
        result.is_role,
        result.is_free,
        result.mx_host,
        result.smtp_code,
        result.smtp_message,
        result.provider.value,
        result.domain,
        result.verified_at,
    ])


def write_results_batch(
    conn: duckdb.DuckDBPyConnection,
    results: list[VerificationResult],
    batch_size: int = 1000,
) -> int:
    """Write a batch of verification results to the database.

    Uses transactions for performance. Returns number of rows written.
    """
    count = 0
    for i in range(0, len(results), batch_size):
        batch = results[i:i + batch_size]
        conn.execute("BEGIN TRANSACTION")
        try:
            for result in batch:
                write_result(conn, result)
            conn.execute("COMMIT")
            count += len(batch)
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return count


def read_emails_from_source(
    source_path: Path,
    table: str = "contacts",
    email_column: str = "email",
    limit: Optional[int] = None,
    exclude_verified_db: Optional[Path] = None,
) -> list[str]:
    """Read email addresses from a people-warehouse DuckDB source.

    Args:
        source_path: Path to the source .duckdb file (e.g., qualified.duckdb).
        table: Table name to read from.
        email_column: Column containing email addresses.
        limit: Optional limit on number of emails to return.
        exclude_verified_db: Path to verified.duckdb -- if provided, excludes
            emails that have already been verified.

    Returns:
        List of email addresses (deduplicated, non-null).
    """
    conn = duckdb.connect(str(source_path), read_only=True)

    try:
        # Check what tables exist
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        if table not in tables:
            # Try to find a table with an email column
            for t in tables:
                cols = [row[0] for row in conn.execute(f"DESCRIBE {t}").fetchall()]
                if email_column in cols:
                    table = t
                    break
            else:
                raise ValueError(
                    f"No table '{table}' and no table with '{email_column}' column found. "
                    f"Available tables: {tables}"
                )

        # Build query
        query = f"SELECT DISTINCT {email_column} FROM {table} WHERE {email_column} IS NOT NULL"

        # Exclude already-verified emails
        if exclude_verified_db and exclude_verified_db.exists():
            verified_conn = duckdb.connect(str(exclude_verified_db), read_only=True)
            try:
                verified_tables = [row[0] for row in verified_conn.execute("SHOW TABLES").fetchall()]
                if "verified_emails" in verified_tables:
                    verified_emails = {
                        row[0] for row in verified_conn.execute(
                            "SELECT email FROM verified_emails"
                        ).fetchall()
                    }
                    logger.info(f"Excluding {len(verified_emails)} already-verified emails")
                else:
                    verified_emails = set()
            finally:
                verified_conn.close()
        else:
            verified_emails = set()

        if limit:
            query += f" LIMIT {limit}"

        rows = conn.execute(query).fetchall()
        emails = [row[0] for row in rows if row[0] and row[0] not in verified_emails]

        logger.info(f"Read {len(emails)} emails from {source_path}:{table}")
        return emails

    finally:
        conn.close()


def get_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    """Get verification statistics from the verified database."""
    total = conn.execute("SELECT COUNT(*) FROM verified_emails").fetchone()[0]

    by_reachability = {}
    for row in conn.execute(
        "SELECT reachability, COUNT(*) FROM verified_emails GROUP BY reachability"
    ).fetchall():
        by_reachability[row[0]] = row[1]

    catch_all_count = conn.execute(
        "SELECT COUNT(*) FROM verified_emails WHERE is_catch_all = true"
    ).fetchone()[0]

    disposable_count = conn.execute(
        "SELECT COUNT(*) FROM verified_emails WHERE is_disposable = true"
    ).fetchone()[0]

    top_domains = conn.execute(
        "SELECT domain, COUNT(*) as cnt FROM verified_emails "
        "GROUP BY domain ORDER BY cnt DESC LIMIT 20"
    ).fetchall()

    return {
        "total": total,
        "by_reachability": by_reachability,
        "catch_all": catch_all_count,
        "disposable": disposable_count,
        "top_domains": [{"domain": r[0], "count": r[1]} for r in top_domains],
    }
