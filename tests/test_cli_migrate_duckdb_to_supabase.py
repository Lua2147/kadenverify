from __future__ import annotations

from click.testing import CliRunner


def test_migrate_duckdb_to_supabase_uses_verification_result_model(tmp_path, monkeypatch) -> None:
    import duckdb

    import cli

    db_path = tmp_path / "verified.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            create table verified_emails (
              email varchar,
              normalized varchar,
              reachability varchar,
              is_deliverable boolean,
              is_catch_all boolean,
              is_disposable boolean,
              is_role boolean,
              is_free boolean,
              mx_host varchar,
              smtp_code integer,
              smtp_message varchar,
              provider varchar,
              domain varchar,
              verified_at timestamp,
              error varchar
            )
            """
        )
        conn.execute(
            """
            insert into verified_emails values
            (
              'user@example.com',
              'user@example.com',
              'safe',
              true,
              false,
              false,
              false,
              true,
              'mx.example.com',
              250,
              'OK',
              'generic',
              'example.com',
              now(),
              null
            )
            """
        )
    finally:
        conn.close()

    class _FakeSupabase:
        def __init__(self):
            self.batches = []

        def upsert_results_batch(self, results, batch_size: int = 500):  # pragma: no cover
            self.batches.append((results, batch_size))
            return len(results)

    fake = _FakeSupabase()
    monkeypatch.setattr(cli, "supabase_client_from_env", lambda: fake)

    runner = CliRunner()
    result = runner.invoke(
        cli.main,
        [
            "migrate-duckdb-to-supabase",
            "--duckdb-path",
            str(db_path),
            "--batch-size",
            "10",
            "--limit",
            "1",
        ],
    )

    # Fails previously with NameError: VerificationResult is not defined.
    assert result.exit_code == 0, result.output
    assert len(fake.batches) == 1
    assert fake.batches[0][0][0].email == "user@example.com"

