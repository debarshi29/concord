"""Connection pool and forward-only migrations (expand-migrate-contract; see docs/runbook.md §9)."""

from __future__ import annotations

from importlib import resources
from typing import Any, cast

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from reconciler.logs import get_logger

Conn = psycopg.Connection[dict[str, Any]]
Pool = ConnectionPool[Conn]

log = get_logger("db")

_MIGRATION_LOCK_KEY = 7_140_014  # arbitrary constant: serialises concurrent `migrate` runs


def make_pool(database_url: str, min_size: int = 1, max_size: int = 10) -> Pool:
    pool = ConnectionPool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        kwargs={"row_factory": dict_row},
        open=False,
    )
    pool.open(wait=True, timeout=30)
    return cast(Pool, pool)


TABLES = "events, idempotency, aggregates, buffer, dlq, effect_log, corrections, reconcile_runs"


def truncate_all(pool: Pool) -> None:
    """Empty every data table (tests, benchmarks and the chaos harness start from a clean slate)."""
    with pool.connection() as conn:
        conn.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")


def _migration_files() -> list[tuple[str, str]]:
    root = resources.files("reconciler") / "migrations"
    items = [
        (p.name, p.read_text(encoding="utf-8")) for p in root.iterdir() if p.name.endswith(".sql")
    ]
    return sorted(items)


def migrate(pool: Pool) -> list[str]:
    """Apply pending migrations in order. Returns the versions applied by this call."""
    applied: list[str] = []
    with pool.connection() as conn:
        # Autocommit, so the session-level lock outlives only *committed* work: with an implicit
        # transaction the unlock ran before the commit, and `conn.transaction()` was a mere savepoint
        # (a failure in migration N then rolled back 1..N-1 too).
        conn.autocommit = True
        conn.execute("SELECT pg_advisory_lock(%s)", (_MIGRATION_LOCK_KEY,))
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            done = {r["version"] for r in conn.execute("SELECT version FROM schema_migrations")}
            for name, sql in _migration_files():
                if name in done:
                    continue
                with conn.transaction():
                    conn.execute(sql)
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (name,))
                applied.append(name)
                log.info("migration applied", extra={"version": name})
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_MIGRATION_LOCK_KEY,))
            conn.autocommit = False  # the connection goes back to the pool
    return applied
