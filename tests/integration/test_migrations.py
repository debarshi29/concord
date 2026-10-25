from __future__ import annotations

import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from reconciler.context import AppContext
from reconciler.db import _migration_files, make_pool, migrate
from tests.helpers import scalar


def test_migrate_is_idempotent_and_recorded(ctx: AppContext) -> None:
    assert migrate(ctx.pool) == []  # already applied by the session fixture
    assert (
        scalar(ctx, "SELECT count(*) FROM schema_migrations WHERE version = '0001_init.sql'") == 1
    )


def test_schema_has_the_spec_tables(ctx: AppContext) -> None:
    for table in (
        "events",
        "idempotency",
        "aggregates",
        "buffer",
        "dlq",
        "effect_log",
        "corrections",
    ):
        assert scalar(ctx, "SELECT to_regclass(%s) IS NOT NULL", f"public.{table}") is True


def test_effect_log_deliberately_allows_duplicates(ctx: AppContext) -> None:
    """The duplicate-effect metric depends on there being NO unique constraint here (see 0001_init.sql)."""
    assert (
        scalar(
            ctx,
            "SELECT count(*) FROM pg_indexes WHERE tablename = 'effect_log' AND position('UNIQUE' in indexdef) > 0 "
            "AND indexname <> 'effect_log_pkey'",
        )
        == 0
    )


# --- concurrency and atomicity, on throwaway databases (the shared one is already migrated) ---


@pytest.fixture
def fresh_db_url(database_url: str) -> Iterator[str]:
    name = f"mig_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        yield make_conninfo(database_url, dbname=name)
    finally:
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def test_concurrent_migrators_apply_each_migration_exactly_once(fresh_db_url: str) -> None:
    barrier = threading.Barrier(4)

    def run() -> list[str]:
        p = make_pool(fresh_db_url, min_size=1, max_size=2)
        try:
            barrier.wait()
            return migrate(p)
        finally:
            p.close()

    with ThreadPoolExecutor(4) as ex:
        results = [
            f.result() for f in [ex.submit(run) for _ in range(4)]
        ]  # raises if any migrator fails

    applied = [name for r in results for name in r]
    assert sorted(applied) == sorted(
        name for name, _ in _migration_files()
    )  # no duplicates, none missed


def test_a_failing_migration_keeps_the_ones_before_it(
    fresh_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    good = ("0001_a.sql", "CREATE TABLE t_a (id int)")
    bad = ("0002_b.sql", "CREATE TABLE t_b (id int); SELECT 1/0")
    monkeypatch.setattr("reconciler.db._migration_files", lambda: [good, bad])
    p = make_pool(fresh_db_url, min_size=1, max_size=2)
    try:
        with pytest.raises(psycopg.errors.DivisionByZero):
            migrate(p)
        with p.connection() as conn:
            done = [r["version"] for r in conn.execute("SELECT version FROM schema_migrations")]
            t_b = conn.execute("SELECT to_regclass('t_b') IS NOT NULL AS x").fetchone()
        assert done == ["0001_a.sql"]  # migration 1 committed on its own; 2 left no trace
        assert t_b is not None and t_b["x"] is False
        # the advisory lock was released: a second run can take it and retry
        monkeypatch.setattr(
            "reconciler.db._migration_files",
            lambda: [good, ("0002_b.sql", "CREATE TABLE t_b (id int)")],
        )
        assert migrate(p) == ["0002_b.sql"]
    finally:
        p.close()
