from __future__ import annotations

from reconciler.context import AppContext
from reconciler.db import migrate
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
