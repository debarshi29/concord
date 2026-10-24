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


def test_claim_query_is_served_by_a_partial_seq_index(ctx: AppContext) -> None:
    """Claiming must not scan processed history: it needs an index ordered by seq over `received` rows."""
    with ctx.pool.connection() as conn:
        conn.execute(
            "INSERT INTO events (id, raw_body, signature_status, status, next_attempt_at, received_at) "
            "SELECT gen_random_uuid(), 'x', 'valid', CASE WHEN g <= 5000 THEN 'processed' ELSE 'received' END, "
            "now() - interval '1 minute', now() FROM generate_series(1, 5100) g"
        )
        conn.execute("ANALYZE events")
        plan = "\n".join(
            str(next(iter(r.values())))
            for r in conn.execute(
                "EXPLAIN SELECT * FROM events WHERE status = 'received' AND next_attempt_at <= now() "
                "ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED"
            )
        )
    assert "events_claim_seq_idx" in plan, plan
