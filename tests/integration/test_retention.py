"""History retention trims finished rows and never anything still in use."""

from __future__ import annotations

from reconciler.clock import FakeClock
from reconciler.context import AppContext
from reconciler.patterns import dlq, idempotency
from reconciler.processor import sweep
from reconciler.reconcile import SourceRecord, reconcile
from reconciler.retention import purge_history
from reconciler.worker import run_until_idle
from tests.helpers import scalar, send
from tests.integration.test_retry_dlq import ListSource

DAY = 24 * 3600


def _age_past_retention(ctx: AppContext, clock: FakeClock) -> None:
    clock.advance(ctx.settings.retention_seconds + ctx.settings.idempotency_ttl_seconds + 1)


def test_finished_history_is_deleted_after_the_window(ctx: AppContext, clock: FakeClock) -> None:
    for v in (1, 2):
        send(ctx, version=v, data={"amount_cents": v})
    send(ctx, version=2, data={"amount_cents": 2})  # a duplicate
    run_until_idle(ctx)
    reconcile(ctx, ListSource(SourceRecord("order-1", 2, {"amount_cents": 2})))
    reconcile(ctx, ListSource(SourceRecord("order-1", 2, {"amount_cents": 9})))  # a correction
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 2

    _age_past_retention(ctx, clock)
    result = sweep(ctx)

    assert result.retention_purged > 0
    for table in ("events", "effect_log", "corrections"):
        assert scalar(ctx, f"SELECT count(*) FROM {table}") == 0, table
    assert (
        scalar(ctx, "SELECT count(*) FROM aggregates") == 1
    )  # live state is never retention's business


def test_nothing_inside_the_window_is_touched(ctx: AppContext, clock: FakeClock) -> None:
    send(ctx)
    run_until_idle(ctx)
    clock.advance(ctx.settings.retention_seconds - 1)
    assert sweep(ctx).retention_purged == 0
    assert scalar(ctx, "SELECT count(*) FROM events") == 1
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 1


def test_retention_zero_keeps_everything(ctx: AppContext, clock: FakeClock) -> None:
    ctx.settings.retention_seconds = 0
    send(ctx)
    run_until_idle(ctx)
    _age_past_retention(ctx, clock)
    clock.advance(400 * DAY)
    assert sweep(ctx).retention_purged == 0
    assert scalar(ctx, "SELECT count(*) FROM events") == 1


def test_event_still_referenced_by_an_idempotency_record_is_kept(
    ctx: AppContext, clock: FakeClock
) -> None:
    """Retention shorter than the idempotency TTL must not break the record's foreign key."""
    ctx.settings.retention_seconds = 60
    ctx.settings.idempotency_ttl_seconds = 3600
    send(ctx)
    run_until_idle(ctx)
    clock.advance(600)  # past retention, inside the idempotency TTL
    sweep(ctx)  # the effect_log row has aged out; the event is pinned by its idempotency record
    assert scalar(ctx, "SELECT count(*) FROM events") == 1
    clock.advance(3600)  # the record expires and is purged first, releasing the event
    assert sweep(ctx).retention_purged > 0
    assert scalar(ctx, "SELECT count(*) FROM events") == 0


def test_open_dlq_entries_and_their_events_are_kept_resolved_ones_go(
    ctx: AppContext, clock: FakeClock
) -> None:
    send(ctx, "order-bad", 1, {"status": "banana"})
    send(ctx, "order-bad2", 1, {"status": "banana"})
    run_until_idle(ctx)
    with ctx.pool.connection() as conn:
        first = conn.execute("SELECT event_id FROM dlq ORDER BY failed_at, event_id").fetchall()
        dlq.discard(conn, ctx.clock.now(), first[0]["event_id"])

    _age_past_retention(ctx, clock)
    sweep(ctx)
    assert (
        scalar(ctx, "SELECT count(*) FROM dlq WHERE resolved_at IS NULL") == 1
    )  # still open: kept
    assert scalar(ctx, "SELECT count(*) FROM dlq") == 1  # the discarded one aged out
    assert scalar(ctx, "SELECT count(*) FROM events WHERE status = 'dead'") == 1
    sweep(ctx)  # its event is released on the pass after the DLQ row goes
    assert scalar(ctx, "SELECT count(*) FROM events") == 1  # only the open entry's event remains


def test_unprocessed_and_buffered_events_are_never_purged(
    ctx: AppContext, clock: FakeClock
) -> None:
    ctx.settings.buffer_window_seconds = 10**9  # keep the gap open so the event stays buffered
    send(ctx, version=1)  # received, never processed
    send(ctx, "order-2", 5)  # buffered behind a gap once processed
    process_only_second_aggregate(ctx)
    _age_past_retention(ctx, clock)
    sweep(ctx)
    assert scalar(ctx, "SELECT count(*) FROM events WHERE status IN ('received', 'buffered')") == 2


def process_only_second_aggregate(ctx: AppContext) -> None:
    """Process exactly the second event so one stays `received` and one becomes `buffered`."""
    from reconciler.processor import process_next

    with ctx.pool.connection() as conn:
        conn.execute(
            "UPDATE events SET next_attempt_at = next_attempt_at + interval '1 day' WHERE aggregate_id = 'order-1'"
        )
    process_next(ctx)


def test_the_latest_reconcile_run_survives_so_the_stale_alert_stays_accurate(
    ctx: AppContext, clock: FakeClock
) -> None:
    reconcile(ctx, ListSource())
    clock.advance(1)
    reconcile(ctx, ListSource())
    _age_past_retention(ctx, clock)
    sweep(ctx)
    assert scalar(ctx, "SELECT count(*) FROM reconcile_runs") == 1


def test_deletes_are_bounded_per_call_and_converge(ctx: AppContext, clock: FakeClock) -> None:
    for v in range(1, 8):
        send(ctx, version=v, data={"amount_cents": v})
    run_until_idle(ctx)
    _age_past_retention(ctx, clock)
    sweep_conn_calls = 0
    with ctx.pool.connection() as conn:
        idempotency.purge_expired(conn, clock.now())  # records expire first, releasing their events
        while True:
            purged = purge_history(conn, clock.now(), ctx.settings.retention_seconds, batch_size=3)
            sweep_conn_calls += 1
            assert max(purged.events, purged.effect_log) <= 3
            if purged.total == 0:
                break
    assert sweep_conn_calls >= 3  # 7 rows at 3 per call
    assert scalar(ctx, "SELECT count(*) FROM events") == 0
