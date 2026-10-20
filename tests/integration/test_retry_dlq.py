from __future__ import annotations

from datetime import timedelta

import pytest

from reconciler.clock import FakeClock
from reconciler.context import AppContext
from reconciler.db import Conn
from reconciler.errors import RetryableError
from reconciler.models import EventRow
from reconciler.patterns import dlq
from reconciler.processor import process_next
from reconciler.reconcile import SourceRecord, reconcile
from reconciler.worker import run_until_idle
from tests.helpers import aggregate, scalar, send, statuses


class ListSource:
    def __init__(self, *records: SourceRecord) -> None:
        self.records = list(records)

    def list_aggregates(self) -> list[SourceRecord]:
        return self.records


def outage(conn: Conn, event: EventRow) -> None:
    raise RetryableError("downstream down")


def ok(conn: Conn, event: EventRow) -> None:
    return None


def _event_row(ctx: AppContext) -> dict[str, object]:
    with ctx.pool.connection() as conn:
        row = conn.execute("SELECT * FROM events ORDER BY seq LIMIT 1").fetchone()
    assert row is not None
    return dict(row)


def test_retryable_failure_rolls_back_and_schedules_backoff(
    ctx: AppContext, clock: FakeClock
) -> None:
    send(ctx)
    ctx.sink = outage
    outcome = process_next(ctx)
    assert outcome is not None and outcome.kind == "retry"
    row = _event_row(ctx)
    assert row["status"] == "received" and row["attempts"] == 1
    assert "downstream down" in str(row["last_error"])
    delay = row["next_attempt_at"] - clock.now()  # type: ignore[operator]
    assert timedelta(0) <= delay <= timedelta(seconds=ctx.retry.ceiling(1))
    # the failed attempt left no trace: no aggregate change, no effect, no idempotency record
    assert aggregate(ctx) is None
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 0
    assert scalar(ctx, "SELECT count(*) FROM idempotency") == 0
    assert process_next(ctx) is None  # not due yet: backoff is respected


def test_backoff_ceiling_grows_and_is_budgeted(ctx: AppContext, clock: FakeClock) -> None:
    send(ctx)
    ctx.sink = outage
    kinds = []
    for attempt in range(1, ctx.retry.max_attempts + 1):
        clock.advance(ctx.retry.cap_seconds + 1)
        outcome = process_next(ctx)
        assert outcome is not None
        kinds.append(outcome.kind)
        if outcome.kind == "retry":
            row = _event_row(ctx)
            assert row["next_attempt_at"] - clock.now() <= timedelta(  # type: ignore[operator]
                seconds=ctx.retry.ceiling(attempt)
            )
    assert kinds == ["retry"] * (ctx.retry.max_attempts - 1) + ["dead"]
    clock.advance(10_000)
    assert process_next(ctx) is None  # a dead event is never retried again


def test_exhausted_budget_dead_letters_with_full_context(ctx: AppContext, clock: FakeClock) -> None:
    send(ctx)
    ctx.sink = outage
    for _ in range(ctx.retry.max_attempts):
        clock.advance(ctx.retry.cap_seconds + 1)
        process_next(ctx)
    with ctx.pool.connection() as conn:
        [entry] = dlq.list_open(conn)
    assert entry["attempts"] == ctx.retry.max_attempts
    assert "downstream down" in entry["last_error"]
    assert (entry["aggregate_id"], entry["version"]) == ("order-1", 1)
    assert statuses(ctx)[("order-1", 1)] == ["dead"]


def test_terminal_error_goes_straight_to_dlq(ctx: AppContext) -> None:
    send(ctx, data={"status": "banana"})
    outcome = process_next(ctx)
    assert outcome is not None and outcome.kind == "dead"
    row = _event_row(ctx)
    assert row["attempts"] == 1 and "TerminalError" in str(row["last_error"])
    assert scalar(ctx, "SELECT count(*) FROM dlq WHERE resolved_at IS NULL") == 1


def test_unexpected_exception_consumes_retry_budget_instead_of_hot_looping(
    ctx: AppContext, clock: FakeClock
) -> None:
    def buggy(conn: Conn, event: EventRow) -> None:
        raise ValueError("bug in sink")

    send(ctx)
    ctx.sink = buggy
    outcome = process_next(ctx)
    assert outcome is not None and outcome.kind == "retry"
    assert "unexpected ValueError" in str(_event_row(ctx)["last_error"])


def test_poison_message_only_blocks_its_own_aggregate(ctx: AppContext) -> None:
    send(ctx, "order-A", 1, {"status": "banana"})  # poison
    send(ctx, "order-B", 1, {"status": "paid"})
    send(ctx, "order-A", 2, {"status": "shipped"})
    send(ctx, "order-B", 2, {"status": "shipped"})
    kinds = [o.kind for o in run_until_idle(ctx)]
    assert kinds == ["dead", "applied", "buffered", "applied"]
    assert aggregate(ctx, "order-B") == (2, {"status": "shipped"})  # healthy stream unaffected
    assert aggregate(ctx, "order-A") == (0, {})  # blast radius = one aggregate
    assert statuses(ctx)[("order-A", 2)] == ["buffered"]


def test_failure_while_draining_is_attributed_to_the_buffered_event(ctx: AppContext) -> None:
    send(ctx, "order-A", 3, {"status": "shipped"})
    send(ctx, "order-A", 2, {"status": "banana"})  # poison, buffered behind the gap
    send(ctx, "order-A", 1, {"status": "pending"})
    run_until_idle(ctx)
    assert aggregate(ctx, "order-A") == (1, {"status": "pending"})  # v1 stays applied
    st = statuses(ctx)
    assert st[("order-A", 1)] == ["processed"]
    assert st[("order-A", 2)] == ["dead"]
    assert st[("order-A", 3)] == ["buffered"]  # drain stopped at the failure


def test_redrive_returns_events_with_a_fresh_budget(ctx: AppContext, clock: FakeClock) -> None:
    send(ctx)
    ctx.sink = outage
    for _ in range(ctx.retry.max_attempts):
        clock.advance(ctx.retry.cap_seconds + 1)
        process_next(ctx)
    assert scalar(ctx, "SELECT count(*) FROM dlq WHERE resolved_at IS NULL") == 1

    ctx.sink = ok  # downstream recovered
    with ctx.pool.connection() as conn, conn.transaction():
        assert dlq.redrive(conn, clock.now()) == 1
        assert dlq.redrive(conn, clock.now()) == 0  # idempotent: nothing left to redrive
    run_until_idle(ctx)
    assert aggregate(ctx) == (1, {"status": "paid"})
    assert scalar(ctx, "SELECT resolution FROM dlq") == "redriven"
    assert scalar(ctx, "SELECT status FROM events") == "processed"


def test_redrive_single_event_by_id(ctx: AppContext, clock: FakeClock) -> None:
    send(ctx, "order-A", 1, {"status": "banana"})
    send(ctx, "order-B", 1, {"status": "banana"})
    run_until_idle(ctx)
    victim = scalar(ctx, "SELECT event_id FROM dlq ORDER BY failed_at, event_id LIMIT 1")
    with ctx.pool.connection() as conn, conn.transaction():
        assert dlq.redrive(conn, clock.now(), victim) == 1
    assert scalar(ctx, "SELECT count(*) FROM dlq WHERE resolved_at IS NULL") == 1


def test_discard_then_reconcile_heals_the_stalled_aggregate(
    ctx: AppContext, clock: FakeClock
) -> None:
    send(ctx, "order-A", 1, {"status": "banana"})
    send(ctx, "order-A", 2, {"status": "shipped"})
    run_until_idle(ctx)
    victim = scalar(ctx, "SELECT event_id FROM dlq")
    with ctx.pool.connection() as conn, conn.transaction():
        assert dlq.discard(conn, clock.now(), victim) is True
        assert dlq.discard(conn, clock.now(), victim) is False
    report = reconcile(ctx, ListSource(SourceRecord("order-A", 2, {"status": "shipped"})))
    assert report.corrected == 1
    assert aggregate(ctx, "order-A") == (2, {"status": "shipped"})
    assert statuses(ctx)[("order-A", 2)] == ["stale"]  # buffered v2 drained as already-covered


@pytest.mark.parametrize("n", [3])
def test_dlq_depth_tracks_open_entries(ctx: AppContext, n: int) -> None:
    for i in range(n):
        send(ctx, f"order-{i}", 1, {"status": "banana"})
    run_until_idle(ctx)
    with ctx.pool.connection() as conn:
        assert dlq.depth(conn) == n
