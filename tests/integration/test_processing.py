from __future__ import annotations

import pytest

from reconciler import signing
from reconciler.clock import FakeClock
from reconciler.context import AppContext
from reconciler.errors import IngestRejected
from reconciler.ingest import ingest
from reconciler.patterns import idempotency
from reconciler.processor import process_next, sweep
from reconciler.settings import GapPolicy
from reconciler.worker import run_until_idle
from tests.helpers import SECRET, aggregate, body_for, scalar, send, statuses


def test_in_order_events_apply(ctx: AppContext) -> None:
    send(ctx, version=1, data={"status": "pending", "amount_cents": 100})
    send(ctx, version=2, data={"status": "paid"})
    outcomes = run_until_idle(ctx)
    assert [o.kind for o in outcomes] == ["applied", "applied"]
    assert aggregate(ctx) == (2, {"status": "paid", "amount_cents": 100})
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 2


def test_process_next_returns_none_when_idle(ctx: AppContext) -> None:
    assert process_next(ctx) is None


def test_replay_yields_same_state_and_no_second_effect(ctx: AppContext) -> None:
    send(ctx, version=1, data={"amount_cents": 5})
    run_until_idle(ctx)
    before = aggregate(ctx)
    send(ctx, version=1, data={"amount_cents": 5})  # same key delivered again
    outcomes = run_until_idle(ctx)
    assert [o.kind for o in outcomes] == ["duplicate"]
    assert aggregate(ctx) == before
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 1


def test_same_key_different_content_cannot_overwrite(ctx: AppContext) -> None:
    send(ctx, version=1, data={"amount_cents": 5})
    run_until_idle(ctx)
    send(ctx, version=1, data={"amount_cents": 999}, event_id="order-1-v1")
    run_until_idle(ctx)
    assert aggregate(ctx) == (1, {"amount_cents": 5})


def test_replay_after_ttl_is_noop(ctx: AppContext, clock: FakeClock) -> None:
    """ADR-0001: past the idempotency TTL the version check is the backstop."""
    send(ctx, version=1, data={"amount_cents": 5})
    run_until_idle(ctx)
    clock.advance(ctx.settings.idempotency_ttl_seconds + 1)
    with ctx.pool.connection() as conn:
        assert idempotency.lookup(conn, "order-1-v1", clock.now()) is None  # record expired
    send(ctx, version=1, data={"amount_cents": 5})
    outcomes = run_until_idle(ctx)
    assert [o.kind for o in outcomes] == ["stale"]
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 1


def test_out_of_order_event_buffers_then_drains_when_gap_fills(ctx: AppContext) -> None:
    send(ctx, version=3, data={"status": "shipped"})
    send(ctx, version=2, data={"status": "paid", "amount_cents": 7})
    assert [o.kind for o in run_until_idle(ctx)] == ["buffered", "buffered"]
    assert aggregate(ctx) == (0, {})
    send(ctx, version=1, data={"status": "pending"})
    run_until_idle(ctx)  # v1 applies, then v2 and v3 drain inside the same transaction
    assert aggregate(ctx) == (3, {"status": "shipped", "amount_cents": 7})
    assert scalar(ctx, "SELECT count(*) FROM buffer") == 0
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 3


def test_duplicate_of_buffered_event_is_harmless(ctx: AppContext) -> None:
    send(ctx, version=2, data={"status": "paid"})
    send(ctx, version=2, data={"status": "paid"})
    send(ctx, version=1, data={"status": "pending"})
    run_until_idle(ctx)
    assert aggregate(ctx) == (2, {"status": "paid"})
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 2
    assert sorted(statuses(ctx)[("order-1", 2)]) == ["duplicate", "processed"]


def test_stale_lower_version_is_ignored(ctx: AppContext) -> None:
    send(ctx, version=1, data={"status": "pending"})
    send(ctx, version=2, data={"status": "paid"})
    run_until_idle(ctx)
    send(
        ctx, version=1, data={"status": "cancelled"}, event_id="other-id"
    )  # different key, old version
    assert [o.kind for o in run_until_idle(ctx)] == ["stale"]
    assert aggregate(ctx) == (2, {"status": "paid"})


def test_gap_window_expiry_parks_and_alerts_under_default_policy(
    ctx: AppContext, clock: FakeClock
) -> None:
    send(ctx, version=2, data={"status": "paid"})
    run_until_idle(ctx)
    clock.advance(ctx.settings.buffer_window_seconds - 1)
    assert sweep(ctx).parked == 0
    clock.advance(2)
    assert sweep(ctx).parked == 1
    assert statuses(ctx)[("order-1", 2)] == ["parked"]
    assert aggregate(ctx) == (0, {})  # nothing applied on top of missing history


def test_gap_window_expiry_applies_under_last_writer_wins(
    ctx: AppContext, clock: FakeClock
) -> None:
    ctx.settings.gap_policy = GapPolicy.LAST_WRITER_WINS
    send(ctx, version=3, data={"status": "shipped"})
    send(ctx, version=2, data={"amount_cents": 9})
    run_until_idle(ctx)
    clock.advance(ctx.settings.buffer_window_seconds + 1)
    result = sweep(ctx)
    assert result.force_applied == 1
    # v2 applied despite the gap at v1, then v3 drained behind it
    assert aggregate(ctx) == (3, {"amount_cents": 9, "status": "shipped"})
    send(ctx, version=1, data={"status": "pending"})  # the late predecessor is now stale
    assert [o.kind for o in run_until_idle(ctx)] == ["stale"]


def test_sweep_purges_expired_idempotency_records(ctx: AppContext, clock: FakeClock) -> None:
    send(ctx, version=1)
    run_until_idle(ctx)
    assert scalar(ctx, "SELECT count(*) FROM idempotency") == 1
    clock.advance(ctx.settings.idempotency_ttl_seconds + 1)
    assert sweep(ctx).purged == 1
    assert scalar(ctx, "SELECT count(*) FROM idempotency") == 0


def test_sweep_purges_old_unauthenticated_rejections_only(
    ctx: AppContext, clock: FakeClock
) -> None:
    body = body_for()
    for sig in (None, "sha256=bad"):
        with pytest.raises(IngestRejected):
            ingest(ctx, body, sig)
    forged_but_signed = b"not json"  # signed: kept for forensics regardless of age
    with pytest.raises(IngestRejected):
        ingest(ctx, forged_but_signed, signing.sign(forged_but_signed, SECRET))
    send(ctx)  # a received event must never be purged
    clock.advance(ctx.settings.rejected_retention_seconds - 1)
    assert sweep(ctx).rejected_purged == 0
    clock.advance(2)
    assert sweep(ctx).rejected_purged == 2
    assert scalar(ctx, "SELECT count(*) FROM events WHERE status = 'rejected'") == 1
    assert scalar(ctx, "SELECT count(*) FROM events WHERE status = 'received'") == 1
