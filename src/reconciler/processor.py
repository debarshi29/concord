"""The processing transaction: idempotency check, version decision, state change, idempotency record.

One database transaction covers: claim the event, lock the aggregate, decide, apply, write the effect
and the idempotency record, drain unblocked successors. A crash anywhere rolls the whole step back, so
effects are exactly-once even though delivery is at-least-once.

Failure handling uses a savepoint: a failed attempt rolls back only its own changes, then the failure
is recorded (retry schedule or DLQ) in the outer transaction that still holds the event row lock.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

from reconciler import domain, retention
from reconciler.context import AppContext
from reconciler.db import Conn
from reconciler.errors import ReconcilerError, RetryableError, TerminalError
from reconciler.logs import get_logger
from reconciler.models import EventRow
from reconciler.patterns import dlq, idempotency
from reconciler.patterns.ordering import Action, decide, window_deadline
from reconciler.settings import GapPolicy
from reconciler.telemetry import span_in_trace

log = get_logger("processor")

OutcomeKind = Literal["applied", "duplicate", "stale", "buffered", "retry", "dead"]


@dataclass(frozen=True)
class Outcome:
    kind: OutcomeKind
    event_id: Any
    detail: str = ""


# --------------------------------------------------------------------------- aggregate access


def lock_aggregate(conn: Conn, aggregate_id: str, now: datetime) -> tuple[dict[str, Any], int]:
    """Create-if-missing then `FOR UPDATE`: serialises all work on one aggregate (ADR-0003)."""
    conn.execute(
        "INSERT INTO aggregates (id, state, version, updated_at) VALUES (%s, '{}'::jsonb, 0, %s) "
        "ON CONFLICT (id) DO NOTHING",
        (aggregate_id, now),
    )
    row = conn.execute(
        "SELECT state, version FROM aggregates WHERE id = %s FOR UPDATE", (aggregate_id,)
    ).fetchone()
    assert row is not None
    return dict(row["state"]), int(row["version"])


def _mark(conn: Conn, event_id: Any, status: str, now: datetime) -> None:
    conn.execute(
        "UPDATE events SET status = %s, processed_at = %s WHERE id = %s", (status, now, event_id)
    )


# --------------------------------------------------------------------------- the decision


def _handle(
    ctx: AppContext, conn: Conn, ev: EventRow, now: datetime, *, force_apply: bool = False
) -> Outcome:
    assert ev.aggregate_id is not None and ev.version is not None and ev.payload is not None
    assert ev.idempotency_key is not None
    state, current = lock_aggregate(conn, ev.aggregate_id, now)

    prior = idempotency.lookup(conn, ev.idempotency_key, now)
    if prior is not None:
        _mark(conn, ev.id, "duplicate", now)
        return Outcome("duplicate", ev.id, json.dumps(prior))

    action = decide(current, ev.version)
    if force_apply and ev.version > current:
        action = Action.APPLY

    if action is Action.STALE:
        _mark(conn, ev.id, "stale", now)
        return Outcome("stale", ev.id)

    if action is Action.BUFFER:
        conn.execute(
            "INSERT INTO buffer (event_id, aggregate_id, awaiting_version, window_expires) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING",
            (
                ev.id,
                ev.aggregate_id,
                current + 1,
                window_deadline(now, ctx.settings.buffer_window_seconds),
            ),
        )
        conn.execute("UPDATE events SET status = 'buffered' WHERE id = %s", (ev.id,))
        return Outcome("buffered", ev.id, f"awaiting v{current + 1}")

    new_state = domain.apply_event(state, ev.payload["data"])  # TerminalError on poison
    ctx.sink(conn, ev)  # RetryableError on downstream outage; rolls back with this step
    conn.execute(
        "UPDATE aggregates SET state = %s::jsonb, version = %s, updated_at = %s WHERE id = %s",
        (json.dumps(new_state), ev.version, now, ev.aggregate_id),
    )
    conn.execute(
        "INSERT INTO effect_log (aggregate_id, version, event_id, applied_at) VALUES (%s, %s, %s, %s)",
        (ev.aggregate_id, ev.version, ev.id, now),
    )
    idempotency.record(
        conn,
        ev.idempotency_key,
        {"result": "applied", "aggregate_id": ev.aggregate_id, "version": ev.version},
        ev.id,
        now,
        ctx.settings.idempotency_ttl_seconds,
    )
    _mark(conn, ev.id, "processed", now)
    return Outcome("applied", ev.id, f"v{ev.version}")


# --------------------------------------------------------------------------- failure handling


def _fail(
    ctx: AppContext, conn: Conn, ev: EventRow, err: ReconcilerError, now: datetime
) -> Outcome:
    """Record a failed attempt: schedule a retry, or dead-letter on terminal error / exhausted budget.

    This is the single place failures are logged (once, with full context).
    """
    attempts = ev.attempts + 1
    text = f"{type(err).__name__}: {err}"
    if isinstance(err, TerminalError) or ctx.retry.exhausted(attempts):
        conn.execute(
            "UPDATE events SET status = 'dead', attempts = %s, last_error = %s, processed_at = %s WHERE id = %s",
            (attempts, text, now, ev.id),
        )
        dlq.send(conn, ev.id, attempts, text, now)
        log.error(
            "event dead-lettered",
            extra={
                "event_id": str(ev.id),
                "aggregate_id": ev.aggregate_id,
                "version": ev.version,
                "attempts": attempts,
                "error": text,
            },
        )
        return Outcome("dead", ev.id, text)
    delay = ctx.retry.delay(attempts)
    conn.execute(
        "UPDATE events SET status = 'received', attempts = %s, last_error = %s, next_attempt_at = %s "
        "WHERE id = %s",
        (attempts, text, now + delay, ev.id),
    )
    log.warning(
        "event will be retried",
        extra={
            "event_id": str(ev.id),
            "aggregate_id": ev.aggregate_id,
            "attempts": attempts,
            "retry_in_s": delay.total_seconds(),
            "error": text,
        },
    )
    return Outcome("retry", ev.id, text)


def _isolated(
    ctx: AppContext, conn: Conn, ev: EventRow, now: datetime, *, force_apply: bool = False
) -> Outcome:
    """Run `_handle` in a savepoint; on failure roll back only this event's changes and record the failure."""
    try:
        with conn.transaction():
            return _handle(ctx, conn, ev, now, force_apply=force_apply)
    except ReconcilerError as err:
        return _fail(ctx, conn, ev, err, now)
    except (
        Exception
    ) as exc:  # unexpected faults still consume the retry budget instead of hot-looping
        return _fail(ctx, conn, ev, RetryableError(f"unexpected {type(exc).__name__}: {exc}"), now)


# --------------------------------------------------------------------------- buffer drain


def drain_aggregate(ctx: AppContext, conn: Conn, aggregate_id: str, now: datetime) -> int:
    """Process buffered/parked events that are now applicable, in version order. Returns how many ran.

    Stops at the first failure: the aggregate cannot advance past a failed event.
    """
    ran = 0
    while True:
        _, current = lock_aggregate(conn, aggregate_id, now)
        row = conn.execute(
            "SELECT e.* FROM events e JOIN buffer b ON b.event_id = e.id "
            "WHERE b.aggregate_id = %s AND e.version <= %s ORDER BY e.version, e.seq LIMIT 1 "
            "FOR UPDATE OF e",
            (aggregate_id, current + 1),
        ).fetchone()
        if row is None:
            return ran
        ev = EventRow.model_validate(row)
        conn.execute("DELETE FROM buffer WHERE event_id = %s", (ev.id,))
        outcome = _isolated(ctx, conn, ev, now)
        ran += 1
        if outcome.kind in ("retry", "dead"):
            return ran


# --------------------------------------------------------------------------- public entry points


def process_next(ctx: AppContext) -> Outcome | None:
    """Claim and process one due event. Returns None when nothing is due."""
    now = ctx.clock.now()
    with ctx.pool.connection() as conn, conn.transaction():
        row = conn.execute(
            "SELECT * FROM events WHERE status = 'received' AND next_attempt_at <= %s "
            "ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED",
            (now,),
        ).fetchone()
        if row is None:
            return None
        ev = EventRow.model_validate(row)
        assert ev.aggregate_id is not None
        attrs: dict[str, str | int] = {
            "event.id": str(ev.id),
            "aggregate.id": ev.aggregate_id,
            "attempt": ev.attempts + 1,
        }
        with span_in_trace("process_event", ev.traceparent, **attrs) as span:
            outcome = _isolated(ctx, conn, ev, now)
            span.set_attribute("outcome", outcome.kind)
            if outcome.kind == "applied":
                drain_aggregate(ctx, conn, ev.aggregate_id, now)
            return outcome


@dataclass(frozen=True)
class SweepResult:
    parked: int
    force_applied: int
    purged: int
    rejected_purged: int
    retention_purged: int


def sweep(ctx: AppContext) -> SweepResult:
    """Housekeeping: resolve expired gap windows per policy (ADR-0002) and purge expired idempotency rows."""
    now = ctx.clock.now()
    parked = forced = 0
    with ctx.pool.connection() as conn, conn.transaction():
        expired = conn.execute(
            "SELECT b.event_id, b.aggregate_id FROM buffer b JOIN events e ON e.id = b.event_id "
            "WHERE b.window_expires <= %s AND e.status = 'buffered' ORDER BY b.aggregate_id, e.version "
            "LIMIT 1000",
            (now,),
        ).fetchall()
        if ctx.settings.gap_policy is GapPolicy.PARK_AND_ALERT:
            ids = [r["event_id"] for r in expired]
            if ids:
                conn.execute("UPDATE events SET status = 'parked' WHERE id = ANY(%s)", (ids,))
            parked = len(ids)
            if parked:
                log.warning("gap window expired; events parked", extra={"count": parked})
        else:
            for agg_id in sorted({r["aggregate_id"] for r in expired}):
                _, current = lock_aggregate(conn, agg_id, now)
                row = conn.execute(
                    "SELECT e.* FROM events e JOIN buffer b ON b.event_id = e.id "
                    "WHERE b.aggregate_id = %s AND b.window_expires <= %s AND e.version > %s "
                    "ORDER BY e.version, e.seq LIMIT 1 FOR UPDATE OF e",
                    (agg_id, now, current),
                ).fetchone()
                if row is not None:
                    ev = EventRow.model_validate(row)
                    conn.execute("DELETE FROM buffer WHERE event_id = %s", (ev.id,))
                    if _isolated(ctx, conn, ev, now, force_apply=True).kind == "applied":
                        forced += 1
                drain_aggregate(ctx, conn, agg_id, now)
        purged = idempotency.purge_expired(conn, now)
        rejected_purged = conn.execute(
            "DELETE FROM events WHERE status = 'rejected' AND signature_status <> 'valid' AND received_at < %s",
            (now - timedelta(seconds=ctx.settings.rejected_retention_seconds),),
        ).rowcount
        history = retention.purge_history(conn, now, ctx.settings.retention_seconds)
    return SweepResult(
        parked=parked,
        force_applied=forced,
        purged=purged,
        rejected_purged=rejected_purged,
        retention_purged=history.total,
    )
