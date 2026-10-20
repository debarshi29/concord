"""Executes a chaos plan against the real system and checks the thesis:

    final replica state == state of a perfect in-order, exactly-once delivery

Phases: hostile delivery (interleaved with worker runs and clock ticks) -> downstream recovers and time
passes -> worker drains -> *audit* (independent drift measurement) -> reconciler pass -> worker drains -> verdict.
"""

from __future__ import annotations

import json
import random
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from reconciler import signing
from reconciler.chaos.model import (
    Deliver,
    Mutation,
    Plan,
    SourceEvent,
    Tick,
    Work,
    expected_state,
)
from reconciler.clock import FakeClock
from reconciler.context import AppContext
from reconciler.db import Conn, truncate_all
from reconciler.errors import IngestRejected, RetryableError
from reconciler.ingest import ingest
from reconciler.models import EventRow
from reconciler.processor import process_next, sweep
from reconciler.reconcile import SourceRecord, reconcile

WRONG_SECRET = "not-the-secret"  # noqa: S105


def reset_database(ctx: AppContext) -> None:
    truncate_all(ctx.pool)


class StaticSource:
    """The sender's source of truth: the final state of the ideal history."""

    def __init__(self, events: tuple[SourceEvent, ...]) -> None:
        self.records = [
            SourceRecord(agg, version, state)
            for agg, (version, state) in expected_state(events).items()
        ]

    def list_aggregates(self) -> Iterable[SourceRecord]:
        return list(self.records)


class FlakySink:
    """Downstream that fails transiently with probability `rate` (set to 0 when 'the outage ends')."""

    def __init__(self, rng: random.Random, rate: float) -> None:
        self.rng = rng
        self.rate = rate

    def __call__(self, conn: Conn, event: EventRow) -> None:
        if self.rate > 0 and self.rng.random() < self.rate:
            raise RetryableError("injected transient downstream failure")


def envelope(ev: SourceEvent, data: dict[str, Any] | None = None) -> bytes:
    return json.dumps(
        {
            "event_id": ev.event_id,
            "type": "order.updated",
            "aggregate_id": ev.aggregate_id,
            "version": ev.version,
            "data": ev.data if data is None else data,
        }
    ).encode()


def deliver(ctx: AppContext, ev: SourceEvent, mutation: Mutation, secret: str) -> None:
    body = envelope(ev)
    sig = signing.sign(body, secret)
    if mutation is Mutation.BAD_SIGNATURE:
        sig = signing.sign(body, WRONG_SECRET)
    elif mutation is Mutation.TRUNCATED_IN_TRANSIT:
        body = body[: len(body) // 2]
    elif mutation is Mutation.TRUNCATED_SIGNED:
        body = body[: len(body) // 2]
        sig = signing.sign(body, secret)
    elif mutation is Mutation.POISON:
        body = envelope(ev, {"status": "banana"})
        sig = signing.sign(body, secret)
    with suppress(
        IngestRejected
    ):  # rejected events are recorded by ingest; the sender just sees a 4xx
        ingest(ctx, body, sig)


@dataclass
class RunResult:
    converged: bool
    duplicate_effects: int
    drift_before_reconcile: int
    corrections: int
    unresolved_events: int
    mismatches: list[str] = field(default_factory=list)
    dlq_depth: int = 0
    rejected: int = 0


def _drain(ctx: AppContext, workers: int, limit: int = 1_000_000) -> None:
    def loop() -> None:
        n = 0
        while n < limit and process_next(ctx) is not None:
            n += 1

    if workers <= 1:
        loop()
        return
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for fut in [pool.submit(loop) for _ in range(workers)]:
            fut.result()


def _snapshot(ctx: AppContext) -> dict[str, tuple[int, dict[str, Any]]]:
    with ctx.pool.connection() as conn:
        return {
            r["id"]: (int(r["version"]), dict(r["state"]))
            for r in conn.execute("SELECT id, version, state FROM aggregates WHERE version > 0")
        }


def _count(ctx: AppContext, sql: str) -> int:
    with ctx.pool.connection() as conn:
        row = conn.execute(sql).fetchone()
        assert row is not None
        return int(row["n"])


def run_plan(
    ctx: AppContext, plan: Plan, workers: int = 1, secret: str = "test-secret"
) -> RunResult:
    """Run `plan` on an *empty* database (call `reset_database` first). `ctx.clock` must be a FakeClock."""
    assert isinstance(ctx.clock, FakeClock)
    clock = ctx.clock
    rng = random.Random(plan.seed)
    sink = FlakySink(rng, plan.sink_failure_rate)
    ctx.sink = sink

    for step in plan.steps:
        if isinstance(step, Deliver):
            deliver(ctx, plan.events[step.event_index], step.mutation, secret)
        elif isinstance(step, Tick):
            clock.advance(step.seconds)
        elif isinstance(step, Work):
            _drain(ctx, workers, limit=max(1, step.count // workers))

    # the downstream outage ends; enough time passes for every backoff and buffer window to expire
    sink.rate = 0.0
    horizon = max(ctx.settings.buffer_window_seconds, ctx.settings.retry_cap_seconds) + 1
    clock.advance(horizon)
    _drain(ctx, workers)
    sweep(ctx)
    _drain(ctx, workers)

    expected = expected_state(plan.events)
    drift_before = _drift(expected, _snapshot(ctx))

    report = reconcile(ctx, StaticSource(plan.events))
    _drain(ctx, workers)
    sweep(ctx)

    final = _snapshot(ctx)
    mismatches = [
        f"{agg}: expected {exp} got {final.get(agg)}"
        for agg, exp in expected.items()
        if final.get(agg) != exp
    ]
    return RunResult(
        converged=not mismatches,
        duplicate_effects=_count(
            ctx, "SELECT count(*) - count(DISTINCT (aggregate_id, version)) AS n FROM effect_log"
        ),
        drift_before_reconcile=drift_before,
        corrections=report.corrected,
        unresolved_events=_count(
            ctx,
            "SELECT count(*) AS n FROM events WHERE status IN ('received', 'buffered', 'parked')",
        ),
        mismatches=mismatches,
        dlq_depth=_count(ctx, "SELECT count(*) AS n FROM dlq WHERE resolved_at IS NULL"),
        rejected=_count(ctx, "SELECT count(*) AS n FROM events WHERE status = 'rejected'"),
    )


def _drift(
    expected: dict[str, tuple[int, dict[str, Any]]], actual: dict[str, tuple[int, dict[str, Any]]]
) -> int:
    return sum(1 for agg, exp in expected.items() if actual.get(agg) != exp)
