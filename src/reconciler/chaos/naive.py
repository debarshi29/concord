"""The 'before' system: a naive webhook handler, run against the same chaos plans.

It verifies the signature (that part is table stakes) and then applies every accepted event in arrival
order: no idempotency key, no version check, no buffering, no retry, no reconciliation. A failed apply is
simply lost, as with a handler that returns 500 to a sender that does not retry.
"""

from __future__ import annotations

import json
import random
from typing import Any

from reconciler.chaos.model import Deliver, Plan, expected_state
from reconciler.chaos.runner import RunResult, _count, _drift, _snapshot, deliver
from reconciler.context import AppContext
from reconciler.domain import apply_event
from reconciler.errors import TerminalError


def run_plan_naive(ctx: AppContext, plan: Plan, secret: str = "test-secret") -> RunResult:
    rng = random.Random(plan.seed)
    for step in plan.steps:
        if isinstance(step, Deliver):
            deliver(ctx, plan.events[step.event_index], step.mutation, secret)

    now = ctx.clock.now()
    with ctx.pool.connection() as conn, conn.transaction():
        rows = conn.execute(
            "SELECT aggregate_id, version, payload FROM events WHERE status = 'received' ORDER BY seq"
        ).fetchall()
        for row in rows:
            if rng.random() < plan.sink_failure_rate:
                continue  # downstream hiccup: the event is lost
            cur = conn.execute(
                "SELECT state FROM aggregates WHERE id = %s", (row["aggregate_id"],)
            ).fetchone()
            state: dict[str, Any] = dict(cur["state"]) if cur else {}
            try:
                new_state = apply_event(state, row["payload"]["data"])
            except TerminalError:
                continue  # poison: the handler crashes, the event is lost
            conn.execute(
                "INSERT INTO aggregates (id, state, version, updated_at) VALUES (%s, %s::jsonb, %s, %s) "
                "ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, version = EXCLUDED.version",
                (row["aggregate_id"], json.dumps(new_state), row["version"], now),
            )
            conn.execute(
                "INSERT INTO effect_log (aggregate_id, version, event_id, applied_at) "
                "VALUES (%s, %s, gen_random_uuid(), %s)",
                (row["aggregate_id"], row["version"], now),
            )

    expected = expected_state(plan.events)
    final = _snapshot(ctx)
    mismatches = [
        f"{a}: expected {e} got {final.get(a)}" for a, e in expected.items() if final.get(a) != e
    ]
    return RunResult(
        converged=not mismatches,
        duplicate_effects=_count(
            ctx, "SELECT count(*) - count(DISTINCT (aggregate_id, version)) AS n FROM effect_log"
        ),
        drift_before_reconcile=_drift(expected, final),
        corrections=0,
        unresolved_events=0,
        mismatches=mismatches,
    )
