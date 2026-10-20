"""A thesis test is only worth something if it can fail. These tests sabotage the real system in the
specific ways the design guards against and assert the chaos harness notices every time."""

from __future__ import annotations

import random

import pytest

import reconciler.processor as processor
from reconciler.chaos.generator import ChaosRates, random_plan
from reconciler.chaos.model import Deliver, Mutation, Plan, SourceEvent
from reconciler.chaos.naive import run_plan_naive
from reconciler.chaos.runner import RunResult, reset_database, run_plan
from reconciler.context import AppContext
from reconciler.patterns import idempotency
from reconciler.patterns.ordering import Action

DUPLICATE_PLAN = Plan(
    events=(
        SourceEvent("order-0", 1, {"amount_cents": 1}),
        SourceEvent("order-0", 2, {"amount_cents": 2}),
    ),
    steps=(Deliver(0), Deliver(1), Deliver(1), Deliver(0)),
)


def _run(ctx: AppContext, plan: Plan) -> RunResult:
    reset_database(ctx)
    return run_plan(ctx, plan)


def test_baseline_the_unbroken_system_passes(ctx: AppContext) -> None:
    result = _run(ctx, DUPLICATE_PLAN)
    assert result.converged and result.duplicate_effects == 0


def test_harness_detects_missing_idempotency_and_version_check(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(processor, "decide", lambda current, version: Action.APPLY)
    monkeypatch.setattr(idempotency, "lookup", lambda conn, key, now: None)
    result = _run(ctx, DUPLICATE_PLAN)
    assert result.duplicate_effects > 0  # replays re-applied


def test_harness_detects_applying_out_of_order(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(processor, "decide", lambda current, version: Action.APPLY)
    plan = Plan(
        events=(
            SourceEvent("order-0", 1, {"status": "pending"}),
            SourceEvent("order-0", 2, {"status": "paid"}),
        ),
        steps=(Deliver(1), Deliver(0)),  # v2 arrives first, then v1 overwrites it
    )
    reset_database(ctx)
    result = run_plan(ctx, plan)
    # the reconciler later repairs the state, but the audit before it shows the ordering bug
    assert result.drift_before_reconcile >= 1


def test_harness_detects_a_lost_write_ahead(ctx: AppContext) -> None:
    """If a delivered event vanished (as when ack precedes persistence), only the reconciler saves us —
    and the drift is visible in the audit metric."""
    plan = Plan(
        events=(SourceEvent("order-0", 1, {"status": "paid"}),), steps=()
    )  # nothing delivered
    result = _run(ctx, plan)
    assert result.drift_before_reconcile == 1 and result.corrections == 1 and result.converged


def test_naive_handler_fails_the_same_chaos(ctx: AppContext) -> None:
    """The 'before' number: a naive handler diverges or double-applies on a large share of plans."""
    failures = 0
    runs = 40
    for seed in range(runs):
        reset_database(ctx)
        plan = random_plan(random.Random(seed), ChaosRates())
        result = run_plan_naive(ctx, plan)
        if not result.converged or result.duplicate_effects > 0:
            failures += 1
    assert failures >= runs // 2, (
        f"naive handler only failed {failures}/{runs}; chaos is too gentle"
    )


def test_clean_in_order_stream_is_fine_for_everyone(ctx: AppContext) -> None:
    plan = Plan(
        events=(
            SourceEvent("order-0", 1, {"status": "pending"}),
            SourceEvent("order-0", 2, {"status": "paid"}),
        ),
        steps=(Deliver(0, Mutation.CLEAN), Deliver(1, Mutation.CLEAN)),
    )
    reset_database(ctx)
    assert run_plan_naive(ctx, plan).converged
