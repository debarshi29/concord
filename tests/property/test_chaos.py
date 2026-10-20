"""The thesis test.

Whatever the network does — duplicate, reorder, delay, drop, corrupt — and whatever the downstream
does transiently, once the system has been given time and one reconciler pass, the replica equals the
state of a perfect in-order exactly-once delivery, with zero duplicate effects.
"""

from __future__ import annotations

import random
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from reconciler.chaos.generator import STATUSES, random_data
from reconciler.chaos.model import Deliver, Mutation, Plan, SourceEvent, Step, Tick, Work
from reconciler.chaos.runner import reset_database, run_plan
from reconciler.context import AppContext
from reconciler.settings import GapPolicy

data_st = st.fixed_dictionaries(
    {},
    optional={
        "status": st.sampled_from(STATUSES),
        "amount_cents": st.integers(0, 10_000),
        "currency": st.sampled_from(["USD", "EUR"]),
        "customer_id": st.sampled_from(["a", "b", "c"]),
    },
).filter(bool)


@st.composite
def plans(draw: st.DrawFn) -> Plan:
    n_aggs = draw(st.integers(1, 3))
    events: list[SourceEvent] = []
    for a in range(n_aggs):
        for v in range(1, draw(st.integers(1, 5)) + 1):
            events.append(SourceEvent(f"order-{a}", v, draw(data_st)))
    idx = st.integers(0, len(events) - 1)
    step_st = st.one_of(
        st.builds(Deliver, idx, st.sampled_from(list(Mutation))),
        st.builds(Deliver, idx),  # bias towards clean deliveries
        st.builds(Tick, st.sampled_from([0.5, 5, 30, 90, 600])),
        st.builds(Work, st.integers(1, 6)),
    )
    steps: list[Step] = draw(st.lists(step_st, max_size=40))
    # a duplicate of a clean delivery is just another Deliver of the same index: Hypothesis finds them
    return Plan(
        events=tuple(events),
        steps=tuple(steps),
        sink_failure_rate=draw(st.sampled_from([0.0, 0.0, 0.3])),
        seed=draw(st.integers(0, 2**31)),
    )


def _check(ctx: AppContext, plan: Plan, workers: int = 1) -> None:
    reset_database(ctx)
    ctx.clock._now = ctx.clock.now().replace(year=2026, month=1, day=1)  # type: ignore[attr-defined]
    result = run_plan(ctx, plan, workers=workers)
    assert result.converged, result.mismatches
    assert result.duplicate_effects == 0
    assert result.unresolved_events == 0


@pytest.mark.parametrize("policy", [GapPolicy.PARK_AND_ALERT, GapPolicy.LAST_WRITER_WINS])
@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(plan=plans())
def test_chaos_converges_to_perfect_delivery(
    ctx: AppContext, policy: GapPolicy, plan: Plan
) -> None:
    ctx.settings.gap_policy = policy
    _check(ctx, plan)


@settings(
    max_examples=15, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(plan=plans())
def test_chaos_converges_with_concurrent_workers(ctx: AppContext, plan: Plan) -> None:
    _check(ctx, plan, workers=4)


def test_seeded_generator_plans_converge(ctx: AppContext) -> None:
    from reconciler.chaos.generator import random_plan

    for seed in range(15):
        _check(ctx, random_plan(random.Random(seed)))


def test_generated_data_is_domain_valid() -> None:
    from reconciler.domain import apply_event

    rng = random.Random(0)
    state: dict[str, Any] = {}
    for _ in range(200):
        state = apply_event(state, random_data(rng))
