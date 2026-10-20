"""Processing the same webhook twice yields the same state (the property named in the engineering standard)."""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from reconciler.chaos.runner import reset_database
from reconciler.context import AppContext
from reconciler.worker import run_until_idle
from tests.helpers import aggregate, scalar, send

patch_st = st.fixed_dictionaries(
    {},
    optional={"status": st.sampled_from(["paid", "shipped"]), "amount_cents": st.integers(0, 50)},
).filter(bool)


@settings(
    max_examples=40, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(
    patches=st.lists(patch_st, min_size=1, max_size=6),
    replays=st.lists(st.integers(0, 5), max_size=12),
)
def test_replaying_any_delivered_event_any_number_of_times_changes_nothing(
    ctx: AppContext, patches: list[dict[str, object]], replays: list[int]
) -> None:
    reset_database(ctx)
    for v, patch in enumerate(patches, start=1):
        send(ctx, version=v, data=patch)
    run_until_idle(ctx)
    state, effects = aggregate(ctx), scalar(ctx, "SELECT count(*) FROM effect_log")

    for r in replays:
        v = r % len(patches) + 1
        send(ctx, version=v, data=patches[v - 1])
    run_until_idle(ctx)

    assert aggregate(ctx) == state
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == effects == len(patches)
