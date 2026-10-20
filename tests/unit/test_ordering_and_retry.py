from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from reconciler.patterns.ordering import Action, decide, window_deadline, window_expired
from reconciler.patterns.retry import RetryPolicy

T0 = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("current", "event", "expected"),
    [
        (0, 1, Action.APPLY),
        (1, 2, Action.APPLY),
        (3, 4, Action.APPLY),  # odd current: guards against `current | 1`-style slips
        (4, 5, Action.APPLY),
        (3, 3, Action.STALE),
        (4, 4, Action.STALE),
        (4, 1, Action.STALE),
        (0, 2, Action.BUFFER),
        (4, 6, Action.BUFFER),
        (4, 1000, Action.BUFFER),
    ],
)
def test_decide_table(current: int, event: int, expected: Action) -> None:
    assert decide(current, event) is expected


@given(current=st.integers(0, 10_000), delta=st.integers(-4, 4))
def test_decide_partitions_every_case_exactly_once(current: int, delta: int) -> None:
    event = max(1, current + delta)  # centred on the boundary so every branch is exercised
    action = decide(current, event)
    assert (action is Action.APPLY) == (event == current + 1)
    assert (action is Action.STALE) == (event <= current)
    assert (action is Action.BUFFER) == (event > current + 1)


def test_window_boundary_is_inclusive() -> None:
    deadline = window_deadline(T0, 60)
    assert deadline == T0 + timedelta(seconds=60)
    assert not window_expired(deadline, T0 + timedelta(seconds=59.999))
    assert window_expired(deadline, T0 + timedelta(seconds=60))
    assert window_expired(deadline, T0 + timedelta(seconds=61))


def policy(base: float = 1, cap: float = 60, attempts: int = 5, seed: int = 0) -> RetryPolicy:
    return RetryPolicy(base, cap, attempts, rng=random.Random(seed))


def test_ceiling_doubles_then_caps() -> None:
    p = policy(base=1, cap=60)
    assert [p.ceiling(n) for n in range(1, 9)] == [1, 2, 4, 8, 16, 32, 60, 60]


def test_ceiling_survives_absurd_attempt_counts() -> None:
    assert policy().ceiling(10_000) == 60


def test_attempt_is_one_based() -> None:
    with pytest.raises(ValueError, match="1-based"):
        policy().ceiling(0)


@given(attempt=st.integers(1, 200), seed=st.integers(0, 10_000))
def test_jittered_delay_is_within_zero_and_ceiling(attempt: int, seed: int) -> None:
    p = policy(seed=seed)
    d = p.delay(attempt)
    assert timedelta(0) <= d <= timedelta(seconds=p.ceiling(attempt))


def test_jitter_actually_spreads_delays() -> None:
    p = policy(base=10, cap=10)
    delays = {p.delay(1) for _ in range(50)}
    assert len(delays) > 40  # full jitter: not a constant


def test_same_seed_same_delays() -> None:
    a, b = policy(seed=42), policy(seed=42)
    assert [a.delay(n) for n in range(1, 6)] == [b.delay(n) for n in range(1, 6)]


def test_budget_boundaries() -> None:
    p = policy(attempts=3)
    assert [p.exhausted(n) for n in range(0, 5)] == [False, False, False, True, True]


@pytest.mark.parametrize(("base", "cap", "attempts"), [(0, 1, 1), (-1, 1, 1), (2, 1, 1), (1, 1, 0)])
def test_invalid_policy_is_rejected(base: float, cap: float, attempts: int) -> None:
    with pytest.raises(ValueError, match="invalid retry policy"):
        RetryPolicy(base, cap, attempts)


def test_policy_is_immutable_and_rng_is_not_part_of_identity() -> None:
    p = policy(seed=1)
    with pytest.raises(AttributeError):
        p.max_attempts = 99  # type: ignore[misc]
    assert policy(seed=1) == policy(seed=2)  # same configuration, different jitter stream
    assert "Random" not in repr(p)


def test_minimal_valid_policies_are_accepted() -> None:
    RetryPolicy(1, 1, 1)  # cap == base and a single attempt are legal


def test_exponent_guard_prevents_float_overflow_and_is_pinned() -> None:
    huge = RetryPolicy(1, 1e30, 100)
    assert huge.ceiling(63) == 2**62
    assert huge.ceiling(5_000) == 2**62


def test_jitter_reaches_below_one_second() -> None:
    p = policy(base=10, cap=10)
    assert min(p.delay(1) for _ in range(300)) < timedelta(seconds=1)  # lower bound is 0, not 1
