"""Correctness must not depend on worker count (ADR-0003)."""

from __future__ import annotations

import random
from concurrent.futures import ThreadPoolExecutor

import pytest

from reconciler.context import AppContext
from reconciler.processor import process_next
from tests.helpers import aggregate, scalar, send


def _drain_with(ctx: AppContext, workers: int) -> None:
    def loop() -> None:
        while process_next(ctx) is not None:
            pass

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for f in [pool.submit(loop) for _ in range(workers)]:
            f.result()


@pytest.mark.parametrize("workers", [2, 4, 8])
def test_shuffled_stream_with_duplicates_converges_under_concurrency(
    ctx: AppContext, workers: int
) -> None:
    rng = random.Random(workers)
    deliveries = [(a, v) for a in range(6) for v in range(1, 9)]
    deliveries += rng.sample(deliveries, 20)  # duplicates
    rng.shuffle(deliveries)
    for a, v in deliveries:
        send(ctx, f"order-{a}", v, {"amount_cents": v * 10})
    _drain_with(ctx, workers)
    for a in range(6):
        assert aggregate(ctx, f"order-{a}") == (8, {"amount_cents": 80})
    assert scalar(ctx, "SELECT count(*) FROM effect_log") == 6 * 8  # exactly one effect per version
    assert (
        scalar(ctx, "SELECT count(*) - count(DISTINCT (aggregate_id, version)) FROM effect_log")
        == 0
    )
    assert scalar(ctx, "SELECT count(*) FROM events WHERE status IN ('received', 'buffered')") == 0


def test_hot_aggregate_serialises_across_workers(ctx: AppContext) -> None:
    versions = list(range(1, 41))
    random.Random(7).shuffle(versions)
    for v in versions:
        send(ctx, "hot", v, {"amount_cents": v})
    _drain_with(ctx, 8)
    assert aggregate(ctx, "hot") == (40, {"amount_cents": 40})
    assert scalar(ctx, "SELECT count(*) FROM effect_log WHERE aggregate_id = 'hot'") == 40
