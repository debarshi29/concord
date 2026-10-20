"""Seeded random plan generation (used by `make chaos`; Hypothesis builds plans directly from the model)."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any

from reconciler.chaos.model import Deliver, Mutation, Plan, SourceEvent, Step, Tick, Work

STATUSES = ("pending", "paid", "shipped", "cancelled")


def random_data(rng: random.Random) -> dict[str, Any]:
    """A non-empty, domain-valid patch."""
    makers: dict[str, Any] = {
        "status": lambda: rng.choice(STATUSES),
        "amount_cents": lambda: rng.randint(0, 100_000),
        "currency": lambda: rng.choice(("USD", "EUR", "GBP")),
        "customer_id": lambda: f"cust-{rng.randint(1, 50)}",
    }
    keys = rng.sample(sorted(makers), k=rng.randint(1, len(makers)))
    return {k: makers[k]() for k in keys}


def build_stream(
    rng: random.Random, n_aggregates: int, max_versions: int
) -> tuple[SourceEvent, ...]:
    events: list[SourceEvent] = []
    for a in range(n_aggregates):
        for v in range(1, rng.randint(1, max_versions) + 1):
            events.append(SourceEvent(f"order-{a}", v, random_data(rng)))
    return tuple(events)


@dataclass(frozen=True)
class ChaosRates:
    drop: float = 0.15
    duplicate: float = 0.3  # chance of each extra copy
    corrupt: float = 0.15
    reorder: float = 0.6  # 0 = in order, 1 = thoroughly shuffled
    tick: float = 0.25
    work: float = 0.4
    sink_failure: float = 0.1


def random_plan(
    rng: random.Random,
    rates: ChaosRates = ChaosRates(),  # noqa: B008 - frozen, safe as default
    n_aggregates: int = 4,
    max_versions: int = 6,
) -> Plan:
    events = build_stream(rng, n_aggregates, max_versions)
    deliveries: list[tuple[float, Deliver]] = []
    for idx in range(len(events)):
        if rng.random() < rates.drop:
            continue
        copies = 1
        while rng.random() < rates.duplicate and copies < 4:
            copies += 1
        for _ in range(copies):
            mutation = Mutation.CLEAN
            if rng.random() < rates.corrupt:
                mutation = rng.choice([m for m in Mutation if m is not Mutation.CLEAN])
            # reordering = jitter on the delivery position; delay = Tick steps interleaved below
            deliveries.append(
                (idx + rng.gauss(0, rates.reorder * len(events) / 2), Deliver(idx, mutation))
            )
    deliveries.sort(key=lambda t: t[0])

    steps: list[Step] = []
    for _, d in deliveries:
        steps.append(d)
        if rng.random() < rates.work:
            steps.append(Work(rng.randint(1, 8)))
        if rng.random() < rates.tick:
            steps.append(Tick(rng.choice((0.5, 5, 30, 120, 600))))
    return Plan(
        events=events,
        steps=tuple(steps),
        sink_failure_rate=rates.sink_failure,
        seed=rng.randint(0, 2**31),
    )
