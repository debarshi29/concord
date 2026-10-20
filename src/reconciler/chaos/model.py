"""Data model for chaos plans: an ideal event stream plus a hostile delivery schedule.

A `Plan` is plain data so that Hypothesis can generate and *shrink* it, and so that a failing
plan can be printed, committed as a regression test, and replayed exactly.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Mutation(StrEnum):
    CLEAN = "clean"
    BAD_SIGNATURE = "bad_signature"  # signature does not match the body
    TRUNCATED_IN_TRANSIT = "truncated_in_transit"  # body cut after signing -> signature mismatch
    TRUNCATED_SIGNED = "truncated_signed"  # sender bug: validly signed, malformed JSON
    POISON = "poison"  # validly signed, schema-valid, semantically invalid -> DLQ


@dataclass(frozen=True)
class SourceEvent:
    """One change in the sender's perfect, in-order history."""

    aggregate_id: str
    version: int
    data: dict[str, Any]

    @property
    def event_id(self) -> str:
        return f"{self.aggregate_id}-v{self.version}"


@dataclass(frozen=True)
class Deliver:
    event_index: int
    mutation: Mutation = Mutation.CLEAN


@dataclass(frozen=True)
class Tick:
    """Advance the clock: models network delay, backoff expiry and buffer-window expiry."""

    seconds: float


@dataclass(frozen=True)
class Work:
    """Let the worker(s) process up to `count` due events right now."""

    count: int


Step = Deliver | Tick | Work


@dataclass(frozen=True)
class Plan:
    events: tuple[SourceEvent, ...]
    steps: tuple[Step, ...]
    sink_failure_rate: float = 0.0
    seed: int = 0


def expected_state(events: tuple[SourceEvent, ...]) -> dict[str, tuple[int, dict[str, Any]]]:
    """What a perfect in-order, exactly-once delivery produces: aggregate_id -> (version, state)."""
    out: dict[str, tuple[int, dict[str, Any]]] = {}
    for ev in sorted(events, key=lambda e: (e.aggregate_id, e.version)):
        _, state = out.get(ev.aggregate_id, (0, {}))
        out[ev.aggregate_id] = (ev.version, {**state, **ev.data})
    return out
