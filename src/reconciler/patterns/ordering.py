"""Per-aggregate ordering decision. Pure functions: no I/O, no framework imports (reused by P3/P7/P8)."""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum


class Action(StrEnum):
    APPLY = "apply"  # exactly the next version: apply now
    STALE = "stale"  # already covered by the aggregate's version: no effect
    BUFFER = "buffer"  # a gap precedes it: park until the gap fills (or the window expires)


def decide(current_version: int, event_version: int) -> Action:
    """Classify an event against an aggregate whose applied version is `current_version`.

    Versions are dense per aggregate (1, 2, 3, ...). This function is also the backstop that keeps
    replays harmless after idempotency records expire (ADR-0001).
    """
    if event_version <= current_version:
        return Action.STALE
    if event_version == current_version + 1:
        return Action.APPLY
    return Action.BUFFER


def window_deadline(now: datetime, window_seconds: float) -> datetime:
    return now + timedelta(seconds=window_seconds)


def window_expired(deadline: datetime, now: datetime) -> bool:
    return deadline <= now
