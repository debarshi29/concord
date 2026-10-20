"""Downstream sink: the side effect that must happen exactly once per event.

It runs inside the processing transaction, so a retryable failure rolls back the state change
with it. A *real* external call here is only at-least-once; closing that gap is the outbox
extension (README, "Extensions").
"""

from __future__ import annotations

import os
from typing import Protocol

from reconciler.db import Conn
from reconciler.errors import RetryableError
from reconciler.models import EventRow


class Sink(Protocol):
    def __call__(self, conn: Conn, event: EventRow) -> None: ...


def noop_sink(conn: Conn, event: EventRow) -> None:
    return None


class OutageFileSink:
    """Fault injection for the incident drill: fails while `path` exists."""

    def __init__(self, path: str) -> None:
        self.path = path

    def __call__(self, conn: Conn, event: EventRow) -> None:
        if self.path and os.path.exists(self.path):
            raise RetryableError("downstream outage (fault injection file present)")
