"""Dependency bundle passed to the ingest/process/reconcile functions (no globals)."""

from __future__ import annotations

from dataclasses import dataclass

from reconciler.clock import Clock, SystemClock
from reconciler.db import Pool
from reconciler.patterns.retry import RetryPolicy
from reconciler.settings import Settings
from reconciler.sinks import OutageFileSink, Sink, noop_sink


@dataclass
class AppContext:
    pool: Pool
    settings: Settings
    clock: Clock
    sink: Sink
    retry: RetryPolicy


def build_context(
    pool: Pool,
    settings: Settings,
    clock: Clock | None = None,
    sink: Sink | None = None,
    retry: RetryPolicy | None = None,
) -> AppContext:
    if sink is None:
        path = settings.fault_downstream_outage_file
        sink = OutageFileSink(path) if path else noop_sink
    return AppContext(
        pool=pool,
        settings=settings,
        clock=clock or SystemClock(),
        sink=sink,
        retry=retry
        or RetryPolicy(
            base_seconds=settings.retry_base_seconds,
            cap_seconds=settings.retry_cap_seconds,
            max_attempts=settings.retry_max_attempts,
        ),
    )
