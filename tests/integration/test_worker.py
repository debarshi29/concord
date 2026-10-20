"""The production loop: processes, sweeps, reconciles, survives iteration failures, stops on request."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from reconciler.context import AppContext
from reconciler.processor import process_next
from reconciler.reconcile import SourceRecord
from reconciler.worker import run_forever, run_until_idle
from tests.helpers import aggregate, scalar, send
from tests.integration.test_retry_dlq import ListSource


def wait_for(predicate: Callable[[], bool], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not reached in time")


def start(
    ctx: AppContext, source: ListSource | None = None
) -> tuple[threading.Thread, threading.Event]:
    ctx.settings.worker_poll_seconds = 0.01
    ctx.settings.sweep_interval_seconds = 0.01
    ctx.settings.reconcile_interval_seconds = 0.01
    stop = threading.Event()
    thread = threading.Thread(target=run_forever, args=(ctx, stop, source), daemon=True)
    thread.start()
    return thread, stop


def test_run_forever_processes_reconciles_and_stops_cleanly(ctx: AppContext) -> None:
    send(ctx, version=1, data={"status": "pending"})
    thread, stop = start(ctx, ListSource(SourceRecord("order-1", 1, {"status": "pending"})))
    try:
        wait_for(lambda: aggregate(ctx) == (1, {"status": "pending"}))
        wait_for(lambda: scalar(ctx, "SELECT count(*) FROM reconcile_runs") >= 1)
    finally:
        stop.set()
        thread.join(timeout=10)
    assert not thread.is_alive()


def test_run_forever_survives_a_failing_iteration(
    ctx: AppContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = process_next
    calls = {"n": 0}

    def flaky(c: AppContext) -> object:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient infrastructure fault")
        return real(c)

    monkeypatch.setattr("reconciler.worker.process_next", flaky)
    send(ctx, version=1)
    thread, stop = start(ctx)
    try:
        wait_for(lambda: aggregate(ctx) is not None and aggregate(ctx)[0] == 1)  # type: ignore[index]
    finally:
        stop.set()
        thread.join(timeout=10)
    assert calls["n"] >= 2  # the loop logged the fault once and carried on


def test_run_until_idle_honours_the_event_cap(ctx: AppContext) -> None:
    for v in range(1, 6):
        send(ctx, version=v, data={"amount_cents": v})
    assert len(run_until_idle(ctx, max_events=3)) == 3
    assert len(run_until_idle(ctx)) == 2
