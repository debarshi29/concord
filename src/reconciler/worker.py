"""Worker loop: claim/process, periodic sweep + alert evaluation, periodic reconciliation."""

from __future__ import annotations

import threading

from reconciler.context import AppContext
from reconciler.logs import get_logger
from reconciler.processor import Outcome, process_next, sweep
from reconciler.reconcile import Source, reconcile
from reconciler.stats import check_alerts

log = get_logger("worker")


def run_until_idle(ctx: AppContext, max_events: int = 1_000_000) -> list[Outcome]:
    """Process due events until none are due. Deterministic driver for tests, benches and the CLI."""
    outcomes: list[Outcome] = []
    while len(outcomes) < max_events:
        outcome = process_next(ctx)
        if outcome is None:
            break
        outcomes.append(outcome)
    return outcomes


def run_forever(ctx: AppContext, stop: threading.Event, source: Source | None = None) -> None:
    next_sweep = 0.0
    next_reconcile = 0.0
    clock = threading.Event()  # used only as an interruptible sleep
    import time

    while not stop.is_set():
        try:
            if process_next(ctx) is None:
                clock.wait(ctx.settings.worker_poll_seconds)
            now = time.monotonic()
            if now >= next_sweep:
                next_sweep = now + ctx.settings.sweep_interval_seconds
                sweep(ctx)
                for alert in check_alerts(ctx):
                    log.error(
                        "ALERT %s: %s",
                        alert.name,
                        alert.message,
                        extra={"alert": alert.name, "value": alert.value},
                    )
            if source is not None and now >= next_reconcile:
                next_reconcile = now + ctx.settings.reconcile_interval_seconds
                report = reconcile(ctx, source)
                log.info(
                    "reconcile finished",
                    extra={"checked": report.checked, "corrected": report.corrected},
                )
        except Exception:
            log.exception("worker iteration failed")  # boundary: logged once, loop continues
            stop.wait(ctx.settings.worker_poll_seconds)
