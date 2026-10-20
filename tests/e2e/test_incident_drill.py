"""Deliberate incident drills (engineering standard: break it on purpose, then follow the runbook).

Each drill is executed *only* through the operator CLI, exactly as `docs/runbook.md` instructs, so the
runbook is a tested artifact: an engineer who has never seen the codebase can follow it at 3 a.m.
The measured resolution times are printed (and recorded in README by `bench/`).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from reconciler.context import AppContext, build_context
from reconciler.processor import process_next
from tests.helpers import aggregate, scalar, send

pytestmark = pytest.mark.e2e


class Operator:
    """What an on-call engineer has: a shell with the service's environment."""

    def __init__(self, ctx: AppContext, tmp: Path, extra: dict[str, str] | None = None) -> None:
        self.env = {
            **os.environ,
            "RECONCILER_DATABASE_URL": ctx.settings.database_url,
            "RECONCILER_WEBHOOK_SECRET": "test-secret",
            "RECONCILER_RETRY_BASE_SECONDS": "0.01",
            "RECONCILER_RETRY_CAP_SECONDS": "0.05",
            "RECONCILER_RETRY_MAX_ATTEMPTS": "3",
            "RECONCILER_FAULT_DOWNSTREAM_OUTAGE_FILE": str(tmp / "outage"),
            "RECONCILER_LOG_LEVEL": "CRITICAL",
            **(extra or {}),
        }
        self.outage = tmp / "outage"

    def run(self, *args: str) -> tuple[int, Any]:
        proc = subprocess.run(  # noqa: S603 - fixed argv, test-controlled
            [sys.executable, "-m", "reconciler.cli", *args],
            env=self.env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        out = proc.stdout.strip()
        return proc.returncode, json.loads(out) if out else None


@pytest.fixture
def live_ctx(ctx: AppContext) -> AppContext:
    """Same database, real clock: the CLI under test uses the system clock too."""
    return build_context(ctx.pool, ctx.settings)


def test_drill_downstream_outage_to_dlq_to_redrive(live_ctx: AppContext, tmp_path: Path) -> None:
    op = Operator(live_ctx, tmp_path)
    op.outage.write_text("outage")  # --- incident: the downstream starts failing ---
    for a in "ABC":
        send(live_ctx, f"order-{a}", 1, {"status": "pending"})
        send(live_ctx, f"order-{a}", 2, {"status": "paid"})

    incident_started = time.monotonic()
    # the worker keeps trying (backoff + jitter, bounded budget) until the budget is exhausted
    for _ in range(60):
        op.run("worker", "--until-idle")
        if op.run("stats")[1]["dlq_depth"] == 3:  # alert fires at the first; wait for all three
            break
        time.sleep(0.1)

    # --- runbook §3 step 1: confirm the alert and its scope ---
    code, alerts = op.run("alerts")
    assert code == 1 and [a["name"] for a in alerts] == ["dlq_depth"]
    _, stats = op.run("stats")
    assert stats["dlq_depth"] == 3  # v1 of each aggregate; v2s are buffered behind them
    assert stats["events_by_status"]["dead"] == 3
    # healthy aggregates are unaffected by design; here every aggregate is hit, so nothing applied
    assert aggregate(live_ctx, "order-A") == (0, {})

    # --- runbook §3 step 2: inspect entries (full context: attempts, last error, aggregate) ---
    _, entries = op.run("dlq", "list")
    assert len(entries) == 3 and all("downstream outage" in e["last_error"] for e in entries)
    assert all(e["attempts"] == 3 for e in entries)

    # --- runbook §3 step 3: fix the downstream, then re-drive ---
    op.outage.unlink()
    assert op.run("dlq", "redrive", "--all") == (0, {"redriven": 3})
    op.run("worker", "--until-idle")

    # --- runbook §3 step 4: verify ---
    resolution_seconds = time.monotonic() - incident_started
    assert op.run("alerts") == (0, [])
    _, stats = op.run("stats")
    assert (
        stats["dlq_depth"] == 0 and stats["buffer_depth"] == 0 and stats["duplicate_effects"] == 0
    )
    for a in "ABC":
        assert aggregate(live_ctx, f"order-{a}") == (2, {"status": "paid"})
    assert (
        scalar(live_ctx, "SELECT count(*) FROM effect_log") == 6
    )  # exactly once, despite the incident
    print(
        f"\nDRILL downstream-outage: resolved with the runbook alone in {resolution_seconds:.1f}s"
    )
    assert resolution_seconds < 120


@contextmanager
def source_of_truth(records: list[dict[str, Any]]) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            body = json.dumps({"items": records, "next_cursor": None}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/orders"
    finally:
        server.shutdown()


def test_drill_poison_message_to_discard_and_reconcile(
    live_ctx: AppContext, tmp_path: Path
) -> None:
    op = Operator(live_ctx, tmp_path)
    send(live_ctx, "order-P", 1, {"status": "banana"})  # poison: invalid business data
    send(live_ctx, "order-P", 2, {"status": "shipped"})
    send(live_ctx, "order-Q", 1, {"status": "paid"})

    incident_started = time.monotonic()
    op.run("worker", "--until-idle")

    # blast radius: only order-P is stuck; order-Q flowed through
    assert aggregate(live_ctx, "order-Q") == (1, {"status": "paid"})
    assert aggregate(live_ctx, "order-P") == (0, {})
    code, alerts = op.run("alerts")
    assert code == 1 and alerts[0]["name"] == "dlq_depth"

    # --- runbook §4: poison message. Inspect, discard, let the reconciler heal the aggregate ---
    _, entries = op.run("dlq", "list")
    assert len(entries) == 1 and "TerminalError" in entries[0]["last_error"]
    assert op.run("dlq", "discard", "--id", entries[0]["event_id"]) == (0, {"discarded": True})

    truth = [
        {"aggregate_id": "order-P", "version": 2, "state": {"status": "shipped"}},
        {"aggregate_id": "order-Q", "version": 1, "state": {"status": "paid"}},
    ]
    with source_of_truth(truth) as url:
        op.env["RECONCILER_RECONCILE_SOURCE_URL"] = url
        code, report = op.run("reconcile")
    assert code == 0 and report["checked"] == 2 and report["corrected"] == 1

    resolution_seconds = time.monotonic() - incident_started
    assert op.run("alerts")[0] == 0
    assert aggregate(live_ctx, "order-P") == (2, {"status": "shipped"})
    assert (
        scalar(live_ctx, "SELECT count(*) FROM events WHERE status IN ('buffered', 'parked')") == 0
    )
    print(f"\nDRILL poison-message: resolved with the runbook alone in {resolution_seconds:.1f}s")
    assert resolution_seconds < 120


def test_drill_crash_mid_transaction_loses_nothing(live_ctx: AppContext, tmp_path: Path) -> None:
    """Kill the worker while it holds the event: the transaction rolls back and another worker finishes it."""
    send(live_ctx, "order-K", 1, {"status": "paid"})
    crashing = build_context(live_ctx.pool, live_ctx.settings)

    def boom(conn: object, event: object) -> None:
        raise KeyboardInterrupt  # simulates a hard stop inside the transaction (not an Exception)

    crashing.sink = boom
    with pytest.raises(KeyboardInterrupt):
        process_next(crashing)
    assert aggregate(live_ctx, "order-K") is None  # nothing half-applied
    assert scalar(live_ctx, "SELECT status FROM events") == "received"  # still claimable
    op = Operator(live_ctx, tmp_path)
    op.run("worker", "--until-idle")
    assert aggregate(live_ctx, "order-K") == (1, {"status": "paid"})
