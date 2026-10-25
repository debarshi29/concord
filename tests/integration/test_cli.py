from __future__ import annotations

import json

import pytest

from reconciler.cli import main
from reconciler.context import AppContext
from reconciler.settings import get_settings
from reconciler.worker import run_until_idle
from tests.helpers import aggregate, scalar, send


@pytest.fixture(autouse=True)
def cli_env(monkeypatch: pytest.MonkeyPatch, ctx: AppContext) -> None:
    monkeypatch.setenv("RECONCILER_DATABASE_URL", ctx.settings.database_url)
    monkeypatch.setenv("RECONCILER_WEBHOOK_SECRET", "test-secret")
    monkeypatch.delenv("RECONCILER_RECONCILE_SOURCE_URL", raising=False)
    get_settings.cache_clear()


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, object]:
    code = main(list(argv))
    out = capsys.readouterr().out
    return code, json.loads(out) if out.strip() else None


def test_migrate_reports_nothing_pending(capsys: pytest.CaptureFixture[str]) -> None:
    assert run(capsys, "migrate") == (0, {"applied": []})


def test_worker_until_idle_processes_the_queue(
    ctx: AppContext, capsys: pytest.CaptureFixture[str]
) -> None:
    send(ctx, version=1)
    send(ctx, version=2, data={"amount_cents": 3})
    code, out = run(capsys, "worker", "--until-idle")
    assert code == 0 and out == {"processed": 2, "by_kind": {"applied": 2}}
    assert aggregate(ctx) == (2, {"status": "paid", "amount_cents": 3})


def test_stats_and_alerts(ctx: AppContext, capsys: pytest.CaptureFixture[str]) -> None:
    code, out = run(capsys, "alerts")
    assert (code, out) == (0, [])
    send(ctx, data={"status": "banana"})
    run_until_idle(ctx)
    code, out = run(capsys, "alerts")
    assert code == 1 and out[0]["name"] == "dlq_depth"  # type: ignore[index]
    code, stats = run(capsys, "stats")
    assert code == 0 and stats["dlq_depth"] == 1  # type: ignore[index]


def test_dlq_list_redrive_and_discard(ctx: AppContext, capsys: pytest.CaptureFixture[str]) -> None:
    send(ctx, "order-A", 1, {"status": "banana"})
    send(ctx, "order-B", 1, {"status": "banana"})
    run_until_idle(ctx)
    code, listed = run(capsys, "dlq", "list")
    assert code == 0 and len(listed) == 2  # type: ignore[arg-type]
    victim = listed[0]["event_id"]  # type: ignore[index]
    assert run(capsys, "dlq", "discard", "--id", victim) == (0, {"discarded": True})
    assert run(capsys, "dlq", "redrive", "--all") == (0, {"redriven": 1})
    assert scalar(ctx, "SELECT count(*) FROM events WHERE status = 'received'") == 1


def test_redrive_requires_a_target(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["dlq", "redrive"]) == 2
    assert "specify --id" in capsys.readouterr().err


def test_reconcile_needs_a_source(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["reconcile"]) == 2
    assert "SOURCE_URL" in capsys.readouterr().err


def test_openapi_prints_the_contract(capsys: pytest.CaptureFixture[str]) -> None:
    code, spec = run(capsys, "openapi")
    assert code == 0 and "/webhooks" in spec["paths"]  # type: ignore[index]


def test_sweep_parks_expired_gaps_and_purges(
    ctx: AppContext, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RECONCILER_BUFFER_WINDOW_SECONDS", "0")
    get_settings.cache_clear()
    send(ctx, version=3)  # gap: v1, v2 missing
    run_until_idle(ctx)
    code, out = run(capsys, "sweep")
    assert (code, out) == (
        0,
        {"parked": 1, "force_applied": 0, "purged": 0, "rejected_purged": 0},
    )
    assert scalar(ctx, "SELECT status FROM events") == "parked"
