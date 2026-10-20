from __future__ import annotations

from reconciler.clock import FakeClock
from reconciler.context import AppContext
from reconciler.processor import sweep
from reconciler.reconcile import SourceRecord, reconcile
from reconciler.stats import check_alerts, collect_stats
from reconciler.worker import run_until_idle
from tests.helpers import send
from tests.integration.test_retry_dlq import ListSource


def names(ctx: AppContext) -> list[str]:
    return sorted(a.name for a in check_alerts(ctx))


def test_quiet_system_has_no_alerts(ctx: AppContext) -> None:
    send(ctx)
    run_until_idle(ctx)
    assert names(ctx) == []
    s = collect_stats(ctx)
    assert s["events_by_status"] == {"processed": 1}
    assert s["duplicate_effects"] == 0 and s["dlq_depth"] == 0
    assert s["last_reconcile"] is None and s["oldest_buffered_age_s"] is None


def test_dlq_depth_alert(ctx: AppContext) -> None:
    send(ctx, data={"status": "banana"})
    run_until_idle(ctx)
    assert names(ctx) == ["dlq_depth"]


def test_dlq_alert_threshold_is_configurable(ctx: AppContext) -> None:
    ctx.settings.alert_dlq_depth = 2
    send(ctx, data={"status": "banana"})
    run_until_idle(ctx)
    assert names(ctx) == []


def test_parked_and_buffer_age_alerts(ctx: AppContext, clock: FakeClock) -> None:
    send(ctx, version=2)
    run_until_idle(ctx)
    assert names(ctx) == []  # still inside its window
    clock.advance(ctx.settings.buffer_window_seconds + 1)
    sweep(ctx)
    assert names(ctx) == ["parked_events"]
    clock.advance(ctx.settings.alert_oldest_buffered_seconds)
    assert names(ctx) == ["buffer_age", "parked_events"]


def test_reconcile_staleness_alert_only_when_a_source_is_configured(
    ctx: AppContext, clock: FakeClock
) -> None:
    assert names(ctx) == []
    ctx.settings.reconcile_source_url = "http://source.test"
    assert names(ctx) == ["reconcile_stale"]  # never ran
    reconcile(ctx, ListSource())
    assert names(ctx) == []
    clock.advance(ctx.settings.alert_reconcile_age_seconds + 1)
    assert names(ctx) == ["reconcile_stale"]


def test_stats_report_corrections_and_duplicate_effects(ctx: AppContext) -> None:
    reconcile(ctx, ListSource(SourceRecord("order-1", 2, {"status": "paid"})))
    s = collect_stats(ctx)
    assert s["corrections_last_24h"] == 1
    assert s["last_reconcile"]["corrected"] == 1
    # an injected duplicate effect must be visible to the metric
    with ctx.pool.connection() as conn:
        for _ in range(2):
            conn.execute(
                "INSERT INTO effect_log (aggregate_id, version, event_id, applied_at) "
                "VALUES ('x', 1, gen_random_uuid(), now())"
            )
    assert collect_stats(ctx)["duplicate_effects"] == 1
