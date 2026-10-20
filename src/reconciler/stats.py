"""Operational read models: the numbers behind `/stats`, `reconciler stats` and the success metrics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any

from reconciler.context import AppContext
from reconciler.patterns import dlq


@dataclass(frozen=True)
class Alert:
    name: str
    message: str
    value: float


def collect_stats(ctx: AppContext) -> dict[str, Any]:
    now = ctx.clock.now()
    with ctx.pool.connection() as conn:
        by_status = {
            r["status"]: int(r["n"])
            for r in conn.execute("SELECT status, count(*) AS n FROM events GROUP BY status")
        }
        dup = conn.execute(
            "SELECT count(*) - count(DISTINCT (aggregate_id, version)) AS n FROM effect_log"
        ).fetchone()
        corr = conn.execute(
            "SELECT count(*) AS n FROM corrections WHERE corrected_at > %s",
            (now - timedelta(hours=24),),
        ).fetchone()
        last = conn.execute(
            "SELECT finished_at, checked, corrected FROM reconcile_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        oldest = conn.execute(
            "SELECT min(received_at) AS t FROM events WHERE status IN ('buffered', 'parked')"
        ).fetchone()
        ttr = conn.execute(
            "SELECT avg(extract(epoch FROM resolved_at - failed_at)) AS s FROM dlq WHERE resolved_at IS NOT NULL"
        ).fetchone()
        assert dup and corr and oldest and ttr
        return {
            "events_by_status": by_status,
            "dlq_depth": dlq.depth(conn),
            "buffer_depth": by_status.get("buffered", 0) + by_status.get("parked", 0),
            "parked_events": by_status.get("parked", 0),
            "duplicate_effects": int(dup["n"]),
            "corrections_last_24h": int(corr["n"]),
            "last_reconcile": None
            if last is None
            else {
                "finished_at": last["finished_at"].isoformat(),
                "checked": last["checked"],
                "corrected": last["corrected"],
            },
            "oldest_buffered_age_s": None
            if oldest["t"] is None
            else (now - oldest["t"]).total_seconds(),
            "dlq_mean_time_to_resolution_s": None if ttr["s"] is None else float(ttr["s"]),
        }


def check_alerts(ctx: AppContext) -> list[Alert]:
    """Threshold alerts. DLQ depth and parked events page; reconcile staleness warns (runbook §3-§5)."""
    s = collect_stats(ctx)
    st = ctx.settings
    alerts: list[Alert] = []
    if s["dlq_depth"] >= st.alert_dlq_depth:
        alerts.append(Alert("dlq_depth", f"{s['dlq_depth']} event(s) in the DLQ", s["dlq_depth"]))
    if s["parked_events"] >= st.alert_parked_events:
        alerts.append(
            Alert(
                "parked_events",
                f"{s['parked_events']} event(s) parked behind a version gap",
                s["parked_events"],
            )
        )
    age = s["oldest_buffered_age_s"]
    if age is not None and age >= st.alert_oldest_buffered_seconds:
        alerts.append(Alert("buffer_age", f"oldest buffered event is {age:.0f}s old", age))
    if st.reconcile_source_url:
        last = s["last_reconcile"]
        if last is None:
            alerts.append(Alert("reconcile_stale", "reconciler has never completed a run", -1))
        else:
            from datetime import datetime

            age_r = (ctx.clock.now() - datetime.fromisoformat(last["finished_at"])).total_seconds()
            if age_r >= st.alert_reconcile_age_seconds:
                alerts.append(
                    Alert("reconcile_stale", f"last reconcile finished {age_r:.0f}s ago", age_r)
                )
    return alerts


def alert_dicts(alerts: list[Alert]) -> list[dict[str, Any]]:
    return [asdict(a) for a in alerts]
