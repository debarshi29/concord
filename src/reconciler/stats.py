"""Operational read models: the numbers behind `/stats`, `reconciler stats` and the success metrics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any

from reconciler.context import AppContext
from reconciler.db import Conn
from reconciler.patterns import dlq


@dataclass(frozen=True)
class Alert:
    name: str
    message: str
    value: float


@dataclass(frozen=True)
class AlertInputs:
    """The few cheap, index-backed numbers the alerts need. The worker evaluates them every sweep, so
    they must not scan `events` or `effect_log` the way the full `/stats` report does."""

    dlq_depth: int
    parked_events: int
    oldest_buffered_at: datetime | None
    last_reconcile: dict[str, Any] | None  # finished_at (datetime), checked, corrected


def _alert_inputs(conn: Conn) -> AlertInputs:
    parked = conn.execute("SELECT count(*) AS n FROM events WHERE status = 'parked'").fetchone()
    oldest = conn.execute(
        "SELECT min(received_at) AS t FROM events WHERE status IN ('buffered', 'parked')"
    ).fetchone()
    last = conn.execute(
        "SELECT finished_at, checked, corrected FROM reconcile_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert parked and oldest
    return AlertInputs(
        dlq_depth=dlq.depth(conn),
        parked_events=int(parked["n"]),
        oldest_buffered_at=oldest["t"],
        last_reconcile=last,
    )


def collect_stats(ctx: AppContext) -> dict[str, Any]:
    now = ctx.clock.now()
    with ctx.pool.connection() as conn:
        inputs = _alert_inputs(conn)
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
        ttr = conn.execute(
            "SELECT avg(extract(epoch FROM resolved_at - failed_at)) AS s FROM dlq WHERE resolved_at IS NOT NULL"
        ).fetchone()
        assert dup and corr and ttr
        last = inputs.last_reconcile
        oldest = inputs.oldest_buffered_at
        return {
            "events_by_status": by_status,
            "dlq_depth": inputs.dlq_depth,
            "buffer_depth": by_status.get("buffered", 0) + by_status.get("parked", 0),
            "parked_events": inputs.parked_events,
            "duplicate_effects": int(dup["n"]),
            "corrections_last_24h": int(corr["n"]),
            "last_reconcile": None
            if last is None
            else {
                "finished_at": last["finished_at"].isoformat(),
                "checked": last["checked"],
                "corrected": last["corrected"],
            },
            "oldest_buffered_age_s": None if oldest is None else (now - oldest).total_seconds(),
            "dlq_mean_time_to_resolution_s": None if ttr["s"] is None else float(ttr["s"]),
        }


def check_alerts(ctx: AppContext) -> list[Alert]:
    """Threshold alerts. DLQ depth and parked events page; reconcile staleness warns (runbook §3-§5)."""
    now = ctx.clock.now()
    with ctx.pool.connection() as conn:
        s = _alert_inputs(conn)
    st = ctx.settings
    alerts: list[Alert] = []
    if s.dlq_depth >= st.alert_dlq_depth:
        alerts.append(Alert("dlq_depth", f"{s.dlq_depth} event(s) in the DLQ", s.dlq_depth))
    if s.parked_events >= st.alert_parked_events:
        alerts.append(
            Alert(
                "parked_events",
                f"{s.parked_events} event(s) parked behind a version gap",
                s.parked_events,
            )
        )
    if s.oldest_buffered_at is not None:
        age = (now - s.oldest_buffered_at).total_seconds()
        if age >= st.alert_oldest_buffered_seconds:
            alerts.append(Alert("buffer_age", f"oldest buffered event is {age:.0f}s old", age))
    if st.reconcile_source_url:
        if s.last_reconcile is None:
            alerts.append(Alert("reconcile_stale", "reconciler has never completed a run", -1))
        else:
            age_r = (now - s.last_reconcile["finished_at"]).total_seconds()
            if age_r >= st.alert_reconcile_age_seconds:
                alerts.append(
                    Alert("reconcile_stale", f"last reconcile finished {age_r:.0f}s ago", age_r)
                )
    return alerts


def alert_dicts(alerts: list[Alert]) -> list[dict[str, Any]]:
    return [asdict(a) for a in alerts]
