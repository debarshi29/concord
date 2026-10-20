"""Periodic reconciliation: heal drift between the replica and the source of truth.

At-least-once delivery plus idempotency still cannot recover an event that was *never delivered*.
The reconciler compares `(version, state)` per aggregate against the source's list endpoint and
overwrites local state where it differs, logging a `corrections` row for every change. Corrections
are a health metric that should trend to zero: a rising count means the main path is losing events.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from reconciler.context import AppContext
from reconciler.logs import get_logger
from reconciler.processor import drain_aggregate, lock_aggregate
from reconciler.telemetry import tracer

log = get_logger("reconcile")


@dataclass(frozen=True)
class SourceRecord:
    aggregate_id: str
    version: int
    state: dict[str, Any]


class Source(Protocol):
    def list_aggregates(self) -> Iterable[SourceRecord]: ...


class HttpSource:
    """Pages through `GET {base_url}?cursor=...` returning `{"items": [...], "next_cursor": str|null}`."""

    def __init__(
        self, base_url: str, client: httpx.Client | None = None, timeout: float = 15.0
    ) -> None:
        self.base_url = base_url
        self.client = client or httpx.Client(timeout=timeout)

    def list_aggregates(self) -> Iterable[SourceRecord]:
        cursor: str | None = None
        while True:
            resp = self.client.get(self.base_url, params={"cursor": cursor} if cursor else None)
            resp.raise_for_status()
            page = resp.json()
            for item in page["items"]:
                yield SourceRecord(item["aggregate_id"], int(item["version"]), dict(item["state"]))
            cursor = page.get("next_cursor")
            if not cursor:
                return


@dataclass(frozen=True)
class ReconcileReport:
    checked: int
    corrected: int
    local_ahead: int
    orphans: int


def _correct(ctx: AppContext, rec: SourceRecord, now: Any) -> str | None:
    """Reconcile one aggregate in its own transaction. Returns the correction kind, if any."""
    with ctx.pool.connection() as conn, conn.transaction():
        state, version = lock_aggregate(conn, rec.aggregate_id, now)
        if version > rec.version:
            kind = "local_ahead"  # source is behind us: flag, never overwrite newer local data
            after = None
        elif version < rec.version:
            kind = "missing" if version == 0 else "behind"
            after = rec
        elif state != rec.state:
            kind = "divergent"
            after = rec
        else:
            drain_aggregate(ctx, conn, rec.aggregate_id, now)
            return None
        conn.execute(
            "INSERT INTO corrections (aggregate_id, kind, before, after, corrected_at) "
            "VALUES (%s, %s, %s::jsonb, %s::jsonb, %s)",
            (
                rec.aggregate_id,
                kind,
                json.dumps({"version": version, "state": state}),
                None
                if after is None
                else json.dumps({"version": after.version, "state": after.state}),
                now,
            ),
        )
        if after is not None:
            conn.execute(
                "UPDATE aggregates SET state = %s::jsonb, version = %s, updated_at = %s WHERE id = %s",
                (json.dumps(after.state), after.version, now, rec.aggregate_id),
            )
            drain_aggregate(ctx, conn, rec.aggregate_id, now)  # buffered successors may now apply
        log.warning(
            "drift corrected" if after is not None else "local ahead of source",
            extra={
                "aggregate_id": rec.aggregate_id,
                "kind": kind,
                "local_version": version,
                "source_version": rec.version,
            },
        )
        return kind


def reconcile(ctx: AppContext, source: Source) -> ReconcileReport:
    started = ctx.clock.now()
    checked = corrected = ahead = 0
    seen: set[str] = set()
    with tracer.start_as_current_span("reconcile") as span:
        for rec in source.list_aggregates():
            seen.add(rec.aggregate_id)
            checked += 1
            kind = _correct(ctx, rec, ctx.clock.now())
            if kind == "local_ahead":
                ahead += 1
            elif kind is not None:
                corrected += 1
        orphans = _flag_orphans(ctx, seen)
        span.set_attribute("checked", checked)
        span.set_attribute("corrected", corrected)
    with ctx.pool.connection() as conn:
        conn.execute(
            "INSERT INTO reconcile_runs (started_at, finished_at, checked, corrected) VALUES (%s, %s, %s, %s)",
            (started, ctx.clock.now(), checked, corrected),
        )
    return ReconcileReport(checked=checked, corrected=corrected, local_ahead=ahead, orphans=orphans)


def _flag_orphans(ctx: AppContext, seen: set[str]) -> int:
    """Aggregates we hold that the source no longer lists. Flagged for a human; never deleted."""
    now = ctx.clock.now()
    with ctx.pool.connection() as conn, conn.transaction():
        ids = [
            r["id"]
            for r in conn.execute("SELECT id FROM aggregates WHERE version > 0")
            if r["id"] not in seen
        ]
        for agg_id in ids:
            conn.execute(
                "INSERT INTO corrections (aggregate_id, kind, before, after, corrected_at) "
                "VALUES (%s, 'orphan', NULL, NULL, %s)",
                (agg_id, now),
            )
    if ids:
        log.warning("aggregates missing from source", extra={"count": len(ids)})
    return len(ids)
