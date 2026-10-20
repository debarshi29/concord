"""Test helpers: build/sign/deliver events and read back state."""

from __future__ import annotations

import json
from typing import Any

from reconciler import signing
from reconciler.context import AppContext
from reconciler.ingest import Accepted, ingest

SECRET = "test-secret"


def body_for(
    aggregate_id: str = "order-1",
    version: int = 1,
    data: dict[str, Any] | None = None,
    event_id: str | None = "default",
    type_: str = "order.updated",
) -> bytes:
    payload: dict[str, Any] = {
        "type": type_,
        "aggregate_id": aggregate_id,
        "version": version,
        "data": {"status": "paid"} if data is None else data,
    }
    if event_id == "default":
        payload["event_id"] = f"{aggregate_id}-v{version}"
    elif event_id is not None:
        payload["event_id"] = event_id
    return json.dumps(payload).encode()


def send(
    ctx: AppContext,
    aggregate_id: str = "order-1",
    version: int = 1,
    data: dict[str, Any] | None = None,
    event_id: str | None = "default",
) -> Accepted:
    body = body_for(aggregate_id, version, data, event_id)
    return ingest(ctx, body, signing.sign(body, SECRET))


def aggregate(ctx: AppContext, aggregate_id: str = "order-1") -> tuple[int, dict[str, Any]] | None:
    with ctx.pool.connection() as conn:
        row = conn.execute(
            "SELECT version, state FROM aggregates WHERE id = %s", (aggregate_id,)
        ).fetchone()
    return None if row is None else (int(row["version"]), dict(row["state"]))


def statuses(ctx: AppContext) -> dict[tuple[str, int], list[str]]:
    """(aggregate_id, version) -> statuses of every received copy, in arrival order."""
    out: dict[tuple[str, int], list[str]] = {}
    with ctx.pool.connection() as conn:
        for r in conn.execute(
            "SELECT aggregate_id, version, status FROM events WHERE aggregate_id IS NOT NULL ORDER BY seq"
        ):
            out.setdefault((r["aggregate_id"], r["version"]), []).append(r["status"])
    return out


def scalar(ctx: AppContext, sql: str, *params: Any) -> Any:
    with ctx.pool.connection() as conn:
        row = conn.execute(sql, params).fetchone()
    assert row is not None
    return next(iter(row.values()))
