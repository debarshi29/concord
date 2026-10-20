"""Operated dead-letter queue: full context on entry, a re-drive path, and a depth signal."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from reconciler.db import Conn


def send(conn: Conn, event_id: UUID, attempts: int, last_error: str, now: datetime) -> None:
    conn.execute(
        "INSERT INTO dlq (event_id, attempts, last_error, failed_at) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (event_id) DO UPDATE SET attempts = EXCLUDED.attempts, "
        "last_error = EXCLUDED.last_error, failed_at = EXCLUDED.failed_at, "
        "resolved_at = NULL, resolution = NULL",
        (event_id, attempts, last_error, now),
    )


def depth(conn: Conn) -> int:
    row = conn.execute("SELECT count(*) AS n FROM dlq WHERE resolved_at IS NULL").fetchone()
    assert row is not None
    return int(row["n"])


def list_open(conn: Conn, limit: int = 100) -> list[dict[str, Any]]:
    return list(
        conn.execute(
            "SELECT d.event_id, e.aggregate_id, e.version, d.attempts, d.last_error, d.failed_at "
            "FROM dlq d JOIN events e ON e.id = d.event_id "
            "WHERE d.resolved_at IS NULL ORDER BY d.failed_at LIMIT %s",
            (limit,),
        )
    )


def redrive(conn: Conn, now: datetime, event_id: UUID | None = None) -> int:
    """Return dead events to the live queue with a fresh attempt budget. Returns how many."""
    rows = conn.execute(
        "SELECT event_id FROM dlq WHERE resolved_at IS NULL AND (%s::uuid IS NULL OR event_id = %s) "
        "ORDER BY failed_at FOR UPDATE",
        (event_id, event_id),
    ).fetchall()
    ids = [r["event_id"] for r in rows]
    if not ids:
        return 0
    conn.execute(
        "UPDATE events SET status = 'received', attempts = 0, next_attempt_at = %s "
        "WHERE id = ANY(%s) AND status = 'dead'",
        (now, ids),
    )
    conn.execute(
        "UPDATE dlq SET resolved_at = %s, resolution = 'redriven' WHERE event_id = ANY(%s)",
        (now, ids),
    )
    return len(ids)


def discard(conn: Conn, now: datetime, event_id: UUID) -> bool:
    """Give up on a dead event (the reconciler will heal the aggregate). Returns whether it existed."""
    cur = conn.execute(
        "UPDATE dlq SET resolved_at = %s, resolution = 'discarded' "
        "WHERE event_id = %s AND resolved_at IS NULL",
        (now, event_id),
    )
    return cur.rowcount > 0
