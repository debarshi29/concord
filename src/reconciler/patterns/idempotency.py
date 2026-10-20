"""Idempotency store. `record` must be called in the *same transaction* as the state change it guards."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from reconciler.db import Conn


def derive_key(header: str | None, event_id: str | None, content_hash: str) -> str:
    """Key precedence (ADR-0001): explicit header, then the sender's event id, else a content hash."""
    if header:
        return header
    if event_id:
        return event_id
    return f"sha256:{content_hash}"


def key_fingerprint(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()[:12]


def lookup(conn: Conn, key: str, now: datetime) -> dict[str, Any] | None:
    """Return the recorded outcome if the key was seen and its record has not expired."""
    row = conn.execute(
        "SELECT outcome FROM idempotency WHERE key = %s AND expires_at > %s", (key, now)
    ).fetchone()
    return None if row is None else dict(row["outcome"])


def record(
    conn: Conn,
    key: str,
    outcome: dict[str, Any],
    event_id: UUID,
    now: datetime,
    ttl_seconds: float,
) -> None:
    conn.execute(
        "INSERT INTO idempotency (key, outcome, event_id, created_at, expires_at) "
        "VALUES (%s, %s::jsonb, %s, %s, %s) "
        "ON CONFLICT (key) DO UPDATE SET outcome = EXCLUDED.outcome, event_id = EXCLUDED.event_id, "
        "created_at = EXCLUDED.created_at, expires_at = EXCLUDED.expires_at",
        (key, json.dumps(outcome), event_id, now, now + timedelta(seconds=ttl_seconds)),
    )


def purge_expired(conn: Conn, now: datetime, batch_size: int = 1000) -> int:
    """Delete expired records in bounded batches (one call = at most `batch_size` rows)."""
    cur = conn.execute(
        "DELETE FROM idempotency WHERE key IN "
        "(SELECT key FROM idempotency WHERE expires_at <= %s LIMIT %s)",
        (now, batch_size),
    )
    return cur.rowcount
