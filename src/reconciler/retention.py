"""History retention: keep the ledgers bounded without losing anything still in use.

`events`, `effect_log`, `corrections`, `reconcile_runs` and resolved `dlq` rows only ever grow. The sweep
trims them past a retention window, a bounded batch per table per call, so catching up after enabling it
(or after an outage) never holds a long transaction.

Never deleted: events still referenced by an idempotency record (its TTL governs those), by an open DLQ
entry or by the buffer; events not yet in a final state; and the latest reconcile run, because the
`reconcile_stale` alert reads it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from reconciler.db import Conn

BATCH_SIZE = 5000


@dataclass(frozen=True)
class Purged:
    events: int = 0
    effect_log: int = 0
    corrections: int = 0
    reconcile_runs: int = 0
    dlq: int = 0

    @property
    def total(self) -> int:
        return self.events + self.effect_log + self.corrections + self.reconcile_runs + self.dlq


def purge_history(
    conn: Conn, now: datetime, retention_seconds: int, batch_size: int = BATCH_SIZE
) -> Purged:
    """Delete rows older than the window. `retention_seconds == 0` disables retention."""
    if retention_seconds <= 0:
        return Purged()
    cutoff = now - timedelta(seconds=retention_seconds)

    dlq = conn.execute(
        "DELETE FROM dlq WHERE event_id IN "
        "(SELECT event_id FROM dlq WHERE resolved_at IS NOT NULL AND resolved_at < %s "
        "ORDER BY resolved_at LIMIT %s)",
        (cutoff, batch_size),
    ).rowcount
    events = conn.execute(
        "DELETE FROM events WHERE id IN ("
        "  SELECT e.id FROM events e"
        "  WHERE e.status IN ('processed', 'duplicate', 'stale', 'rejected', 'dead')"
        "    AND e.received_at < %s"
        "    AND NOT EXISTS (SELECT 1 FROM idempotency i WHERE i.event_id = e.id)"
        "    AND NOT EXISTS (SELECT 1 FROM dlq d WHERE d.event_id = e.id)"
        "    AND NOT EXISTS (SELECT 1 FROM buffer b WHERE b.event_id = e.id)"
        "  ORDER BY e.received_at LIMIT %s)",
        (cutoff, batch_size),
    ).rowcount
    effect_log = conn.execute(
        "DELETE FROM effect_log WHERE id IN "
        "(SELECT id FROM effect_log WHERE applied_at < %s ORDER BY applied_at LIMIT %s)",
        (cutoff, batch_size),
    ).rowcount
    corrections = conn.execute(
        "DELETE FROM corrections WHERE id IN "
        "(SELECT id FROM corrections WHERE corrected_at < %s ORDER BY corrected_at LIMIT %s)",
        (cutoff, batch_size),
    ).rowcount
    runs = conn.execute(
        "DELETE FROM reconcile_runs WHERE id IN "
        "(SELECT id FROM reconcile_runs WHERE finished_at < %s "
        "   AND id <> (SELECT max(id) FROM reconcile_runs) ORDER BY finished_at LIMIT %s)",
        (cutoff, batch_size),
    ).rowcount
    return Purged(
        events=events, effect_log=effect_log, corrections=corrections, reconcile_runs=runs, dlq=dlq
    )
