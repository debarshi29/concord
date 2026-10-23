"""Wire-level contracts: the webhook envelope, plus the rows the rest of the code passes around."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

SUPPORTED_EVENT_TYPES = ("order.updated",)


class Envelope(BaseModel):
    """Boundary schema. Validates the *envelope* only; business validity of `data` is the
    domain handler's job, so a semantically bad (poison) event is accepted, stored and dead-lettered
    rather than silently refused."""

    model_config = ConfigDict(extra="ignore", strict=True)

    event_id: str | None = Field(default=None, min_length=1, max_length=200)
    type: str = Field(min_length=1, max_length=100)
    aggregate_id: str = Field(min_length=1, max_length=128)
    version: int = Field(ge=1, le=2_000_000_000)
    data: dict[str, Any]
    # Senders put ISO-8601 strings on the wire; strict mode would reject every one of them.
    occurred_at: datetime | None = Field(default=None, strict=False)

    def content_hash(self) -> str:
        canonical = json.dumps(
            {
                "type": self.type,
                "aggregate_id": self.aggregate_id,
                "version": self.version,
                "data": self.data,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


EventStatus = Literal[
    "received", "buffered", "parked", "processed", "duplicate", "stale", "dead", "rejected"
]


class EventRow(BaseModel):
    """A row of `events` as the processor sees it."""

    model_config = ConfigDict(extra="ignore")

    id: UUID
    seq: int
    idempotency_key: str | None
    aggregate_id: str | None
    version: int | None
    event_type: str | None
    payload: dict[str, Any] | None
    status: EventStatus
    attempts: int
    traceparent: str | None
    received_at: datetime
