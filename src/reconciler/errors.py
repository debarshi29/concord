"""Single error taxonomy: retryable vs terminal, client fault vs server fault.

Errors are typed values; callers make policy decisions mechanically from the class, never by
parsing messages. Each error is logged exactly once, at the boundary that handles it.
"""

from __future__ import annotations

from enum import StrEnum


class RejectCode(StrEnum):
    """Why ingestion refused an event (client faults; never retried by us)."""

    INVALID_SIGNATURE = "invalid_signature"
    MISSING_SIGNATURE = "missing_signature"
    MALFORMED_JSON = "malformed_json"
    INVALID_ENVELOPE = "invalid_envelope"
    UNSUPPORTED_EVENT_TYPE = "unsupported_event_type"


class ReconcilerError(Exception):
    """Base class. `retryable` tells the retry policy what to do."""

    retryable: bool = False


class RetryableError(ReconcilerError):
    """Transient server-side fault (downstream outage, timeout). Retried with backoff."""

    retryable = True


class TerminalError(ReconcilerError):
    """Will never succeed on retry (poison message). Routed straight to the DLQ."""


class IngestRejected(ReconcilerError):
    """Client fault at the boundary; carries a typed code and the id of the recorded event."""

    def __init__(self, code: RejectCode, detail: str, event_id: str | None = None) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail
        self.event_id = event_id
