"""Ingestion boundary: verify, validate, derive the key, persist *before* acknowledging.

Every received body is recorded. A rejected event is stored with a typed reason and never processed;
nothing is silently dropped. Raising `IngestRejected` happens only after the row is committed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import ValidationError

from reconciler import signing
from reconciler.context import AppContext
from reconciler.errors import IngestRejected, RejectCode
from reconciler.models import SUPPORTED_EVENT_TYPES, Envelope
from reconciler.patterns import idempotency
from reconciler.telemetry import current_traceparent, tracer


@dataclass(frozen=True)
class Accepted:
    event_id: UUID
    idempotency_key: str


def _clean(body: bytes) -> str:
    # Postgres `text` cannot hold NUL; garbage bodies must still be recordable.
    return body.decode("utf-8", errors="replace").replace("\x00", "�")


def _insert(
    ctx: AppContext,
    *,
    event_id: UUID,
    now: datetime,
    body: bytes,
    signature_status: str,
    status: str,
    reject_reason: str | None = None,
    envelope: Envelope | None = None,
    key: str | None = None,
) -> None:
    with ctx.pool.connection() as conn:
        conn.execute(
            "INSERT INTO events (id, idempotency_key, aggregate_id, version, event_type, raw_body, payload, "
            "signature_status, status, reject_reason, next_attempt_at, traceparent, received_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s)",
            (
                event_id,
                key,
                envelope.aggregate_id if envelope else None,
                envelope.version if envelope else None,
                envelope.type if envelope else None,
                _clean(body),
                json.dumps(envelope.model_dump(mode="json")) if envelope else None,
                signature_status,
                status,
                reject_reason,
                now,
                current_traceparent(),
                now,
            ),
        )


def _reject(
    ctx: AppContext,
    event_id: UUID,
    now: datetime,
    body: bytes,
    signature_status: str,
    code: RejectCode,
    detail: str,
) -> IngestRejected:
    _insert(
        ctx,
        event_id=event_id,
        now=now,
        body=body,
        signature_status=signature_status,
        status="rejected",
        reject_reason=code.value,
    )
    return IngestRejected(code, detail, str(event_id))


def ingest(
    ctx: AppContext,
    body: bytes,
    signature: str | None,
    idempotency_header: str | None = None,
) -> Accepted:
    event_id = uuid4()
    now = ctx.clock.now()
    with tracer.start_as_current_span("ingest_webhook") as span:
        span.set_attribute("event.id", str(event_id))
        secrets = [
            ctx.settings.webhook_secret.get_secret_value(),
            ctx.settings.webhook_secret_previous.get_secret_value(),
        ]
        if not signature:
            raise _reject(
                ctx,
                event_id,
                now,
                body,
                "missing",
                RejectCode.MISSING_SIGNATURE,
                "no signature header",
            )
        if not signing.verify(body, signature, secrets):
            raise _reject(
                ctx,
                event_id,
                now,
                body,
                "invalid",
                RejectCode.INVALID_SIGNATURE,
                "signature mismatch",
            )

        try:
            raw: Any = json.loads(body)
        except (ValueError, RecursionError) as exc:
            raise _reject(
                ctx, event_id, now, body, "valid", RejectCode.MALFORMED_JSON, "body is not JSON"
            ) from exc
        try:
            envelope = Envelope.model_validate(raw)
        except ValidationError as exc:
            fields = ",".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
            raise _reject(
                ctx,
                event_id,
                now,
                body,
                "valid",
                RejectCode.INVALID_ENVELOPE,
                f"invalid fields: {fields}",
            ) from exc
        if envelope.type not in SUPPORTED_EVENT_TYPES:
            raise _reject(
                ctx,
                event_id,
                now,
                body,
                "valid",
                RejectCode.UNSUPPORTED_EVENT_TYPE,
                f"unsupported type {envelope.type!r}",
            )

        key = idempotency.derive_key(idempotency_header, envelope.event_id, envelope.content_hash())
        span.set_attribute("aggregate.id", envelope.aggregate_id)
        span.set_attribute("aggregate.version", envelope.version)
        # Commit happens when the connection context exits; the caller acks only after we return.
        _insert(
            ctx,
            event_id=event_id,
            now=now,
            body=body,
            signature_status="valid",
            status="received",
            envelope=envelope,
            key=key,
        )
        return Accepted(event_id=event_id, idempotency_key=key)
