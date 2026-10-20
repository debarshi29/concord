from __future__ import annotations

import json

import pytest

from reconciler import signing
from reconciler.context import AppContext
from reconciler.errors import IngestRejected, RejectCode
from reconciler.ingest import ingest
from tests.helpers import SECRET, body_for, scalar, send


def _event(ctx: AppContext, event_id: object) -> dict[str, object]:
    with ctx.pool.connection() as conn:
        row = conn.execute("SELECT * FROM events WHERE id = %s", (event_id,)).fetchone()
    assert row is not None
    return dict(row)


def test_valid_event_is_persisted_before_ack(ctx: AppContext) -> None:
    accepted = send(ctx)
    row = _event(ctx, accepted.event_id)
    assert row["status"] == "received"
    assert row["signature_status"] == "valid"
    assert row["idempotency_key"] == "order-1-v1"
    assert row["aggregate_id"] == "order-1" and row["version"] == 1
    assert json.loads(str(row["raw_body"]))["data"] == {"status": "paid"}


def test_key_precedence_header_then_event_id_then_content_hash(ctx: AppContext) -> None:
    body = body_for(event_id="evt-9")
    sig = signing.sign(body, SECRET)
    assert ingest(ctx, body, sig, idempotency_header="hdr-1").idempotency_key == "hdr-1"
    assert ingest(ctx, body, sig).idempotency_key == "evt-9"
    anon = body_for(event_id=None)
    key = ingest(ctx, anon, signing.sign(anon, SECRET)).idempotency_key
    assert key.startswith("sha256:") and len(key) == len("sha256:") + 64
    # content-derived keys are stable for identical content
    assert key == ingest(ctx, anon, signing.sign(anon, SECRET)).idempotency_key


@pytest.mark.parametrize(
    ("signature", "code", "sig_status"),
    [
        (None, RejectCode.MISSING_SIGNATURE, "missing"),
        ("sha256=deadbeef", RejectCode.INVALID_SIGNATURE, "invalid"),
        (signing.sign(b"{}", "wrong-secret"), RejectCode.INVALID_SIGNATURE, "invalid"),
    ],
)
def test_bad_signatures_are_recorded_and_never_processable(
    ctx: AppContext, signature: str | None, code: RejectCode, sig_status: str
) -> None:
    with pytest.raises(IngestRejected) as exc:
        ingest(ctx, body_for(), signature)
    assert exc.value.code is code
    row = _event(ctx, exc.value.event_id)
    assert row["status"] == "rejected" and row["reject_reason"] == code.value
    assert row["signature_status"] == sig_status
    assert scalar(ctx, "SELECT count(*) FROM events WHERE status = 'received'") == 0


@pytest.mark.parametrize(
    ("body", "code"),
    [
        (b"not json at all \x00\xff", RejectCode.MALFORMED_JSON),
        (b"[1, 2, 3]", RejectCode.INVALID_ENVELOPE),
        (b'{"type": "order.updated"}', RejectCode.INVALID_ENVELOPE),
        (
            json.dumps(
                {"type": "order.updated", "aggregate_id": "a", "version": 0, "data": {}}
            ).encode(),
            RejectCode.INVALID_ENVELOPE,
        ),
        (
            json.dumps(
                {"type": "order.updated", "aggregate_id": "a", "version": "1", "data": {}}
            ).encode(),
            RejectCode.INVALID_ENVELOPE,
        ),
        (
            json.dumps(
                {"type": "order.deleted", "aggregate_id": "a", "version": 1, "data": {}}
            ).encode(),
            RejectCode.UNSUPPORTED_EVENT_TYPE,
        ),
    ],
)
def test_malformed_events_get_a_typed_rejection_and_are_recorded(
    ctx: AppContext, body: bytes, code: RejectCode
) -> None:
    with pytest.raises(IngestRejected) as exc:
        ingest(ctx, body, signing.sign(body, SECRET))
    assert exc.value.code is code
    row = _event(ctx, exc.value.event_id)
    assert row["status"] == "rejected" and row["reject_reason"] == code.value
    assert row["signature_status"] == "valid"


def test_secret_rotation_accepts_previous_secret(ctx: AppContext) -> None:
    from pydantic import SecretStr

    ctx.settings.webhook_secret = SecretStr("new-secret")
    ctx.settings.webhook_secret_previous = SecretStr(SECRET)
    body = body_for()
    assert ingest(
        ctx, body, signing.sign(body, SECRET)
    )  # old secret still accepted during rotation
    assert ingest(ctx, body, signing.sign(body, "new-secret"))
    with pytest.raises(IngestRejected):
        ingest(ctx, body, signing.sign(body, "unrelated"))


def test_duplicate_deliveries_are_all_recorded(ctx: AppContext) -> None:
    send(ctx)
    send(ctx)
    assert scalar(ctx, "SELECT count(*) FROM events WHERE idempotency_key = 'order-1-v1'") == 2
