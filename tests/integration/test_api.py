from __future__ import annotations

import json
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from reconciler import signing
from reconciler.api import create_app
from reconciler.context import AppContext
from reconciler.worker import run_until_idle
from tests.helpers import SECRET, aggregate, body_for, scalar


@pytest.fixture
def client(ctx: AppContext) -> TestClient:
    return TestClient(create_app(ctx))


def post(client: TestClient, body: bytes, signature: str | None = "auto", **headers: str):  # type: ignore[no-untyped-def]
    h = dict(headers)
    if signature == "auto":
        h["X-Signature"] = signing.sign(body, SECRET)
    elif signature is not None:
        h["X-Signature"] = signature
    return client.post("/webhooks", content=body, headers=h)


def test_accepts_valid_webhook_with_202(client: TestClient, ctx: AppContext) -> None:
    r = post(client, body_for(version=1, data={"status": "paid"}))
    assert r.status_code == 202
    assert r.json()["status"] == "accepted" and r.json()["idempotency_key"] == "order-1-v1"
    run_until_idle(ctx)
    assert aggregate(ctx) == (1, {"status": "paid"})


def test_idempotency_key_header_is_honoured(client: TestClient) -> None:
    r = post(client, body_for(event_id=None), **{"Idempotency-Key": "abc-123"})
    assert r.json()["idempotency_key"] == "abc-123"


@pytest.mark.parametrize(
    ("body", "signature", "status", "code"),
    [
        (body_for(), None, 401, "missing_signature"),
        (body_for(), "sha256=00", 401, "invalid_signature"),
        (b"{nope", "auto", 400, "malformed_json"),
        (json.dumps({"type": "order.updated"}).encode(), "auto", 422, "invalid_envelope"),
        (body_for(type_="order.deleted"), "auto", 422, "unsupported_event_type"),
    ],
)
def test_rejections_are_typed_and_carry_the_recorded_event_id(
    client: TestClient, body: bytes, signature: str | None, status: int, code: str
) -> None:
    r = post(client, body, signature)
    assert r.status_code == status
    err = r.json()["error"]
    assert err["code"] == code and err["retryable"] is False and err["event_id"]


def test_health_and_readiness(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_readiness_fails_when_database_is_unreachable(ctx: AppContext) -> None:
    class Broken:
        def connection(self) -> None:
            raise RuntimeError("db down")

    bad = AppContext(Broken(), ctx.settings, ctx.clock, ctx.sink, ctx.retry)  # type: ignore[arg-type]
    r = TestClient(create_app(bad)).get("/readyz")
    assert r.status_code == 503


def test_stats_and_alerts_endpoints(client: TestClient, ctx: AppContext) -> None:
    post(client, body_for(data={"status": "banana"}))
    run_until_idle(ctx)
    stats = client.get("/stats").json()
    assert stats["dlq_depth"] == 1 and stats["events_by_status"] == {"dead": 1}
    alerts = client.get("/alerts").json()["alerts"]
    assert [a["name"] for a in alerts] == ["dlq_depth"]


def test_app_refuses_to_boot_without_a_webhook_secret(ctx: AppContext) -> None:
    ctx.settings.webhook_secret = SecretStr("")
    with pytest.raises(ValueError, match="WEBHOOK_SECRET"):
        create_app(ctx)


def test_openapi_documents_the_webhook_contract(client: TestClient) -> None:
    spec = client.get("/openapi.json").json()
    op = spec["paths"]["/webhooks"]["post"]
    assert set(op["responses"]) >= {"202", "400", "401", "422"}


def test_non_ascii_signature_is_a_401_and_recorded(client: TestClient, ctx: AppContext) -> None:
    r = client.post("/webhooks", content=body_for(), headers={"X-Signature": b"sha256=\xe9"})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_signature"


def test_oversized_body_is_refused_unread_and_unrecorded(
    client: TestClient, ctx: AppContext
) -> None:
    ctx.settings.max_body_bytes = 1000
    r = post(client, b"x" * 1001, signature=None)
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "payload_too_large"
    assert (
        scalar(ctx, "SELECT count(*) FROM events") == 0
    )  # nothing stored for an unauthenticated flood


def test_oversized_chunked_body_without_content_length_is_refused(
    client: TestClient, ctx: AppContext
) -> None:
    ctx.settings.max_body_bytes = 1000

    def chunks() -> Iterator[bytes]:
        for _ in range(5):
            yield b"x" * 300

    r = client.post("/webhooks", content=chunks())  # chunked: no declared length to check
    assert r.status_code == 413


def test_body_at_the_limit_is_accepted(client: TestClient, ctx: AppContext) -> None:
    body = body_for(data={"note": "y" * 100})
    ctx.settings.max_body_bytes = len(body)
    assert post(client, body).status_code == 202


def _ops_client(ctx: AppContext, token: str) -> TestClient:
    ctx.settings.ops_token = SecretStr(token)
    return TestClient(create_app(ctx))


def test_ops_endpoints_require_the_token_when_one_is_configured(ctx: AppContext) -> None:
    c = _ops_client(ctx, "s3cret-token")
    for path in ("/stats", "/alerts"):
        assert c.get(path).status_code == 401
        assert c.get(path, headers={"Authorization": "Bearer nope"}).status_code == 401
        assert c.get(path, headers={"Authorization": b"Bearer \xe9"}).status_code == 401
        assert (
            c.get(path, headers={"Authorization": "s3cret-token"}).status_code == 401
        )  # not Bearer
        assert c.get(path, headers={"Authorization": "Bearer s3cret-token"}).status_code == 200
    assert c.get("/stats").headers["www-authenticate"] == "Bearer"


def test_probes_and_the_webhook_stay_open_when_a_token_is_configured(ctx: AppContext) -> None:
    c = _ops_client(ctx, "s3cret-token")
    assert c.get("/healthz").status_code == 200 and c.get("/readyz").status_code == 200
    assert post(c, body_for()).status_code == 202


def test_ops_endpoints_are_open_without_a_token(client: TestClient) -> None:
    assert client.get("/stats").status_code == 200 and client.get("/alerts").status_code == 200
