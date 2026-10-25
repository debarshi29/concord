"""HTTP boundary. Acknowledges (202) only after the event is durably committed."""

from __future__ import annotations

import secrets
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from reconciler import __version__
from reconciler.context import AppContext
from reconciler.errors import IngestRejected, RejectCode
from reconciler.ingest import ingest
from reconciler.logs import get_logger
from reconciler.stats import alert_dicts, check_alerts, collect_stats

log = get_logger("api")

_STATUS = {
    RejectCode.MISSING_SIGNATURE: 401,
    RejectCode.INVALID_SIGNATURE: 401,
    RejectCode.MALFORMED_JSON: 400,
    RejectCode.INVALID_ENVELOPE: 422,
    RejectCode.UNSUPPORTED_EVENT_TYPE: 422,
    RejectCode.PAYLOAD_TOO_LARGE: 413,
}


class AcceptedBody(BaseModel):
    event_id: str
    idempotency_key: str
    status: str = "accepted"


class ErrorDetail(BaseModel):
    code: RejectCode
    detail: str
    retryable: bool = False
    event_id: str | None = None


class ErrorBody(BaseModel):
    error: ErrorDetail


class BodyTooLarge(Exception):
    """The request body exceeded `max_body_bytes`; it was refused without being read to the end."""


async def _read_body(request: Request, limit: int) -> bytes:
    """Read the body, refusing it as soon as it exceeds `limit`.

    Unlike `request.body()`, this never buffers more than `limit` bytes, and a declared
    `Content-Length` over the limit is refused before reading anything.
    """
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise BodyTooLarge
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise BodyTooLarge
        chunks.append(chunk)
    return b"".join(chunks)


def create_app(ctx: AppContext) -> FastAPI:
    ctx.settings.require_webhook_secret()  # fail loudly at boot, not at the first webhook
    ops_token = ctx.settings.ops_token.get_secret_value()
    if not ops_token:
        log.warning(
            "RECONCILER_OPS_TOKEN is unset: /stats and /alerts are open; bind to an internal network"
        )
    app = FastAPI(title="Idempotent Webhook Reconciler", version=__version__)

    def require_ops_token(authorization: str | None = Header(default=None)) -> None:
        if not ops_token:
            return
        scheme, _, presented = (authorization or "").partition(" ")
        given = presented.encode("utf-8", errors="replace")
        if scheme != "Bearer" or not secrets.compare_digest(given, ops_token.encode()):
            raise HTTPException(
                status_code=401,
                detail="invalid or missing token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @app.post(
        "/webhooks",
        status_code=202,
        response_model=AcceptedBody,
        responses={
            400: {"model": ErrorBody},
            401: {"model": ErrorBody},
            413: {"model": ErrorBody},
            422: {"model": ErrorBody},
        },
    )
    async def receive(
        request: Request,
        x_signature: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ) -> Any:
        try:
            body = await _read_body(request, ctx.settings.max_body_bytes)
        except BodyTooLarge:
            # Refused unread and unrecorded: storing unauthenticated oversized bodies is the attack.
            log.warning("webhook body over limit", extra={"limit": ctx.settings.max_body_bytes})
            return JSONResponse(
                status_code=413,
                content=ErrorBody(
                    error=ErrorDetail(
                        code=RejectCode.PAYLOAD_TOO_LARGE,
                        detail=f"body exceeds {ctx.settings.max_body_bytes} bytes",
                    )
                ).model_dump(mode="json"),
            )
        try:
            accepted = await run_in_threadpool(ingest, ctx, body, x_signature, idempotency_key)
        except IngestRejected as rej:
            return JSONResponse(
                status_code=_STATUS[rej.code],
                content=ErrorBody(
                    error=ErrorDetail(code=rej.code, detail=rej.detail, event_id=rej.event_id)
                ).model_dump(mode="json"),
            )
        return AcceptedBody(
            event_id=str(accepted.event_id), idempotency_key=accepted.idempotency_key
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> Any:
        try:
            with ctx.pool.connection() as conn:
                conn.execute("SELECT 1")
        except Exception:
            return JSONResponse(status_code=503, content={"status": "database unavailable"})
        return {"status": "ready"}

    @app.get("/stats", dependencies=[Depends(require_ops_token)])
    def stats() -> dict[str, Any]:
        return collect_stats(ctx)

    @app.get("/alerts", dependencies=[Depends(require_ops_token)])
    def alerts() -> dict[str, Any]:
        return {"alerts": alert_dicts(check_alerts(ctx))}

    return app
