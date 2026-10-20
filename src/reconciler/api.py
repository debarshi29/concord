"""HTTP boundary. Acknowledges (202) only after the event is durably committed."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from reconciler import __version__
from reconciler.context import AppContext
from reconciler.errors import IngestRejected, RejectCode
from reconciler.ingest import ingest
from reconciler.stats import alert_dicts, check_alerts, collect_stats

_STATUS = {
    RejectCode.MISSING_SIGNATURE: 401,
    RejectCode.INVALID_SIGNATURE: 401,
    RejectCode.MALFORMED_JSON: 400,
    RejectCode.INVALID_ENVELOPE: 422,
    RejectCode.UNSUPPORTED_EVENT_TYPE: 422,
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


def create_app(ctx: AppContext) -> FastAPI:
    ctx.settings.require_webhook_secret()  # fail loudly at boot, not at the first webhook
    app = FastAPI(title="Idempotent Webhook Reconciler", version=__version__)

    @app.post(
        "/webhooks",
        status_code=202,
        response_model=AcceptedBody,
        responses={400: {"model": ErrorBody}, 401: {"model": ErrorBody}, 422: {"model": ErrorBody}},
    )
    async def receive(
        request: Request,
        x_signature: str | None = Header(default=None),
        idempotency_key: str | None = Header(default=None),
    ) -> Any:
        body = await request.body()
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

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        return collect_stats(ctx)

    @app.get("/alerts")
    def alerts() -> dict[str, Any]:
        return {"alerts": alert_dicts(check_alerts(ctx))}

    return app
