"""One webhook = one trace, from receipt through retries to the final outcome."""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from reconciler.clock import FakeClock
from reconciler.context import AppContext
from reconciler.db import Conn
from reconciler.errors import RetryableError
from reconciler.models import EventRow
from reconciler.processor import process_next
from reconciler.reconcile import reconcile
from reconciler.settings import Settings
from reconciler.telemetry import current_traceparent, setup_tracing
from tests.helpers import send
from tests.integration.test_retry_dlq import ListSource


@pytest.fixture(scope="module")
def exporter() -> InMemorySpanExporter:
    exp = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exp))
    trace.set_tracer_provider(provider)
    return exp


def test_ingest_and_every_processing_attempt_share_one_trace(
    ctx: AppContext, clock: FakeClock, exporter: InMemorySpanExporter
) -> None:
    exporter.clear()

    def flaky(conn: Conn, event: EventRow) -> None:
        raise RetryableError("down")

    send(ctx)
    ctx.sink = flaky
    process_next(ctx)  # attempt 1 fails
    clock.advance(ctx.retry.cap_seconds + 1)
    ctx.sink = lambda conn, event: None
    process_next(ctx)  # attempt 2 succeeds

    spans = exporter.get_finished_spans()
    ingest = [s for s in spans if s.name == "ingest_webhook"]
    attempts = [s for s in spans if s.name == "process_event"]
    assert len(ingest) == 1 and len(attempts) == 2
    assert {s.context.trace_id for s in attempts} == {ingest[0].context.trace_id}
    assert [s.attributes["outcome"] for s in attempts] == ["retry", "applied"]  # type: ignore[index]
    assert [s.attributes["attempt"] for s in attempts] == [1, 2]  # type: ignore[index]


def test_reconcile_emits_a_span_with_counts(
    ctx: AppContext, exporter: InMemorySpanExporter
) -> None:
    exporter.clear()
    reconcile(ctx, ListSource())
    [span] = [s for s in exporter.get_finished_spans() if s.name == "reconcile"]
    assert span.attributes is not None and span.attributes["checked"] == 0


def test_traceparent_absent_outside_a_span() -> None:
    assert current_traceparent() is None


def test_setup_tracing_none_is_a_noop() -> None:
    assert setup_tracing(Settings(otel_exporter="none")) is None
