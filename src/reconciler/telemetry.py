"""OpenTelemetry setup and trace-context carriage through the database.

The ingestion span's W3C `traceparent` is stored on the event row, so every later processing attempt,
retry, DLQ transition and re-drive joins the *same trace* as the original webhook receipt.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import context, propagate, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SpanExporter
from opentelemetry.trace import Span

from reconciler.settings import Settings

tracer = trace.get_tracer("reconciler")


def setup_tracing(settings: Settings) -> TracerProvider | None:
    if settings.otel_exporter == "none":
        return None
    exporter: SpanExporter
    if settings.otel_exporter == "console":
        exporter = ConsoleSpanExporter()
    else:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        exporter = OTLPSpanExporter()
    provider = TracerProvider(resource=Resource.create({"service.name": "webhook-reconciler"}))
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return provider


def current_traceparent() -> str | None:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


@contextmanager
def span_in_trace(name: str, traceparent: str | None, **attrs: str | int) -> Iterator[Span]:
    """Start a span as a child of the stored `traceparent` (or a new root if absent)."""
    parent = propagate.extract({"traceparent": traceparent}) if traceparent else None
    token = context.attach(parent) if parent is not None else None
    try:
        with tracer.start_as_current_span(name, attributes=dict(attrs)) as span:
            yield span
    finally:
        if token is not None:
            context.detach(token)
