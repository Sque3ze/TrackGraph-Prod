"""Tracing and instrumentation utilities for the TrackGraph backend."""

from __future__ import annotations

import logging
import os
from typing import Optional, Sequence
from fastapi import FastAPI

try:
    from backend import history
except ModuleNotFoundError:
    pass


def _is_low_value_span_value(value: object) -> bool:
    """Return True if the value clearly references low-value routes (/healthz, /metrics)."""
    if value is None:
        return False
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", "ignore")
        except Exception:
            return False
    text = str(value).strip().lower()
    if not text:
        return False
    suffixes = ("/healthz", "/metrics", "/metrics/")
    if any(text == suffix for suffix in suffixes):
        return True
    if any(text.endswith(suffix) for suffix in suffixes):
        return True
    prefixes = ("get /healthz", "head /healthz", "get /metrics", "head /metrics")
    return any(text.startswith(prefix) for prefix in prefixes)


def _span_is_low_value(span) -> bool:
    """Inspect name + common HTTP attributes to detect low-value spans (health/metrics)."""
    try:
        attributes = getattr(span, "attributes", {}) or {}
    except Exception:
        attributes = {}

    candidates = [
        getattr(span, "name", ""),
        attributes.get("http.target"),
        attributes.get("http.route"),
        attributes.get("http.path"),
        attributes.get("http.request.uri"),
        attributes.get("url.path"),
        attributes.get("http.url"),
        attributes.get("url.full"),
    ]
    return any(_is_low_value_span_value(value) for value in candidates)


def _configure_tracing(app: FastAPI) -> None:
    if getattr(app, "_otel_tracing_initialized", False):
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter as OTLPSpanExporterGRPC,
    )
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.propagate import set_global_textmap
    from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExportResult, SpanExporter

    # Decide protocol/endpoints from env
    proto = (
        os.getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        or os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
        or "grpc"
    ).strip().lower()
    traces_ep = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
    generic_ep = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

    exporter = None
    chosen: Optional[str] = None
    ep_used: Optional[str] = None

    if proto.startswith("http"):
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter as OTLPSpanExporterHTTP,
            )

            if traces_ep:
                ep_used = traces_ep
            elif generic_ep:
                ep_used = generic_ep.rstrip("/") + "/v1/traces"
            else:
                ep_used = "http://localhost:4318/v1/traces"
            exporter = OTLPSpanExporterHTTP(endpoint=ep_used)
            chosen = f"otlp-http:{ep_used}"
        except Exception:
            http_like = (traces_ep or generic_ep or "").startswith("http")
            if http_like:
                logging.getLogger("startup").warning(
                    "otel_exporter_unavailable",
                    extra={
                        "protocol": proto,
                        "endpoint": (traces_ep or generic_ep),
                        "reason": "missing otlp http exporter",
                    },
                )
                exporter = None
                chosen = "disabled"
            else:
                proto = "grpc"

    if exporter is None and proto == "grpc":
        ep_used = traces_ep or generic_ep or "grpc://localhost:4317"
        insecure = ep_used.startswith(("grpc://", "http://"))
        exporter = OTLPSpanExporterGRPC(endpoint=ep_used, insecure=insecure)
        chosen = f"otlp-grpc:{ep_used}"

    if chosen:
        logging.getLogger("startup").info(
            "otel_exporter",
            extra={"protocol": proto, "endpoint": ep_used, "chosen": chosen},
        )

    if exporter is not None:

        class _LowValueFilterSpanExporter(SpanExporter):
            """Wrap another exporter and drop health/metrics spans before export."""

            def __init__(self, delegate: SpanExporter):
                self._delegate = delegate

            def export(self, spans: Sequence):
                filtered = [span for span in spans if not _span_is_low_value(span)]
                if not filtered:
                    return SpanExportResult.SUCCESS
                return self._delegate.export(filtered)

            def shutdown(self) -> None:
                return self._delegate.shutdown()

            def force_flush(self, timeout_millis: int | None = None) -> bool:
                if timeout_millis is None:
                    return self._delegate.force_flush()
                return self._delegate.force_flush(timeout_millis=timeout_millis)

        exporter = _LowValueFilterSpanExporter(exporter)

    resource = Resource.create(
        {
            "service.name": os.getenv("OTEL_SERVICE_NAME", "backend"),
            "service.namespace": "trackgraph",
        }
    )

    provider = TracerProvider(resource=resource, id_generator=AwsXRayIdGenerator())
    trace.set_tracer_provider(provider)

    from opentelemetry.propagators.aws import AwsXRayPropagator

    set_global_textmap(AwsXRayPropagator())

    if exporter is not None:
        provider.add_span_processor(BatchSpanProcessor(exporter))
    else:
        logging.getLogger("startup").info(
            "otel_exporter_disabled", extra={"protocol": proto, "endpoint": ep_used}
        )

    FastAPIInstrumentor().instrument_app(app, excluded_urls=r"^/(healthz|metrics)$")
    RequestsInstrumentor().instrument()

    setattr(app, "_otel_tracing_initialized", True)


def setup(app: FastAPI, demo_mode: bool) -> None:
    """Wire tracing, logging, and metrics for the application."""
    if demo_mode:
        # Skip tracing + instrumentation in demo mode.
        return

    try:
        _configure_tracing(app)
    except Exception:
        # Never break the app if tracing setup fails.
        pass

    try:
        from instrumentation import make_metrics_asgi_app, request_logger_middleware

        if not getattr(app, "_instrumentation_initialized", False):
            app.middleware("http")(request_logger_middleware)
            app.mount("/metrics", make_metrics_asgi_app())
            setattr(app, "_instrumentation_initialized", True)
    except Exception:
        # Never break the app if instrumentation fails to import.
        pass
