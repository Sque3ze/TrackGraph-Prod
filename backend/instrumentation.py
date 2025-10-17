# Logging + metrics helpers for the FastAPI backend.
# Emits JSON logs and a couple of Prometheus counters with low overhead.

import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import Request
from urllib.parse import parse_qs
import hashlib
from starlette.responses import Response
from pythonjsonlogger import jsonlogger
from prometheus_client import Counter, Histogram


SERVICE_NAME = os.getenv("SERVICE_NAME", "backend")
LOG_METRICS_SCRAPES = os.getenv("LOG_METRICS_SCRAPES", "0").strip().lower() in ("1", "true", "yes", "on")


def _get_log_level() -> int:
    # Read LOG_LEVEL and fall back to INFO if it's missing or wrong.
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    return getattr(logging, level, logging.INFO)


def configure_logging() -> None:
    # Set up JSON logging once; skip if a handler is already attached.
    root = logging.getLogger()
    root.setLevel(_get_log_level())

    # Avoid adding duplicate JSON handlers
    has_json_handler = False
    for h in root.handlers:
        fmt = getattr(h, "formatter", None)
        if isinstance(fmt, jsonlogger.JsonFormatter):
            has_json_handler = True
            break

    if not has_json_handler:
        handler = logging.StreamHandler(stream=sys.stdout)
        formatter = jsonlogger.JsonFormatter()
        handler.setFormatter(formatter)
        root.addHandler(handler)


# Prometheus counters with low-cardinality labels.
REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)
REQUEST_LATENCY = Histogram(
    "http_request_latency_seconds",
    "Request latency seconds",
    ["method", "path"],
)

# Simple 'up' gauge so scrapes see the service.
from prometheus_client import Gauge
UP_METRIC = Gauge("up", "Whether the service is up (1) or down (0)")
UP_METRIC.set(1)  # Set to 1 to indicate the service is up


_request_logger = logging.getLogger("request")
_metrics_logger = logging.getLogger("metrics")


def _iso_timestamp() -> str:
    # Format a UTC timestamp like 2025-10-08T12:34:56.789Z.
    return datetime.now(timezone.utc).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _sanitize_query(path: str, query_str: Optional[str]):
    # Trim noisy query strings before logging them.
    if not query_str:
        return None
    try:
        # Minimize very long lists for batch endpoints
        if path in ("/api/tracks-batch", "/api/artists-batch"):
            q = parse_qs(query_str, keep_blank_values=True)
            ids_values = q.get("ids", [])
            if ids_values:
                joined = ",".join(ids_values)
                count = len([x for x in joined.split(",") if x])
                digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:10]
                return {"ids_count": count, "ids_hash": digest}
            return {}
        # Truncate extremely long queries in general
        if len(query_str) > 500:
            return {"len": len(query_str)}
        return query_str
    except Exception:
        return "<unparsed>"


async def request_logger_middleware(request: Request, call_next):
    # Log each request once and bump the Prometheus counters.
    request_id = str(uuid.uuid4())
    method = request.method
    path = request.url.path
    query_str = request.url.query or None
    xff = request.headers.get("x-forwarded-for")
    client_ip = (xff.split(",")[0].strip() if xff else (getattr(request.client, "host", None))) or None
    user_agent = request.headers.get("user-agent") or None
    host_hdr = request.headers.get("host") or None
    xfh = request.headers.get("x-forwarded-host") or None
    xfp = request.headers.get("x-forwarded-proto") or None
    xfp_port = request.headers.get("x-forwarded-port") or None

    start = time.perf_counter()
    status_code: Optional[int] = None
    response: Optional[Response] = None
    error_field: Optional[str] = None

    try:
        response = await call_next(request)
        status_code = int(getattr(response, "status_code", 200) or 200)
        return response
    except Exception as exc:
        status_code = 500
        # Keep the error concise: type + message
        error_field = f"{exc.__class__.__name__}: {exc}"
        raise
    finally:
        elapsed_s = max(time.perf_counter() - start, 0.0)
        latency_ms = int(round(elapsed_s * 1000))
        resp_bytes: Optional[int] = None
        if response is not None:
            clen = response.headers.get("content-length")
            if clen and clen.isdigit():
                try:
                    resp_bytes = int(clen)
                except ValueError:
                    resp_bytes = None

        # Skip metrics/logs for health endpoints.
        skip_paths = {"/healthz", "/metrics", "/metrics/"}

        # Update Prometheus if this path isn't skipped.
        if path not in skip_paths:
            try:
                if status_code is not None:
                    REQUEST_COUNT.labels(method=method, path=path, status=str(status_code)).inc()
                REQUEST_LATENCY.labels(method=method, path=path).observe(elapsed_s)
            except Exception:
                pass

        # Emit the JSON access log.
        if path not in skip_paths:
            log_fields = {
                "timestamp": _iso_timestamp(),
                "service": SERVICE_NAME,
                "request_id": request_id,
                "method": method,
                "path": path,
                "query": _sanitize_query(path, query_str),
                "status": status_code,
                "latency_ms": latency_ms,
                "client_ip": client_ip,
                "user_agent": user_agent,
                "host": host_hdr,
                "x_forwarded_host": xfh,
                "x_forwarded_proto": xfp,
                "x_forwarded_port": xfp_port,
                "resp_bytes": resp_bytes,
            }
            if error_field:
                log_fields["error"] = error_field

            if status_code and int(status_code) >= 500:
                _request_logger.error("request", extra=log_fields)
            else:
                _request_logger.info("request", extra=log_fields)

        # Echo request id back to the caller.
        if response is not None:
            try:
                response.headers["X-Request-ID"] = request_id
            except Exception:
                pass


# Set up logging as soon as the module loads.
configure_logging()

# Log a few OTEL env vars so we can see what the runtime picked up.
try:
    _startup_logger = logging.getLogger("startup")
    _startup_logger.info(
        "otel_env",
        extra={
            "otel_exporter_otlp_endpoint": os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
            "otel_service_name": os.getenv("OTEL_SERVICE_NAME"),
            "otel_resource_attributes": os.getenv("OTEL_RESOURCE_ATTRIBUTES"),
            "otel_propagators": os.getenv("OTEL_PROPAGATORS"),
        },
    )
except Exception:
    # Never fail startup because of this log.
    pass


def make_metrics_asgi_app():
    # Wrap the Prometheus ASGI app so we can log scrape headers.
    try:
        from prometheus_client import make_asgi_app  # type: ignore
    except Exception:
        async def _noop(scope, receive, send):
            from starlette.responses import PlainTextResponse
            resp = PlainTextResponse("metrics unavailable\n", status_code=503)
            await resp(scope, receive, send)
        return _noop

    prom_app = make_asgi_app()

    async def _wrapped(scope, receive, send):
        if LOG_METRICS_SCRAPES and scope.get("type") == "http":
            headers = {k.decode("latin1"): v.decode("latin1") for k, v in scope.get("headers", [])}
            path = scope.get("path")
            _metrics_logger.info(
                "metrics_scrape",
                extra={
                    "timestamp": _iso_timestamp(),
                    "service": SERVICE_NAME,
                    "method": headers.get(":method") or "GET",
                    "path": path,
                    "host": headers.get("host"),
                    "x_forwarded_host": headers.get("x-forwarded-host"),
                    "x_forwarded_proto": headers.get("x-forwarded-proto"),
                    "x_forwarded_port": headers.get("x-forwarded-port"),
                    "user_agent": headers.get("user-agent"),
                },
            )
        return await prom_app(scope, receive, send)

    return _wrapped
