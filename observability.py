"""Observability utilities for logging, metrics, and tracing."""

from __future__ import annotations

import contextlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.responses import Response

try:  # pragma: no cover - optional dependency
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
except ModuleNotFoundError:  # pragma: no cover - executed when prometheus not installed
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"

    class _NoOpMetric:
        def labels(self, *_args: Any, **_kwargs: Any) -> "_NoOpMetric":
            return self

        def inc(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def set(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    def _counter(*_args: Any, **_kwargs: Any) -> _NoOpMetric:
        return _NoOpMetric()

    def _gauge(*_args: Any, **_kwargs: Any) -> _NoOpMetric:
        return _NoOpMetric()

    def _generate_latest() -> bytes:
        return b""

    Counter = _counter  # type: ignore[assignment]
    Gauge = _gauge  # type: ignore[assignment]
    generate_latest = _generate_latest  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from opentelemetry import trace as _ot_trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter as _OTLPSpanExporter,
    )
    from opentelemetry.instrumentation.fastapi import (
        FastAPIInstrumentor as _FastAPIInstrumentor,
    )
    from opentelemetry.sdk.resources import Resource as _Resource
    from opentelemetry.sdk.trace import TracerProvider as _TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor as _BatchSpanProcessor,
        ConsoleSpanExporter as _ConsoleSpanExporter,
    )
except ModuleNotFoundError:  # pragma: no cover - executed when tracing not installed
    _ot_trace = None
    _OTLPSpanExporter = None
    _FastAPIInstrumentor = None
    _Resource = None
    _TracerProvider = None
    _BatchSpanProcessor = None
    _ConsoleSpanExporter = None

ot_trace = _ot_trace
OTLPSpanExporter = _OTLPSpanExporter
FastAPIInstrumentor = _FastAPIInstrumentor
Resource = _Resource
TracerProvider = _TracerProvider
BatchSpanProcessor = _BatchSpanProcessor
ConsoleSpanExporter = _ConsoleSpanExporter

__all__ = [
    "CONNECTION_EVENTS",
    "CONNECTION_GAUGE",
    "ERROR_COUNTER",
    "MESSAGE_COUNTER",
    "configure_observability",
    "get_tracer",
]


CONNECTION_GAUGE = Gauge(
    "swifty_active_connections",
    "Number of active WebSocket connections maintained by the server.",
)
CONNECTION_EVENTS = Counter(
    "swifty_connection_events_total",
    "Total number of WebSocket connection lifecycle events.",
    ["event"],
)
MESSAGE_COUNTER = Counter(
    "swifty_messages_total",
    "Total number of messages processed by the server.",
    ["direction", "channel"],
)
ERROR_COUNTER = Counter(
    "swifty_errors_total",
    "Total number of errors encountered by the server.",
    ["type"],
)


class JsonFormatter(logging.Formatter):
    """Logging formatter that emits JSON payloads for structured logging."""

    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03d"

    _reserved = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
    }

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        data: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        correlation_id = getattr(record, "correlation_id", None)
        if correlation_id:
            data["correlation_id"] = correlation_id

        extra: Dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key in self._reserved or key.startswith("_"):
                continue
            if key == "correlation_id":
                continue
            extra[key] = value

        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            data["stack"] = record.stack_info
        if extra:
            data.update(extra)

        return json.dumps(data, default=str)


def configure_logging() -> None:
    """Configure application logging for structured JSON output."""

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    root.addHandler(handler)


def _parse_otlp_headers(raw_headers: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    for part in raw_headers.split(","):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        headers[key.strip()] = value.strip()
    return headers


_tracing_configured = False


def configure_tracing(app: FastAPI) -> None:
    """Configure OpenTelemetry tracing for the FastAPI application."""

    if (
        ot_trace is None
        or TracerProvider is None
        or FastAPIInstrumentor is None
        or BatchSpanProcessor is None
        or Resource is None
    ):
        return

    global _tracing_configured
    if _tracing_configured:
        return

    resource = Resource.create({"service.name": "swifty-server"})
    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if otlp_endpoint and OTLPSpanExporter is not None:
        headers_raw = os.getenv("OTEL_EXPORTER_OTLP_HEADERS", "")
        headers = _parse_otlp_headers(headers_raw) if headers_raw else None
        span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, headers=headers))
    else:
        if ConsoleSpanExporter is None:
            return
        span_processor = BatchSpanProcessor(ConsoleSpanExporter())

    provider.add_span_processor(span_processor)
    ot_trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
    _tracing_configured = True


def configure_metrics(app: FastAPI) -> None:
    """Expose Prometheus metrics via the standard /metrics endpoint."""

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        data = generate_latest()
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)


def configure_observability(app: FastAPI) -> None:
    """Configure logging, metrics, and tracing for the application."""

    configure_logging()
    configure_metrics(app)
    configure_tracing(app)


class _NoOpTracer:
    """Fallback tracer used when OpenTelemetry is not installed."""

    def start_as_current_span(self, *_args: Any, **_kwargs: Any):  # noqa: D401
        return contextlib.nullcontext()


def get_tracer(name: str):
    """Return an OpenTelemetry tracer or a no-op implementation when unavailable."""

    if not ot_trace:
        return _NoOpTracer()
    return ot_trace.get_tracer(name)
