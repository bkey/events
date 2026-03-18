import contextvars
from datetime import datetime, timezone
import json
import logging
import traceback
from typing import Any

# Populated per-request by TraceIDMiddleware; accessible from any log record
trace_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "trace_id", default="-"
)


class _TraceFilter(logging.Filter):
    """Injects the current trace ID into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = trace_id_var.get()
        return True


class _JSONFormatter(logging.Formatter):
    """Emits each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "trace_id": getattr(record, "trace_id", "-"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = traceback.format_exception(*record.exc_info)
        return json.dumps(payload)


def configure_logging(level: int = logging.INFO) -> None:
    """Replace the root handler with a structured JSON handler."""
    handler = logging.StreamHandler()
    handler.setFormatter(_JSONFormatter())
    handler.addFilter(_TraceFilter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
