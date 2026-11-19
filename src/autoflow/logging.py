"""Structured logging helpers for Autoflow."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, TextIO

_RESERVED = {
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
    "message",
    "asctime",
}


class ContextFilter(logging.Filter):
    """Inject default contextual fields onto every log record."""

    def __init__(self, **fields: Any) -> None:
        super().__init__()
        self.fields = dict(fields)

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in self.fields.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            try:
                json.dumps(value)
                payload[key] = value
            except (TypeError, ValueError):
                payload[key] = repr(value)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class PrettyFormatter(logging.Formatter):
    """Human-readable formatter used on an interactive console."""

    def format(self, record: logging.LogRecord) -> str:
        extra_bits = []
        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            extra_bits.append(f"{key}={value}")
        suffix = f" [{' '.join(extra_bits)}]" if extra_bits else ""
        message = record.getMessage()
        return f"{record.levelname:<8} {record.name}: {message}{suffix}"


def configure_logging(
    *,
    level: str = "INFO",
    json_output: bool = False,
    stream: TextIO | None = None,
    context: dict[str, Any] | None = None,
) -> logging.Logger:
    """Configure the ``autoflow`` logger and return it.

    Safe to call more than once: existing handlers on the autoflow logger are
    replaced so tests and the CLI can reconfigure freely.
    """
    logger = logging.getLogger("autoflow")
    logger.handlers.clear()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(JsonFormatter() if json_output else PrettyFormatter())
    if context:
        handler.addFilter(ContextFilter(**context))
    logger.addHandler(handler)
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a child of the autoflow logger (or the root autoflow logger)."""
    if name is None or name == "autoflow":
        return logging.getLogger("autoflow")
    if name.startswith("autoflow."):
        return logging.getLogger(name)
    return logging.getLogger(f"autoflow.{name}")
