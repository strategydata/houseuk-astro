"""Shared logging helpers for non-Airflow entrypoints.

Airflow configures logging itself, so DAG/task modules should not call
``configure_logging`` from this module.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

_DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_RESERVED_LOG_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


class JsonFormatter(logging.Formatter):
    """Serialize log records as JSON for machine parsing."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON."""
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        payload.update(
            {
                key: value
                for key, value in record.__dict__.items()
                if key not in _RESERVED_LOG_RECORD_FIELDS and not key.startswith("_")
            }
        )

        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger."""
    return logging.getLogger(name)


def configure_json_logging(
    level: str | None = None,
) -> None:
    """Configure root logging for non-Airflow scripts.

    Environment variables:
    - ``LOG_LEVEL`` (default: ``INFO``)
    """
    configured_level = level if level is not None else os.getenv("LOG_LEVEL")
    resolved_level = (configured_level or "INFO").upper()

    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.setLevel(resolved_level)
        for handler in root_logger.handlers:
            handler.setLevel(resolved_level)
        return

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.setLevel(resolved_level)

    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(resolved_level)


def _parse_bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}
