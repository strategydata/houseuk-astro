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

        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_RECORD_FIELDS and not key.startswith("_"):
                payload[key] = value

        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger."""
    return logging.getLogger(name)


def configure_logging(
    level: str | None = None,
    json_logs: bool | None = None,
    force: bool = False,
) -> None:
    """Configure root logging for non-Airflow scripts.

    Environment variables:
    - ``LOG_LEVEL`` (default: ``INFO``)
    - ``LOG_JSON`` (default: ``false``)
    """

    configured_level = level if level is not None else os.getenv("LOG_LEVEL")
    resolved_level = (configured_level or "INFO").upper()
    use_json = (
        _parse_bool_env(os.getenv("LOG_JSON", "false")) if json_logs is None else json_logs
    )

    root_logger = logging.getLogger()
    if root_logger.handlers and not force:
        root_logger.setLevel(resolved_level)
        for handler in root_logger.handlers:
            handler.setLevel(resolved_level)
        return

    formatter: logging.Formatter
    if use_json:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(resolved_level)

    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(resolved_level)


def _parse_bool_env(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}
