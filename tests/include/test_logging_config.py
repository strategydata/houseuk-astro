"""Tests for JSON logging helpers."""

from __future__ import annotations

import json
import logging

from include import logging_config


def _reset_root_logger() -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(logging.NOTSET)


def test_json_formatter_includes_extra_fields() -> None:
    """Extra fields should be serialized into the JSON payload."""
    formatter = logging_config.JsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.custom_field = "custom"

    payload = json.loads(formatter.format(record))

    assert payload["message"] == "hello"
    assert payload["custom_field"] == "custom"


def test_configure_json_logging_initializes_handler() -> None:
    """configure_json_logging should attach a JSON handler when none exist."""
    _reset_root_logger()

    logging_config.configure_json_logging(level="DEBUG")

    root = logging.getLogger()
    assert root.handlers
    assert root.level == logging.DEBUG
    assert isinstance(root.handlers[0].formatter, logging_config.JsonFormatter)


def test_configure_json_logging_updates_existing_handlers() -> None:
    """Existing handlers should be reused and have their level updated."""
    _reset_root_logger()

    handler = logging.StreamHandler()
    handler.setLevel(logging.WARNING)
    root = logging.getLogger()
    root.addHandler(handler)

    logging_config.configure_json_logging(level="INFO")

    assert root.level == logging.INFO
    assert handler.level == logging.INFO
    assert len(root.handlers) == 1

    _reset_root_logger()
