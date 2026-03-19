"""Test airflow utility helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from include import airflow_utils

if TYPE_CHECKING:
    import pytest


def test_slack_failed_task_invokes_notification(monkeypatch: pytest.MonkeyPatch) -> None:
    """slack_failed_task should forward the context to the Slack notifier."""
    called: dict[str, Any] = {}

    def fake_send_slack_notification(**kwargs: Any) -> Callable[[dict[str, object]], None]:
        called["kwargs"] = kwargs

        def _inner(context: dict[str, object]) -> None:
            called["context"] = context

        return _inner

    monkeypatch.setattr(airflow_utils, "send_slack_notification", fake_send_slack_notification)

    context: dict[str, object] = {"dag": "test"}
    airflow_utils.slack_failed_task(context)

    assert called["kwargs"]["channel"] == "#data-science-pipelines"
    assert called["context"] == context
