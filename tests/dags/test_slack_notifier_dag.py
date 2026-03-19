"""Tests for the Slack notifier DAG."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "slack_notifier_dag_under_test"
MODULE_FILE = REPO_ROOT / "dags" / "notification" / "slack_notifier_dag.py"


def load_module() -> ModuleType:
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_slack_notifier_dag_has_failing_task() -> None:
    module = load_module()
    dag = module.slack_notifier_dag()
    assert "failing_task" in dag.task_ids

    task = dag.get_task("failing_task")
    with pytest.raises(ValueError, match="Simulated failure"):
        task.python_callable()
