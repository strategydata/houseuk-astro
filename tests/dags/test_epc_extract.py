"""Tests for the EPC extraction Airflow DAG."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "epc_extract_dag_under_test"
MODULE_FILE = REPO_ROOT / "dags" / "extract" / "epc_extract.py"


def load_module() -> ModuleType:
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_epc_extract_dag_builds_task() -> None:
    module = load_module()
    dag = module.epc_extract()
    task = dag.get_task("epc_extract_task")
    command = task.arguments[0]

    assert "python extract/epc/src/execute.py incremental" in command
    assert module.EPC_AUTH_TOKEN in task.secrets
