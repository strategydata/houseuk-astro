"""Tests for the InsideAirbnb Airflow DAG."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "insideairbnb_dag_under_test"
MODULE_FILE = REPO_ROOT / "dags" / "extract" / "insideairbnb_extract.py"


def load_module() -> ModuleType:
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_insideairbnb_dag_builds_tasks_for_each_market() -> None:
    module = load_module()
    dag = module.insideairbnb_extract()

    assert len(dag.task_ids) == len(module.INSIDE_AIRBNB_MARKETS)

    for market in module.INSIDE_AIRBNB_MARKETS:
        task_id = f"insideairbnb_extract_{market['city']}"
        task = dag.get_task(task_id)
        command = task.arguments[0]
        assert f"--city '{market['city']}'" in command
        assert f"--country-slug '{market['country_slug']}'" in command
        assert f"--region-slug '{market['region_slug']}'" in command
        assert f"--market-slug '{market['market_slug']}'" in command
        assert f"--page-url '{market['page_url']}'" in command
        assert f"--bucket '{module.BUCKET}'" in command
