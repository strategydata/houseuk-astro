"""Test the crime extractor."""

from __future__ import annotations

import importlib.util
import runpy
import sys
import types
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "crime_execute_under_test"
MODULE_FILE = REPO_ROOT / "extract" / "crime" / "current" / "execute.py"


def load_module() -> ModuleType:
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_crime_execute_main_invokes_fire() -> None:
    called: dict[str, object] = {}

    def fake_fire(arg: object) -> None:
        called["arg"] = arg

    fake_fire_module = types.SimpleNamespace(Fire=fake_fire)

    with patch.dict(sys.modules, {"fire": fake_fire_module}):
        runpy.run_path(str(MODULE_FILE), run_name="__main__")

    assert callable(called["arg"])
