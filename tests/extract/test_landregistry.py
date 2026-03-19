"""Test the Land Registry extractor."""

from __future__ import annotations

import runpy
import sys
import types
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_FILE = REPO_ROOT / "extract" / "landregistry" / "src" / "execute.py"


def test_landregistry_execute_main_invokes_fire() -> None:
    called: dict[str, object] = {}

    def fake_fire(arg: object) -> None:
        called["arg"] = arg

    fake_fire_module = types.SimpleNamespace(Fire=fake_fire)

    with patch.dict(sys.modules, {"fire": fake_fire_module}):
        runpy.run_path(str(MODULE_FILE), run_name="__main__")

    assert callable(called["arg"])
