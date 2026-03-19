"""TDD tests for InsideAirbnb S3 upload behavior."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "insideairbnb_execute_under_test_upload"
MODULE_FILE = REPO_ROOT / "extract" / "airbnb" / "src" / "execute.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_execute_module() -> ModuleType:
    fake_boto3 = types.SimpleNamespace(client=MagicMock(name="boto3_client"))
    fake_fire = types.SimpleNamespace(Fire=MagicMock(name="Fire"))

    with patch.dict(sys.modules, {"boto3": fake_boto3, "fire": fake_fire}):
        if MODULE_NAME in sys.modules:
            del sys.modules[MODULE_NAME]
        spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


@pytest.fixture
def execute_module() -> ModuleType:
    return load_execute_module()


def test_main_streams_each_market_snapshot_to_s3(
    execute_module: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expect main() to stream every resolved market snapshot to S3."""
    html = """https://data.insideairbnb.com/united-kingdom/england/bristol/2025-09-26/data/listings.csv.gz
https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz"""
    response = MagicMock()
    response.text = html

    config_path = tmp_path / "airbnb.yml"
    config_path.write_text(
        """globals:
  data_index_url: https://insideairbnb.com/data/
markets:
  - city: bristol
    country_slug: united-kingdom
    region_slug: england
    market_slug: bristol
  - city: london
    country_slug: united-kingdom
    region_slug: england
    market_slug: london
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        execute_module,
        "get_market_urls",
        lambda _: {
            "bristol_2025-09-26": (
                "https://data.insideairbnb.com/united-kingdom/england/bristol/2025-09-26/data/listings.csv.gz"
            ),
            "london_2025-09-14": (
                "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz"
            ),
        },
    )

    monkeypatch.setattr(execute_module, "stream_to_s3", MagicMock())

    execute_module.main()

    execute_module.stream_to_s3.assert_any_call(
        url="https://data.insideairbnb.com/united-kingdom/england/bristol/2025-09-26/data/listings.csv.gz",
        key="raw/airbnb/bristol_2025-09-26.csv.gz",
    )
    execute_module.stream_to_s3.assert_any_call(
        url="https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz",
        key="raw/airbnb/london_2025-09-14.csv.gz",
    )
