import importlib.util
import sys
import types
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "insideairbnb_execute_under_test"
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


def test_get_market_urls_returns_latest_for_city(execute_module: ModuleType, tmp_path: Path) -> None:
    html = """https://data.insideairbnb.com/united-kingdom/england/bristol/2025-03-19/data/listings.csv.gz
https://data.insideairbnb.com/united-kingdom/england/bristol/2025-09-26/data/listings.csv.gz
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
""",
        encoding="utf-8",
    )

    with patch.object(execute_module, "make_request", return_value=response) as mock_request:
        results = execute_module.get_market_urls(str(config_path))

    mock_request.assert_called_once_with("GET", "https://insideairbnb.com/data/", timeout=30)
    assert results == {
        "bristol_2025-03-19": (
            "https://data.insideairbnb.com/united-kingdom/england/bristol/2025-03-19/data/listings.csv.gz"
        ),
    }


def test_get_market_urls_returns_empty_when_no_match(
    execute_module: ModuleType,
    tmp_path: Path,
) -> None:
    response = MagicMock()
    response.text = "https://example.com/no-listings-link"

    config_path = tmp_path / "airbnb.yml"
    config_path.write_text(
        """globals:
  data_index_url: https://insideairbnb.com/data/
markets:
  - city: bristol
    country_slug: united-kingdom
    region_slug: england
    market_slug: bristol
""",
        encoding="utf-8",
    )

    with patch.object(execute_module, "make_request", return_value=response):
        results = execute_module.get_market_urls(str(config_path))

    assert results == {}
