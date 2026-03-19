"""Test the EPC execution pipeline."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "epc_execute_under_test"
MODULE_FILE = REPO_ROOT / "extract" / "epc" / "src" / "execute.py"


def load_execute_module() -> ModuleType:
    """Load the EPC execute module from its file path."""
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]

    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bulk_calls_stream_for_each_year(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test bulk mode streams each year in the range."""
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline(execute_module.EPCConfig(auth_token="token-value"))
    called: list[str] = []

    def fake_stream(identifier: str) -> None:
        called.append(identifier)

    monkeypatch.setattr(pipeline, "_stream_to_s3", fake_stream)
    pipeline.bulk(start_year=2024, end_year=2025)

    assert called == ["2024", "2025"]


def test_incremental_specific_month_calls_single_identifier(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test incremental mode for a specific month streams once."""
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline(execute_module.EPCConfig(auth_token="token-value"))
    called: list[str] = []

    def fake_stream(identifier: str) -> None:
        called.append(identifier)

    monkeypatch.setattr(pipeline, "_stream_to_s3", fake_stream)
    pipeline.incremental(year=2026, month=2)

    assert called == ["2026-02"]


def test_pipeline_requires_auth_token() -> None:
    """Test that the pipeline enforces a non-empty auth token."""
    execute_module = load_execute_module()
    config = execute_module.EPCConfig(auth_token="")
    with pytest.raises(ValueError, match="EPC auth token is required") as exc:
        execute_module.EPCPipeline(config=config)
    assert "EPC auth token is required" in str(exc.value)


def test_pipeline_reads_auth_token_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test auth token is read from the environment."""
    monkeypatch.setenv("EPC_AUTH_TOKEN", "from-env-token")
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline()

    assert pipeline.config.auth_token == "from-env-token"
