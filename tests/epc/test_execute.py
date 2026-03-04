import importlib.util
import logging
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "epc_execute_under_test"
MODULE_FILE = REPO_ROOT / "extract" / "epc" / "src" / "execute.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_execute_module():
    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]

    spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stream_to_s3_calls_airflow_helper_and_returns_true(monkeypatch):
    execute_module = load_execute_module()
    captured: dict[str, object] = {}

    def fake_stream(**kwargs):
        captured.update(kwargs)
        return {
            "ok": True,
            "status_code": "SUCCESS",
            "message": "ok",
            "http_status": 200,
        }

    monkeypatch.setattr(
        execute_module, "stream_url_to_s3", types.SimpleNamespace(function=fake_stream)
    )
    pipeline = execute_module.EPCPipeline(
        execute_module.EPCConfig(
            bucket="quibbler-house-data-lake",
            auth_token="token-value",
            user_agent="ua/1.0",
        )
    )

    success = pipeline._stream_to_s3("2025")

    assert success is True
    assert captured["url"] == "https://epc.opendatacommunities.org/api/v1/files/domestic-2025.zip"
    assert captured["bucket"] == "quibbler-house-data-lake"
    assert captured["s3_key"] == "raw/epc/2025/domestic-2025.zip"
    assert captured["headers"]["Authorization"] == "Basic token-value"
    assert captured["headers"]["User-Agent"] == "ua/1.0"
    assert captured["raise_on_error"] is False


def test_stream_to_s3_returns_false_on_not_found_without_error_log(monkeypatch, caplog):
    execute_module = load_execute_module()

    def fake_stream(**kwargs):
        return {
            "ok": False,
            "status_code": "HTTP_NOT_FOUND",
            "message": "missing",
            "http_status": 404,
        }

    monkeypatch.setattr(
        execute_module, "stream_url_to_s3", types.SimpleNamespace(function=fake_stream)
    )
    caplog.set_level(logging.INFO, logger=execute_module.logger.name)
    pipeline = execute_module.EPCPipeline()

    assert pipeline._stream_to_s3("2025-01") is False
    assert not [rec for rec in caplog.records if rec.levelno >= logging.ERROR]


def test_stream_to_s3_other_failures_are_info_only(monkeypatch, caplog):
    execute_module = load_execute_module()

    def fake_stream(**kwargs):
        return {
            "ok": False,
            "status_code": "S3_UPLOAD_FAILED",
            "message": "upload failed",
            "http_status": 200,
        }

    monkeypatch.setattr(
        execute_module, "stream_url_to_s3", types.SimpleNamespace(function=fake_stream)
    )
    caplog.set_level(logging.INFO, logger=execute_module.logger.name)
    pipeline = execute_module.EPCPipeline()

    assert pipeline._stream_to_s3("2025-01") is False
    assert not [rec for rec in caplog.records if rec.levelno >= logging.ERROR]


def test_bulk_calls_stream_for_each_year(monkeypatch):
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline()
    called: list[str] = []

    monkeypatch.setattr(pipeline, "_stream_to_s3", lambda identifier: called.append(identifier))
    pipeline.bulk(start_year=2024, end_year=2025)

    assert called == ["2024", "2025"]


def test_incremental_specific_month_calls_single_identifier(monkeypatch):
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline()
    called: list[str] = []

    monkeypatch.setattr(pipeline, "_stream_to_s3", lambda identifier: called.append(identifier))
    pipeline.incremental(year=2026, month=2)

    assert called == ["2026-02"]


def test_incremental_scan_breaks_after_current_month_failure(monkeypatch):
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline()
    called: list[str] = []

    class _FakeNow:
        @staticmethod
        def date():
            class _FakeDate:
                @staticmethod
                def isoformat():
                    return "2026-03-04"

            return _FakeDate()

        year = 2026
        month = 3

    class _FakeDatetime:
        @staticmethod
        def now():
            return _FakeNow()

    def fake_stream(identifier: str):
        called.append(identifier)
        # Fail at current month, then scan should stop.
        return identifier != "2026-03"

    monkeypatch.setattr(execute_module, "datetime", _FakeDatetime)
    monkeypatch.setattr(pipeline, "_stream_to_s3", fake_stream)
    pipeline.incremental(year=2026)

    assert called == ["2026-01", "2026-02", "2026-03"]


def test_pipeline_requires_auth_token():
    execute_module = load_execute_module()
    config = execute_module.EPCConfig(auth_token="")
    try:
        execute_module.EPCPipeline(config=config)
        raise AssertionError("Expected ValueError for empty auth token")
    except ValueError as exc:
        assert "EPC auth token is required" in str(exc)
