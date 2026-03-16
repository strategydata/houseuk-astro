import importlib.util
import logging
import sys
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


def test_stream_to_s3_calls_helper_and_returns_true(monkeypatch):
    execute_module = load_execute_module()
    captured: dict[str, object] = {}

    def fake_stream(*, url, bucket, key, headers, connect_timeout_seconds=10.0, read_timeout_seconds=300.0):
        captured.update(
            {
                "url": url,
                "bucket": bucket,
                "key": key,
                "headers": headers,
                "connect_timeout_seconds": connect_timeout_seconds,
                "read_timeout_seconds": read_timeout_seconds,
            },
        )

    monkeypatch.setattr(execute_module, "stream_to_s3", fake_stream)
    pipeline = execute_module.EPCPipeline(
        execute_module.EPCConfig(
            bucket="quibbler-house-data-lake",
            auth_token="token-value",
            user_agent="ua/1.0",
        ),
    )

    success = pipeline._stream_to_s3("2025")

    assert success is True
    assert captured["url"] == "https://epc.opendatacommunities.org/api/v1/files/domestic-2025.zip"
    assert captured["bucket"] == "quibbler-house-data-lake"
    assert captured["key"] == "raw/epc/2025/domestic-2025.zip"
    assert captured["headers"]["Authorization"] == "Basic token-value"
    assert captured["headers"]["User-Agent"] == "ua/1.0"


def test_stream_to_s3_returns_false_on_http_error(monkeypatch, caplog):
    execute_module = load_execute_module()

    def fake_stream(**kwargs):
        raise execute_module.requests.HTTPError("404 Not Found")

    monkeypatch.setattr(execute_module, "stream_to_s3", fake_stream)
    caplog.set_level(logging.INFO, logger=execute_module.logger.name)
    pipeline = execute_module.EPCPipeline(execute_module.EPCConfig(auth_token="token-value"))

    assert pipeline._stream_to_s3("2025-01") is False
    assert not [rec for rec in caplog.records if rec.levelno >= logging.ERROR]


def test_stream_to_s3_s3_failures_are_info_only(monkeypatch, caplog):
    execute_module = load_execute_module()

    def fake_stream(**kwargs):
        raise execute_module.BotoCoreError()

    monkeypatch.setattr(execute_module, "stream_to_s3", fake_stream)
    caplog.set_level(logging.INFO, logger=execute_module.logger.name)
    pipeline = execute_module.EPCPipeline(execute_module.EPCConfig(auth_token="token-value"))

    assert pipeline._stream_to_s3("2025-01") is False
    assert not [rec for rec in caplog.records if rec.levelno >= logging.ERROR]


def test_bulk_calls_stream_for_each_year(monkeypatch):
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline(execute_module.EPCConfig(auth_token="token-value"))
    called: list[str] = []

    monkeypatch.setattr(pipeline, "_stream_to_s3", lambda identifier: called.append(identifier))
    pipeline.bulk(start_year=2024, end_year=2025)

    assert called == ["2024", "2025"]


def test_incremental_specific_month_calls_single_identifier(monkeypatch):
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline(execute_module.EPCConfig(auth_token="token-value"))
    called: list[str] = []

    monkeypatch.setattr(pipeline, "_stream_to_s3", lambda identifier: called.append(identifier))
    pipeline.incremental(year=2026, month=2)

    assert called == ["2026-02"]


def test_incremental_scan_breaks_after_current_month_failure(monkeypatch):
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline(execute_module.EPCConfig(auth_token="token-value"))
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


def test_pipeline_reads_auth_token_from_env(monkeypatch):
    monkeypatch.setenv("EPC_AUTH_TOKEN", "from-env-token")
    execute_module = load_execute_module()
    pipeline = execute_module.EPCPipeline()

    assert pipeline.config.auth_token == "from-env-token"
