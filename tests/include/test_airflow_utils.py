import sys
from pathlib import Path

import pytest
import requests
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from include.airflow_utils import StreamUrlToS3Error, stream_url_to_s3


class _FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code
        self.raw = object()

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _S3Ok:
    def upload_fileobj(self, raw, bucket: str, key: str) -> None:
        return None


class _S3Fail:
    def upload_fileobj(self, raw, bucket: str, key: str) -> None:
        raise ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "denied"}},
            "PutObject",
        )


def _fn():
    return stream_url_to_s3.function


def test_stream_url_to_s3_success_returns_structured_result(monkeypatch):
    monkeypatch.setattr(
        "include.airflow_utils.requests.get", lambda *args, **kwargs: _FakeResponse(200)
    )
    monkeypatch.setattr("include.airflow_utils.boto3.client", lambda *args, **kwargs: _S3Ok())

    result = _fn()("https://example.com/source.zip", "bucket", "key", raise_on_error=False)

    assert result == {
        "ok": True,
        "status_code": "SUCCESS",
        "message": "Upload completed successfully.",
        "http_status": 200,
    }


def test_stream_url_to_s3_maps_404_without_raising(monkeypatch):
    monkeypatch.setattr(
        "include.airflow_utils.requests.get", lambda *args, **kwargs: _FakeResponse(404)
    )
    monkeypatch.setattr("include.airflow_utils.boto3.client", lambda *args, **kwargs: _S3Ok())

    result = _fn()("https://example.com/missing.zip", "bucket", "key", raise_on_error=False)

    assert result["ok"] is False
    assert result["status_code"] == "HTTP_NOT_FOUND"
    assert result["http_status"] == 404
    assert "404" in result["message"]


def test_stream_url_to_s3_raises_status_error_by_default(monkeypatch):
    monkeypatch.setattr(
        "include.airflow_utils.requests.get", lambda *args, **kwargs: _FakeResponse(404)
    )
    monkeypatch.setattr("include.airflow_utils.boto3.client", lambda *args, **kwargs: _S3Ok())

    with pytest.raises(StreamUrlToS3Error) as exc:
        _fn()("https://example.com/missing.zip", "bucket", "key")
    assert "HTTP_NOT_FOUND" in str(exc.value)


def test_stream_url_to_s3_maps_s3_upload_error_without_raising(monkeypatch):
    monkeypatch.setattr(
        "include.airflow_utils.requests.get", lambda *args, **kwargs: _FakeResponse(200)
    )
    monkeypatch.setattr("include.airflow_utils.boto3.client", lambda *args, **kwargs: _S3Fail())

    result = _fn()("https://example.com/source.zip", "bucket", "key", raise_on_error=False)

    assert result["ok"] is False
    assert result["status_code"] == "S3_UPLOAD_FAILED"
    assert result["http_status"] == 200


def test_stream_url_to_s3_raises_on_invalid_input_by_default():
    with pytest.raises(StreamUrlToS3Error) as exc:
        _fn()("", "bucket", "key")
    assert "INVALID_INPUT" in str(exc.value)


def test_stream_url_to_s3_maps_timeout_without_raising(monkeypatch):
    def _raise_timeout(*args, **kwargs):
        raise requests.Timeout("timeout")

    monkeypatch.setattr("include.airflow_utils.requests.get", _raise_timeout)
    monkeypatch.setattr("include.airflow_utils.boto3.client", lambda *args, **kwargs: _S3Ok())

    result = _fn()("https://example.com/source.zip", "bucket", "key", raise_on_error=False)

    assert result["ok"] is False
    assert result["status_code"] == "REQUEST_TIMEOUT"
    assert result["http_status"] is None
