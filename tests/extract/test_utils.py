"""Tests for extract.utils helpers."""

from __future__ import annotations

import io
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from extract import utils


def test_make_request_get_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return the response when GET succeeds."""
    response = MagicMock()
    response.raise_for_status.return_value = None

    def fake_get(url: str, **kwargs: Any) -> MagicMock:
        assert url == "https://example.com"
        assert "timeout" in kwargs
        return response

    monkeypatch.setattr(utils.requests, "get", fake_get)

    result = utils.make_request("GET", "https://example.com", timeout=5)

    assert result is response


def test_make_request_invalid_method_raises() -> None:
    """Reject unsupported request methods."""
    with pytest.raises(ValueError, match="Invalid request type"):
        utils.make_request("PUT", "https://example.com")


def test_make_request_retries_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry when a timeout occurs."""
    response = MagicMock()
    response.raise_for_status.return_value = None
    calls: list[str] = []

    def fake_get(url: str, **kwargs: Any) -> MagicMock:  # noqa: ARG001
        calls.append(url)
        if len(calls) == 1:
            raise requests.exceptions.Timeout
        return response

    monkeypatch.setattr(utils.requests, "get", fake_get)

    result = utils.make_request("GET", "https://example.com", timeout=1, max_retry_count=2)

    assert result is response
    assert len(calls) == 2


def test_make_request_retries_after_429(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sleep and retry when a 429 response is received."""
    sleep_calls: list[int] = []

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)

    response_429 = MagicMock()
    response_429.status_code = utils.HTTP_TOO_MANY_REQUESTS
    response_429.headers = {"Retry-After": "1"}
    response_429.raise_for_status.side_effect = requests.exceptions.RequestException

    response_ok = MagicMock()
    response_ok.raise_for_status.return_value = None

    responses = [response_429, response_ok]

    def fake_get(url: str, **kwargs: Any) -> MagicMock:  # noqa: ARG001
        return responses.pop(0)

    monkeypatch.setattr(utils.requests, "get", fake_get)
    monkeypatch.setattr(utils.time, "sleep", fake_sleep)

    result = utils.make_request("GET", "https://example.com", timeout=1, max_retry_count=2)

    assert result is response_ok
    assert sleep_calls == [21]


def test_make_request_raises_for_non_429(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise the original exception when response is not 429."""
    response = MagicMock()
    response.status_code = 500
    response.raise_for_status.side_effect = requests.exceptions.RequestException

    monkeypatch.setattr(utils.requests, "get", lambda url, **kwargs: response)  # noqa: ARG005

    with pytest.raises(requests.exceptions.RequestException):
        utils.make_request("GET", "https://example.com")


def test_stream_to_s3_uploads_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stream response content into S3."""
    s3_client = MagicMock()
    response = MagicMock()
    response.__enter__.return_value = response
    response.__exit__.return_value = None
    response.raw = io.BytesIO(b"data")
    response.raise_for_status.return_value = None

    monkeypatch.setattr(utils.boto3, "client", lambda *args, **kwargs: s3_client)  # noqa: ARG005
    monkeypatch.setattr(utils.requests, "get", lambda *args, **kwargs: response)  # noqa: ARG005

    utils.stream_to_s3("https://example.com/file", "raw/test/file.csv", args={"bucket": "bucket"})

    s3_client.upload_fileobj.assert_called_once_with(response.raw, "bucket", "raw/test/file.csv")
