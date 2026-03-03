import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_NAME = "insideairbnb_execute_under_test"
MODULE_FILE = REPO_ROOT / "extract" / "airbnb" / "src" / "execute.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_execute_module():
    fake_boto3 = types.SimpleNamespace(client=MagicMock(name="boto3_client"))
    fake_fire = types.SimpleNamespace(Fire=MagicMock(name="Fire"))

    with patch.dict(sys.modules, {"boto3": fake_boto3, "fire": fake_fire}):
        if MODULE_NAME in sys.modules:
            del sys.modules[MODULE_NAME]
        spec = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_FILE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


@pytest.fixture
def execute_module():
    return load_execute_module()


def test_selects_latest_snapshot_for_market(execute_module):
    html = "\n".join(
        [
            "https://data.insideairbnb.com/united-kingdom/england/bristol/2025-03-19/data/listings.csv.gz",
            "https://data.insideairbnb.com/united-kingdom/england/bristol/2025-09-26/data/listings.csv.gz",
            "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz",
        ]
    )
    response = MagicMock()
    response.text = html

    with patch.object(execute_module.requests, "get", return_value=response) as mock_get:
        url, snapshot_date = execute_module.resolve_latest_listings_url(
            page_url="https://insideairbnb.com/bristol/",
            country_slug="united-kingdom",
            region_slug="england",
            market_slug="bristol",
        )

    mock_get.assert_called_once_with("https://insideairbnb.com/bristol/", timeout=30)
    response.raise_for_status.assert_called_once()
    assert (
        url
        == "https://data.insideairbnb.com/united-kingdom/england/bristol/2025-09-26/data/listings.csv.gz"
    )
    assert snapshot_date == "2025-09-26"


def test_raises_when_no_matching_dataset_link_found(execute_module):
    response = MagicMock()
    response.text = "https://example.com/no-listings-link"

    with patch.object(execute_module.requests, "get", return_value=response):
        with pytest.raises(ValueError, match="No listings dataset URL found"):
            execute_module.resolve_latest_listings_url(
                page_url="https://insideairbnb.com/bristol/",
                country_slug="united-kingdom",
                region_slug="england",
                market_slug="bristol",
            )


def test_uploads_dated_file_and_refreshes_latest_pointer(execute_module):
    resolved_url = "https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz"
    resolved_date = "2025-09-14"
    s3 = MagicMock()

    with patch.object(
        execute_module,
        "resolve_latest_listings_url",
        return_value=(resolved_url, resolved_date),
    ) as resolve_mock, patch.object(execute_module, "stream_to_s3") as stream_mock, patch.object(
        execute_module, "_s3_client", return_value=s3
    ):
        execute_module.extract_latest_market_snapshot(
            city="london",
            country_slug="united-kingdom",
            region_slug="england",
            market_slug="london",
            page_url="https://insideairbnb.com/london/",
            bucket="quibbler-house-data-lake",
        )

    resolve_mock.assert_called_once_with(
        page_url="https://insideairbnb.com/london/",
        country_slug="united-kingdom",
        region_slug="england",
        market_slug="london",
    )
    stream_mock.assert_called_once_with(
        url=resolved_url,
        bucket="quibbler-house-data-lake",
        key="raw/airbnb/london/listings_2025-09-14.csv.gz",
    )
    s3.copy_object.assert_called_once_with(
        Bucket="quibbler-house-data-lake",
        CopySource={
            "Bucket": "quibbler-house-data-lake",
            "Key": "raw/airbnb/london/listings_2025-09-14.csv.gz",
        },
        Key="raw/airbnb/london/latest/listings.csv.gz",
        MetadataDirective="REPLACE",
        Metadata={
            "source_url": resolved_url,
            "snapshot_date": "2025-09-14",
            "city": "london",
        },
    )
