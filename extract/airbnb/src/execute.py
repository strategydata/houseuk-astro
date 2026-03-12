"""InsideAirbnb extractor entrypoint.

This module resolves the latest InsideAirbnb listings dataset for a market,
uploads the dated snapshot to S3, and refreshes a stable `latest` object key.
"""

import logging
import os
import re

import boto3
import requests
from fire import Fire

from extract.utils import stream_to_s3
from include.logging_config import configure_logging

LISTINGS_URL_PATTERN = re.compile(
    r"https://data\.insideairbnb\.com/(?P<country>[^/]+)/(?P<region>[^/]+)/(?P<market>[^/]+)/(?P<snapshot_date>\d{4}-\d{2}-\d{2})/data/listings\.csv\.gz",
)
logger = logging.getLogger(__name__)


def _s3_client():
    """Create an S3 client using environment-based AWS credentials.

    Returns
    -------
    botocore.client.S3
        Configured S3 client.

    """
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_account_id=os.getenv("AWS_ACCOUNT_ID"),
    )


def resolve_latest_listings_url(
    page_url: str,
    country_slug: str,
    region_slug: str,
    market_slug: str,
) -> tuple[str, str]:
    """Resolve the newest listings dataset URL for a target market page.

    Parameters
    ----------
    page_url : str
        InsideAirbnb market page URL (e.g. ``https://insideairbnb.com/london/``).
    country_slug : str
        Country path segment expected in the dataset URL.
    region_slug : str
        Region path segment expected in the dataset URL.
    market_slug : str
        Market path segment expected in the dataset URL.

    Returns
    -------
    tuple[str, str]
        A tuple ``(dataset_url, snapshot_date)`` where ``snapshot_date``
        is in ``YYYY-MM-DD`` format.

    Raises
    ------
    requests.HTTPError
        If the page request fails.
    ValueError
        If no matching listings URL is found.

    """
    response = requests.get(page_url, timeout=30)
    response.raise_for_status()

    matches = []
    for match in LISTINGS_URL_PATTERN.finditer(response.text):
        if (
            match.group("country") == country_slug
            and match.group("region") == region_slug
            and match.group("market") == market_slug
        ):
            matches.append(
                {
                    "url": match.group(0),
                    "snapshot_date": match.group("snapshot_date"),
                },
            )

    if not matches:
        raise ValueError(
            f"No listings dataset URL found for {country_slug}/{region_slug}/{market_slug} at {page_url}",
        )

    latest = max(matches, key=lambda item: item["snapshot_date"])
    return latest["url"], latest["snapshot_date"]


def extract_latest_market_snapshot(
    city: str,
    country_slug: str,
    region_slug: str,
    market_slug: str,
    page_url: str,
    bucket: str,
) -> None:
    """Download and publish the newest InsideAirbnb market snapshot.

    This function uploads a dated object and then copies it to a stable
    ``latest`` key with metadata for lineage.

    Parameters
    ----------
    city : str
        City identifier used in S3 object keys.
    country_slug : str
        Country segment used to match InsideAirbnb dataset URLs.
    region_slug : str
        Region segment used to match InsideAirbnb dataset URLs.
    market_slug : str
        Market segment used to match InsideAirbnb dataset URLs.
    page_url : str
        InsideAirbnb page URL for the market.
    bucket : str
        Target S3 bucket.

    Returns
    -------
    None
        Uploads objects to S3 as side effects.

    """
    url, snapshot_date = resolve_latest_listings_url(
        page_url=page_url,
        country_slug=country_slug,
        region_slug=region_slug,
        market_slug=market_slug,
    )

    dated_key = f"raw/airbnb/{city}/listings_{snapshot_date}.csv.gz"
    latest_key = f"raw/airbnb/{city}/latest/listings.csv.gz"

    stream_to_s3(url=url, bucket=bucket, key=dated_key)

    s3 = _s3_client()
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": dated_key},
        Key=latest_key,
        MetadataDirective="REPLACE",
        Metadata={
            "source_url": url,
            "snapshot_date": snapshot_date,
            "city": city,
        },
    )

    logger.info(
        "source=%s action=%s city=%s snapshot_date=%s bucket=%s dated_key=%s latest_key=%s",
        "airbnb",
        "upload_success",
        city,
        snapshot_date,
        bucket,
        dated_key,
        latest_key,
    )


if __name__ == "__main__":
    configure_logging()
    Fire(extract_latest_market_snapshot)
