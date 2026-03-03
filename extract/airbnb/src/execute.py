"""InsideAirbnb extractor entrypoint.

Resolves the latest listings snapshot URL for a market and uploads both a dated object and a `latest/` pointer object to S3.
"""

import os
import re

import boto3
import requests
from fire import Fire

from extract.utils import stream_to_s3

LISTINGS_URL_PATTERN = re.compile(
    r"https://data\.insideairbnb\.com/(?P<country>[^/]+)/(?P<region>[^/]+)/(?P<market>[^/]+)/(?P<snapshot_date>\d{4}-\d{2}-\d{2})/data/listings\.csv\.gz"
)


def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_account_id=os.getenv("AWS_ACCOUNT_ID"),
    )


def resolve_latest_listings_url(page_url: str, country_slug: str, region_slug: str, market_slug: str):
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
                }
            )

    if not matches:
        raise ValueError(
            f"No listings dataset URL found for {country_slug}/{region_slug}/{market_slug} at {page_url}"
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
):
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

    print(
        f"Uploaded {city} snapshot {snapshot_date} from {url} to s3://{bucket}/{dated_key} and refreshed s3://{bucket}/{latest_key}"
    )


if __name__ == "__main__":
    Fire(extract_latest_market_snapshot)

