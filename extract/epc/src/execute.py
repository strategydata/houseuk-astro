import logging
from datetime import datetime

import boto3
import fire
import requests

from include.logging_config import configure_logging

# --- Configuration ---
AUTH_TOKEN = "a2FuZzcud2FuZ0BnbWFpbC5jb206ZjQ4MzViN2NlMGEwZjA4ZTQ1MTA3MzZhYmRhYTc3ZTNhM2FhYWU1Yw=="
S3_BUCKET = "quibbler-house-data-lake"
BASE_URL = "https://epc.opendatacommunities.org/api/v1/files"

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)


class EPCPipeline:
    def _stream_to_s3(self, identifier):
        """Internal helper to stream a specific file to S3."""
        file_name = f"domestic-{identifier}.zip"
        url = f"{BASE_URL}/{file_name}"

        # Extract year for the S3 partition path (works for '2024' or '2026-01')
        year_folder = identifier.split("-")[0]
        s3_key = f"raw/epc/{year_folder}/{file_name}"

        headers = {"Authorization": f"Basic {AUTH_TOKEN}", "User-Agent": "curl/7.68.0"}

        logger.info("source=%s action=%s file_name=%s", "epc", "process_start", file_name)
        try:
            with requests.get(url, headers=headers, stream=True) as r:
                if r.status_code == 200:
                    logger.info(
                        "source=%s action=%s bucket=%s s3_key=%s",
                        "epc",
                        "stream_to_s3",
                        S3_BUCKET,
                        s3_key,
                    )
                    s3.upload_fileobj(r.raw, S3_BUCKET, s3_key)
                    logger.info(
                        "source=%s action=%s file_name=%s s3_key=%s",
                        "epc",
                        "upload_success",
                        file_name,
                        s3_key,
                    )
                    return True
                elif r.status_code == 404:
                    logger.warning(
                        "source=%s action=%s file_name=%s status_code=%s",
                        "epc",
                        "source_not_found",
                        file_name,
                        r.status_code,
                    )
                    return False
                else:
                    logger.error(
                        "source=%s action=%s file_name=%s status_code=%s",
                        "epc",
                        "source_request_failed",
                        file_name,
                        r.status_code,
                    )
                    return False
        except Exception as e:
            logger.exception(
                "source=%s action=%s identifier=%s error=%s",
                "epc",
                "connection_error",
                identifier,
                e,
            )
            return False

    def bulk(self, start_year=2008, end_year=2025):
        """Downloads yearly history data."""
        logger.info(
            "source=%s action=%s start_year=%s end_year=%s run_date=%s",
            "epc",
            "bulk_start",
            start_year,
            end_year,
            datetime.now().date().isoformat(),
        )
        for year in range(start_year, end_year + 1):
            self._stream_to_s3(str(year))

    def incremental(self, year=None, month=None):
        """
        Downloads monthly data.
        If year/month not provided, it defaults to the current month.
        """
        now = datetime.now()
        target_year = year or now.year

        if month:
            # Download a specific month
            identifier = f"{target_year}-{int(month):02d}"
            self._stream_to_s3(identifier)
        else:
            # Try to download all available months for the target year until 404
            logger.info(
                "source=%s action=%s target_year=%s run_date=%s",
                "epc",
                "incremental_month_scan",
                target_year,
                now.date().isoformat(),
            )
            for m in range(1, 13):
                identifier = f"{target_year}-{m:02d}"
                success = self._stream_to_s3(identifier)
                if not success and m >= now.month:
                    break


if __name__ == "__main__":
    configure_logging()
    fire.Fire(EPCPipeline)
