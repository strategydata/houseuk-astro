import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import fire

from include.airflow_utils import stream_url_to_s3
from include.logging_config import configure_logging

DEFAULT_AUTH_TOKEN = (
    "a2FuZzcud2FuZ0BnbWFpbC5jb206ZjQ4MzViN2NlMGEwZjA4ZTQ1MTA3MzZhYmRhYTc3ZTNhM2FhYWU1Yw=="
)
DEFAULT_S3_BUCKET = "quibbler-house-data-lake"
DEFAULT_BASE_URL = "https://epc.opendatacommunities.org/api/v1/files"
DEFAULT_USER_AGENT = "curl/7.68.0"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EPCConfig:
    bucket: str = DEFAULT_S3_BUCKET
    base_url: str = DEFAULT_BASE_URL
    auth_token: str = os.getenv("EPC_AUTH_TOKEN", DEFAULT_AUTH_TOKEN)
    user_agent: str = os.getenv("EPC_USER_AGENT", DEFAULT_USER_AGENT)


class EPCPipeline:
    def __init__(self, config: EPCConfig | None = None):
        self.config = config or EPCConfig()
        if not self.config.auth_token:
            raise ValueError(
                "EPC auth token is required. Set EPC_AUTH_TOKEN or pass EPCConfig(auth_token=...)."
            )

    @staticmethod
    def _stream_callable():
        # stream_url_to_s3 is an Airflow task decorator; call underlying function in CLI context.
        return getattr(stream_url_to_s3, "function", stream_url_to_s3)

    def _request_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Basic {self.config.auth_token}",
            "User-Agent": self.config.user_agent,
        }

    def _stream_result(self, identifier: str) -> tuple[str, str, dict[str, Any]]:
        file_name = f"domestic-{identifier}.zip"
        url = f"{self.config.base_url}/{file_name}"
        year_folder = identifier.split("-")[0]
        s3_key = f"raw/epc/{year_folder}/{file_name}"

        logger.info("source=%s action=%s file_name=%s", "epc", "process_start", file_name)
        result = self._stream_callable()(
            url=url,
            bucket=self.config.bucket,
            s3_key=s3_key,
            headers=self._request_headers(),
            raise_on_error=False,
        )
        return file_name, s3_key, result

    def _stream_to_s3(self, identifier: str) -> bool:
        """Stream a specific EPC zip file to S3 and return success/failure."""
        file_name, s3_key, result = self._stream_result(identifier)
        if result.get("ok"):
            logger.info(
                "source=%s action=%s file_name=%s s3_key=%s",
                "epc",
                "upload_success",
                file_name,
                s3_key,
            )
            return True

        status_code = result.get("status_code")
        message = result.get("message")
        http_status = result.get("http_status")

        if status_code == "HTTP_NOT_FOUND":
            logger.info(
                "source=%s action=%s file_name=%s status_code=%s message=%s",
                "epc",
                "source_not_found",
                file_name,
                status_code,
                message,
            )
            return False

        # Error details are already logged centrally in include.airflow_utils.stream_url_to_s3.
        logger.info(
            "source=%s action=%s file_name=%s status_code=%s http_status=%s message=%s",
            "epc",
            "source_request_failed",
            file_name,
            status_code,
            http_status,
            message,
        )
        return False

    def bulk(self, start_year: int = 2008, end_year: int = 2025) -> None:
        """Download yearly history files from `start_year` to `end_year` (inclusive)."""
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

    def incremental(self, year: int | None = None, month: int | None = None) -> None:
        """Download monthly files for a year or a specific month."""
        now = datetime.now()
        target_year = year or now.year

        if month:
            identifier = f"{target_year}-{int(month):02d}"
            self._stream_to_s3(identifier)
            return

        logger.info(
            "source=%s action=%s target_year=%s run_date=%s",
            "epc",
            "incremental_month_scan",
            target_year,
            now.date().isoformat(),
        )
        for month_number in range(1, 13):
            identifier = f"{target_year}-{month_number:02d}"
            success = self._stream_to_s3(identifier)
            if not success and month_number >= now.month:
                break


if __name__ == "__main__":
    configure_logging()
    fire.Fire(EPCPipeline())
