"""Extract EPC data and stream archives to S3."""

<<<<<<< HEAD
import logging
import os
from dataclasses import dataclass
from datetime import datetime
=======
import datetime
import logging
import os
from dataclasses import dataclass
>>>>>>> c349a767111b9aa1414ec28efd96804cdd7ccf74

import fire
import requests
from botocore.exceptions import BotoCoreError, ClientError

from extract.utils import stream_to_s3
from include.logging_config import configure_json_logging

DEFAULT_S3_BUCKET = "quibbler-house-data-lake"
DEFAULT_BASE_URL = "https://epc.opendatacommunities.org/api/v1/files"
DEFAULT_USER_AGENT = "curl/7.68.0"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EPCConfig:
    """Configuration for the EPC extraction pipeline."""

    bucket: str = DEFAULT_S3_BUCKET
    base_url: str = DEFAULT_BASE_URL
    auth_token: str = os.getenv("EPC_AUTH_TOKEN", "")
    user_agent: str = os.getenv("EPC_USER_AGENT", DEFAULT_USER_AGENT)


class EPCPipeline:
    """Run EPC extraction workflows."""

    def __init__(self, config: EPCConfig | None = None) -> None:
        """Initialize the EPC pipeline with configuration."""
        self.config = config or EPCConfig()
        if not self.config.auth_token:
            message = "EPC auth token is required. Set EPC_AUTH_TOKEN or pass EPCConfig(auth_token=...)."
            raise ValueError(message)

    def _request_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Basic {self.config.auth_token}",
            "User-Agent": self.config.user_agent,
        }

    def _stream_target(self, identifier: str) -> tuple[str, str, str]:
        file_name = f"domestic-{identifier}.zip"
        url = f"{self.config.base_url}/{file_name}"
        year_folder = identifier.split("-", maxsplit=1)[0]
        s3_key = f"raw/epc/{year_folder}/{file_name}"
        return file_name, url, s3_key

    def _stream_to_s3(self, identifier: str) -> bool:
        """Stream a specific EPC zip file to S3 and return success/failure."""
        file_name, url, s3_key = self._stream_target(identifier)
        logger.info("source=%s action=%s file_name=%s", "epc", "process_start", file_name)
        try:
            stream_to_s3(
                url=url,
                key=s3_key,
<<<<<<< HEAD
                args=
                {
=======
                args={
>>>>>>> c349a767111b9aa1414ec28efd96804cdd7ccf74
                    "bucket": self.config.bucket,
                    "headers": self._request_headers(),
                },
            )
        except requests.HTTPError:
            logger.exception(
                "file_name=%s epc source_request_failed",
                file_name,
            )
            return False
        except (BotoCoreError, ClientError):
            logger.exception(
                "file_name=%s,epc s3_upload_failed",
                file_name,
            )
            return False
<<<<<<< HEAD
        except Exception as exc: # noqa: BLE001
            logger.info(
                "source=%s action=%s file_name=%s error=%s",
                "epc",
                "unexpected_error",
=======
        except Exception:
            logger.exception(
                "file_name=%s epc unexpected_error unexpected_error",
>>>>>>> c349a767111b9aa1414ec28efd96804cdd7ccf74
                file_name,
            )
            return False

        logger.info(
            "source=%s action=%s file_name=%s s3_key=%s",
            "epc",
            "upload_success",
            file_name,
            s3_key,
        )
        return True

    def bulk(self, start_year: int = 2008, end_year: int = 2025) -> None:
        """Download yearly history files from `start_year` to `end_year` (inclusive)."""
        logger.info(
            "source=%s action=%s start_year=%s end_year=%s run_date=%s",
            "epc",
            "bulk_start",
            start_year,
            end_year,
<<<<<<< HEAD
            datetime.now(tz=datetime.UTC).date().isoformat(),
=======
            datetime.datetime.now(tz=datetime.UTC).date().isoformat(),
>>>>>>> c349a767111b9aa1414ec28efd96804cdd7ccf74
        )
        for year in range(start_year, end_year + 1):
            self._stream_to_s3(str(year))

    def incremental(self, year: int | None = None, month: int | None = None) -> None:
        """Download monthly files for a year or a specific month."""
<<<<<<< HEAD
        now = datetime.now(tz=datetime.UTC)
=======
        now = datetime.datetime.now(tz=datetime.UTC)
>>>>>>> c349a767111b9aa1414ec28efd96804cdd7ccf74
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
    configure_json_logging()
    fire.Fire(EPCPipeline())
