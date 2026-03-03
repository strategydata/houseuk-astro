"""Land Registry extractor CLI entrypoint.

This module exposes the shared streaming helper as a Fire CLI command.
"""

import logging

from fire import Fire

from extract.utils import stream_to_s3
from include.logging_config import configure_logging

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    configure_logging()
    logger.info("source=%s action=%s", "land_registry", "cli_start")
    Fire(stream_to_s3)
