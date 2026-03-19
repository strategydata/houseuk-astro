"""InsideAirbnb extractor entrypoint.

This module resolves the latest InsideAirbnb listings dataset for a market,
uploads the dated snapshot to S3, and refreshes a stable `latest` object key.
"""

import logging
import re
from pathlib import Path

import yaml
from fire import Fire

from extract.utils import make_request, stream_to_s3

logger = logging.getLogger(__name__)


def get_market_urls(
    config_path: str,
) -> dict[str, str]:
    """Fetch listing dataset URLs from the InsideAirbnb index page.

    Parameters
    ----------
    config_path : str
        Path to the YAML config containing the index URL and market slugs.

    Returns
    -------
    dict[str, str]
        Mapping of "{city}_{date}" keys to listings CSV URLs.

    """
    with Path(config_path).open() as f:
        config = yaml.safe_load(f)
    global__conf = config.get("globals", {})
    index_url = global__conf.get("data_index_url")
    logger.info("Fetching market URLs from index page: %s", index_url)
    response = make_request("GET", index_url, timeout=30)
    logger.info("Successfully fetched index page. Parsing HTML content.")
    html_content = response.text
    results = {}
    for m in config.get("markets", []):
        logger.info("Processing market: %s", m)
        city = m["city"]

        # Building the regex pattern for this specific city
        # Note the double {{ }} to escape the f-string for the regex counts
        pattern = (
            rf"https://data\.insideairbnb\.com/"
            rf"{m['country_slug']}/{m['region_slug']}/{m['market_slug']}/"
            r"(?P<date>\d{4}-\d{2}-\d{2})/data/listings\.csv\.gz"
        )
        match = re.search(pattern, html_content)
        if match:
            url = match.group(0)
            date = match.group("date")

            # Key format: city_date
            results[f"{city}_{date}"] = url
            logger.info("Found: %s -> %s", city, date)
        else:
            logger.warning("No match found for city: %s", city)

    return results


def main() -> None:
    """Run the extraction pipeline."""
    market_urls = get_market_urls("extract/airbnb/airbnb.yml")
    logger.info("Market URLs: %s", market_urls)
    for file_name, url in market_urls.items():
        logger.info("Processing market snapshot: %s", file_name)
        key = f"raw/airbnb/{file_name}.csv.gz"
        stream_to_s3(url=url, key=key)
        logger.info("Successfully uploaded %s to   %s", file_name, key)


if __name__ == "__main__":
    Fire(main)
