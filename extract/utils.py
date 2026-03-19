"""Shared extraction utilities.

This module contains helpers reused by multiple extract scripts.
"""

import logging
import os
import time
from typing import Any

import boto3
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
HTTP_TOO_MANY_REQUESTS = 429


def stream_to_s3(
    url: str,
    key: str,
    args: dict[str, Any] | None = None,
) -> None:
    """Stream a remote file directly to S3.

    The download is streamed from the source URL and uploaded to S3
    without loading the full payload into memory.

    Parameters
    ----------
    url : str
        HTTP(S) URL to download.
    key : str
        Target S3 object key.
    args : dict[str, Any]
        Dictionary containing the following keys:
        - bucket : str
            Target S3 bucket name.
        - headers : dict[str, str] | None, optional
            Optional HTTP headers to send with the request.
        - connect_timeout_seconds : float, optional
            Connection timeout in seconds. Default is 10.0.
        - read_timeout_seconds : float, optional
            Read timeout in seconds. Default is 300.0.

    Returns
    -------
    None
        This function uploads data as a side effect.

    Raises
    ------
    requests.HTTPError
        If the source URL returns a non-success status code.
    botocore.exceptions.BotoCoreError
        If the S3 upload fails.

    """
    args = args or {}

    bucket: str = args.get("bucket", "quibbler-house-data-lake")
    headers: dict[str, str] | None = args.get("headers")
    connect_timeout_seconds: float = args.get("connect_timeout_seconds", 10.0)
    read_timeout_seconds: float = args.get("read_timeout_seconds", 300.0)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    request_headers = headers or {}
    timeout = (connect_timeout_seconds, read_timeout_seconds)
    with requests.get(url, stream=True, headers=request_headers, timeout=timeout) as response:
        response.raise_for_status()
        s3.upload_fileobj(response.raw, bucket, key)


def make_request(
<<<<<<< HEAD
    request_type: str, url: str, current_retry_count: int = 0, max_retry_count: int = 3, **kwargs: Any,
=======
    request_type: str,
    url: str,
    current_retry_count: int = 0,
    max_retry_count: int = 3,
    **kwargs: Any,
>>>>>>> c349a767111b9aa1414ec28efd96804cdd7ccf74
) -> requests.Response:
    """Make an HTTP GET or POST request with error handling."""

    def get_backoff_time(wait_time: int, additional_backoff: int, retry_count: int) -> int:
        return wait_time + (additional_backoff * (retry_count + 1))

    additional_backoff = 20
    kwargs.setdefault("timeout", additional_backoff)

    if current_retry_count >= max_retry_count:
<<<<<<< HEAD
        msg =f"Manually raising Client Error: Too many retries when calling the {url}."
=======
        msg = f"Manually raising Client Error: Too many retries when calling the {url}."
>>>>>>> c349a767111b9aa1414ec28efd96804cdd7ccf74
        raise requests.exceptions.HTTPError(
            msg,
        )
    try:
        if request_type == "GET":
<<<<<<< HEAD
            response = requests.get(url,**kwargs) # noqa: S113
        elif request_type == "POST":
            response = requests.post(url,**kwargs) # noqa: S113
=======
            response = requests.get(url, **kwargs)  # noqa: S113
        elif request_type == "POST":
            response = requests.post(url, **kwargs)  # noqa: S113
>>>>>>> c349a767111b9aa1414ec28efd96804cdd7ccf74
        else:
            msg = f"Invalid request type: {request_type}. Only 'GET' and 'POST' are supported."
            raise ValueError(msg)

    # error before reponse was returned
    except requests.exceptions.Timeout:
        backoff_time = get_backoff_time(
            kwargs.get("timeout", additional_backoff),
            additional_backoff,
            current_retry_count,
        )
        logger.info("For this request, increasing request timeout time to: %d", backoff_time)
        # add some buffer to sleep
        kwargs["timeout"] = backoff_time
        # Make the request again
        return make_request(
            request_type=request_type,
            url=url,
            current_retry_count=current_retry_count + 1,
            max_retry_count=max_retry_count,
            **kwargs,
        )

    # response was returned, check for error status
    try:
        response.raise_for_status()
    # error after reponse was returned
    except requests.exceptions.RequestException:
        # if too many requests, calculate time to wait
        if response.status_code == HTTP_TOO_MANY_REQUESTS:
            backoff_time = get_backoff_time(
                # if no retry-after exists, wait default time
                int(response.headers.get("Retry-After", additional_backoff)),
                additional_backoff,
                current_retry_count,
            )
            logger.info("Too many requests... Sleeping for %d seconds", backoff_time)
            time.sleep(backoff_time)
            # Make the request again
            return make_request(
                request_type=request_type,
                url=url,
                current_retry_count=current_retry_count + 1,
                max_retry_count=max_retry_count,
                **kwargs,
            )
        logger.exception("request exception for url %s", url)
        raise

    return response
