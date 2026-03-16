"""Shared extraction utilities.

This module contains helpers reused by multiple extract scripts.
"""

import os

import boto3
import requests
from typing import Any

def stream_to_s3(
    args:dict[str, Any],
) -> None:
    """Stream a remote file directly to S3.

    The download is streamed from the source URL and uploaded to S3
    without loading the full payload into memory.

    Parameters
    ----------
    args : dict[str, Any]
        Dictionary containing the following keys:
        - url : str
            HTTP(S) URL to download.
        - bucket : str
            Target S3 bucket name.
        - key : str
            Target S3 object key.
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
    url: str = args["url"]
    bucket: str = args["bucket"]
    key: str = args["key"]
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
