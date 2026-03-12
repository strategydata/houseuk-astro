"""Shared extraction utilities.

This module contains helpers reused by multiple extract scripts.
"""

import os

import boto3
import requests


def stream_to_s3(url: str, bucket: str, key: str) -> None:
    """Stream a remote file directly to S3.

    The download is streamed from the source URL and uploaded to S3
    without loading the full payload into memory.

    Parameters
    ----------
    url : str
        HTTP(S) URL to download.
    bucket : str
        Target S3 bucket name.
    key : str
        Target S3 object key.

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
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_account_id=os.getenv("AWS_ACCOUNT_ID"),
    )
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        s3.upload_fileobj(response.raw, bucket, key)
