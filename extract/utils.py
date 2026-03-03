"""Shared extraction utilities used by multiple extract scripts."""

import boto3
import requests
import os


def stream_to_s3(url: str, bucket: str, key: str) -> None:
    """stream_to_s3 Upload a file from a URL to an S3 bucket.

    Args:
        url (str): url of the file to stream
        bucket (str): The name of the bucket to upload to.
        key (str): The name of the key to upload to.

    """
    s3 =boto3.client('s3',
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    aws_account_id =os.getenv("AWS_ACCOUNT_ID")
    )
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        s3.upload_fileobj(response.raw, bucket, key)
