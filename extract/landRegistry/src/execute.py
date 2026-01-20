import os
import requests
import boto3
import logging
from fire import Fire


def stream_to_s3(url: str ="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv", 
                 bucket: str ="quibbler-house-data-lake", 
                 key: str ="raw/land_registry/pp-monthly-update-new-version.csv") -> None:
    """stream_to_s3 Upload a file from a URL to an S3 bucket.

    Args:
        url (str): url of the file to stream
        bucket (str): The name of the bucket to upload to.
        key (str): The name of the key to upload to.

    """
    s3 = boto3.client('s3')

    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        s3.upload_fileobj(response.raw, bucket, key)

if __name__ == "__main__":
    print("Executing Land Registry data ingestion script")
    config_dict = os.environ.copy()
    logging.info("Starting Land Registry data ingestion")
    logging.info(f"Configuration: {config_dict}")
    # url ="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv"
    url ="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
    bucket = "quibbler-house-data-lake"
    key = "raw/land_registry/pp-complete.csv"
    # key = "raw/land_registry/pp-monthly-update-new-version.csv"
    Fire(stream_to_s3)