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
    logging.info("Creating S3 client")
    s3 = boto3.client('s3',
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    aws_account_id =os.getenv("AWS_ACCOUNT_ID")
    )

    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        s3.upload_fileobj(response.raw, bucket, key)

if __name__ == "__main__":
    print("Executing Land Registry data ingestion script")
    logging.info(f"Uploading data from {os.getenv("AWS_ACCOUNT_ID")} to Secret {os.getenv("AWS_SECRET_ACCESS_KEY")} Access Key Id {os.getenv("AWS_ACCESS_KEY_ID")}")
    logging.info("Starting Land Registry data ingestion")

    # url ="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv"
    url ="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
    bucket = "quibbler-house-data-lake"
    key = "raw/land_registry/pp-complete.csv"
    # key = "raw/land_registry/pp-monthly-update-new-version.csv"
    Fire(stream_to_s3)