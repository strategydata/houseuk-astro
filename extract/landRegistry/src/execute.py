import os
import requests
import boto3
from fire import Fire
from extract.utils import stream_to_s3

if __name__ == "__main__":
    # url ="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv"
    url ="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
    bucket = "quibbler-house-data-lake"
    key = "raw/land_registry/pp-complete.csv"
    # key = "raw/land_registry/pp-monthly-update-new-version.csv"
    Fire(stream_to_s3)