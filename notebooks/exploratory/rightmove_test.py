import boto3
import requests

from include.airflow_utils import logger

AUTH_TOKEN = "a2FuZzcud2FuZ0BnbWFpbC5jb206ZjQ4MzViN2NlMGEwZjA4ZTQ1MTA3MzZhYmRhYTc3ZTNhM2FhYWU1Yw=="
S3_BUCKET = "quibbler-house-data-lake"
BASE_URL = "https://epc.opendatacommunities.org/api/v1/files"

file_name = "domestic-2025.zip"
url = f"{BASE_URL}/{file_name}"
year_folder = "2025"
s3_key = f"raw/epc/{year_folder}/{file_name}"

headers = {"Authorization": f"Basic {AUTH_TOKEN}", "User-Agent": "curl/7.68.0"}
s3 = boto3.client("s3")
headers={}
with requests.get(url, stream=True, headers=headers) as r:
    if r.status_code == 200:
        s3.upload_fileobj(r.raw, S3_BUCKET, s3_key)
    logger.info("Uploaded data from %s to s3://%s/%s the code is %s", url, S3_BUCKET, s3_key,r.status_code)
