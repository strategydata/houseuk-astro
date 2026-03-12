import os
import sys
from pathlib import Path

import boto3
import pytest
import responses
from moto import mock_aws
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from include.airflow_utils import stream_url_to_s3

@pytest.fixture(scope="function")
def s3_setup():
    """s3_boto Create an S3 boto3 client and return the client object
    """
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        yield conn

@responses.activate
def test_stream_url_to_s3_success(s3_setup):
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    test_url = "https://example.com/testfile.txt"
    test_content = "This is a test file."
    responses.add(responses.GET, test_url, body=test_content, status=200)

    # Call underlying function to avoid Airflow TaskFlow wrapper in unit tests.
    result = stream_url_to_s3.__wrapped__(
        url=test_url,
        bucket="test-bucket",
        s3_key="testfile.txt",
        raise_on_error=False,
    )

    assert result["ok"] is True
    assert result["status_code"] == "SUCCESS"
    assert result["message"] == "Upload completed successfully."

    s3_object = s3_setup.get_object(Bucket="test-bucket", Key="testfile.txt")
    assert s3_object["Body"].read().decode() == test_content
