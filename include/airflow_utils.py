"""Shared Airflow constants and helper utilities.

This module centralizes DAG defaults, KubernetesPodOperator defaults, Slack failure notification wiring, and repository bootstrap commands used by DAG tasks.
"""

import logging
from datetime import timedelta
from typing import Any

import boto3
import requests
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.sdk import task
from botocore.exceptions import BotoCoreError, ClientError
from kubernetes.client import models as k8s

logger = logging.getLogger(__name__)
DATA_IMAGE = "ghcr.io/strategydata/data-infrastructure:2026.03.0"
IMAGE_URL = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/src/airflow/ui/public/pin_100.png"
SSH_REPO = "git@github.com:strategydata/houseuk-astro.git"
HTTP_REPO = "https://github.com/strategydata/houseuk-astro.git"
GIT_BRANCH = "main"


class StreamUrlToS3Error(RuntimeError):
    """Raised when stream_url_to_s3 fails and raise_on_error is enabled."""


@task
def stream_url_to_s3(
    url: str,
    bucket: str,
    s3_key: str,
    headers: dict[str, str] | None = None,
    connect_timeout_seconds: float = 10.0,
    read_timeout_seconds: float = 300.0,
    raise_on_error: bool = True,
) -> dict[str, Any]:
    """Stream a URL response body directly into S3 with structured error results.

    Return payload always includes:
    - ``status_code``: short machine-readable code.
    - ``ok``: boolean success flag.
    - ``message``: human-readable summary.

    By default this task raises on errors so Airflow retries/failure callbacks still work.
    """

    def _build_error_result(
        status_code: str,
        message: str,
        http_status: int | None = None,
    ) -> dict[str, Any]:
        result = {
            "ok": False,
            "status_code": status_code,
            "message": message,
            "http_status": http_status,
        }
        if raise_on_error:
            raise StreamUrlToS3Error(f"{status_code}: {message}")
        return result

    if not url or not bucket or not s3_key:
        message = "Invalid input: url, bucket and s3_key are required."
        logger.error(
            "stream_url_to_s3 failed: status_code=%s url=%s bucket=%s s3_key=%s reason=%s",
            "INVALID_INPUT",
            url,
            bucket,
            s3_key,
            message,
        )
        return _build_error_result("INVALID_INPUT", message)

    request_headers = headers or {}
    s3 = boto3.client("s3")
    timeout = (connect_timeout_seconds, read_timeout_seconds)

    try:
        with requests.get(url, stream=True, headers=request_headers, timeout=timeout) as response:
            source_http_status = response.status_code
            try:
                response.raise_for_status()
            except requests.HTTPError:
                http_status = response.status_code
                if http_status == 404:
                    status_code = "HTTP_NOT_FOUND"
                    message = f"Source URL not found (404): {url}"
                elif http_status == 401:
                    status_code = "HTTP_UNAUTHORIZED"
                    message = "Unauthorized (401): check authentication headers/token."
                elif http_status == 403:
                    status_code = "HTTP_FORBIDDEN"
                    message = "Forbidden (403): credentials do not have permission."
                else:
                    status_code = "HTTP_ERROR"
                    message = f"HTTP error {http_status} while requesting source URL."

                logger.error(
                    "stream_url_to_s3 failed: status_code=%s http_status=%s url=%s bucket=%s s3_key=%s",
                    status_code,
                    http_status,
                    url,
                    bucket,
                    s3_key,
                )
                return _build_error_result(status_code, message, http_status=http_status)

            try:
                s3.upload_fileobj(response.raw, bucket, s3_key)
            except (ClientError, BotoCoreError) as exc:
                message = "S3 upload failed while streaming content."
                logger.exception(
                    "stream_url_to_s3 failed: status_code=%s url=%s bucket=%s s3_key=%s reason=%s",
                    "S3_UPLOAD_FAILED",
                    url,
                    bucket,
                    s3_key,
                    exc,
                )
                return _build_error_result(
                    "S3_UPLOAD_FAILED",
                    message,
                    http_status=response.status_code,
                )

    except requests.Timeout as exc:
        message = "Source request timed out."
        logger.exception(
            "stream_url_to_s3 failed: status_code=%s url=%s bucket=%s s3_key=%s reason=%s",
            "REQUEST_TIMEOUT",
            url,
            bucket,
            s3_key,
            exc,
        )
        return _build_error_result("REQUEST_TIMEOUT", message)
    except requests.ConnectionError as exc:
        message = "Network connection failed while reaching source URL."
        logger.exception(
            "stream_url_to_s3 failed: status_code=%s url=%s bucket=%s s3_key=%s reason=%s",
            "CONNECTION_ERROR",
            url,
            bucket,
            s3_key,
            exc,
        )
        return _build_error_result("CONNECTION_ERROR", message)
    except requests.RequestException as exc:
        message = "Request failed due to an unexpected HTTP client error."
        logger.exception(
            "stream_url_to_s3 failed: status_code=%s url=%s bucket=%s s3_key=%s reason=%s",
            "REQUEST_ERROR",
            url,
            bucket,
            s3_key,
            exc,
        )
        return _build_error_result("REQUEST_ERROR", message)
    except StreamUrlToS3Error:
        raise
    except Exception as exc:
        message = "Unexpected error while streaming URL to S3."
        logger.exception(
            "stream_url_to_s3 failed: status_code=%s url=%s bucket=%s s3_key=%s reason=%s",
            "UNEXPECTED_ERROR",
            url,
            bucket,
            s3_key,
            exc,
        )
        return _build_error_result("UNEXPECTED_ERROR", message)

    logger.info("Uploaded data from %s to s3://%s/%s", url, bucket, s3_key)
    return {
        "ok": True,
        "status_code": "SUCCESS",
        "message": "Upload completed successfully.",
        "http_status": source_http_status,
    }


def slack_failed_task(context):
    """slack_failed_task Function to be used as a callable for no_failure_callback

    Args:
        context (_type_): _description_
    """

    blocks_val = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Looks like a airflow Dag run has *failed* in Airflow :rotating_light: :\n",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*DAG:*\n{{ti.dag_id}}"},
                {"type": "mrkdwn", "text": "*Task:*\n{{ti.task_id}}"},
                {"type": "mrkdwn", "text": "*Logs:*\n<{{ti.log_url}}|View Logs>"},
            ],
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Task Logs"},
                    "url": "{{ti.log_url}}",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View DAG Runs"},
                    "url": "http://localhost:8080/dags/{{ti.dag_id}}",
                },
            ],
        },
    ]
    return send_slack_notification(
        slack_conn_id="airflow_con_slack_conn_notification_api",
        channel="#data-science-pipelines",
        blocks=blocks_val,
        icon_url=IMAGE_URL,
    )(context)


amber_dags_defaults = {
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}

container_resources = k8s.V1ResourceRequirements(
    requests={"memory": "2Gi", "cpu": "800m"},
    limits={"memory": "4Gi", "cpu": "800m"},
)

amber_kube_defaults = {
    "get_logs": True,
    "is_delete_operator_pod": False,
    "container_resources": container_resources,
    "startup_timeout_seconds": 200,
    "cmds": ["/bin/bash", "-c"],
}


data_test_ssh_key_cmd = """
    mkdir ~/.ssh/ &&
    touch ~/.ssh/id_rsa && touch ~/.ssh/config &&
    echo "$GIT_DATA_TESTS_PRIVATE_KEY" > ~/.ssh/id_rsa && chmod 0400 ~/.ssh/id_rsa &&
    echo "$GIT_DATA_TESTS_CONFIG" > ~/.ssh/config"""

clone_repo_cmd = f"""
    {data_test_ssh_key_cmd} &&
    if [[ -z "$GIT_COMMIT" ]]; then
        export GIT_COMMIT="HEAD"
    fi
    if [[ -z "$GIT_DATA_TESTS_PRIVATE_KEY" ]]; then
        export REPO="{HTTP_REPO}";
        else
        export REPO="{SSH_REPO}";
    fi &&
    git clone -b {GIT_BRANCH} --single-branch --depth 1 $REPO &&
    echo "checking out commit $GIT_COMMIT" &&
    cd houseuk-astro &&
    git checkout $GIT_COMMIT &&
    cd .."""


clone_and_setup_repo_cmd = f"""
    {clone_repo_cmd} &&
    cd houseuk-astro"""
