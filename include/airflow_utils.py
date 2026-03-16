"""Shared Airflow constants and helper utilities.

This module centralizes DAG defaults, KubernetesPodOperator defaults, Slack failure notification wiring, and repository bootstrap commands used by DAG tasks.
"""

import logging
from datetime import timedelta
from typing import Any
import time
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


def make_request(
    request_type: str,
    url: str,
    current_retry_count: int = 0,
    max_retry_count: int = 3,
    **kwargs: Any
)-> requests.Response:
    """Generic function to make an HTTP GET and POST request with error handling."""

    def get_backoff_time(wait_time, additional_backoff, retry_count):
        backoff_time = wait_time + (additional_backoff * (retry_count + 1))
        return backoff_time

    additional_backoff = 20

    if current_retry_count >= max_retry_count:
        raise requests.exceptions.HTTPError(f"Manually raising Client Error: \
            Too many retries when calling the {url}.")
    try:
        if request_type == "GET":
            response = requests.get(url, **kwargs)
        elif request_type == "POST":
            response = requests.post(url, **kwargs)
        else:
            raise ValueError("Invalid request type")

    # error before reponse was returned
    except requests.exceptions.Timeout:
        backoff_time = get_backoff_time(
            kwargs.get("timeout", additional_backoff),
            additional_backoff,
            current_retry_count,
        )
        logging.info(
            f"For this request, increasing request timeout time to: {backoff_time}"
        )
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
        if response.status_code == 429:
            backoff_time = get_backoff_time(
                # if no retry-after exists, wait default time
                int(response.headers.get("Retry-After", additional_backoff)),
                additional_backoff,
                current_retry_count,
            )
            logging.info(f"Too many requests... Sleeping for {backoff_time} seconds")
            time.sleep(backoff_time)
            # Make the request again
            return make_request(
                request_type=request_type,
                url=url,
                current_retry_count=current_retry_count + 1,
                max_retry_count=max_retry_count,
                **kwargs,
            )
        logging.error(f"request exception for url {url}, see below")
        raise

    return response

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
