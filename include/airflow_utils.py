import airflow
from airflow.sdk import dag, task
import requests
import boto3
import urllib
from datetime import timedelta
import logging
from airflow.providers.slack.notifications.slack import send_slack_notification
from kubernetes.client import models as k8s
import os
DATA_IMAGE="ghcr.io/strategydata/data-infrastructure:latest"
IMAGE_URL = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/src/airflow/ui/public/pin_100.png"
SSH_REPO="git@github.com:strategydata/houseuk-astro.git"
HTTP_REPO="https://github.com/strategydata/houseuk-astro.git"
GIT_BRANCH= "main"
@task
def stream_url_to_s3(url: str, bucket: str, s3_key: str, chunk_size: int = 8192):    
    s3 = boto3.client('s3')
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with s3.upload_fileobj(r.raw, bucket, s3_key) as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    print(f"Uploaded data from {url} to s3://{bucket}/{s3_key}")

def slack_failed_task(context):
    """slack_failed_task Function to be used as a callable for no_failure_callback

    Args:
        context (_type_): _description_
    """

    ti = context.get("ti")
    blocks_val=[
        		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "Looks like a airflow Dag run has *failed* in Airflow :rotating_light: :\n"
			}
		},
		{
			"type": "divider"
		},
		{
			"type": "section",
			"fields": [
				{
					"type": "mrkdwn",
					"text": "*DAG:*\n{{ti.dag_id}}"
				},
				{
					"type": "mrkdwn",
					"text": "*Task:*\n{{ti.task_id}}"
				},
				{
					"type": "mrkdwn",
					"text": "*Logs:*\n<{{ti.log_url}}|View Logs>"
				}
			]
		},
		{
			"type": "actions",
			"elements": [
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "View Task Logs"
					},
					"url": "{{ti.log_url}}"
				},
				{
					"type": "button",
					"text": {
						"type": "plain_text",
						"text": "View DAG Runs"
					},
					"url": "http://localhost:8080/dags/{{ti.dag_id}}"
				}
			]
		}

    ]
    return send_slack_notification(
        slack_conn_id="airflow_con_slack_conn_notification_api",
        channel = "#data-science-pipelines",
        blocks = blocks_val,
        icon_url=IMAGE_URL,
    )(context)

amber_dags_defaults={
    "on_failure_callback": slack_failed_task,
    "owner":"airflow",
    "retries":1,
    "retry_delay":timedelta(minutes=5),
    "depends_on_past":False,
    }

container_resources = k8s.V1ResourceRequirements(
	requests={"memory":"2Gi","cpu":"800m"},
	limits={"memory":"4Gi","cpu":"800m"},
)

amber_kube_defaults={
	"get_logs":True,
 	"is_delete_operator_pod":False,
 	"container_resources":container_resources,
 	"startup_timeout_seconds":200,
	"cmds":["/bin/bash","-c"],
	}


data_test_ssh_key_cmd= f"""
	mkdir ~/.ssh/ &&
	touch ~/.ssh/id_rsa && touch ~/.ssh/config &&
	echo "$GIT_DATA_TESTS_PRIVATE_KEY" > ~/.ssh/id_rsa && chmod 0400 ~/.ssh/id_rsa &&
	echo "$GIT_DATA_TESTS_CONFIG" > ~/.ssh/config"""

clone_repo_cmd =f"""
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

    
clone_and_setup_repo_cmd= f"""
	{clone_repo_cmd} &&
	cd houseuk-astro"""