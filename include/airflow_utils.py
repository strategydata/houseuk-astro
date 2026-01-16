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
	"image_pull_policy":"Always",
 	"is_delete_operator_pod":True,
	"cmd":["/bin/bash","-c"],
 	"container_resources":container_resources,
 	"execution_timeout":timedelta(hours=23),
	"cmds":["/bin/bash","-c"],
    "namespace": os.environ.get("NAMESPACE"),
	}