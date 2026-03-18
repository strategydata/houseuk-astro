"""Airflow DAG that schedules the Land Registry extractor in a Kubernetes pod."""

from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag
from kubernetes.client import models as k8s

from dags.kube_secrets import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from include.airflow_utils import (
    DATA_IMAGE,
    amber_dags_defaults,
    amber_kube_defaults,
    clone_and_setup_repo_cmd,
)

LAND_REGISTRY_BUCKET = "quibbler-house-data-lake"
LAND_REGISTRY_MONTHLY_URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/"
    "pp-monthly-update-new-version.csv"
)
LAND_REGISTRY_MONTHLY_KEY = "raw/land_registry/pp-monthly-update-new-version.csv"


@dag(
    dag_id="landregistry_extract",
    schedule="0 6 1 * *",
    start_date=datetime(2026, 1, 1,tzinfo=datetime.UTC),
    catchup=False,
    default_args=amber_dags_defaults,
)
def landregistry_extract() -> None:
    """Build the Land Registry extract DAG."""
    landregistry_extract_cmd = f"""
    {clone_and_setup_repo_cmd} &&
    python extract/landregistry/src/execute.py \
      --url="{LAND_REGISTRY_MONTHLY_URL}" \
      --bucket="{LAND_REGISTRY_BUCKET}" \
      --key="{LAND_REGISTRY_MONTHLY_KEY}"
    """

    KubernetesPodOperator(
        **amber_kube_defaults,
        image=DATA_IMAGE,
        image_pull_secrets=[k8s.V1LocalObjectReference(name="amber-ghcr-registry")],
        kubernetes_conn_id="k8s_conn",
        task_id="landregistry_extract_task",
        name="landregistry-extract-pod",
        secrets=[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY],
        arguments=[landregistry_extract_cmd],
        do_xcom_push=False,
    )


landregistry_extract()
