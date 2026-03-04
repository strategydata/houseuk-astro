"""Airflow DAG that schedules the EPC extractor in a Kubernetes pod."""

from datetime import datetime

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag
from kubernetes.client import models as k8s

from dags.kube_secrets import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, EPC_AUTH_TOKEN
from include.airflow_utils import (
    DATA_IMAGE,
    amber_dags_defaults,
    amber_kube_defaults,
    clone_and_setup_repo_cmd,
)


@dag(
    dag_id="epc_extract",
    schedule="0 8 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=amber_dags_defaults,
)
def epc_extract():
    epc_extract_cmd = f"""
    {clone_and_setup_repo_cmd} &&
    python extract/epc/src/execute.py incremental
    """

    KubernetesPodOperator(
        **amber_kube_defaults,
        image=DATA_IMAGE,
        image_pull_secrets=[k8s.V1LocalObjectReference(name="amber-ghcr-registry")],
        kubernetes_conn_id="k8s_conn",
        task_id="epc_extract_task",
        name="epc-extract-pod",
        secrets=[
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            EPC_AUTH_TOKEN,
        ],
        arguments=[epc_extract_cmd],
        do_xcom_push=False,
    )


epc_extract()
