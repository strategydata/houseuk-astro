"""Airflow DAG that schedules annual EPC backfill refresh for the previous year."""

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

# For a run on Jan 1 (for example 2026-01-01), data_interval_start.year resolves to 2025.
ANNUAL_TARGET_YEAR_TEMPLATE = "{{ data_interval_start.year }}"


@dag(
    dag_id="epc_annual_extract",
    schedule="0 9 1 1 *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=amber_dags_defaults,
)
def epc_annual_extract():
    epc_annual_extract_cmd = f"""
    {clone_and_setup_repo_cmd} &&
    python extract/epc/src/execute.py bulk \
      --start_year {ANNUAL_TARGET_YEAR_TEMPLATE} \
      --end_year {ANNUAL_TARGET_YEAR_TEMPLATE}
    """

    KubernetesPodOperator(
        **amber_kube_defaults,
        image=DATA_IMAGE,
        image_pull_secrets=[k8s.V1LocalObjectReference(name="amber-ghcr-registry")],
        kubernetes_conn_id="k8s_conn",
        task_id="epc_annual_extract_task",
        name="epc-annual-extract-pod",
        secrets=[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, EPC_AUTH_TOKEN],
        arguments=[epc_annual_extract_cmd],
        do_xcom_push=False,
    )


epc_annual_extract()
