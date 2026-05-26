"""Airflow DAG to run dbt transforms with DuckDB against S3 data."""

from datetime import UTC, datetime

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


@dag(
    dag_id="dbt_transform",
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    default_args=amber_dags_defaults,
)
def dbt_transform() -> None:
    """Run dbt models on a daily schedule."""
    dbt_cmd = f"""
    {clone_and_setup_repo_cmd} &&
    dbt --version &&
    dbt run --project-dir transform --profiles-dir transform --target prod
    """

    KubernetesPodOperator(
        **amber_kube_defaults,
        image=DATA_IMAGE,
        image_pull_secrets=[k8s.V1LocalObjectReference(name="amber-ghcr-registry")],
        kubernetes_conn_id="k8s_conn",
        task_id="dbt_transform_task",
        name="dbt-transform-pod",
        secrets=[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY],
        env_vars={"DBT_PROFILES_DIR": "transform"},
        arguments=[dbt_cmd],
        do_xcom_push=False,
    )


dbt_transform()
