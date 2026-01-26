from airflow.sdk import dag,task
from datetime import datetime, timedelta
from include.airflow_utils import amber_dags_defaults, amber_kube_defaults, clone_and_setup_repo_cmd, DATA_IMAGE
from dags.kube_secrets import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

@dag(
    dag_id="crime_extract",
    schedule="0 7 1 * *",
    start_date=datetime(2026, 1, 1),
    default_args=amber_dags_defaults
)
def crime_extract():
    crime_extract_cmd = f"""
    {clone_and_setup_repo_cmd} &&
    python extract/crime/src/execute.py"""
    
    crime_kube = KubernetesPodOperator(
        **amber_kube_defaults,
        image=DATA_IMAGE,
        image_pull_secrets=[k8s.V1LocalObjectReference(name="amber-ghcr-registry")],
        kubernetes_conn_id="k8s_conn",
        task_id="crime_extract_task",
        name="crime-extract-pod",
        secrets=[
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            ],
        arguments=[crime_extract_cmd],
        do_xcom_push=True,
    )
    crime_kube
crime_extract()