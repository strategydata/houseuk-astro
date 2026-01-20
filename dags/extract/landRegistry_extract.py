from airflow.sdk import dag,task
from include.airflow_utils import stream_url_to_s3, DATA_IMAGE,amber_kube_defaults,amber_dags_defaults,clone_and_setup_repo_cmd
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from dags.kube_secrets import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
import os
from datetime import datetime, timedelta
from include.kubernetes_helpers import get_affinity, get_toleration
from airflow.configuration import conf
from kubernetes.client import models as k8s
from airflow.configuration import conf
import logging
config_dict = os.environ.copy()


default_args ={
    'owner': 'airflow',
}

@dag(
    dag_id="landRegistry_extract",
    schedule="0 6 1 * *",
    start_date=datetime(2026, 1, 1),
    default_args=amber_dags_defaults,
)
def landRegistry_extract():
    landRegistry_extract_cmd = f"""
    {clone_and_setup_repo_cmd} &&
    python extract/landRegistry/src/execute.py"""
    
    landRegistry_kube = KubernetesPodOperator(
        **amber_kube_defaults,
        image=DATA_IMAGE,
        image_pull_secrets=[k8s.V1LocalObjectReference(name="amber-ghcr-registry")],
        kubernetes_conn_id="k8s_conn",
        task_id="landRegistry_extract_task",
        name="landRegistry-extract-pod",
        secrets=[
            AWS_ACCESS_KEY_ID,
            AWS_SECRET_ACCESS_KEY,
            ],
        # affinity=get_affinity("extraction"),
        # tolerations=get_toleration("extraction"),
        arguments=[landRegistry_extract_cmd],
    )

    landRegistry_kube
    
landRegistry_extract()