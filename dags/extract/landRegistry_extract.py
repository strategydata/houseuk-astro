from airflow.sdk import dag,task
from include.airflow_utils import stream_url_to_s3, DATA_IMAGE,amber_kube_defaults,amber_dags_defaults
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
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
    
    """
    # namespace = conf.get("kubernetes", "NAMESPACE")
    landRegistry_kube = KubernetesPodOperator(
        # **amber_kube_defaults,
        image=DATA_IMAGE,
        kubernetes_conn_id="k8s_conn",
        task_id="landRegistry_extract_task",
        name="landRegistry-extract-pod",
        # affinity=get_affinity("extraction"),
        # tolerations=get_toleration("extraction"),
        arguments=["echo Hello World"],
        is_delete_operator_pod=True,
        get_logs=True
    )
    # landRegistry_kube=KubernetesPodOperator(
    #     kubernetes_conn_id="k8s_conn",
    #     image="hello-world",
    #     cmds=["bash", "-cx"],
    #     arguments=["echo", "10", "echo pwd"],
    #     labels={"foo": "bar"},
    #     name="airflow-private-image-pod",
    #     is_delete_operator_pod=True,
    #     task_id="task-two",
    #     get_logs=True,
    # )
    # logging.info(f"Using namespace: {namespace}")
    # landRegistry_kube = KubernetesPodOperator(
    #     kubernetes_conn_id="k8s_conn",
    #     image="hello-world",
    #     name="airflow-test-pod",
    #     task_id="task-one",
    #     is_delete_operator_pod=True,
    #     get_logs=True,
    # )

    landRegistry_kube
    
landRegistry_extract()