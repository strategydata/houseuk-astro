from airflow.sdk import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

from include.airflow_utils import (
    DATA_IMAGE,
    amber_dags_defaults,
    amber_kube_defaults,
    clone_and_setup_repo_cmd,
)
from dags.kube_secrets import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

BUCKET = "quibbler-house-data-lake"

# Each run resolves the latest snapshot URL from InsideAirbnb at runtime.
INSIDE_AIRBNB_MARKETS = [
    {
        "city": "bristol",
        "country_slug": "united-kingdom",
        "region_slug": "england",
        "market_slug": "bristol",
        "page_url": "https://insideairbnb.com/bristol/",
    },
    {
        "city": "edinburgh",
        "country_slug": "united-kingdom",
        "region_slug": "scotland",
        "market_slug": "edinburgh",
        "page_url": "https://insideairbnb.com/edinburgh/",
    },
    {
        "city": "greater-manchester",
        "country_slug": "united-kingdom",
        "region_slug": "england",
        "market_slug": "greater-manchester",
        "page_url": "https://insideairbnb.com/greater-manchester/",
    },
    {
        "city": "london",
        "country_slug": "united-kingdom",
        "region_slug": "england",
        "market_slug": "london",
        "page_url": "https://insideairbnb.com/london/",
    },
]


@dag(
    dag_id="insideairbnb_extract",
    schedule="0 5 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=amber_dags_defaults,
)
def insideairbnb_extract():
    previous_task = None

    for market in INSIDE_AIRBNB_MARKETS:
        cmd = (
            f"{clone_and_setup_repo_cmd} && "
            f"python extract/airbnb/src/execute.py "
            f"--city '{market['city']}' "
            f"--country-slug '{market['country_slug']}' "
            f"--region-slug '{market['region_slug']}' "
            f"--market-slug '{market['market_slug']}' "
            f"--page-url '{market['page_url']}' "
            f"--bucket '{BUCKET}'"
        )

        task = KubernetesPodOperator(
            **amber_kube_defaults,
            image=DATA_IMAGE,
            image_pull_secrets=[k8s.V1LocalObjectReference(name="amber-ghcr-registry")],
            kubernetes_conn_id="k8s_conn",
            task_id=f"insideairbnb_extract_{market['city']}",
            name=f"insideairbnb-extract-{market['city']}",
            secrets=[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY],
            arguments=[cmd],
            do_xcom_push=False,
        )

        if previous_task:
            previous_task >> task
        previous_task = task


insideairbnb_extract()