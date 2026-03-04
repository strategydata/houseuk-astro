"""Reusable Kubernetes Secret bindings for AWS credentials in DAG task pods."""

from airflow.providers.cncf.kubernetes.secret import Secret

AWS_ACCESS_KEY_ID = Secret(
    deploy_type="env",
    deploy_target="AWS_ACCESS_KEY_ID",
    secret="airflow-credentials",
    key="AWS_ACCESS_KEY_ID",
)

AWS_SECRET_ACCESS_KEY = Secret(
    deploy_type="env",
    deploy_target="AWS_SECRET_ACCESS_KEY",
    secret="airflow-credentials",
    key="AWS_SECRET_ACCESS_KEY",
)

EPC_AUTH_TOKEN = Secret(
    deploy_type="env",
    deploy_target="EPC_AUTH_TOKEN",
    secret="airflow-credentials",
    key="EPC_AUTH_TOKEN",
)
