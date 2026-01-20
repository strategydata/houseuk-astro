from airflow.providers.cncf.kubernetes.secret import Secret


AWS_ACCESS_KEY_ID=Secret(
    deploy_type="env",
    deploy_target="AWS_ACCESS_KEY_ID",
    secret="airflow-credentials",
    key="AWS_ACCESS_KEY_ID",
)

AWS_SECRET_ACCESS_KEY =Secret(
    deploy_type="env",
    deploy_target="AWS_SECRET_ACCESS_KEY",
    secret="airflow-credentials",
    key="AWS_SECRET_ACCESS_KEY",
    )

