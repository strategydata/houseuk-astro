from dags import kube_secrets


def test_epc_auth_token_secret_binding():
    assert kube_secrets.EPC_AUTH_TOKEN.deploy_type == "env"
    assert kube_secrets.EPC_AUTH_TOKEN.deploy_target == "EPC_AUTH_TOKEN"
    assert kube_secrets.EPC_AUTH_TOKEN.secret == "airflow-credentials"
    assert kube_secrets.EPC_AUTH_TOKEN.key == "EPC_AUTH_TOKEN"
