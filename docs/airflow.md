# Airflow

## DAGs

- `insideairbnb_extract`: Monthly schedule (`0 5 1 * *`) to ingest multiple UK markets.
- `crime_extract`: Monthly schedule (`0 7 1 * *`) for UK Crime data.
- `landRegistry_extract`: Monthly schedule (`0 6 1 * *`) for Land Registry data.
- `epc_extract`: Monthly schedule (`0 8 1 * *`) for EPC incremental ingestion.
- `epc_annual_extract`: Yearly schedule (`0 9 1 1 *`) to refresh the previous year's EPC dataset.
- `slack`: On-demand test DAG for Slack failure notifications.

## Shared configuration

Shared DAG defaults and Kubernetes execution settings are centralized in:

- `include/airflow_utils.py`
- `include/kubernetes_helpers.py`
- `dags/kube_secrets.py`

The container image used by extractor DAGs is pinned to a versioned tag
(`ghcr.io/strategydata/data-infrastructure:2026.03.0`) in `include/airflow_utils.py`.

## Secrets

Kubernetes pod environment secrets are declared in `dags/kube_secrets.py` and
mounted in DAG tasks via the `secrets=[...]` argument of `KubernetesPodOperator`.

Current mappings include:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `EPC_AUTH_TOKEN`

## Execution model

DAG tasks mainly run via `KubernetesPodOperator`, clone this repository, and then execute extractor scripts with CLI arguments.

## Local validation

With Docker running, validate DAG parsing and run tests in the Airflow image:

```bash
astro dev parse
astro dev pytest tests/include
```

For fast unit tests outside the container:

```bash
uv run pytest tests/include
```
