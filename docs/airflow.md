# Airflow

## DAGs

- `insideairbnb_extract`: Monthly schedule (`0 5 1 * *`) to ingest multiple UK markets.
- `crime_extract`: Monthly schedule (`0 7 1 * *`) for UK Crime data.
- `landRegistry_extract`: Monthly schedule (`0 6 1 * *`) for Land Registry data.
- `slack`: On-demand test DAG for Slack failure notifications.

## Shared configuration

Shared DAG defaults and Kubernetes execution settings are centralized in:

- `include/airflow_utils.py`
- `include/kubernetes_helpers.py`
- `dags/kube_secrets.py`

The container image used by extractor DAGs is pinned to a versioned tag
(`ghcr.io/strategydata/data-infrastructure:2026.03.0`) in `include/airflow_utils.py`.

## Execution model

DAG tasks mainly run via `KubernetesPodOperator`, clone this repository, and then execute extractor scripts with CLI arguments.
