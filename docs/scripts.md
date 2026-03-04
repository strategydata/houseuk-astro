# Script Catalog

This catalog summarizes operational scripts and what each one is responsible for.

| Path | Type | Responsibility |
| --- | --- | --- |
| `extract/utils.py` | Python module | Shared `stream_to_s3` helper for streaming HTTP downloads to S3. |
| `extract/airbnb/src/execute.py` | Python script | Resolves latest InsideAirbnb snapshot and writes dated + latest objects to S3. |
| `extract/epc/src/execute.py` | Python script | EPC extractor CLI; builds year/month identifiers and delegates transfer/error handling to `include.airflow_utils.stream_url_to_s3`. |
| `extract/crime/current/execute.py` | Python script | UK Crime extractor CLI wrapper around shared streaming utility. |
| `extract/landRegistry/src/execute.py` | Python script | Land Registry extractor CLI wrapper around shared streaming utility. |
| `dags/extract/insideairbnb_extract.py` | Airflow DAG | Schedules market-by-market InsideAirbnb extraction tasks in Kubernetes pods. |
| `dags/extract/crime_extract.py` | Airflow DAG | Schedules UK Crime extraction task in Kubernetes pod. |
| `dags/extract/landRegistry_extract.py` | Airflow DAG | Schedules Land Registry extraction task in Kubernetes pod. |
| `dags/notification/slack_notifier_dag.py` | Airflow DAG | Test DAG for validating Slack failure alerting. |
| `dags/kube_secrets.py` | Python module | Declares reusable Kubernetes Secret mappings for AWS credentials. |
| `include/airflow_utils.py` | Python module | Shared Airflow constants, Kubernetes defaults, Slack callback, and bootstrap commands. |
| `include/logging_config.py` | Python module | Shared logging bootstrap for non-Airflow Python entrypoints (`LOG_LEVEL`, optional JSON formatting). |
| `include/kubernetes_helpers.py` | Python module | Affinity/toleration selection helpers per workload type/environment. |
| `lambda/stream_unzip_s3.py` | Lambda Python handler | Unzips S3 `.zip` files, uploads extracted entries, archives and removes original objects. |
| `lambda/s3_file_router/src/main.rs` | Rust Lambda bootstrap | Runtime entrypoint that registers the router handler. |
| `lambda/s3_file_router/src/event_handler.rs` | Rust module | Routes compressed S3 files to Lambda or ECS based on size and archives originals. |
| `extract/crime/next/src/main.rs` | Rust prototype | Experimental next-gen crime extractor implementation. |
| `.github/workflows/ci.yml` | GitHub Actions workflow | Runs Python quality gates and Rust unit tests on push/PR. |
| `.github/workflows/docs.yml` | GitHub Actions workflow | Builds and publishes docs to `gh-pages` with `mkdocs gh-deploy`. |
| `.pre-commit-config.yaml` | Pre-commit config | Enforces formatting, linting, and basic file hygiene checks locally. |

## Reading order for new contributors

1. `docs/architecture.md`
2. `docs/extract.md`
3. `docs/airflow.md`
4. `docs/lambda.md`
5. API reference pages
