# Architecture

## Components

- `extract/`: Python and Rust ingestion scripts for external data sources.
- `dags/`: Airflow DAG definitions that schedule extraction and notification jobs.
- `include/`: Shared DAG helper modules (defaults, Kubernetes helpers, notification callbacks).
- `lambda/`: AWS Lambda services for S3 file routing and decompression workflows.
- `tests/`: Unit and DAG tests.
- `.github/workflows/`: CI quality gates and docs publishing pipelines.

## High-level flow

1. Airflow schedules extractor jobs.
2. Extractors pull remote datasets and write them to S3 under `raw/` prefixes.
3. S3-triggered routing Lambda archives compressed files and dispatches processing based on size.
4. Small compressed files are sent to the unzip Lambda; large files are sent to ECS Fargate.

Note: the unzip Lambda currently handles `.zip` archives. `.gz` handling is routed
by the router and should be configured to use ECS or an alternate handler.

## Storage conventions

- `raw/...`: Landing zone for newly ingested source files.
- `archive/...`: Archived source objects after routing/decompression.
- `latest/...`: Pointer-style keys for most recent dataset versions (where applicable).
