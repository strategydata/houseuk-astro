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
3. S3-triggered Lambda handlers route compressed files and archive originals.
4. Large files are routed to ECS Fargate; smaller files are handled by Lambda unzip functions.

## Storage conventions

- `raw/...`: Landing zone for newly ingested source files.
- `archive/...`: Archived source objects after routing/decompression.
- `latest/...`: Pointer-style keys for most recent dataset versions (where applicable).
