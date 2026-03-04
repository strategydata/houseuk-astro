# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added
- Added shared non-Airflow logging bootstrap at `include/logging_config.py` with:
  - `get_logger(name)`
  - `configure_logging()`
  - `LOG_LEVEL` environment variable (default `INFO`)
  - Optional JSON output via `LOG_JSON=true`
- Added EPC extractor package stubs (`extract/epc/__init__.py`, `extract/epc/src/__init__.py`) to support imports and tooling.
- Added EPC Airflow DAG at `dags/extract/epc_extract.py` to schedule incremental EPC ingestion in Kubernetes pods.
- Added EPC secret mapping `EPC_AUTH_TOKEN` in `dags/kube_secrets.py` for KubernetesPodOperator env injection.
- Added test coverage:
  - `tests/include/test_airflow_utils.py` for `stream_url_to_s3` status/error behavior.
  - `tests/epc/test_execute.py` for EPC pipeline integration with shared stream helper.
  - `tests/dags/test_kube_secrets.py` for EPC secret binding validation.

### Changed
- Replaced runtime `print(...)` usage with `logging` calls across extractor/DAG modules.
- Standardized structured log messages in extractor entrypoints (`source`, `action`, and run/object context fields).
- Non-Airflow entrypoints now call `configure_logging()`:
  - `extract/airbnb/src/execute.py`
  - `extract/crime/current/execute.py`
  - `extract/landRegistry/src/execute.py`
  - `extract/epc/src/execute.py`
- CI now runs Ruff through pre-commit hooks (`ruff-check`, `ruff-format`) using `astral-sh/ruff-pre-commit`.
- Ruff policy now enables `T201` (print detection) in `pyproject.toml` with per-file ignore for `tests/**` and `examples/**`.
- Refactored `include.airflow_utils.stream_url_to_s3` to return structured status payloads and clear error categories while preserving Airflow failure semantics by default.
- Refactored `extract/epc/src/execute.py` to use `stream_url_to_s3` instead of ad-hoc HTTP/S3 logic.
- Removed EPC auth token hardcoding fallback; EPC token is now required from env (`EPC_AUTH_TOKEN`) or explicit config.

### Documentation
- Updated docs for CI/CD, local development, script catalog, and extract layer coverage.
- Updated EPC extractor README to match implemented CLI behavior.
- Updated Airflow docs and script catalog to include EPC DAG and secret-injection model.
