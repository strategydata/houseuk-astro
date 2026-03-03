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

### Documentation
- Updated docs for CI/CD, local development, script catalog, and extract layer coverage.
- Updated EPC extractor README to match implemented CLI behavior.
