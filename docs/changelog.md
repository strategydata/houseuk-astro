# Changelog

Project changelog is tracked in the repository root `CHANGELOG.md`.

## Latest (Unreleased)

- Added shared logging helper `include/logging_config.py` for non-Airflow entrypoints.
- Standardized extractor logs to structured messages and replaced runtime `print(...)` usage with `logging`.
- Integrated Ruff via pre-commit (`astral-sh/ruff-pre-commit`) in CI.
- Enabled Ruff `T201` policy in `pyproject.toml` to block `print(...)` outside `tests/**` and `examples/**`.
