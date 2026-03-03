# CI/CD

## Workflows

- `.github/workflows/ci.yml`
  - Runs on push and pull requests.
  - Python checks:
    - `uv sync --frozen --group dev --group docs`
    - `uv run ruff check extract lambda/stream_unzip_s3.py tests/airbnb tests/include`
    - `uv run mypy`
    - `uv run pytest tests/airbnb tests/include`
    - `uv run mkdocs build`
  - Rust checks:
    - `cargo test --locked` in `lambda/s3_file_router`

- `.github/workflows/docs.yml`
  - Runs on push to `main`/`master` and manual dispatch.
  - Installs docs group with `uv`.
  - Publishes docs via `uv run mkdocs gh-deploy --force`.

## Dependency workflow

- Dependency source of truth: `pyproject.toml`
- Locked versions: `uv.lock`
- Compatibility export: `requirements.txt` (generated from `uv.lock`)

Common commands:

```bash
uv lock
uv sync --frozen --group dev --group docs
uv export --format requirements-txt --no-hashes --no-dev --no-group docs --output-file requirements.txt
```

## Pre-commit

Local consistency hooks are configured in `.pre-commit-config.yaml`:

- `trailing-whitespace`
- `end-of-file-fixer`
- `check-yaml`
- `check-merge-conflict`
- `ruff-format`
- `ruff-check`

Set up once:

```bash
uv run pre-commit install
```
