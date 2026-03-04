# EPC Extract

Documentation and plan for ingesting EPC datasets from Open Data Communities.

## Source

- Base URL pattern: `https://epc.opendatacommunities.org/files/domestic-YYYY.zip`
- Example: `https://epc.opendatacommunities.org/files/domestic-2025.zip`

## Target S3 Layout

- `raw/epc/domestic-YYYY.zip`

## Current Status

- Extraction code is implemented in `extract/epc/src/execute.py`.
- The CLI supports:
  - `bulk --start_year=YYYY --end_year=YYYY`
  - `incremental --year=YYYY [--month=MM]`
- Transfer logic is delegated to `include.airflow_utils.stream_url_to_s3` for
  consistent timeout handling, status codes, and S3 error handling.

## Example Invocation

```bash
uv run python extract/epc/src/execute.py bulk --start_year=2024 --end_year=2025
uv run python extract/epc/src/execute.py incremental --year=2026
uv run python extract/epc/src/execute.py incremental --year=2026 --month=1
```

## Logging

- Uses shared `include/logging_config.py` bootstrap for non-Airflow logging.
- Default level is `INFO` (`LOG_LEVEL` override supported).
- Set `LOG_JSON=true` for JSON structured output.
- Transport-layer errors are logged centrally by `stream_url_to_s3`; the EPC
  extractor only emits concise workflow logs to avoid duplicate noisy errors.

## Runtime Configuration

The EPC CLI can be configured with environment variables:

- `EPC_AUTH_TOKEN`: Basic auth token for Open Data Communities requests.
- `EPC_USER_AGENT`: user-agent header sent to the source API.
- `LOG_LEVEL`: log verbosity for CLI runs.
- `LOG_JSON`: if true, emit JSON logs.
