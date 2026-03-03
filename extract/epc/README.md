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

## Example Invocation

```bash
python extract/epc/src/execute.py bulk --start_year=2024 --end_year=2025
python extract/epc/src/execute.py incremental --year=2026
python extract/epc/src/execute.py incremental --year=2026 --month=1
```

## Logging

- Uses shared `include/logging_config.py` bootstrap for non-Airflow logging.
- Default level is `INFO` (`LOG_LEVEL` override supported).
- Set `LOG_JSON=true` for JSON structured output.
