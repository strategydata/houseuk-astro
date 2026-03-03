# UK Crime Extract (Current Python)

Downloads a monthly UK Police archive ZIP and stores it in S3.

## Source

- Base archive URL: `https://data.police.uk/data/archive/`
- File pattern: `YYYY-MM.zip`
- Example: `https://data.police.uk/data/archive/2025-12.zip`

## Entrypoint

- Script: `extract/crime/current/execute.py`
- CLI function: `stream_to_s3(url, bucket, key)`

## Required CLI Args

- `url`
- `bucket`
- `key`

## Example

```bash
python extract/crime/current/execute.py \
  --url="https://data.police.uk/data/archive/2025-12.zip" \
  --bucket="quibbler-house-data-lake" \
  --key="raw/crime/2025-12.zip"
```

## Airflow

- DAG: `dags/extract/crime_extract.py`
- Schedule: `0 7 1 * *` (monthly)
- Start date: `2026-01-01`

Note: DAG command path should target `extract/crime/current/execute.py`.