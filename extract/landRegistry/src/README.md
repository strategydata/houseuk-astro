# Land Registry Price Paid Extract

Ingests Land Registry Price Paid data for England and Wales into S3.

## Source Files

- Complete history:
  - `http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv`
- Monthly update:
  - `http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv`

## Entrypoint

- Script: `extract/landRegistry/src/execute.py`
- CLI function: `stream_to_s3(url, bucket, key)`

## Required CLI Args

- `url`
- `bucket`
- `key`

## Examples

Full history:

```bash
python extract/landRegistry/src/execute.py \
  --url="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv" \
  --bucket="quibbler-house-data-lake" \
  --key="raw/land_registry/pp-complete.csv"
```

Monthly refresh:

```bash
python extract/landRegistry/src/execute.py \
  --url="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv" \
  --bucket="quibbler-house-data-lake" \
  --key="raw/land_registry/pp-monthly-update-new-version.csv"
```

## Airflow

- DAG: `dags/extract/landRegistry_extract.py`
- Schedule: `0 6 1 * *` (monthly)
- Start date: `2026-01-01`
