# Extract Layer

This folder contains all raw ingestion jobs that pull external datasets into S3.

## Shared Utility

- Script: `extract/utils.py`
- Function: `stream_to_s3(url, bucket, key)`
- Behavior: streams a remote file directly to S3 (`upload_fileobj`) to avoid loading full files into memory.

## Extractors

| Extractor | Path | Runtime | Status | Primary Output |
| --- | --- | --- | --- | --- |
| InsideAirbnb | `extract/airbnb` | Python | Active | `raw/airbnb/{city}/...` |
| UK Crime (current) | `extract/crime/current` | Python | Active | `raw/crime/{YYYY-MM}.zip` |
| UK Crime (next) | `extract/crime/next` | Rust | Experimental | Planned `raw/crime/...` + processed files |
| Land Registry | `extract/landRegistry/src` | Python | Active | `raw/land_registry/...` |
| EPC | `extract/epc` | Python | Active | `raw/epc/{YYYY}/domestic-{YYYY[-MM]}.zip` |

## Airflow DAG Mapping

- `dags/extract/insideairbnb_extract.py` -> `extract/airbnb/src/execute.py`
- `dags/extract/crime_extract.py` -> intended to run `extract/crime/current/execute.py`
- `dags/extract/landRegistry_extract.py` -> `extract/landRegistry/src/execute.py`
- `dags/extract/epc_extract.py` -> `extract/epc/src/execute.py` (incremental)
- `dags/extract/epc_annual_extract.py` -> `extract/epc/src/execute.py` (bulk yearly refresh)

## Local Run Pattern

All Python extractors that use `fire` expose CLI args directly:

```bash
python <extractor>/execute.py --url="<source_url>" --bucket="<s3_bucket>" --key="<s3_key>"
```

InsideAirbnb uses market metadata instead of a direct dataset URL:

```bash
python extract/airbnb/src/execute.py \
  --city="london" \
  --country-slug="united-kingdom" \
  --region-slug="england" \
  --market-slug="london" \
  --page-url="https://insideairbnb.com/london/" \
  --bucket="quibbler-house-data-lake"
```
