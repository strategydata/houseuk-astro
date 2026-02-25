# InsideAirbnb Extract

Ingests the latest `listings.csv.gz` snapshot for UK markets from InsideAirbnb into S3.

## Scope

Markets currently configured in Airflow:

- Bristol (`insideairbnb.com/bristol`)
- Edinburgh (`insideairbnb.com/edinburgh`)
- Greater Manchester (`insideairbnb.com/greater-manchester`)
- London (`insideairbnb.com/london`)

## How It Works

1. Open market page (for example `https://insideairbnb.com/london/`).
2. Discover all dataset URLs matching:
   - `https://data.insideairbnb.com/{country}/{region}/{market}/{YYYY-MM-DD}/data/listings.csv.gz`
3. Pick latest snapshot date.
4. Upload to dated key:
   - `raw/airbnb/{city}/listings_{YYYY-MM-DD}.csv.gz`
5. Refresh stable pointer key:
   - `raw/airbnb/{city}/latest/listings.csv.gz`

This gives both historical snapshots and a stable "latest" object.

## Entrypoint

- Script: `extract/airbnb/src/execute.py`
- CLI function: `extract_latest_market_snapshot(...)`

### Required CLI Args

- `city`
- `country_slug`
- `region_slug`
- `market_slug`
- `page_url`
- `bucket`

### Example

```bash
python extract/airbnb/src/execute.py \
  --city="london" \
  --country-slug="united-kingdom" \
  --region-slug="england" \
  --market-slug="london" \
  --page-url="https://insideairbnb.com/london/" \
  --bucket="quibbler-house-data-lake"
```

## Airflow

- DAG: `dags/extract/insideairbnb_extract.py`
- Schedule: `0 5 1 * *` (monthly)
- Start date: `2026-01-01`

## Tests

- `tests/airbnb/test_insideairbnb_execute.py`
- Coverage includes latest URL resolution, no-match behavior, and S3 copy refresh behavior.