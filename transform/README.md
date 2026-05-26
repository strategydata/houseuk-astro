# Transform Layer (dbt + DuckDB)

This folder holds the dbt project for daily transforms. It reads raw data in S3 and writes curated
Parquet outputs back to S3.

## Quick Start (local)

```bash
pip install dbt-duckdb duckdb
dbt run --project-dir transform --profiles-dir transform --target prod
```

## S3 Layout (assumed)

- Raw: `s3://quibbler-house-data-lake/raw/...`
- Curated: `s3://quibbler-house-data-lake/curated/...`

Update `dbt_project.yml` vars if your paths differ.

## Profile

`transform/profiles.yml` uses DuckDB with the `httpfs` extension to read S3. It relies on
standard AWS environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional
`AWS_SESSION_TOKEN`).

## Notes

- Rightmove schema and GeoJSON structure vary by source. The staging models include defaults that
  you should adjust once you confirm your exact JSON shapes.
- Land Registry data does not include bedroom counts. If you want price stats by bedrooms, plan to
  join with EPC or Rightmove listings later.
