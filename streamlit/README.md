# Streamlit Dashboard

Simple exploratory dashboard for curated housing metrics.

## Run Locally

```bash
pip install streamlit duckdb pandas
streamlit run streamlit/app.py
```

The app reads curated Parquet from S3 using DuckDB's `httpfs` extension. It expects standard AWS
credentials in your environment.
