# houseuk-astro Docs

`houseuk-astro` is a data platform project that orchestrates UK housing-related ingestion pipelines with Airflow, extraction scripts, and AWS Lambda services.

## What this documentation covers

- End-to-end architecture and data flow
- Script-by-script responsibilities
- Airflow DAG behavior and scheduling
- Lambda components and routing logic
- Auto-generated Python API reference from source code

## Quick start

Install dependencies:

```bash
pip install -r requirements.txt
```

Serve docs locally:

```bash
mkdocs serve
```

Build static docs:

```bash
mkdocs build
```

Generated static files are written to `site/`.
