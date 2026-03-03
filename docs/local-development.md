# Local Development

## Run extractors manually

InsideAirbnb:

```bash
python extract/airbnb/src/execute.py --city="london" --country-slug="united-kingdom" --region-slug="england" --market-slug="london" --page-url="https://insideairbnb.com/london/" --bucket="<your-bucket>"
```

UK Crime current extractor:

```bash
python extract/crime/current/execute.py --url="https://data.police.uk/data/archive/2025-12.zip" --bucket="<your-bucket>" --key="raw/crime/2025-12.zip"
```

Land Registry extractor:

```bash
python extract/landRegistry/src/execute.py --url="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv" --bucket="<your-bucket>" --key="raw/land_registry/pp-complete.csv"
```

## Build docs

```bash
mkdocs build
```

## Preview docs

```bash
mkdocs serve
```
