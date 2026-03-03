# EPC Extract

Documentation and plan for ingesting EPC datasets from Open Data Communities.

## Source

- Base URL pattern: `https://epc.opendatacommunities.org/files/domestic-YYYY.zip`
- Example: `https://epc.opendatacommunities.org/files/domestic-2025.zip`

## Target S3 Layout

- `raw/epc/domestic-YYYY.zip`

## Current Status

- Extraction code is not implemented yet in this repository.
- This folder currently tracks source conventions and expected output naming.

## Planned CLI Contract

When implemented, the extractor should accept:

- `url`
- `bucket`
- `key`

and use the same streaming upload pattern as other Python extractors.

## Example Planned Invocation

```bash
python extract/epc/src/execute.py \
  --url="https://epc.opendatacommunities.org/files/domestic-2025.zip" \
  --bucket="quibbler-house-data-lake" \
  --key="raw/epc/domestic-2025.zip"
```
