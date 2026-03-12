# Lambda

## `lambda/stream_unzip_s3.py`

Python Lambda handler that:

1. Reads S3 event records.
2. Skips non-zip and already archived keys.
3. Extracts zip entries and uploads each file back to S3.
4. Moves original zip into `archive/YYYY/MM/DD/...` and deletes source key.

## `lambda/s3_file_router`

Rust Lambda that:

1. Parses S3 event records.
2. Archives the source compressed file under `ARCHIVE_PREFIX` (default `archive/`).
3. Routes `.zip` and `.gz` objects by file size threshold:
   - Below threshold: async invoke unzip Lambda.
   - Above threshold: launch ECS Fargate task.

Environment variables control thresholds, archive behavior, and ECS/Lambda targets:

- `FARGATE_THRESHOLD_MB` (default: `512`)
- `ARCHIVE_PREFIX` (default: `archive`)
- `ARCHIVE_STORAGE_CLASS` (default: `INTELLIGENT_TIERING`)
- `UNZIP_LAMBDA_FUNCTION_NAME` (required)
- `ECS_CLUSTER_ARN` (required)
- `ECS_TASK_DEFINITION_ARN` (required)
- `ECS_CONTAINER_NAME` (required)
- `ECS_SUBNETS` (required, comma-separated)
- `ECS_SECURITY_GROUPS` (required, comma-separated)
- `ECS_ASSIGN_PUBLIC_IP` (optional, `true`/`false`)

Note: `lambda/stream_unzip_s3.py` only processes `.zip` archives. If `.gz` payloads
should be handled, configure routing thresholds or ECS tasks accordingly.
