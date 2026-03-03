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
2. Archives the source compressed file.
3. Routes by file size threshold:
   - Below threshold: async invoke unzip Lambda.
   - Above threshold: launch ECS Fargate task.

Environment variables control thresholds, archive behavior, and ECS/Lambda targets.
