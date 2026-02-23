import boto3
import io
import os
import zipfile
import mimetypes
from datetime import datetime
from aws_lambda_powertools import Logger


logger=Logger()
s3 = boto3.client('s3')
ARCHIVE_PREFIX = "archive/"


def build_archive_key(key:str) -> str:
    today = datetime.now().strftime("%Y/%m/%d")
    base_name=os.path.basename(key)
    name,ext =os.path.splitext(base_name)
    unique_name =f"{name}_{datetime.now().strftime('%H%M%S%f')}{ext}"
    archive_key = f"{ARCHIVE_PREFIX}{today}/{unique_name}"
    return archive_key

@logger.inject_lambda_context
def lambda_handler(event, context):
    # 1. Get bucket and key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    old_key = event['Records'][0]['s3']['object']['key']
    logger.info("Processing file", extra={"bucket": bucket, "key": old_key})
    
    if not old_key.endswith('.zip'):
        return {"status": "skipped","reason":"not a zip files"}
    if old_key.startswith(ARCHIVE_PREFIX ):
        return {"status": "skipped","reason":"archiving"}
    
    target_prefix = os.path.dirname(old_key)
    if target_prefix:
        target_prefix +="/"

    # # 2. Get the object from S3
    try:
        response = s3.get_object(Bucket=bucket, Key=old_key)
        zip_data = io.BytesIO(response['Body'].read())

        with zipfile.ZipFile(zip_data) as z:
            for key in z.namelist():
                # Skip directories
                if key.endswith('/'):
                    continue
                
                # if not key.lower().endswith(('.csv', '.json')):
                #     print(f"Skipping {key} as it is not a CSV or JSON file.")
                #     continue
                    
                new_key = f"{target_prefix}{key}"
                
        #         # 4. Stream the individual file back to S3
                with z.open(key) as extracted_file:
                    content_type, _ = mimetypes.guess_type(key)
                    
                    s3.upload_fileobj(
                        Fileobj=extracted_file,
                        Bucket=bucket,
                        Key=new_key,
                        ExtraArgs={'ContentType': content_type or 'application/octet-stream'}
                    )
        archive_key= build_archive_key(old_key)
        logger.info("Archive old zip file", extra={"archive_key": archive_key, "old_key": old_key,"bucket": bucket})
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': old_key},
            Key=archive_key
        )
        s3.delete_object(Bucket=bucket, Key=old_key)
        logger.info("Deleted old zip file", extra={"archive_key": archive_key, "old_key": old_key,"bucket": bucket})
        return {"status": "success",
                "archive_key": archive_key
                }
    except Exception as e:
        logger.error("Error processing file", extra={"error": str(e), "bucket": bucket, "key": old_key})
        return {"status": "error", "message": str(e)}