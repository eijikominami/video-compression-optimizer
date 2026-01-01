"""Async Task Submit Lambda Function.

Handles task submission for async video conversion workflow:
1. Validates input parameters
2. Generates presigned URLs for S3 upload
3. Creates task record in DynamoDB
4. Starts Step Functions execution

Requirements: 1.1, 1.2, 1.3
"""

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

# API Request Schema Constants
VALID_QUALITY_PRESETS = ["balanced", "high", "compression", "balanced+", "high+"]

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Environment variables
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")
PRESIGNED_URL_EXPIRATION = int(os.environ.get("PRESIGNED_URL_EXPIRATION", "3600"))
TTL_DAYS = int(os.environ.get("TTL_DAYS", "90"))


def get_dynamodb_table():
    """Get DynamoDB table resource."""
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table(DYNAMODB_TABLE)


def get_s3_client():
    """Get S3 client."""
    return boto3.client("s3")


def get_sfn_client():
    """Get Step Functions client."""
    return boto3.client("stepfunctions")


def generate_presigned_url(s3_key: str, expiration: int = PRESIGNED_URL_EXPIRATION) -> str:
    """Generate presigned URL for S3 upload."""
    s3 = get_s3_client()
    url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": S3_BUCKET, "Key": s3_key},
        ExpiresIn=expiration,
    )
    return url


def create_task_record(
    task_id: str,
    user_id: str,
    quality_preset: str,
    files: list[dict],
) -> dict:
    """Create task record in DynamoDB."""
    table = get_dynamodb_table()
    now = datetime.now(timezone.utc)
    ttl = int((now + timedelta(days=TTL_DAYS)).timestamp())

    item = {
        "task_id": task_id,
        "sk": "TASK",
        "user_id": user_id,
        "status": "PENDING",
        "quality_preset": quality_preset,
        "files": files,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "progress_percentage": 0,
        "current_step": "pending",
        "ttl": ttl,
    }

    table.put_item(Item=item)
    logger.info(f"Created task record: {task_id}")
    return item


def start_step_functions_execution(
    task_id: str,
    user_id: str,
    quality_preset: str,
    files: list[dict],
) -> str:
    """Start Step Functions execution."""
    sfn = get_sfn_client()

    input_data = {
        "task_id": task_id,
        "user_id": user_id,
        "quality_preset": quality_preset,
        "files": files,
    }

    response = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=f"vco-{task_id}",
        input=json.dumps(input_data),
    )

    execution_arn = response["executionArn"]
    logger.info(f"Started Step Functions execution: {execution_arn}")
    return execution_arn


def update_task_execution_arn(task_id: str, execution_arn: str) -> None:
    """Update task record with Step Functions execution ARN."""
    table = get_dynamodb_table()
    now = datetime.now(timezone.utc)

    table.update_item(
        Key={"task_id": task_id, "sk": "TASK"},
        UpdateExpression="SET execution_arn = :arn, updated_at = :updated",
        ExpressionAttributeValues={
            ":arn": execution_arn,
            ":updated": now.isoformat(),
        },
    )


def validate_request(body: dict) -> tuple[bool, str | None, str | None]:
    """Validate request body.

    Returns:
        Tuple of (is_valid, error_message, error_code)
    """
    required_fields = ["user_id", "files"]

    for field in required_fields:
        if field not in body:
            return False, f"Missing required field: {field}", "MISSING_FIELD"

    files = body.get("files", [])
    if not files:
        return False, "At least one file is required", "EMPTY_FILES"

    if not isinstance(files, list):
        return False, "files must be a list", "INVALID_FILES"

    for i, file in enumerate(files):
        if not isinstance(file, dict):
            return False, f"files[{i}] must be an object", "INVALID_FILE"
        if "filename" not in file:
            return False, f"files[{i}] missing filename", "MISSING_FILENAME"
        if "original_uuid" not in file:
            return False, f"files[{i}] missing original_uuid", "MISSING_UUID"

    quality_preset = body.get("quality_preset", "balanced")
    if quality_preset not in VALID_QUALITY_PRESETS:
        return (
            False,
            f"Invalid quality_preset. Must be one of: {VALID_QUALITY_PRESETS}",
            "INVALID_PRESET",
        )

    return True, None, None


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda handler for task submission.

    Expected request body:
    {
        "user_id": "user-123",
        "quality_preset": "balanced",  // optional, default: balanced
        "files": [
            {
                "filename": "video1.mov",
                "original_uuid": "ABC-123-DEF"
            }
        ]
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "task_id": "uuid-v4",
            "status": "PENDING",
            "upload_urls": [
                {
                    "file_id": "uuid-v4",
                    "filename": "video1.mov",
                    "upload_url": "https://..."
                }
            ]
        }
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Parse request body
        if isinstance(event.get("body"), str):
            body = json.loads(event["body"])
        else:
            body = event.get("body", event)

        # Validate request
        is_valid, error_message, _error_code = validate_request(body)
        if not is_valid:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": error_message}),
            }

        # Extract parameters
        user_id = body["user_id"]
        quality_preset = body.get("quality_preset", "balanced")
        input_files = body["files"]

        # Use task_id from request if provided (CLI pre-uploads files)
        # Otherwise generate a new one (for presigned URL flow)
        task_id = body.get("task_id") or str(uuid.uuid4())

        # Prepare files with IDs and S3 keys
        files = []
        upload_urls = []

        for input_file in input_files:
            # Use file_id from request if provided (CLI pre-uploads files)
            file_id = input_file.get("file_id") or str(uuid.uuid4())
            filename = input_file["filename"]
            original_uuid = input_file["original_uuid"]

            # Use source_s3_key from request if provided (CLI pre-uploads files)
            # Otherwise generate a new one
            source_s3_key = (
                input_file.get("source_s3_key") or f"input/{task_id}/{file_id}/{filename}"
            )

            # Use metadata_s3_key from request if provided (CLI pre-uploads files)
            metadata_s3_key = input_file.get("metadata_s3_key")

            file_record = {
                "file_id": file_id,
                "original_uuid": original_uuid,
                "filename": filename,
                "source_s3_key": source_s3_key,
                "status": "PENDING",
                "retry_count": 0,
                "preset_attempts": [],
            }
            # Add metadata_s3_key if provided
            if metadata_s3_key:
                file_record["metadata_s3_key"] = metadata_s3_key

            files.append(file_record)

            # Generate presigned URL for upload
            upload_url = generate_presigned_url(source_s3_key)
            upload_urls.append(
                {
                    "file_id": file_id,
                    "filename": filename,
                    "upload_url": upload_url,
                }
            )

        # Create task record in DynamoDB
        create_task_record(task_id, user_id, quality_preset, files)

        # Start Step Functions execution
        execution_arn = start_step_functions_execution(task_id, user_id, quality_preset, files)

        # Update task with execution ARN
        update_task_execution_arn(task_id, execution_arn)

        # Return response
        response_body = {
            "task_id": task_id,
            "status": "PENDING",
            "quality_preset": quality_preset,
            "upload_urls": upload_urls,
        }

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_body),
        }

    except ClientError as e:
        logger.exception(f"AWS service error: {e}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"AWS service error: {str(e)}"}),
        }
    except Exception as e:
        logger.exception(f"Task submission failed: {e}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)}),
        }
