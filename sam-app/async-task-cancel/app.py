"""Async Task Cancel Lambda Function.

Handles task cancellation for async video conversion workflow:
1. Stop Step Functions execution
2. Cancel running MediaConvert jobs
3. Clean up S3 temporary files
4. Update task status to CANCELLED

Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Environment variables
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
MEDIACONVERT_ENDPOINT = os.environ.get("MEDIACONVERT_ENDPOINT", "")


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


def get_mediaconvert_client():
    """Get MediaConvert client with custom endpoint."""
    if MEDIACONVERT_ENDPOINT:
        return boto3.client("mediaconvert", endpoint_url=MEDIACONVERT_ENDPOINT)
    # Get endpoint dynamically if not configured
    mc = boto3.client("mediaconvert")
    endpoints = mc.describe_endpoints()
    endpoint_url = endpoints["Endpoints"][0]["Url"]
    return boto3.client("mediaconvert", endpoint_url=endpoint_url)


def get_task(task_id: str, user_id: str) -> dict | None:
    """Get task by ID with user authorization."""
    table = get_dynamodb_table()

    response = table.get_item(Key={"task_id": task_id, "sk": "TASK"})

    item = response.get("Item")
    if not item:
        return None

    # Authorize: check user_id matches
    if item.get("user_id") != user_id:
        logger.warning(
            f"User {user_id} attempted to cancel task {task_id} owned by {item.get('user_id')}"
        )
        return None

    return item


def stop_step_functions_execution(execution_arn: str) -> bool:
    """Stop Step Functions execution."""
    if not execution_arn:
        return True

    try:
        sfn = get_sfn_client()
        sfn.stop_execution(
            executionArn=execution_arn,
            error="UserCancelled",
            cause="Task cancelled by user",
        )
        logger.info(f"Stopped Step Functions execution: {execution_arn}")
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "ExecutionDoesNotExist":
            logger.info(f"Execution already completed or does not exist: {execution_arn}")
            return True
        logger.exception(f"Failed to stop Step Functions execution: {e}")
        return False


def cancel_mediaconvert_jobs(files: list[dict]) -> list[str]:
    """Cancel running MediaConvert jobs for task files."""
    cancelled_jobs = []

    try:
        mc = get_mediaconvert_client()

        for file in files:
            job_id = file.get("mediaconvert_job_id")
            if not job_id:
                continue

            try:
                # Check job status first
                job = mc.get_job(Id=job_id)
                status = job["Job"]["Status"]

                if status in ["SUBMITTED", "PROGRESSING"]:
                    mc.cancel_job(Id=job_id)
                    cancelled_jobs.append(job_id)
                    logger.info(f"Cancelled MediaConvert job: {job_id}")
                else:
                    logger.info(f"MediaConvert job {job_id} already in status: {status}")

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "NotFoundException":
                    logger.info(f"MediaConvert job not found: {job_id}")
                else:
                    logger.warning(f"Failed to cancel MediaConvert job {job_id}: {e}")

    except Exception as e:
        logger.exception(f"Error cancelling MediaConvert jobs: {e}")

    return cancelled_jobs


def cleanup_s3_files(task_id: str, files: list[dict]) -> int:
    """Clean up S3 temporary files for the task."""
    deleted_count = 0
    s3 = get_s3_client()

    # Collect all S3 keys to delete
    keys_to_delete = []

    for file in files:
        # Source file
        source_key = file.get("source_s3_key")
        if source_key:
            keys_to_delete.append(source_key)

        # Output file
        output_key = file.get("output_s3_key")
        if output_key:
            keys_to_delete.append(output_key)

        # Metadata file
        metadata_key = file.get("metadata_s3_key")
        if metadata_key:
            keys_to_delete.append(metadata_key)

    # Also delete any files under the task prefix
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for prefix in [f"input/{task_id}/", f"output/{task_id}/", f"temp/{task_id}/"]:
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys_to_delete.append(obj["Key"])
    except ClientError as e:
        logger.warning(f"Error listing S3 objects: {e}")

    # Remove duplicates
    keys_to_delete = list(set(keys_to_delete))

    # Delete in batches of 1000 (S3 limit)
    for i in range(0, len(keys_to_delete), 1000):
        batch = keys_to_delete[i : i + 1000]
        if batch:
            try:
                s3.delete_objects(
                    Bucket=S3_BUCKET,
                    Delete={"Objects": [{"Key": key} for key in batch]},
                )
                deleted_count += len(batch)
                logger.info(f"Deleted {len(batch)} S3 objects")
            except ClientError as e:
                logger.warning(f"Error deleting S3 objects: {e}")

    return deleted_count


def update_task_cancelled(task_id: str) -> None:
    """Update task status to CANCELLED."""
    table = get_dynamodb_table()
    now = datetime.now(timezone.utc)

    table.update_item(
        Key={"task_id": task_id, "sk": "TASK"},
        UpdateExpression="SET #status = :status, updated_at = :updated, completed_at = :completed",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "CANCELLED",
            ":updated": now.isoformat(),
            ":completed": now.isoformat(),
        },
    )
    logger.info(f"Updated task {task_id} status to CANCELLED")


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda handler for task cancellation.

    POST /tasks/{task_id}/cancel

    Headers:
    - X-User-Id: Required for authorization

    Returns:
    {
        "statusCode": 200,
        "body": {
            "task_id": "...",
            "status": "CANCELLED",
            "cancelled_jobs": [...],
            "deleted_files": 5
        }
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Extract user_id from headers
        headers = event.get("headers", {})
        user_id = headers.get("X-User-Id") or headers.get("x-user-id")

        if not user_id:
            return {
                "statusCode": 401,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Missing X-User-Id header"}),
            }

        # Get task_id from path parameters
        path_params = event.get("pathParameters") or {}
        task_id = path_params.get("task_id")

        if not task_id:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Missing task_id"}),
            }

        # Get task and verify ownership
        task = get_task(task_id, user_id)

        if not task:
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Task not found"}),
            }

        # Check if task can be cancelled
        current_status = task.get("status")
        terminal_statuses = ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED", "CANCELLED"]

        if current_status in terminal_statuses:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(
                    {"error": f"Task cannot be cancelled in status: {current_status}"}
                ),
            }

        # Stop Step Functions execution
        execution_arn = task.get("execution_arn")
        sfn_stopped = stop_step_functions_execution(execution_arn)

        # Cancel MediaConvert jobs
        files = task.get("files", [])
        cancelled_jobs = cancel_mediaconvert_jobs(files)

        # Clean up S3 files
        deleted_files = cleanup_s3_files(task_id, files)

        # Update task status
        update_task_cancelled(task_id)

        response_body = {
            "task_id": task_id,
            "status": "CANCELLED",
            "sfn_stopped": sfn_stopped,
            "cancelled_jobs": cancelled_jobs,
            "deleted_files": deleted_files,
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
        logger.exception(f"Task cancellation failed: {e}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)}),
        }
