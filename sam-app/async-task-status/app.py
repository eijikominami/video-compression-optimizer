"""Async Task Status Lambda Function.

Handles task status queries for async video conversion workflow:
1. Get single task details by task_id
2. List tasks for a user with optional status filter
3. Authorize access by user_id

Requirements: 2.1, 2.2, 2.3
"""

import json
import logging
import os
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal types from DynamoDB."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            # Convert to int if it's a whole number, otherwise float
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super().default(obj)


# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Environment variables
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
PRESIGNED_URL_EXPIRATION = int(os.environ.get("PRESIGNED_URL_EXPIRATION", "3600"))


def get_dynamodb_table():
    """Get DynamoDB table resource."""
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table(DYNAMODB_TABLE)


def get_s3_client():
    """Get S3 client."""
    return boto3.client("s3")


def generate_download_url(s3_key: str, expiration: int = PRESIGNED_URL_EXPIRATION) -> str:
    """Generate presigned URL for S3 download."""
    s3 = get_s3_client()
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": s3_key},
        ExpiresIn=expiration,
    )
    return url


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
            f"User {user_id} attempted to access task {task_id} owned by {item.get('user_id')}"
        )
        return None

    return item


def list_tasks(
    user_id: str,
    status_filter: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """List tasks for a user with optional status filter."""
    table = get_dynamodb_table()

    if status_filter:
        # Use GSI2 for status-based query, then filter by user_id
        response = table.query(
            IndexName="GSI2-StatusTasks",
            KeyConditionExpression=Key("status").eq(status_filter),
            FilterExpression="user_id = :uid",
            ExpressionAttributeValues={":uid": user_id},
            Limit=limit,
            ScanIndexForward=False,  # Most recent first
        )
    else:
        # Use GSI1 for user-based query
        response = table.query(
            IndexName="GSI1-UserTasks",
            KeyConditionExpression=Key("user_id").eq(user_id),
            Limit=limit,
            ScanIndexForward=False,  # Most recent first
        )

    return response.get("Items", [])


def get_mediaconvert_client():
    """Get MediaConvert client with endpoint."""
    mc = boto3.client("mediaconvert")
    endpoints = mc.describe_endpoints()
    endpoint_url = endpoints["Endpoints"][0]["Url"]
    return boto3.client("mediaconvert", endpoint_url=endpoint_url)


def get_mediaconvert_job_progress(job_id: str) -> int:
    """Get MediaConvert job progress percentage.

    Args:
        job_id: MediaConvert job ID

    Returns:
        Job progress percentage (0-100), or 0 if unable to get progress
    """
    try:
        mc = get_mediaconvert_client()
        response = mc.get_job(Id=job_id)
        job = response.get("Job", {})
        # jobPercentComplete is available when job is PROGRESSING
        return job.get("JobPercentComplete", 0)
    except Exception as e:
        logger.warning(f"Failed to get MediaConvert job progress for {job_id}: {e}")
        return 0


def calculate_progress(files: list[dict]) -> tuple[int, str]:
    """Calculate progress percentage and current step from file statuses.

    Progress is calculated dynamically at query time (not stored in DynamoDB).

    File status to progress mapping:
    - PENDING: 0%
    - CONVERTING: 0-30% (scaled from MediaConvert jobPercentComplete)
    - VERIFYING: 65% (fixed, SSIM calculation has no progress API)
    - COMPLETED/FAILED: 100%

    Task progress is the average of all file progress percentages.

    Returns:
        Tuple of (progress_percentage, current_step)
    """
    if not files:
        return 0, "pending"

    total_progress = 0
    current_step = "pending"

    for f in files:
        status = f.get("status", "PENDING")

        if status == "PENDING":
            # 0%
            pass
        elif status == "CONVERTING":
            # 0-30% range, scaled from MediaConvert jobPercentComplete
            job_id = f.get("mediaconvert_job_id")
            if job_id:
                mc_progress = get_mediaconvert_job_progress(job_id)
                # Scale 0-100% to 0-30%
                total_progress += int(mc_progress * 0.3)
            # else: 0%
            current_step = "converting"
        elif status == "VERIFYING":
            # 65% fixed (SSIM calculation has no progress API)
            total_progress += 65
            current_step = "verifying"
        elif status in ("COMPLETED", "FAILED"):
            # 100%
            total_progress += 100

    # Calculate average progress
    progress = int(total_progress / len(files))

    # Determine current step (most advanced active state)
    completed_count = sum(1 for f in files if f.get("status") in ("COMPLETED", "FAILED"))
    if completed_count == len(files):
        current_step = "completed"

    return progress, current_step


def calculate_progress_simple(files: list[dict]) -> tuple[int, str]:
    """Calculate progress percentage without MediaConvert API calls.

    Used for list view where we don't want to make API calls for each task.
    Uses fixed values for CONVERTING status instead of querying MediaConvert.

    File status to progress mapping:
    - PENDING: 0%
    - CONVERTING: 15% (midpoint of 0-30% range)
    - VERIFYING: 65% (fixed)
    - COMPLETED/FAILED: 100%

    Returns:
        Tuple of (progress_percentage, current_step)
    """
    if not files:
        return 0, "pending"

    total_progress = 0
    current_step = "pending"

    for f in files:
        status = f.get("status", "PENDING")

        if status == "PENDING":
            pass  # 0%
        elif status == "CONVERTING":
            total_progress += 15  # Midpoint of 0-30%
            current_step = "converting"
        elif status == "VERIFYING":
            total_progress += 65
            current_step = "verifying"
        elif status in ("COMPLETED", "FAILED"):
            total_progress += 100

    progress = int(total_progress / len(files))

    completed_count = sum(1 for f in files if f.get("status") in ("COMPLETED", "FAILED"))
    if completed_count == len(files):
        current_step = "completed"

    return progress, current_step


def format_task_response(task: dict, include_download_urls: bool = False) -> dict:
    """Format task for API response."""
    files = task.get("files", [])

    # Format files for response
    formatted_files = []
    for file in files:
        formatted_file = {
            "file_id": file.get("file_id"),
            "filename": file.get("filename"),
            "status": file.get("status"),
            "error_message": file.get("error_message"),
            "output_s3_key": file.get("output_s3_key"),
            "best_effort": file.get("best_effort", False),
        }

        # Extract quality_result fields from nested structure
        quality_result = file.get("quality_result", {})
        if quality_result:
            # Handle nested body structure from Lambda response
            body = quality_result.get("body", quality_result)
            formatted_file["quality_result"] = {
                "ssim_score": body.get("ssim_score"),
                "original_size": body.get("original_size"),
                "converted_size": body.get("converted_size"),
                "compression_ratio": body.get("compression_ratio"),
                "space_saved_bytes": body.get("space_saved_bytes"),
                "space_saved_percent": body.get("space_saved_percent"),
            }
            # Also set output_size_bytes from quality_result if not already set
            if not formatted_file.get("output_size_bytes"):
                formatted_file["output_size_bytes"] = body.get("converted_size")

        # Add download URL for completed files if requested
        if (
            include_download_urls
            and task.get("status") in ["COMPLETED", "PARTIALLY_COMPLETED"]
            and file.get("status") == "COMPLETED"
            and file.get("output_s3_key")
        ):
            formatted_file["download_url"] = generate_download_url(file["output_s3_key"])

        formatted_files.append(formatted_file)

    # Calculate progress from file statuses
    progress, current_step = calculate_progress(files)

    return {
        "task_id": task.get("task_id"),
        "status": task.get("status"),
        "quality_preset": task.get("quality_preset"),
        "progress_percentage": progress,
        "current_step": current_step,
        "estimated_completion_time": task.get("estimated_completion_time"),
        "files": formatted_files,
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
        "started_at": task.get("started_at"),
        "completed_at": task.get("completed_at"),
        "error_message": task.get("error_message"),
    }


def format_task_summary(task: dict) -> dict:
    """Format task summary for list response."""
    files = task.get("files", [])
    completed_count = sum(1 for f in files if f.get("status") == "COMPLETED")
    failed_count = sum(1 for f in files if f.get("status") == "FAILED")

    # Use simple progress calculation (no MediaConvert API calls)
    progress, _ = calculate_progress_simple(files)

    return {
        "task_id": task.get("task_id"),
        "status": task.get("status"),
        "quality_preset": task.get("quality_preset"),
        "progress_percentage": progress,
        "file_count": len(files),
        "completed_count": completed_count,
        "failed_count": failed_count,
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
    }


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda handler for task status queries.

    GET /tasks/{task_id}
    - Returns detailed task status with download URLs

    GET /tasks?status={status}&limit={limit}
    - Returns list of tasks for the user

    Headers:
    - X-User-Id: Required for authorization

    Returns:
    {
        "statusCode": 200,
        "body": { ... task details or list ... }
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Extract user_id from headers
        headers = event.get("headers", {})
        # Handle case-insensitive headers
        user_id = headers.get("X-User-Id") or headers.get("x-user-id")

        if not user_id:
            return {
                "statusCode": 401,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Missing X-User-Id header"}),
            }

        # Get path parameters
        path_params = event.get("pathParameters") or {}
        task_id = path_params.get("task_id")

        # Get query parameters
        query_params = event.get("queryStringParameters") or {}

        if task_id:
            # Get single task
            task = get_task(task_id, user_id)

            if not task:
                return {
                    "statusCode": 404,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "Task not found"}),
                }

            response_body = format_task_response(task, include_download_urls=True)

            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(response_body, cls=DecimalEncoder),
            }
        else:
            # List tasks
            status_filter = query_params.get("status")
            limit = int(query_params.get("limit", "20"))
            limit = min(limit, 100)  # Cap at 100

            tasks = list_tasks(user_id, status_filter, limit)
            task_summaries = [format_task_summary(t) for t in tasks]

            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"tasks": task_summaries}, cls=DecimalEncoder),
            }

    except ClientError as e:
        logger.exception(f"AWS service error: {e}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"AWS service error: {str(e)}"}),
        }
    except Exception as e:
        logger.exception(f"Status query failed: {e}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)}),
        }
