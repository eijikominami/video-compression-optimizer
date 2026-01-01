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
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# API Response Schema Constants
VALID_TASK_STATUSES = [
    "PENDING",
    "UPLOADING",
    "CONVERTING",
    "VERIFYING",
    "COMPLETED",
    "PARTIALLY_COMPLETED",
    "FAILED",
    "CANCELLED",
]

VALID_FILE_STATUSES = [
    "PENDING",
    "CONVERTING",
    "VERIFYING",
    "COMPLETED",
    "DOWNLOADED",
    "REMOVED",
    "FAILED",
]

VALID_CURRENT_STEPS = ["pending", "converting", "verifying", "completed"]

TASK_DETAIL_REQUIRED_FIELDS = [
    "task_id",
    "status",
    "quality_preset",
    "progress_percentage",
    "current_step",
    "files",
    "created_at",
    "updated_at",
]

TASK_SUMMARY_REQUIRED_FIELDS = [
    "task_id",
    "status",
    "quality_preset",
    "progress_percentage",
    "file_count",
    "completed_count",
    "failed_count",
    "created_at",
    "updated_at",
]

FILE_REQUIRED_FIELDS = ["file_id", "filename", "status"]


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


def validate_task_detail_response(response: dict) -> tuple[bool, str | None]:
    """Validate task detail response against schema.

    Args:
        response: Task detail response dict

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields
    for field in TASK_DETAIL_REQUIRED_FIELDS:
        if field not in response:
            return False, f"Missing required field: {field}"

    # Validate status
    if response["status"] not in VALID_TASK_STATUSES:
        return False, f"Invalid task status: {response['status']}"

    # Validate current_step
    if response["current_step"] not in VALID_CURRENT_STEPS:
        return False, f"Invalid current_step: {response['current_step']}"

    # Validate progress_percentage
    progress = response["progress_percentage"]
    if not isinstance(progress, int) or progress < 0 or progress > 100:
        return False, f"Invalid progress_percentage: {progress}"

    # Validate files
    for i, file in enumerate(response.get("files", [])):
        is_valid, error = validate_file_response(file, i)
        if not is_valid:
            return False, error

    return True, None


def validate_file_response(file: dict, index: int) -> tuple[bool, str | None]:
    """Validate file response against schema.

    Args:
        file: File response dict
        index: File index for error messages

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields
    for field in FILE_REQUIRED_FIELDS:
        if field not in file:
            return False, f"File {index} missing required field: {field}"

    # Validate status
    if file["status"] not in VALID_FILE_STATUSES:
        return False, f"File {index} has invalid status: {file['status']}"

    return True, None


def validate_task_summary_response(response: dict) -> tuple[bool, str | None]:
    """Validate task summary response against schema.

    Args:
        response: Task summary response dict

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields
    for field in TASK_SUMMARY_REQUIRED_FIELDS:
        if field not in response:
            return False, f"Missing required field: {field}"

    # Validate status
    if response["status"] not in VALID_TASK_STATUSES:
        return False, f"Invalid task status: {response['status']}"

    # Validate progress_percentage
    progress = response["progress_percentage"]
    if not isinstance(progress, int) or progress < 0 or progress > 100:
        return False, f"Invalid progress_percentage: {progress}"

    # Validate counts
    for count_field in ["file_count", "completed_count", "failed_count"]:
        count = response[count_field]
        if not isinstance(count, int) or count < 0:
            return False, f"Invalid {count_field}: {count}"

    return True, None


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
    - COMPLETED/DOWNLOADED/FAILED: 100%

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
        elif status in ("COMPLETED", "DOWNLOADED", "FAILED"):
            # 100%
            total_progress += 100

    # Calculate average progress
    progress = int(total_progress / len(files))

    # Determine current step (most advanced active state)
    # DOWNLOADED is also a terminal state
    completed_count = sum(
        1 for f in files if f.get("status") in ("COMPLETED", "DOWNLOADED", "FAILED")
    )
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
    - COMPLETED/DOWNLOADED/FAILED: 100%

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
        elif status in ("COMPLETED", "DOWNLOADED", "FAILED"):
            total_progress += 100

    progress = int(total_progress / len(files))

    # DOWNLOADED is also a terminal state
    completed_count = sum(
        1 for f in files if f.get("status") in ("COMPLETED", "DOWNLOADED", "FAILED")
    )
    if completed_count == len(files):
        current_step = "completed"

    return progress, current_step


def calculate_file_progress(file: dict) -> int:
    """Calculate progress percentage for a single file.

    File status to progress mapping:
    - PENDING: 0%
    - CONVERTING: 0-30% (scaled from MediaConvert jobPercentComplete)
    - VERIFYING: 65% (fixed, SSIM calculation has no progress API)
    - COMPLETED/DOWNLOADED/FAILED: 100%

    Returns:
        Progress percentage (0-100)
    """
    status = file.get("status", "PENDING")

    if status == "PENDING":
        return 0
    elif status == "CONVERTING":
        # 0-30% range, scaled from MediaConvert jobPercentComplete
        job_id = file.get("mediaconvert_job_id")
        if job_id:
            mc_progress = get_mediaconvert_job_progress(job_id)
            # Scale 0-100% to 0-30%
            return int(mc_progress * 0.3)
        return 0
    elif status == "VERIFYING":
        return 65
    elif status in ("COMPLETED", "DOWNLOADED", "FAILED"):
        return 100
    return 0


def format_task_response(task: dict, include_download_urls: bool = False) -> dict:
    """Format task for API response."""
    files = task.get("files", [])

    # Format files for response
    formatted_files = []
    for file in files:
        # Calculate individual file progress
        file_progress = calculate_file_progress(file)

        formatted_file = {
            "file_id": file.get("file_id"),
            "filename": file.get("filename"),
            "status": file.get("status"),
            "conversion_progress_percentage": file_progress,
            "error_message": file.get("error_message"),
            "output_s3_key": file.get("output_s3_key"),
            "best_effort": file.get("best_effort", False),
            "download_available": file.get("download_available", True),
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
    # DOWNLOADED is also a successful completion state
    completed_count = sum(1 for f in files if f.get("status") in ("COMPLETED", "DOWNLOADED"))
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


def update_file_status_to_downloaded(task_id: str, file_id: str, user_id: str) -> dict | None:
    """Update file status to DOWNLOADED after successful download.

    Args:
        task_id: Task ID
        file_id: File ID
        user_id: User ID for authorization

    Returns:
        Updated file info or None if not found/unauthorized
    """
    # Get task and verify authorization
    task = get_task(task_id, user_id)
    if not task:
        return None

    # Find the file
    files = task.get("files", [])
    file_index = None
    for i, f in enumerate(files):
        if f.get("file_id") == file_id:
            file_index = i
            break

    if file_index is None:
        return None

    # Update the file's status to DOWNLOADED
    table = get_dynamodb_table()
    now = datetime.now(timezone.utc).isoformat()

    table.update_item(
        Key={"task_id": task_id, "sk": "TASK"},
        UpdateExpression=f"SET files[{file_index}].#status = :status, files[{file_index}].downloaded_at = :downloaded_at, updated_at = :updated_at",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "DOWNLOADED",
            ":downloaded_at": now,
            ":updated_at": now,
        },
    )

    return {
        "file_id": file_id,
        "status": "DOWNLOADED",
        "downloaded_at": now,
    }


def cleanup_file(task_id: str, file_id: str, user_id: str, action: str) -> dict:
    """Cleanup file: update status and delete S3 file.

    Processing order (status is source of truth):
    1. Update DynamoDB file status (DOWNLOADED or REMOVED)
    2. Delete S3 output file
    3. If S3 deletion fails, log warning but return success

    Args:
        task_id: Task ID
        file_id: File ID
        user_id: User ID for authorization
        action: "downloaded" (after import) or "removed" (on delete/clear)

    Returns:
        Cleanup result dict with file_id, status, s3_deleted, completed_at

    Raises:
        ValueError: If task/file not found or unauthorized
        RuntimeError: If status update fails
    """
    # Validate action
    if action not in ("downloaded", "removed"):
        raise ValueError(f"Invalid action: {action}. Must be 'downloaded' or 'removed'")

    # Get task and verify authorization
    task = get_task(task_id, user_id)
    if not task:
        raise ValueError(f"Task not found or unauthorized: {task_id}")

    # Find the file
    files = task.get("files", [])
    file_index = None
    target_file = None
    for i, f in enumerate(files):
        if f.get("file_id") == file_id:
            file_index = i
            target_file = f
            break

    if file_index is None:
        raise ValueError(f"File not found: {file_id}")

    # Determine new status based on action
    new_status = "DOWNLOADED" if action == "downloaded" else "REMOVED"

    # Step 1: Update DynamoDB status (this is the source of truth)
    table = get_dynamodb_table()
    now = datetime.now(timezone.utc).isoformat()

    try:
        table.update_item(
            Key={"task_id": task_id, "sk": "TASK"},
            UpdateExpression=f"SET files[{file_index}].#status = :status, files[{file_index}].cleanup_at = :cleanup_at, updated_at = :updated_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": new_status,
                ":cleanup_at": now,
                ":updated_at": now,
            },
        )
        logger.info(f"Updated file status to {new_status}: task={task_id}, file={file_id}")
    except ClientError as e:
        logger.error(f"Failed to update file status: {e}")
        raise RuntimeError(f"Failed to update file status: {e}")

    # Step 2: Delete S3 file (after status update succeeds)
    s3_deleted = False
    s3_key = target_file.get("output_s3_key")

    if s3_key:
        try:
            s3 = get_s3_client()
            s3.delete_object(Bucket=S3_BUCKET, Key=s3_key)
            s3_deleted = True
            logger.info(f"Deleted S3 file: {s3_key}")
        except ClientError as e:
            # S3 deletion failure is not critical - log warning and continue
            logger.warning(f"Failed to delete S3 file {s3_key}: {e}")
    else:
        logger.warning(f"No S3 key found for file: {file_id}")

    return {
        "file_id": file_id,
        "status": new_status,
        "s3_deleted": s3_deleted,
        "completed_at": now,
    }


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda handler for task status queries and file cleanup.

    GET /tasks/{task_id}
    - Returns detailed task status with download URLs

    GET /tasks?status={status}&limit={limit}
    - Returns list of tasks for the user

    POST /tasks/{task_id}/files/{file_id}/cleanup
    - Cleanup file: update status and delete S3 file

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
        file_id = path_params.get("file_id")

        # Get query parameters
        query_params = event.get("queryStringParameters") or {}

        # Check for cleanup endpoint
        resource = event.get("resource", "")
        http_method = event.get("httpMethod", "")

        if resource == "/tasks/{task_id}/files/{file_id}/cleanup" and http_method == "POST":
            # Handle file cleanup (status update + S3 deletion)
            body = event.get("body", "{}")
            if isinstance(body, str):
                body = json.loads(body)

            action = body.get("action")

            if not action:
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "Missing action in request body"}),
                }

            if action not in ("downloaded", "removed"):
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(
                        {"error": "Invalid action. Must be 'downloaded' or 'removed'"}
                    ),
                }

            try:
                result = cleanup_file(task_id, file_id, user_id, action)
                return {
                    "statusCode": 200,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(result),
                }
            except ValueError as e:
                return {
                    "statusCode": 404,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": str(e)}),
                }
            except RuntimeError as e:
                return {
                    "statusCode": 500,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": str(e)}),
                }

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
