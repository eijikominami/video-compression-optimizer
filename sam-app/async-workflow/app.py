"""Async Workflow Lambda Function.

Handles workflow orchestration for async video conversion:
- validate_input: Validate task input
- update_status: Update task status in DynamoDB
- start_conversion: Start MediaConvert job
- check_conversion_status: Check MediaConvert job status
- handle_conversion_error: Handle MediaConvert errors with retry logic
- handle_quality_failure: Handle SSIM failures with adaptive retry
- file_complete: Mark file as completed/failed
- aggregate_results: Aggregate file results to determine task status
- handle_error: Handle workflow errors

Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6
"""

import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3


def convert_floats_to_decimal(obj: Any) -> Any:
    """Convert float values to Decimal for DynamoDB compatibility.

    DynamoDB does not support Python float types directly.
    This function recursively converts all float values to Decimal.
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    return obj


# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Environment variables
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
MEDIACONVERT_ROLE_ARN = os.environ.get("MEDIACONVERT_ROLE_ARN", "")
QUALITY_CHECKER_ARN = os.environ.get("QUALITY_CHECKER_ARN", "")

# MediaConvert error codes
TRANSIENT_ERRORS = {1517, 1522, 1550, 1999}
CONFIG_ERRORS = {1010, 1030, 1040, 1401, 1432, 1433}

# Preset chain for adaptive quality
PRESET_CHAIN = ["balanced", "high"]


def get_dynamodb_table():
    """Get DynamoDB table resource."""
    dynamodb = boto3.resource("dynamodb")
    return dynamodb.Table(DYNAMODB_TABLE)


def get_mediaconvert_client():
    """Get MediaConvert client with endpoint."""
    mc = boto3.client("mediaconvert")
    endpoints = mc.describe_endpoints()
    endpoint_url = endpoints["Endpoints"][0]["Url"]
    return boto3.client("mediaconvert", endpoint_url=endpoint_url)


def validate_input(event: dict) -> dict:
    """Validate task input."""
    task_id = event.get("task_id")
    files = event.get("files", [])

    if not task_id:
        raise ValueError("Missing task_id")

    if not files:
        raise ValueError("No files to process")

    return {"valid": True, "file_count": len(files)}


def update_status(event: dict) -> dict:
    """Update task status in DynamoDB."""
    table = get_dynamodb_table()
    task_id = event.get("task_id")
    status = event.get("status")
    current_step = event.get("current_step")
    now = datetime.now(timezone.utc)

    update_expr = "SET #status = :status, updated_at = :updated"
    expr_names = {"#status": "status"}
    expr_values = {":status": status, ":updated": now.isoformat()}

    if current_step:
        update_expr += ", current_step = :step"
        expr_values[":step"] = current_step

    if status in ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED"]:
        update_expr += ", completed_at = :completed"
        expr_values[":completed"] = now.isoformat()

    table.update_item(
        Key={"task_id": task_id, "sk": "TASK"},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )

    return {"updated": True, "status": status}


def start_conversion(event: dict) -> dict:
    """Start MediaConvert job for a file."""
    task_id = event.get("task_id")
    file_info = event.get("file", {})
    quality_preset = event.get("quality_preset", "balanced")

    source_s3_key = file_info.get("source_s3_key")
    file_id = file_info.get("file_id")
    filename = file_info.get("filename", "video.mp4")

    # Generate output key using original filename (same as sync mode)
    # Output: {original_stem}_h265.mp4 (e.g., IMG_1234_h265.mp4)
    from pathlib import Path

    original_stem = Path(filename).stem
    output_s3_key = f"output/{task_id}/{file_id}/{original_stem}.mp4"

    mc = get_mediaconvert_client()

    # Create MediaConvert job settings
    job_settings = create_job_settings(source_s3_key, output_s3_key, quality_preset)

    response = mc.create_job(
        Role=MEDIACONVERT_ROLE_ARN,
        Settings=job_settings,
        Tags={"task_id": task_id, "file_id": file_id},
    )

    job_id = response["Job"]["Id"]
    logger.info(f"Started MediaConvert job {job_id} for file {file_id}")

    # Update file status to CONVERTING in DynamoDB
    update_file_status(task_id, file_id, "CONVERTING", {"mediaconvert_job_id": job_id})

    return {"job_id": job_id, "output_s3_key": output_s3_key}


def create_job_settings(source_key: str, output_key: str, preset: str) -> dict:
    """Create MediaConvert job settings.

    Settings are aligned with sync mode (mediaconvert.py) for consistency.
    """
    # Quality settings based on preset (same as sync mode)
    quality_settings = {
        "balanced": {"max_bitrate": 20_000_000, "quality_level": 7},
        "high": {"max_bitrate": 50_000_000, "quality_level": 9},
        "compression": {"max_bitrate": 10_000_000, "quality_level": 5},
        "balanced+": {"max_bitrate": 20_000_000, "quality_level": 7},
        "high+": {"max_bitrate": 50_000_000, "quality_level": 9},
    }

    settings = quality_settings.get(preset, quality_settings["balanced"])

    return {
        "Inputs": [
            {
                "FileInput": f"s3://{S3_BUCKET}/{source_key}",
                "AudioSelectors": {"Audio Selector 1": {"DefaultSelection": "DEFAULT"}},
                "VideoSelector": {},
                "TimecodeSource": "ZEROBASED",
            }
        ],
        "OutputGroups": [
            {
                "Name": "File Group",
                "OutputGroupSettings": {
                    "Type": "FILE_GROUP_SETTINGS",
                    "FileGroupSettings": {
                        "Destination": f"s3://{S3_BUCKET}/{output_key.rsplit('/', 1)[0]}/"
                    },
                },
                "Outputs": [
                    {
                        "NameModifier": "_h265",
                        "ContainerSettings": {
                            "Container": "MP4",
                            "Mp4Settings": {
                                "CslgAtom": "INCLUDE",
                                "FreeSpaceBox": "EXCLUDE",
                                "MoovPlacement": "PROGRESSIVE_DOWNLOAD",
                            },
                        },
                        "VideoDescription": {
                            "CodecSettings": {
                                "Codec": "H_265",
                                "H265Settings": {
                                    "RateControlMode": "QVBR",
                                    "QvbrSettings": {
                                        "QvbrQualityLevel": settings["quality_level"],
                                        "QvbrQualityLevelFineTune": 0,
                                    },
                                    "MaxBitrate": settings["max_bitrate"],
                                    "GopSize": 90,
                                    "GopSizeUnits": "FRAMES",
                                    "ParNumerator": 1,
                                    "ParDenominator": 1,
                                    "ParControl": "SPECIFIED",
                                    "NumberBFramesBetweenReferenceFrames": 3,
                                    "NumberReferenceFrames": 3,
                                    "Slices": 1,
                                    "InterlaceMode": "PROGRESSIVE",
                                    "SceneChangeDetect": "ENABLED",
                                    "MinIInterval": 0,
                                    "AdaptiveQuantization": "HIGH",
                                    "FlickerAdaptiveQuantization": "ENABLED",
                                    "SpatialAdaptiveQuantization": "ENABLED",
                                    "TemporalAdaptiveQuantization": "ENABLED",
                                    "UnregisteredSeiTimecode": "DISABLED",
                                    "SampleAdaptiveOffsetFilterMode": "ADAPTIVE",
                                    "WriteMp4PackagingType": "HVC1",
                                    "AlternateTransferFunctionSei": "DISABLED",
                                },
                            },
                            "ScalingBehavior": "DEFAULT",
                            "TimecodeInsertion": "DISABLED",
                            "AntiAlias": "ENABLED",
                            "Sharpness": 50,
                            "AfdSignaling": "NONE",
                            "DropFrameTimecode": "ENABLED",
                            "RespondToAfd": "NONE",
                            "ColorMetadata": "INSERT",
                        },
                        "AudioDescriptions": [
                            {
                                "CodecSettings": {
                                    "Codec": "AAC",
                                    "AacSettings": {
                                        "Bitrate": 128000,
                                        "CodingMode": "CODING_MODE_2_0",
                                        "SampleRate": 48000,
                                        "RateControlMode": "CBR",
                                        "RawFormat": "NONE",
                                        "Specification": "MPEG4",
                                        "AudioDescriptionBroadcasterMix": "NORMAL",
                                    },
                                },
                                "AudioSourceName": "Audio Selector 1",
                            }
                        ],
                        "Extension": "mp4",
                    }
                ],
            }
        ],
        "TimecodeConfig": {"Source": "ZEROBASED"},
    }


def check_conversion_status(event: dict) -> dict:
    """Check MediaConvert job status."""
    job_id = event.get("job_id")
    task_id = event.get("task_id")
    file_info = event.get("file", {})
    file_id = file_info.get("file_id")

    mc = get_mediaconvert_client()
    response = mc.get_job(Id=job_id)
    job = response["Job"]

    status = job["Status"]
    result = {"status": status, "job_id": job_id}

    if status == "COMPLETE":
        # Update file status to VERIFYING before quality check
        if task_id and file_id:
            update_file_status(task_id, file_id, "VERIFYING")
        # Get output file path from job settings
        # MediaConvert output: {Destination}/{InputBaseName}{NameModifier}.{Extension}
        # e.g., s3://bucket/output/task/file/ + MVI_8155 + _h265 + .mp4
        output_group = job["Settings"]["OutputGroups"][0]
        destination = output_group["OutputGroupSettings"]["FileGroupSettings"]["Destination"]
        name_modifier = output_group["Outputs"][0].get("NameModifier", "")
        extension = output_group["Outputs"][0].get("Extension", "mp4")

        # Get input filename to construct output path
        input_file = job["Settings"]["Inputs"][0]["FileInput"]
        input_basename = Path(input_file).stem  # e.g., MVI_8155

        # Construct full output path
        output_filename = f"{input_basename}{name_modifier}.{extension}"
        output_s3_uri = destination + output_filename
        result["output_s3_key"] = output_s3_uri.replace(f"s3://{S3_BUCKET}/", "")

    elif status == "ERROR":
        error_code = job.get("ErrorCode", 0)
        error_message = job.get("ErrorMessage", "Unknown error")
        result["error_code"] = error_code
        result["error_message"] = error_message

    return result


def handle_conversion_error(event: dict) -> dict:
    """Handle MediaConvert error with retry logic."""
    task_id = event.get("task_id")
    file_info = event.get("file", {})
    error_code = event.get("error_code", 0)
    error_message = event.get("error_message", "")

    file_id = file_info.get("file_id")
    retry_count = file_info.get("retry_count", 0)

    # Check if error is retryable
    is_retryable = error_code in TRANSIENT_ERRORS and retry_count < 3

    if is_retryable:
        # Increment retry count
        update_file_status(task_id, file_id, "PENDING", {"retry_count": retry_count + 1})
        return {"should_retry": True, "retry_count": retry_count + 1}

    # Not retryable
    update_file_status(
        task_id,
        file_id,
        "FAILED",
        {"error_code": error_code, "error_message": error_message},
    )
    return {"should_retry": False, "error_code": error_code}


def handle_quality_failure(event: dict) -> dict:
    """Handle SSIM quality failure with adaptive retry.

    For adaptive presets (ending with +):
    - Try next preset in chain if available
    - If all presets exhausted, use best-effort mode (accept with warning)

    For non-adaptive presets:
    - If this is a retry from an adaptive preset (preset_attempts not empty),
      use best-effort mode
    - Otherwise fail immediately

    Best-effort mode: Accept the result even if below threshold, with a warning.
    This matches sync behavior where the best SSIM result is selected.

    Returns updated file object with preset_attempts for Step Functions state.
    """
    task_id = event.get("task_id")
    file_info = event.get("file", {})
    quality_preset = event.get("quality_preset", "balanced")
    quality_result = event.get("quality_result", {})

    file_id = file_info.get("file_id")
    preset_attempts = file_info.get("preset_attempts", []).copy()

    # Check if adaptive preset (ends with +)
    is_adaptive = quality_preset.endswith("+")

    # Check if this is a retry from an adaptive preset
    is_retry_from_adaptive = len(preset_attempts) > 0

    if not is_adaptive and not is_retry_from_adaptive:
        # Non-adaptive and not a retry: fail immediately
        update_file_status(
            task_id,
            file_id,
            "FAILED",
            {"error_message": "SSIM threshold not met", "quality_result": quality_result},
        )
        return {"should_retry": False, "reason": "non_adaptive_preset"}

    if is_adaptive:
        # Adaptive: try next preset in chain
        base_preset = quality_preset.rstrip("+")
        current_index = PRESET_CHAIN.index(base_preset) if base_preset in PRESET_CHAIN else 0

        if current_index < len(PRESET_CHAIN) - 1:
            next_preset = PRESET_CHAIN[current_index + 1]  # high (without +)
            preset_attempts.append(quality_preset)
            update_file_status(task_id, file_id, "PENDING", {"preset_attempts": preset_attempts})
            # Return updated file object for Step Functions state
            updated_file = file_info.copy()
            updated_file["preset_attempts"] = preset_attempts
            return {
                "should_retry": True,
                "next_preset": next_preset,
                "updated_file": updated_file,
            }

    # No more presets to try (or retry from adaptive failed)
    # Use best-effort mode: accept the best result even if below threshold
    # This matches sync behavior where the highest SSIM result is selected
    return {
        "should_retry": False,
        "reason": "best_effort",
        "accept_anyway": True,
        "quality_result": quality_result,
    }


def file_complete(event: dict) -> dict:
    """Mark file as completed or failed.

    For COMPLETED files, also deletes the input file from S3 to save storage.
    For FAILED files, also deletes the input file from S3 (no retry needed).
    """
    task_id = event.get("task_id")
    file_info = event.get("file", {})
    status = event.get("status")
    output_s3_key = event.get("output_s3_key")
    quality_result = event.get("quality_result")
    error = event.get("error")
    error_message = event.get("error_message")
    best_effort = event.get("best_effort", False)

    file_id = file_info.get("file_id")

    updates = {}
    if output_s3_key:
        updates["output_s3_key"] = output_s3_key
    if quality_result:
        updates["quality_result"] = quality_result
    if best_effort:
        updates["best_effort"] = True
    # Support both error (object) and error_message (string) parameters
    if error_message:
        updates["error_message"] = str(error_message)
    elif error:
        updates["error_message"] = str(error)

    update_file_status(task_id, file_id, status, updates)

    # Delete input file from S3 after file processing completes (success or failure)
    if status in ("COMPLETED", "FAILED"):
        source_s3_key = file_info.get("source_s3_key")
        if source_s3_key:
            delete_s3_file(source_s3_key)

    return {"file_id": file_id, "status": status}


def aggregate_results(event: dict) -> dict:
    """Aggregate file results to determine task status."""
    task_id = event.get("task_id")
    results = event.get("results", [])

    completed_count = sum(
        1 for r in results if r.get("file_result", {}).get("status") == "COMPLETED"
    )
    failed_count = sum(1 for r in results if r.get("file_result", {}).get("status") == "FAILED")
    total_count = len(results)

    if completed_count == total_count:
        final_status = "COMPLETED"
    elif failed_count == total_count:
        final_status = "FAILED"
    else:
        final_status = "PARTIALLY_COMPLETED"

    logger.info(
        f"Task {task_id}: {completed_count}/{total_count} completed, "
        f"{failed_count}/{total_count} failed -> {final_status}"
    )

    return {
        "final_status": final_status,
        "completed_count": completed_count,
        "failed_count": failed_count,
        "total_count": total_count,
    }


def handle_error(event: dict) -> dict:
    """Handle workflow-level error."""
    task_id = event.get("task_id")
    error = event.get("error", {})

    logger.error(f"Workflow error for task {task_id}: {error}")

    # Update task status to FAILED
    table = get_dynamodb_table()
    now = datetime.now(timezone.utc)

    table.update_item(
        Key={"task_id": task_id, "sk": "TASK"},
        UpdateExpression="SET #status = :status, updated_at = :updated, "
        "completed_at = :completed, error_message = :error",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": "FAILED",
            ":updated": now.isoformat(),
            ":completed": now.isoformat(),
            ":error": str(error),
        },
    )

    return {"handled": True, "status": "FAILED"}


def update_file_status(
    task_id: str, file_id: str, status: str, updates: dict | None = None
) -> None:
    """Update file status in DynamoDB task record."""
    table = get_dynamodb_table()

    # Get current task
    response = table.get_item(Key={"task_id": task_id, "sk": "TASK"})
    task = response.get("Item", {})
    files = task.get("files", [])

    # Update the specific file
    for file in files:
        if file.get("file_id") == file_id:
            file["status"] = status
            if updates:
                # Convert float values to Decimal for DynamoDB compatibility
                converted_updates = convert_floats_to_decimal(updates)
                file.update(converted_updates)
            break

    # Update task
    now = datetime.now(timezone.utc)
    table.update_item(
        Key={"task_id": task_id, "sk": "TASK"},
        UpdateExpression="SET files = :files, updated_at = :updated",
        ExpressionAttributeValues={":files": files, ":updated": now.isoformat()},
    )


def delete_s3_file(s3_key: str) -> bool:
    """Delete a file from S3.

    Args:
        s3_key: S3 key to delete

    Returns:
        True if deleted successfully, False otherwise
    """
    try:
        s3 = boto3.client("s3")
        s3.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        logger.info(f"Deleted S3 file: {s3_key}")
        return True
    except Exception as e:
        logger.warning(f"Failed to delete S3 file {s3_key}: {e}")
        return False


def lambda_handler(event: dict, context: Any) -> dict:
    """Lambda handler for workflow actions."""
    logger.info(f"Received event: {json.dumps(event)}")

    action = event.get("action")

    handlers = {
        "validate_input": validate_input,
        "update_status": update_status,
        "start_conversion": start_conversion,
        "check_conversion_status": check_conversion_status,
        "handle_conversion_error": handle_conversion_error,
        "handle_quality_failure": handle_quality_failure,
        "file_complete": file_complete,
        "aggregate_results": aggregate_results,
        "handle_error": handle_error,
    }

    handler = handlers.get(action)
    if not handler:
        raise ValueError(f"Unknown action: {action}")

    return handler(event)
