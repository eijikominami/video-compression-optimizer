"""Data model conversion functions.

Single location for all data model conversions between
CLI models and API request/response formats.

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
"""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus
from vco.models.types import ConversionResult, VideoInfo


def async_file_to_api(file: AsyncFile) -> dict[str, Any]:
    """Convert AsyncFile to API response format.

    This is the ONLY function for AsyncFile -> API conversion.

    Args:
        file: AsyncFile object to convert

    Returns:
        Dictionary in API response format

    Requirements: 2.1, 2.3
    """
    return {
        "file_id": file.file_id,
        "original_uuid": file.uuid,  # For backward compatibility
        "filename": file.filename,
        "file_size": file.file_size,  # Include base field
        "capture_date": file.capture_date.isoformat() if file.capture_date else None,
        "location": list(file.location) if file.location else None,
        "source_s3_key": file.source_s3_key,
        "output_s3_key": file.output_s3_key,
        "metadata_s3_key": file.metadata_s3_key,
        "status": file.status.value,
        "mediaconvert_job_id": file.mediaconvert_job_id,
        "quality_result": file.quality_result,
        "error_code": file.error_code,
        "error_message": file.error_message,
        "retry_count": file.retry_count,
        "preset_attempts": file.preset_attempts,
        "source_size_bytes": file.source_size_bytes,
        "output_size_bytes": file.output_size_bytes,
        "output_checksum": file.output_checksum,
        "checksum_algorithm": file.checksum_algorithm,
        "downloaded_at": file.downloaded_at.isoformat() if file.downloaded_at else None,
        "download_available": file.download_available,
    }


def api_to_async_file(data: dict[str, Any]) -> AsyncFile:
    """Convert API response to AsyncFile.

    This is the ONLY function for API -> AsyncFile conversion.

    Args:
        data: Dictionary from API response

    Returns:
        AsyncFile object

    Requirements: 2.2, 2.4
    """
    return AsyncFile(
        # Base fields - use uuid instead of original_uuid
        uuid=data.get("original_uuid", data.get("uuid", "")),
        filename=data["filename"],
        file_size=data.get("file_size", data.get("source_size_bytes", 0)),
        capture_date=datetime.fromisoformat(data["capture_date"])
        if data.get("capture_date")
        else None,
        location=tuple(data["location"]) if data.get("location") else None,
        # AsyncFile specific fields
        file_id=data["file_id"],
        source_s3_key=data.get("source_s3_key", ""),
        output_s3_key=data.get("output_s3_key"),
        metadata_s3_key=data.get("metadata_s3_key"),
        status=FileStatus(data.get("status", "PENDING")),
        mediaconvert_job_id=data.get("mediaconvert_job_id"),
        quality_result=data.get("quality_result"),
        error_code=data.get("error_code"),
        error_message=data.get("error_message"),
        retry_count=data.get("retry_count", 0),
        preset_attempts=data.get("preset_attempts", []),
        source_size_bytes=data.get("source_size_bytes"),
        output_size_bytes=data.get("output_size_bytes"),
        output_checksum=data.get("output_checksum"),
        checksum_algorithm=data.get("checksum_algorithm", "ETag"),
        downloaded_at=datetime.fromisoformat(data["downloaded_at"])
        if data.get("downloaded_at")
        else None,
        download_available=data.get("download_available", False),
    )


def async_task_to_api(task: AsyncTask) -> dict[str, Any]:
    """Convert AsyncTask to API response format.

    This is the ONLY function for AsyncTask -> API conversion.

    Args:
        task: AsyncTask object to convert

    Returns:
        Dictionary in API response format

    Requirements: 2.1, 2.3
    """
    return {
        "task_id": task.task_id,
        "user_id": task.user_id,
        "status": task.status.value,
        "quality_preset": task.quality_preset,
        "files": [async_file_to_api(f) for f in task.files],
        "created_at": task.created_at.isoformat(),
        "updated_at": task.updated_at.isoformat(),
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "execution_arn": task.execution_arn,
        "error_message": task.error_message,
        "progress_percentage": task.progress_percentage,
        "current_step": task.current_step,
        "estimated_completion_time": (
            task.estimated_completion_time.isoformat() if task.estimated_completion_time else None
        ),
        "max_concurrent": task.max_concurrent,
    }


def api_to_async_task(data: dict[str, Any]) -> AsyncTask:
    """Convert API response to AsyncTask.

    This is the ONLY function for API -> AsyncTask conversion.

    Args:
        data: Dictionary from API response

    Returns:
        AsyncTask object

    Requirements: 2.2, 2.4
    """
    return AsyncTask(
        task_id=data["task_id"],
        user_id=data["user_id"],
        status=TaskStatus(data["status"]),
        quality_preset=data["quality_preset"],
        files=[api_to_async_file(f) for f in data.get("files", [])],
        created_at=datetime.fromisoformat(data["created_at"]),
        updated_at=datetime.fromisoformat(data["updated_at"]),
        started_at=(datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None),
        completed_at=(
            datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None
        ),
        execution_arn=data.get("execution_arn"),
        error_message=data.get("error_message"),
        ttl=data.get("ttl"),
        progress_percentage=data.get("progress_percentage", 0),
        current_step=data.get("current_step"),
        estimated_completion_time=(
            datetime.fromisoformat(data["estimated_completion_time"])
            if data.get("estimated_completion_time")
            else None
        ),
        max_concurrent=data.get("max_concurrent", 5),
    )


# =============================================================================
# New Unified Conversion Functions
# =============================================================================


def video_info_to_async_file(video: VideoInfo, file_id: str | None = None) -> AsyncFile:
    """Convert VideoInfo to AsyncFile for async workflow.

    This is the ONLY function for VideoInfo -> AsyncFile conversion.

    Args:
        video: VideoInfo object from scan
        file_id: Unique file ID (generates UUID if not provided)

    Returns:
        AsyncFile object ready for async processing
    """
    if file_id is None:
        file_id = str(uuid.uuid4())

    return AsyncFile(
        # Base fields from VideoInfo
        uuid=video.uuid,
        filename=video.filename,
        file_size=video.file_size,
        capture_date=video.capture_date,
        location=video.location,
        # AsyncFile specific fields
        file_id=file_id,
        source_s3_key="",  # Will be set during upload
        status=FileStatus.PENDING,
        source_size_bytes=video.file_size,  # For backward compatibility
    )


def async_file_to_conversion_result(async_file: AsyncFile, original_path: Path) -> ConversionResult:
    """Convert AsyncFile to ConversionResult for import workflow.

    This is the ONLY function for AsyncFile -> ConversionResult conversion.

    Args:
        async_file: AsyncFile from async workflow
        original_path: Path to original video file

    Returns:
        ConversionResult object for import
    """
    return ConversionResult(
        # Base fields from AsyncFile
        uuid=async_file.uuid,
        filename=async_file.filename,
        file_size=async_file.file_size,
        capture_date=async_file.capture_date,
        location=async_file.location,
        # ConversionResult specific fields
        success=async_file.status == FileStatus.COMPLETED,
        original_path=original_path,
        converted_path=None,  # Will be set after download
        quality_result=None,  # Will be populated from async_file.quality_result
        error_message=async_file.error_message,
        mediaconvert_job_id=async_file.mediaconvert_job_id,
        best_effort=False,  # Will be determined from quality_result
    )


def video_info_to_conversion_result(
    video: VideoInfo, success: bool = False, error_message: str | None = None
) -> ConversionResult:
    """Convert VideoInfo to ConversionResult for sync workflow.

    This is the ONLY function for VideoInfo -> ConversionResult conversion.

    Args:
        video: VideoInfo object from scan
        success: Whether conversion succeeded
        error_message: Error message if conversion failed

    Returns:
        ConversionResult object
    """
    return ConversionResult(
        # Base fields from VideoInfo
        uuid=video.uuid,
        filename=video.filename,
        file_size=video.file_size,
        capture_date=video.capture_date,
        location=video.location,
        # ConversionResult specific fields
        success=success,
        original_path=video.path,
        error_message=error_message,
    )
