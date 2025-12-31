"""Data model conversion functions.

Single location for all data model conversions between
CLI models and API request/response formats.

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
"""

from datetime import datetime
from typing import Any

from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus


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
        "original_uuid": file.original_uuid,
        "filename": file.filename,
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
    downloaded_at = None
    if data.get("downloaded_at"):
        downloaded_at = datetime.fromisoformat(data["downloaded_at"])

    return AsyncFile(
        file_id=data["file_id"],
        original_uuid=data.get("original_uuid", ""),
        filename=data["filename"],
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
        downloaded_at=downloaded_at,
        download_available=data.get("download_available", True),
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
