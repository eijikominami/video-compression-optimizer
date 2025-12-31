"""Data models for async workflow tasks.

This module defines the data structures for managing asynchronous video
conversion tasks that run on AWS Step Functions.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class TaskStatus(Enum):
    """Status of an async conversion task."""

    PENDING = "PENDING"
    UPLOADING = "UPLOADING"
    CONVERTING = "CONVERTING"
    VERIFYING = "VERIFYING"
    COMPLETED = "COMPLETED"
    PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FileStatus(Enum):
    """Status of an individual file within a task.

    Status transitions:
    - PENDING: Waiting to start or retry
    - CONVERTING: MediaConvert job running (0-30% progress)
    - VERIFYING: SSIM quality check running (65% progress, fixed)
    - COMPLETED: Successfully processed (100%)
    - FAILED: Processing failed (100%)
    """

    PENDING = "PENDING"
    CONVERTING = "CONVERTING"
    VERIFYING = "VERIFYING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class AsyncFile:
    """An individual file within an async conversion task.

    Attributes:
        file_id: Unique identifier for this file (UUID v4)
        original_uuid: UUID from Photos library
        filename: Original filename
        source_s3_key: S3 key for the uploaded source file
        output_s3_key: S3 key for the converted output file
        metadata_s3_key: S3 key for the metadata sidecar file
        status: Current processing status (PENDING, CONVERTING, VERIFYING, COMPLETED, FAILED)
        mediaconvert_job_id: MediaConvert job ID (for progress lookup during CONVERTING)
        quality_result: Quality verification result (SSIM, compression ratio, etc.)
        error_code: Error code if processing failed
        error_message: Error message if processing failed
        retry_count: Number of retry attempts for transient errors
        preset_attempts: List of presets attempted (for adaptive presets)
        source_size_bytes: Original file size in bytes
        output_size_bytes: Converted file size in bytes
        output_checksum: Checksum of the output file (ETag or SHA256)
        checksum_algorithm: Algorithm used for checksum ("ETag" or "SHA256")
    """

    file_id: str
    original_uuid: str
    filename: str
    source_s3_key: str
    output_s3_key: str | None = None
    metadata_s3_key: str | None = None
    status: FileStatus = FileStatus.PENDING
    mediaconvert_job_id: str | None = None
    quality_result: dict | None = None
    error_code: int | None = None
    error_message: str | None = None
    retry_count: int = 0
    preset_attempts: list[str] = field(default_factory=list)
    source_size_bytes: int | None = None
    output_size_bytes: int | None = None
    output_checksum: str | None = None
    checksum_algorithm: str = "ETag"

    def to_dict(self) -> dict:
        """Convert to dictionary for DynamoDB storage."""
        return {
            "file_id": self.file_id,
            "original_uuid": self.original_uuid,
            "filename": self.filename,
            "source_s3_key": self.source_s3_key,
            "output_s3_key": self.output_s3_key,
            "metadata_s3_key": self.metadata_s3_key,
            "status": self.status.value,
            "mediaconvert_job_id": self.mediaconvert_job_id,
            "quality_result": self.quality_result,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "preset_attempts": self.preset_attempts,
            "source_size_bytes": self.source_size_bytes,
            "output_size_bytes": self.output_size_bytes,
            "output_checksum": self.output_checksum,
            "checksum_algorithm": self.checksum_algorithm,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AsyncFile":
        """Create from dictionary (DynamoDB data)."""
        return cls(
            file_id=data["file_id"],
            original_uuid=data["original_uuid"],
            filename=data["filename"],
            source_s3_key=data["source_s3_key"],
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
        )


@dataclass
class AsyncTask:
    """An async conversion task managed by Step Functions.

    Attributes:
        task_id: Unique identifier for this task (UUID v4)
        user_id: User identifier for authorization
        status: Current task status
        quality_preset: Quality preset name (balanced, high, balanced+, etc.)
        files: List of files to process
        created_at: Task creation timestamp
        updated_at: Last update timestamp
        started_at: Processing start timestamp
        completed_at: Processing completion timestamp
        execution_arn: Step Functions execution ARN
        error_message: Error message if task failed
        ttl: DynamoDB TTL (Unix timestamp, 90 days after completion)
        progress_percentage: Overall progress (0-100)
        current_step: Current processing step name
        estimated_completion_time: Estimated completion timestamp
        max_concurrent: Maximum concurrent file processing
    """

    task_id: str
    user_id: str
    status: TaskStatus
    quality_preset: str
    files: list[AsyncFile]
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    execution_arn: str | None = None
    error_message: str | None = None
    ttl: int | None = None
    progress_percentage: int = 0
    current_step: str | None = None
    estimated_completion_time: datetime | None = None
    max_concurrent: int = 5

    def to_dict(self) -> dict:
        """Convert to dictionary for DynamoDB storage."""
        return {
            "task_id": self.task_id,
            "user_id": self.user_id,
            "status": self.status.value,
            "quality_preset": self.quality_preset,
            "files": [f.to_dict() for f in self.files],
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "execution_arn": self.execution_arn,
            "error_message": self.error_message,
            "ttl": self.ttl,
            "progress_percentage": self.progress_percentage,
            "current_step": self.current_step,
            "estimated_completion_time": (
                self.estimated_completion_time.isoformat()
                if self.estimated_completion_time
                else None
            ),
            "max_concurrent": self.max_concurrent,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AsyncTask":
        """Create from dictionary (DynamoDB data)."""
        return cls(
            task_id=data["task_id"],
            user_id=data["user_id"],
            status=TaskStatus(data["status"]),
            quality_preset=data["quality_preset"],
            files=[AsyncFile.from_dict(f) for f in data.get("files", [])],
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            started_at=(
                datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None
            ),
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

    def calculate_progress(self) -> int:
        """Calculate overall progress percentage.

        Progress breakdown:
        - PENDING: 0%
        - UPLOADING: 10%
        - CONVERTING: 10% + (file completion rate × 70%)
        - VERIFYING: 80% + (verification completion rate × 15%)
        - COMPLETED/PARTIALLY_COMPLETED: 100%

        Returns:
            Progress percentage (0-100)
        """
        if self.status == TaskStatus.PENDING:
            return 0
        elif self.status == TaskStatus.UPLOADING:
            return 10
        elif self.status == TaskStatus.CONVERTING:
            if not self.files:
                return 10
            completed = sum(
                1 for f in self.files if f.status in (FileStatus.COMPLETED, FileStatus.FAILED)
            )
            file_progress = completed / len(self.files)
            return 10 + int(file_progress * 70)
        elif self.status == TaskStatus.VERIFYING:
            if not self.files:
                return 80
            verified = sum(1 for f in self.files if f.quality_result is not None)
            verify_progress = verified / len(self.files)
            return 80 + int(verify_progress * 15)
        elif self.status in (TaskStatus.COMPLETED, TaskStatus.PARTIALLY_COMPLETED):
            return 100
        elif self.status in (TaskStatus.FAILED, TaskStatus.CANCELLED):
            return self.progress_percentage  # Keep last known progress
        return 0

    def estimate_completion_time(
        self, avg_conversion_time_per_file: float = 600.0
    ) -> datetime | None:
        """Estimate completion time based on remaining files.

        Args:
            avg_conversion_time_per_file: Average conversion time in seconds
                (default: 10 minutes)

        Returns:
            Estimated completion datetime, or None if cannot estimate
        """
        if self.status in (
            TaskStatus.COMPLETED,
            TaskStatus.PARTIALLY_COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        ):
            return None

        if not self.files:
            return None

        remaining = sum(
            1 for f in self.files if f.status in (FileStatus.PENDING, FileStatus.CONVERTING)
        )

        if remaining == 0:
            return datetime.now()

        # Account for concurrency
        batches = (remaining + self.max_concurrent - 1) // self.max_concurrent
        estimated_seconds = batches * avg_conversion_time_per_file

        from datetime import timedelta

        return datetime.now() + timedelta(seconds=estimated_seconds)

    def get_completed_files(self) -> list[AsyncFile]:
        """Get list of successfully completed files."""
        return [f for f in self.files if f.status == FileStatus.COMPLETED]

    def get_failed_files(self) -> list[AsyncFile]:
        """Get list of failed files."""
        return [f for f in self.files if f.status == FileStatus.FAILED]


@dataclass
class DownloadProgress:
    """Download progress for resume functionality.

    Tracks the progress of downloading a converted file from S3,
    allowing resumption if the download is interrupted.

    Attributes:
        task_id: Parent task ID
        file_id: File ID within the task
        total_bytes: Total file size in bytes
        downloaded_bytes: Bytes downloaded so far
        local_temp_path: Path to the temporary download file
        s3_key: S3 key of the file being downloaded
        checksum: Expected checksum for verification
        last_updated: Last progress update timestamp
    """

    task_id: str
    file_id: str
    total_bytes: int
    downloaded_bytes: int
    local_temp_path: str
    s3_key: str
    checksum: str | None = None
    last_updated: datetime = field(default_factory=datetime.now)

    @property
    def is_complete(self) -> bool:
        """Check if download is complete."""
        return self.downloaded_bytes >= self.total_bytes

    @property
    def progress_percentage(self) -> int:
        """Calculate download progress percentage."""
        if self.total_bytes == 0:
            return 0
        return int(self.downloaded_bytes / self.total_bytes * 100)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON storage."""
        return {
            "task_id": self.task_id,
            "file_id": self.file_id,
            "total_bytes": self.total_bytes,
            "downloaded_bytes": self.downloaded_bytes,
            "local_temp_path": self.local_temp_path,
            "s3_key": self.s3_key,
            "checksum": self.checksum,
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DownloadProgress":
        """Create from dictionary (JSON data)."""
        return cls(
            task_id=data["task_id"],
            file_id=data["file_id"],
            total_bytes=data["total_bytes"],
            downloaded_bytes=data["downloaded_bytes"],
            local_temp_path=data["local_temp_path"],
            s3_key=data["s3_key"],
            checksum=data.get("checksum"),
            last_updated=datetime.fromisoformat(data["last_updated"]),
        )


def aggregate_task_status(file_statuses: list[FileStatus]) -> TaskStatus:
    """Aggregate file statuses to determine overall task status.

    Args:
        file_statuses: List of individual file statuses

    Returns:
        Aggregated task status:
        - COMPLETED if all files completed successfully
        - FAILED if all files failed
        - PARTIALLY_COMPLETED if some succeeded and some failed
    """
    if not file_statuses:
        return TaskStatus.FAILED

    completed_count = sum(1 for s in file_statuses if s == FileStatus.COMPLETED)
    failed_count = sum(1 for s in file_statuses if s == FileStatus.FAILED)
    total = len(file_statuses)

    if completed_count == total:
        return TaskStatus.COMPLETED
    elif failed_count == total:
        return TaskStatus.FAILED
    else:
        return TaskStatus.PARTIALLY_COMPLETED
