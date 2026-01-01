"""Async download service for downloading completed tasks.

This service handles:
1. Download converted files from S3
2. Resume interrupted downloads
3. Verify file integrity
4. Add to review queue

Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 9.6
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

from vco.services.async_status import StatusCommand, TaskDetail
from vco.services.download_progress import DownloadProgress, DownloadProgressStore
from vco.services.review import ReviewService

logger = logging.getLogger(__name__)


@dataclass
class FileDownloadResult:
    """Result of a single file download."""

    file_id: str
    filename: str
    success: bool
    local_path: Path | None = None
    error_message: str | None = None
    checksum_verified: bool = False
    resumed: bool = False


@dataclass
class DownloadResult:
    """Result of task download."""

    task_id: str
    success: bool
    total_files: int
    downloaded_files: int
    failed_files: int
    added_to_queue: int
    results: list[FileDownloadResult] = field(default_factory=list)
    error_message: str | None = None


class DownloadCommand:
    """Handler for download command.

    Downloads completed task results from S3.

    Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 9.6
    """

    def __init__(
        self,
        api_url: str,
        s3_bucket: str,
        region: str = "ap-northeast-1",
        profile_name: str | None = None,
        progress_store: DownloadProgressStore | None = None,
        review_service: ReviewService | None = None,
        output_dir: Path | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ):
        """Initialize DownloadCommand.

        Args:
            api_url: API Gateway URL for async workflow
            s3_bucket: S3 bucket for downloads
            region: AWS region
            profile_name: AWS profile name (optional)
            progress_store: Store for download progress (created if not provided)
            review_service: Service for review queue (created if not provided)
            output_dir: Output directory for downloads
            progress_callback: Callback for download progress updates
        """
        self.api_url = api_url.rstrip("/")
        self.s3_bucket = s3_bucket
        self.region = region
        self.profile_name = profile_name
        self.progress_callback = progress_callback

        # Initialize AWS clients
        session = boto3.Session(profile_name=profile_name, region_name=region)
        config = Config(retries={"max_attempts": 3, "mode": "adaptive"})
        self.s3_client = session.client("s3", config=config)
        self.session = session

        # Initialize services
        self.progress_store = progress_store or DownloadProgressStore()
        self.review_service = review_service or ReviewService()
        self.status_command = StatusCommand(
            api_url=api_url, region=region, profile_name=profile_name
        )

        # Output directory
        if output_dir is None:
            output_dir = Path.home() / "Movies" / "VideoCompressionOptimizer" / "converted"
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def download(
        self,
        task_id: str,
        output_dir: Path | None = None,
        resume: bool = True,
        user_id: str | None = None,
    ) -> DownloadResult:
        """Download completed task results.

        1. Check task status (COMPLETED or PARTIALLY_COMPLETED)
        2. Download each successful file
        3. Verify checksums
        4. Add to review queue

        Args:
            task_id: Task ID to download
            output_dir: Output directory (optional, uses default)
            resume: Whether to resume interrupted downloads
            user_id: User identifier (defaults to machine ID)

        Returns:
            DownloadResult with download details

        Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 9.6
        """
        if output_dir:
            self.output_dir = output_dir
            self.output_dir.mkdir(parents=True, exist_ok=True)

        if user_id is None:
            user_id = self._get_machine_id()

        # Get task details
        try:
            task = self.status_command.get_task_detail(task_id, user_id)
        except Exception as e:
            return DownloadResult(
                task_id=task_id,
                success=False,
                total_files=0,
                downloaded_files=0,
                failed_files=0,
                added_to_queue=0,
                error_message=f"Failed to get task details: {e}",
            )

        # Check task status
        if task.status not in ("COMPLETED", "PARTIALLY_COMPLETED"):
            return DownloadResult(
                task_id=task_id,
                success=False,
                total_files=len(task.files),
                downloaded_files=0,
                failed_files=0,
                added_to_queue=0,
                error_message=f"Task is not ready for download. Status: {task.status}",
            )

        # Filter successful files only (Requirement 9.6)
        successful_files = [f for f in task.files if f.status == "COMPLETED"]

        if not successful_files:
            return DownloadResult(
                task_id=task_id,
                success=False,
                total_files=len(task.files),
                downloaded_files=0,
                failed_files=len(task.files),
                added_to_queue=0,
                error_message="No successful files to download",
            )

        # Validate output_s3_key for all successful files (Requirement 5.6, Property 7)
        files_missing_s3_key = [f for f in successful_files if not f.output_s3_key]
        if files_missing_s3_key:
            missing_filenames = [f.filename for f in files_missing_s3_key]
            return DownloadResult(
                task_id=task_id,
                success=False,
                total_files=len(task.files),
                downloaded_files=0,
                failed_files=len(files_missing_s3_key),
                added_to_queue=0,
                error_message=f"Files missing output_s3_key: {', '.join(missing_filenames)}",
            )

        result = DownloadResult(
            task_id=task_id,
            success=True,
            total_files=len(task.files),
            downloaded_files=0,
            failed_files=len(task.files) - len(successful_files),
            added_to_queue=0,
        )

        # Download each successful file
        for file_detail in successful_files:
            file_result = self._download_file(
                task_id=task_id,
                file_detail=file_detail,
                task=task,
                resume=resume,
            )
            result.results.append(file_result)

            if file_result.success:
                result.downloaded_files += 1

                # Update download status in DynamoDB
                self._update_download_status(task_id, file_detail.file_id, "completed")

                # Add to review queue
                if self._add_to_review_queue(task_id, file_detail, file_result.local_path, task):
                    result.added_to_queue += 1
            else:
                result.failed_files += 1

        # Clear task progress if all files downloaded
        if result.downloaded_files == len(successful_files):
            self.progress_store.clear_task(task_id)

        result.success = result.downloaded_files > 0

        return result

    def _download_file(
        self,
        task_id: str,
        file_detail,
        task: TaskDetail,
        resume: bool,
    ) -> FileDownloadResult:
        """Download a single file with resume support.

        Args:
            task_id: Task ID
            file_detail: File detail from task
            task: Task detail
            resume: Whether to resume interrupted downloads

        Returns:
            FileDownloadResult

        Requirements: 4.2, 4.3, 4.5
        """
        file_id = file_detail.file_id
        filename = file_detail.filename

        # Determine output path
        output_filename = Path(filename).stem + "_h265.mp4"
        output_path = self.output_dir / output_filename
        temp_path = self.output_dir / f".{output_filename}.tmp"

        # Use output_s3_key from task detail if available, otherwise construct it
        if file_detail.output_s3_key:
            s3_key = file_detail.output_s3_key
        else:
            # Fallback to constructed path (for backward compatibility)
            s3_key = f"output/{task_id}/{file_id}/{output_filename}"

        try:
            # Get file size from S3
            head_response = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            total_bytes = head_response["ContentLength"]
            etag = head_response.get("ETag", "").strip('"')

            # Check for existing progress
            existing_progress = self.progress_store.get_progress(task_id, file_id)
            start_byte = 0
            resumed = False

            if resume and existing_progress and temp_path.exists():
                # Verify temp file size matches progress
                actual_size = temp_path.stat().st_size
                if actual_size == existing_progress.downloaded_bytes:
                    start_byte = actual_size
                    resumed = True
                    logger.info(f"Resuming download for {filename} from byte {start_byte}")
                else:
                    # Progress mismatch, start fresh
                    temp_path.unlink(missing_ok=True)
                    self.progress_store.clear_progress(task_id, file_id)

            # Download file
            if start_byte < total_bytes:
                self._download_with_progress(
                    s3_key=s3_key,
                    local_path=temp_path,
                    task_id=task_id,
                    file_id=file_id,
                    total_bytes=total_bytes,
                    start_byte=start_byte,
                    checksum=etag,
                    display_filename=output_filename,
                )

            # Verify checksum (Requirement 4.2)
            checksum_ok = self._verify_checksum(temp_path, etag)

            if not checksum_ok:
                # Retry once
                logger.warning(f"Checksum mismatch for {filename}, retrying...")
                temp_path.unlink(missing_ok=True)
                self.progress_store.clear_progress(task_id, file_id)

                self._download_with_progress(
                    s3_key=s3_key,
                    local_path=temp_path,
                    task_id=task_id,
                    file_id=file_id,
                    total_bytes=total_bytes,
                    start_byte=0,
                    checksum=etag,
                    display_filename=output_filename,
                )

                checksum_ok = self._verify_checksum(temp_path, etag)

                if not checksum_ok:
                    return FileDownloadResult(
                        file_id=file_id,
                        filename=filename,
                        success=False,
                        error_message="Checksum verification failed after retry",
                    )

            # Move temp file to final location
            temp_path.rename(output_path)

            # Set file dates from metadata (same as sync mode)
            # Use capture_date from file_detail if available
            self._set_file_dates(output_path, file_detail)

            # Clear progress
            self.progress_store.clear_progress(task_id, file_id)

            # Delete output file from S3 after successful download
            self._delete_s3_file(s3_key)

            return FileDownloadResult(
                file_id=file_id,
                filename=filename,
                success=True,
                local_path=output_path,
                checksum_verified=checksum_ok,
                resumed=resumed,
            )

        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"File not found in S3: {s3_key}")
            return FileDownloadResult(
                file_id=file_id,
                filename=filename,
                success=False,
                error_message="File not found in S3. The conversion may still be in progress.",
            )
        except Exception as e:
            # Extract user-friendly message from boto exceptions
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg:
                error_msg = "File not found in S3. The conversion may still be in progress."
            elif "403" in error_msg or "Forbidden" in error_msg:
                error_msg = "Access denied to S3 file. Check your AWS credentials."
            elif "ExpiredToken" in error_msg:
                error_msg = "AWS credentials have expired. Please refresh your credentials."

            logger.warning(f"Failed to download {filename}: {error_msg}")
            return FileDownloadResult(
                file_id=file_id,
                filename=filename,
                success=False,
                error_message=error_msg,
            )

    def _download_with_progress(
        self,
        s3_key: str,
        local_path: Path,
        task_id: str,
        file_id: str,
        total_bytes: int,
        start_byte: int,
        checksum: str,
        display_filename: str | None = None,
    ) -> None:
        """Download file with progress tracking.

        Args:
            s3_key: S3 key
            local_path: Local file path
            task_id: Task ID
            file_id: File ID
            total_bytes: Total file size
            start_byte: Starting byte for resume
            checksum: Expected checksum
            display_filename: Filename to display in progress (defaults to local_path.name)
        """
        # Use display_filename if provided, otherwise use local_path.name
        progress_filename = display_filename or local_path.name

        # Prepare download parameters
        extra_args = {}
        if start_byte > 0:
            extra_args["Range"] = f"bytes={start_byte}-"

        # Open file for writing (append if resuming)
        mode = "ab" if start_byte > 0 else "wb"

        # Create progress tracker
        downloaded = start_byte

        def progress_callback(bytes_amount):
            nonlocal downloaded
            downloaded += bytes_amount

            # Save progress periodically (every 1MB)
            if downloaded % (1024 * 1024) == 0 or downloaded >= total_bytes:
                progress = DownloadProgress(
                    task_id=task_id,
                    file_id=file_id,
                    total_bytes=total_bytes,
                    downloaded_bytes=downloaded,
                    local_temp_path=str(local_path),
                    s3_key=s3_key,
                    checksum=checksum,
                )
                self.progress_store.save_progress(progress)

            # Report progress
            if self.progress_callback:
                self.progress_callback(
                    progress_filename,
                    int((downloaded / total_bytes) * 100),
                    downloaded,
                    total_bytes,
                )

        # Download
        if start_byte > 0:
            # Use get_object for range requests
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket, Key=s3_key, Range=f"bytes={start_byte}-"
            )

            with open(local_path, mode) as f:
                for chunk in response["Body"].iter_chunks(chunk_size=8192):
                    f.write(chunk)
                    progress_callback(len(chunk))
        else:
            # Use download_file for full downloads
            self.s3_client.download_file(
                self.s3_bucket,
                s3_key,
                str(local_path),
                Callback=progress_callback,
            )

    def _verify_checksum(self, local_path: Path, expected_etag: str) -> bool:
        """Verify file checksum against S3 ETag.

        Args:
            local_path: Local file path
            expected_etag: Expected ETag from S3

        Returns:
            True if checksum matches

        Requirements: 4.2
        """
        # S3 ETag for single-part uploads is MD5
        # For multipart uploads, it's more complex
        if "-" in expected_etag:
            # Multipart upload - skip verification for now
            logger.warning("Multipart upload detected, skipping checksum verification")
            return True

        try:
            md5_hash = hashlib.md5()
            with open(local_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    md5_hash.update(chunk)

            calculated = md5_hash.hexdigest()
            return calculated == expected_etag

        except Exception as e:
            logger.warning(f"Checksum verification failed: {e}")
            return False

    def _add_to_review_queue(
        self,
        task_id: str,
        file_detail,
        local_path: Path,
        task: TaskDetail,
    ) -> bool:
        """Add downloaded file to review queue.

        Args:
            task_id: Task ID
            file_detail: File detail
            local_path: Local file path
            task: Task detail

        Returns:
            True if added successfully, False if failed or already exists
        """
        try:
            # Check if already in review queue (prevent duplicates)
            existing = self.review_service.get_pending_by_uuid(file_detail.file_id)
            if existing:
                logger.info(f"File {file_detail.filename} already in review queue, skipping")
                return False

            # Create a minimal conversion result for review service
            from vco.models.types import ConversionResult

            # Get quality result from file detail
            quality_result = None
            if file_detail.ssim_score is not None:
                from vco.quality.checker import QualityResult

                quality_result = QualityResult(
                    is_acceptable=True,
                    ssim_score=file_detail.ssim_score,
                    original_size=file_detail.original_size_bytes or 0,
                    converted_size=file_detail.output_size_bytes or 0,
                    space_saved_bytes=file_detail.space_saved_bytes or 0,
                    space_saved_percent=file_detail.space_saved_percent or 0.0,
                )

            result = ConversionResult(
                uuid=file_detail.file_id,
                filename=file_detail.filename,
                success=True,
                original_path=Path(file_detail.filename),  # Original path not available
                converted_path=local_path,
                quality_result=quality_result,
            )

            review_item = self.review_service.add_to_queue(result)
            return review_item is not None

        except Exception as e:
            logger.warning(f"Failed to add to review queue: {e}")
            return False

    def _get_machine_id(self) -> str:
        """Get machine identifier for user_id.

        Returns:
            Machine ID string
        """
        import hashlib
        import platform

        machine_info = f"{platform.node()}-{platform.machine()}"
        return hashlib.sha256(machine_info.encode()).hexdigest()[:32]

    def _set_file_dates(self, file_path: Path, file_detail) -> None:
        """Set file dates from metadata (same as sync mode).

        Uses capture_date from file_detail if available.
        Falls back to current time if no date is available.

        Args:
            file_path: Path to the downloaded file
            file_detail: File detail from task containing metadata
        """
        import os
        from datetime import datetime

        try:
            # Get capture_date from file_detail if available
            capture_date = getattr(file_detail, "capture_date", None)

            if capture_date:
                # Convert to timestamp
                if isinstance(capture_date, str):
                    # Parse ISO format string
                    capture_date = datetime.fromisoformat(capture_date.replace("Z", "+00:00"))

                timestamp = capture_date.timestamp()

                # Set both access time and modification time
                os.utime(file_path, (timestamp, timestamp))
                logger.info(f"Set file dates for {file_path.name} to {capture_date}")
            else:
                logger.debug(
                    f"No capture_date available for {file_path.name}, keeping default dates"
                )

        except Exception as e:
            logger.warning(f"Failed to set file dates for {file_path.name}: {e}")

    def _delete_s3_file(self, s3_key: str) -> bool:
        """Delete a file from S3 after successful download.

        Args:
            s3_key: S3 key to delete

        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.s3_bucket, Key=s3_key)
            logger.info(f"Deleted S3 output file: {s3_key}")
            return True
        except Exception as e:
            logger.warning(f"Failed to delete S3 file {s3_key}: {e}")
            return False

    def _update_download_status(self, task_id: str, file_id: str, status: str) -> bool:
        """Update file status to DOWNLOADED in DynamoDB.

        Args:
            task_id: Task ID
            file_id: File ID
            status: Status value (only "completed" is supported, which sets status to DOWNLOADED)

        Returns:
            True if updated successfully, False otherwise
        """
        if status != "completed":
            logger.warning(f"Unsupported status: {status}, only 'completed' is supported")
            return False

        try:
            self.status_command.update_file_status_to_downloaded(task_id, file_id)
            logger.info(f"Updated file status to DOWNLOADED for {file_id}")
            return True
        except Exception as e:
            logger.warning(f"Failed to update file status for {file_id}: {e}")
            return False
