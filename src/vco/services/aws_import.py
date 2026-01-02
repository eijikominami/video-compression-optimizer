"""AWS Import Service for downloading and preparing AWS completed files.

This service provides a unified interface for importing AWS-processed videos
via the cleanup API for atomic status updates and S3 file deletion.

Requirements: 3.1, 3.3, 3.4, 10.1, 10.6
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

from vco.models.types import ImportableItem
from vco.services.async_status import StatusCommand
from vco.services.download_progress import DownloadProgress, DownloadProgressStore

logger = logging.getLogger(__name__)


@dataclass
class AwsDownloadResult:
    """Result of downloading a single AWS file."""

    success: bool
    task_id: str
    file_id: str
    local_path: Path | None = None
    metadata_path: Path | None = None
    error_message: str | None = None
    checksum_verified: bool = False
    download_resumed: bool = False


@dataclass
class CleanupResult:
    """Result of AWS file cleanup operation."""

    success: bool
    file_id: str
    status: str  # "DOWNLOADED" or "REMOVED"
    s3_deleted: bool
    completed_at: str | None = None
    error_message: str | None = None


class AwsImportService:
    """Service for importing videos from AWS.

    Provides:
    - List completed files from AWS
    - Download and prepare files for import
    - Cleanup files via API (atomic status update + S3 deletion)

    Requirements: 3.1, 3.3, 3.4
    """

    def __init__(
        self,
        api_url: str,
        s3_bucket: str,
        region: str = "ap-northeast-1",
        profile_name: str | None = None,
        progress_store: DownloadProgressStore | None = None,
        output_dir: Path | None = None,
    ):
        """Initialize AwsImportService.

        Args:
            api_url: API Gateway URL for async workflow
            s3_bucket: S3 bucket for downloads
            region: AWS region
            profile_name: AWS profile name (optional)
            progress_store: Store for download progress
            output_dir: Output directory for downloads
        """
        self.api_url = api_url.rstrip("/")
        self.s3_bucket = s3_bucket
        self.region = region
        self.profile_name = profile_name
        self.progress_store = progress_store or DownloadProgressStore()

        # Output directory
        if output_dir is None:
            output_dir = Path.home() / "Movies" / "VideoCompressionOptimizer" / "converted"
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize AWS clients
        session = boto3.Session(profile_name=profile_name, region_name=region)
        config = Config(retries={"max_attempts": 3, "mode": "adaptive"})
        self.s3_client = session.client("s3", config=config)
        self.session = session

        # Initialize status command for listing
        self.status_command = StatusCommand(
            api_url=api_url,
            region=region,
            profile_name=profile_name,
        )

    def list_completed_files(self, user_id: str | None = None) -> list[ImportableItem]:
        """List all completed files from AWS.

        Retrieves COMPLETED and PARTIALLY_COMPLETED tasks,
        then extracts successful files as ImportableItem.

        Args:
            user_id: User identifier (defaults to machine ID)

        Returns:
            List of ImportableItem for AWS completed files

        Requirements: 1.1
        """
        if user_id is None:
            user_id = self._get_machine_id()

        items: list[ImportableItem] = []

        try:
            # Get tasks with completed status
            tasks = self.status_command.list_tasks(user_id=user_id, limit=100)

            for task_summary in tasks:
                if task_summary.status not in ("COMPLETED", "PARTIALLY_COMPLETED"):
                    continue

                # Get task details for file information
                try:
                    task_detail = self.status_command.get_task_detail(task_summary.task_id, user_id)
                except Exception as e:
                    logger.warning(f"Failed to get task detail for {task_summary.task_id}: {e}")
                    continue

                # Extract successful files that haven't been downloaded yet
                # Files with status DOWNLOADED are excluded (already downloaded)
                for file_detail in task_detail.files:
                    if file_detail.status != "COMPLETED":
                        continue

                    item = self._file_detail_to_importable_item(
                        task_id=task_summary.task_id,
                        file_detail=file_detail,
                    )
                    items.append(item)

        except Exception as e:
            logger.warning(f"Failed to list completed files from AWS: {e}")
            raise RuntimeError(f"Failed to list AWS completed files: {e}") from e

        return items

    def download_and_prepare(
        self,
        task_id: str,
        file_id: str,
        user_id: str | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ) -> AwsDownloadResult:
        """Download a single file from S3 and prepare for import.

        Downloads the file directly from S3, verifies checksum, and returns local path.
        Does NOT delete S3 file or update status - use cleanup_file() for that.

        Args:
            task_id: Task ID
            file_id: File ID
            user_id: User identifier
            progress_callback: Callback for download progress (filename, percent, downloaded, total)

        Returns:
            AwsDownloadResult with local path if successful

        Requirements: 3.1, 3.3
        """
        if user_id is None:
            user_id = self._get_machine_id()

        try:
            # Get task detail to find the specific file
            task_detail = self.status_command.get_task_detail(task_id, user_id)

            # Find the file
            file_detail = None
            for f in task_detail.files:
                if f.file_id == file_id:
                    file_detail = f
                    break

            if file_detail is None:
                return AwsDownloadResult(
                    success=False,
                    task_id=task_id,
                    file_id=file_id,
                    error_message=f"File not found: {file_id}",
                )

            if file_detail.status != "COMPLETED":
                return AwsDownloadResult(
                    success=False,
                    task_id=task_id,
                    file_id=file_id,
                    error_message=f"File not ready: status={file_detail.status}",
                )

            # Get S3 key
            s3_key = file_detail.output_s3_key
            if not s3_key:
                return AwsDownloadResult(
                    success=False,
                    task_id=task_id,
                    file_id=file_id,
                    error_message="File missing output_s3_key",
                )

            # Determine output path
            output_filename = Path(file_detail.filename).stem + "_h265.mp4"
            output_path = self.output_dir / output_filename
            temp_path = self.output_dir / f".{output_filename}.tmp"

            # Download file from S3
            download_result = self._download_s3_file(
                s3_key=s3_key,
                output_path=output_path,
                temp_path=temp_path,
                task_id=task_id,
                file_id=file_id,
                progress_callback=progress_callback,
            )

            if not download_result["success"]:
                return AwsDownloadResult(
                    success=False,
                    task_id=task_id,
                    file_id=file_id,
                    error_message=download_result.get("error_message", "Download failed"),
                )

            # Download metadata file if available
            metadata_path = None
            metadata_s3_key = getattr(file_detail, "metadata_s3_key", None)
            if metadata_s3_key:
                try:
                    metadata_filename = f"{Path(file_detail.filename).stem}_metadata.json"
                    metadata_path = self.output_dir / metadata_filename
                    self.s3_client.download_file(
                        self.s3_bucket, metadata_s3_key, str(metadata_path)
                    )
                except Exception as e:
                    logger.warning(f"Failed to download metadata for {file_id}: {e}")

            return AwsDownloadResult(
                success=True,
                task_id=task_id,
                file_id=file_id,
                local_path=output_path,
                metadata_path=metadata_path,
                checksum_verified=download_result.get("checksum_verified", False),
                download_resumed=download_result.get("resumed", False),
            )

        except Exception as e:
            # Extract user-friendly message
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg:
                error_msg = "File not found in S3. The conversion may still be in progress."
            elif "403" in error_msg or "Forbidden" in error_msg:
                error_msg = "Access denied to S3 file. Check your AWS credentials."
            elif "ExpiredToken" in error_msg:
                error_msg = "AWS credentials have expired. Please refresh your credentials."

            logger.warning(f"Failed to download file {task_id}:{file_id}: {error_msg}")
            return AwsDownloadResult(
                success=False,
                task_id=task_id,
                file_id=file_id,
                error_message=error_msg,
            )

    def _download_s3_file(
        self,
        s3_key: str,
        output_path: Path,
        temp_path: Path,
        task_id: str,
        file_id: str,
        progress_callback: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        """Download file from S3 with progress tracking and resume support.

        Args:
            s3_key: S3 object key
            output_path: Final output path
            temp_path: Temporary file path during download
            task_id: Task ID for progress tracking
            file_id: File ID for progress tracking
            progress_callback: Callback for progress updates

        Returns:
            Dict with success, checksum_verified, resumed, error_message
        """
        try:
            # Get file info from S3
            head_response = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            total_bytes = head_response["ContentLength"]
            etag = head_response.get("ETag", "").strip('"')

            # Check for existing progress (resume support)
            existing_progress = self.progress_store.get_progress(task_id, file_id)
            start_byte = 0
            resumed = False

            if existing_progress and temp_path.exists():
                actual_size = temp_path.stat().st_size
                if actual_size == existing_progress.downloaded_bytes:
                    start_byte = actual_size
                    resumed = True
                    logger.info(f"Resuming download from byte {start_byte}")
                else:
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
                    progress_callback=progress_callback,
                )

            # Verify checksum
            checksum_ok = self._verify_checksum(temp_path, etag)

            if not checksum_ok:
                # Retry once
                logger.warning("Checksum mismatch, retrying download...")
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
                    progress_callback=progress_callback,
                )

                checksum_ok = self._verify_checksum(temp_path, etag)
                if not checksum_ok:
                    return {
                        "success": False,
                        "error_message": "Checksum verification failed after retry",
                    }

            # Move temp file to final location
            temp_path.rename(output_path)
            self.progress_store.clear_progress(task_id, file_id)

            return {
                "success": True,
                "checksum_verified": checksum_ok,
                "resumed": resumed,
            }

        except self.s3_client.exceptions.NoSuchKey:
            return {
                "success": False,
                "error_message": "File not found in S3",
            }
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e),
            }

    def _download_with_progress(
        self,
        s3_key: str,
        local_path: Path,
        task_id: str,
        file_id: str,
        total_bytes: int,
        start_byte: int,
        checksum: str,
        progress_callback: Callable[..., Any] | None = None,
    ) -> None:
        """Download file with progress tracking.

        Args:
            s3_key: S3 object key
            local_path: Local file path
            task_id: Task ID
            file_id: File ID
            total_bytes: Total file size
            start_byte: Starting byte for resume
            checksum: Expected checksum
            progress_callback: Callback for progress updates
        """
        downloaded = start_byte

        def callback(bytes_amount: int) -> None:
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
            if progress_callback:
                progress_callback(
                    local_path.name,
                    int((downloaded / total_bytes) * 100),
                    downloaded,
                    total_bytes,
                )

        if start_byte > 0:
            # Range request for resume
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket, Key=s3_key, Range=f"bytes={start_byte}-"
            )
            with open(local_path, "ab") as f:
                for chunk in response["Body"].iter_chunks(chunk_size=8192):
                    f.write(chunk)
                    callback(len(chunk))
        else:
            # Full download
            self.s3_client.download_file(
                self.s3_bucket,
                s3_key,
                str(local_path),
                Callback=callback,
            )

    def _verify_checksum(self, local_path: Path, expected_etag: str) -> bool:
        """Verify file checksum against S3 ETag.

        Args:
            local_path: Local file path
            expected_etag: Expected ETag from S3

        Returns:
            True if checksum matches
        """
        # Multipart uploads have different ETag format
        if "-" in expected_etag:
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

    def cleanup_file(
        self,
        task_id: str,
        file_id: str,
        action: str = "downloaded",
        user_id: str | None = None,
    ) -> CleanupResult:
        """Cleanup AWS file: update status and delete S3 file atomically.

        Calls the cleanup API endpoint which:
        1. Updates DynamoDB file status (DOWNLOADED or REMOVED)
        2. Deletes S3 output file

        Args:
            task_id: Task ID
            file_id: File ID
            action: "downloaded" (after import) or "removed" (on delete/clear)
            user_id: User identifier

        Returns:
            CleanupResult with status and s3_deleted flag

        Requirements: 10.1, 10.6
        """
        if user_id is None:
            user_id = self._get_machine_id()

        if action not in ("downloaded", "removed"):
            return CleanupResult(
                success=False,
                file_id=file_id,
                status="",
                s3_deleted=False,
                error_message=f"Invalid action: {action}. Must be 'downloaded' or 'removed'",
            )

        try:
            import boto3
            import requests  # type: ignore[import-untyped]
            from botocore.auth import SigV4Auth
            from botocore.awsrequest import AWSRequest

            # Create AWS session
            session = boto3.Session(profile_name=self.profile_name, region_name=self.region)
            credentials = session.get_credentials()

            # Build request URL
            url = f"{self.api_url}/tasks/{task_id}/files/{file_id}/cleanup"

            # Prepare request body
            body = json.dumps({"action": action})

            # Sign request with SigV4
            request = AWSRequest(
                method="POST",
                url=url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "X-User-Id": user_id,
                },
            )
            SigV4Auth(credentials, "execute-api", self.region).add_auth(request)

            # Make request
            response = requests.post(
                url,
                data=body,
                headers=dict(request.headers),
                timeout=30,
            )

            if response.status_code == 200:
                result = response.json()
                return CleanupResult(
                    success=True,
                    file_id=result.get("file_id", file_id),
                    status=result.get(
                        "status", "DOWNLOADED" if action == "downloaded" else "REMOVED"
                    ),
                    s3_deleted=result.get("s3_deleted", False),
                    completed_at=result.get("completed_at"),
                )
            elif response.status_code == 404:
                return CleanupResult(
                    success=False,
                    file_id=file_id,
                    status="",
                    s3_deleted=False,
                    error_message=f"Task or file not found: {task_id}:{file_id}",
                )
            else:
                error_body = response.json() if response.text else {}
                return CleanupResult(
                    success=False,
                    file_id=file_id,
                    status="",
                    s3_deleted=False,
                    error_message=error_body.get("error", f"API error: {response.status_code}"),
                )

        except Exception as e:
            # Extract user-friendly message
            error_msg = str(e)
            if "404" in error_msg or "Not Found" in error_msg:
                error_msg = f"Task or file not found: {task_id}:{file_id}"
            elif "403" in error_msg or "Forbidden" in error_msg:
                error_msg = "Access denied. Check your AWS credentials."
            elif "ExpiredToken" in error_msg:
                error_msg = "AWS credentials have expired. Please refresh your credentials."

            logger.warning(f"Failed to cleanup file {task_id}:{file_id}: {error_msg}")
            return CleanupResult(
                success=False,
                file_id=file_id,
                status="",
                s3_deleted=False,
                error_message=error_msg,
            )

    def _file_detail_to_importable_item(
        self,
        task_id: str,
        file_detail,
    ) -> ImportableItem:
        """Convert FileDetail to ImportableItem.

        Args:
            task_id: Task ID
            file_detail: FileDetail from StatusCommand

        Returns:
            ImportableItem
        """
        # Calculate compression ratio if not available
        compression_ratio = file_detail.compression_ratio or 0.0
        if (
            compression_ratio == 0.0
            and file_detail.original_size_bytes
            and file_detail.output_size_bytes
            and file_detail.output_size_bytes > 0
        ):
            compression_ratio = file_detail.original_size_bytes / file_detail.output_size_bytes

        # Generate converted filename
        converted_filename = Path(file_detail.filename).stem + "_h265.mp4"

        return ImportableItem(
            item_id=f"{task_id}:{file_detail.file_id}",
            source="aws",
            original_filename=file_detail.filename,
            converted_filename=converted_filename,
            original_size=file_detail.original_size_bytes or 0,
            converted_size=file_detail.output_size_bytes or 0,
            compression_ratio=compression_ratio,
            ssim_score=file_detail.ssim_score or 0.0,
            albums=[],  # Albums not available from AWS
            capture_date=None,  # Capture date not available from AWS
            task_id=task_id,
            file_id=file_detail.file_id,
            s3_key=file_detail.output_s3_key,
        )

    def _get_machine_id(self) -> str:
        """Get machine identifier for user_id."""
        import platform

        machine_info = f"{platform.node()}-{platform.machine()}"
        return hashlib.sha256(machine_info.encode()).hexdigest()[:32]
