"""AWS Import Service for downloading and preparing AWS completed files.

This service wraps DownloadCommand and StatusCommand to provide
a unified interface for importing AWS-processed videos.

Requirements: 3.1, 3.3, 3.4, 10.1, 10.6
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from vco.models.types import ImportableItem
from vco.services.async_download import DownloadCommand
from vco.services.async_status import StatusCommand
from vco.services.download_progress import DownloadProgressStore

logger = logging.getLogger(__name__)


@dataclass
class AwsDownloadResult:
    """Result of downloading a single AWS file."""

    success: bool
    task_id: str
    file_id: str
    local_path: Path | None = None
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

    Wraps DownloadCommand and StatusCommand to provide:
    - List completed files from AWS
    - Download and prepare files for import
    - Delete S3 files after successful import

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

        # Initialize status command for listing
        self.status_command = StatusCommand(
            api_url=api_url,
            region=region,
            profile_name=profile_name,
        )

        # Download command will be created per-download for progress callback
        self._download_command: DownloadCommand | None = None

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

        Downloads the file, verifies checksum, and returns local path.
        Does NOT add to review queue (UnifiedImportService handles that).

        Args:
            task_id: Task ID
            file_id: File ID
            user_id: User identifier
            progress_callback: Callback for download progress

        Returns:
            AwsDownloadResult with local path if successful

        Requirements: 3.1, 3.3
        """
        if user_id is None:
            user_id = self._get_machine_id()

        # Create download command with progress callback
        download_cmd = DownloadCommand(
            api_url=self.api_url,
            s3_bucket=self.s3_bucket,
            region=self.region,
            profile_name=self.profile_name,
            progress_store=self.progress_store,
            review_service=None,  # Don't add to review queue
            output_dir=self.output_dir,
            progress_callback=progress_callback,
        )

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

            # Download the file using internal method
            file_result = download_cmd._download_file(
                task_id=task_id,
                file_detail=file_detail,
                task=task_detail,
                resume=True,
            )

            return AwsDownloadResult(
                success=file_result.success,
                task_id=task_id,
                file_id=file_id,
                local_path=file_result.local_path,
                error_message=file_result.error_message,
                checksum_verified=file_result.checksum_verified,
                download_resumed=file_result.resumed,
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

    def delete_s3_file(
        self,
        task_id: str,
        file_id: str,
        user_id: str | None = None,
    ) -> bool:
        """Delete S3 output file after successful import.

        Note: This method is deprecated. Use cleanup_file() instead.

        Args:
            task_id: Task ID
            file_id: File ID
            user_id: User identifier

        Returns:
            True if deleted successfully

        Requirements: 3.4
        """
        if user_id is None:
            user_id = self._get_machine_id()

        try:
            import boto3
            from botocore.config import Config

            # Get task detail to find S3 key
            task_detail = self.status_command.get_task_detail(task_id, user_id)

            # Find the file
            file_detail = None
            for f in task_detail.files:
                if f.file_id == file_id:
                    file_detail = f
                    break

            if file_detail is None:
                logger.warning(f"File not found for deletion: {task_id}:{file_id}")
                return False

            s3_key = file_detail.output_s3_key
            if not s3_key:
                # Construct key if not available
                output_filename = Path(file_detail.filename).stem + "_h265.mp4"
                s3_key = f"output/{task_id}/{file_id}/{output_filename}"

            # Delete from S3
            session = boto3.Session(profile_name=self.profile_name, region_name=self.region)
            config = Config(retries={"max_attempts": 3, "mode": "adaptive"})
            s3_client = session.client("s3", config=config)

            s3_client.delete_object(Bucket=self.s3_bucket, Key=s3_key)
            logger.info(f"Deleted S3 file: {s3_key}")
            return True

        except Exception as e:
            logger.warning(f"Failed to delete S3 file {task_id}:{file_id}: {e}")
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
            import requests
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
        import hashlib
        import platform

        machine_info = f"{platform.node()}-{platform.machine()}"
        return hashlib.sha256(machine_info.encode()).hexdigest()[:32]
