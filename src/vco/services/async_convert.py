"""Async convert service for submitting conversion tasks to AWS.

This service handles:
1. Upload source files to S3
2. Submit task to API Gateway
3. Return task ID for tracking

Requirements: 1.1, 1.2, 1.4
"""

import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.config import Config

from vco.analyzer.analyzer import ConversionCandidate
from vco.metadata.manager import MetadataManager, VideoMetadata

logger = logging.getLogger(__name__)


@dataclass
class AsyncTaskResult:
    """Result of async task submission."""

    task_id: str
    status: str
    file_count: int
    message: str
    api_url: str | None = None
    error_message: str | None = None


@dataclass
class UploadProgress:
    """Progress information for file upload."""

    filename: str
    progress_percent: int
    uploaded_bytes: int
    total_bytes: int


class AsyncConvertCommand:
    """Handler for async conversion command.

    Submits conversion tasks to AWS Step Functions via API Gateway.
    Files are uploaded to S3 before task submission.

    Requirements: 1.1, 1.2, 1.4
    """

    def __init__(
        self,
        api_url: str,
        s3_bucket: str,
        region: str = "ap-northeast-1",
        profile_name: str | None = None,
        progress_callback: Callable | None = None,
    ):
        """Initialize AsyncConvertCommand.

        Args:
            api_url: API Gateway URL for async workflow
            s3_bucket: S3 bucket for file uploads
            region: AWS region
            profile_name: AWS profile name (optional)
            progress_callback: Callback for upload progress updates
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
        self.api_client = session.client("apigatewaymanagementapi", config=config)

        # For signing requests to API Gateway
        self.session = session

        # Metadata manager for extracting video metadata
        self.metadata_manager = MetadataManager()

    def execute(
        self,
        candidates: list[ConversionCandidate],
        quality_preset: str = "balanced",
        user_id: str | None = None,
    ) -> AsyncTaskResult:
        """Submit async conversion task.

        1. Generate task ID
        2. Upload source files to S3
        3. Submit task to API Gateway
        4. Return task ID for tracking

        Args:
            candidates: List of conversion candidates
            quality_preset: Quality preset for conversion
            user_id: User identifier (defaults to machine ID)

        Returns:
            AsyncTaskResult with task ID and status

        Requirements: 1.1, 1.2, 1.4
        """
        if not candidates:
            return AsyncTaskResult(
                task_id="",
                status="ERROR",
                file_count=0,
                message="No candidates provided",
                error_message="No candidates provided",
            )

        # Generate task ID
        task_id = str(uuid.uuid4())

        # Use machine ID as user_id if not provided
        if user_id is None:
            user_id = self._get_machine_id()

        logger.info(f"Submitting async task {task_id} with {len(candidates)} files")

        # Filter local candidates only
        local_candidates = []
        skipped_candidates = []

        for candidate in candidates:
            video = candidate.video
            if self._check_file_available(video.path):
                local_candidates.append(candidate)
            else:
                skipped_candidates.append(candidate)
                logger.warning(f"Skipping iCloud-only file: {video.filename}")

        if not local_candidates:
            return AsyncTaskResult(
                task_id=task_id,
                status="ERROR",
                file_count=0,
                message="No local files available for upload",
                error_message="All files are in iCloud only. Download them in Photos app first.",
            )

        # Upload files to S3
        files_data = []
        try:
            for i, candidate in enumerate(local_candidates):
                video = candidate.video
                file_id = str(uuid.uuid4())

                # Report progress
                self._report_progress(
                    video.filename,
                    int((i / len(local_candidates)) * 100),
                    0,
                    video.file_size,
                )

                # Upload source file
                source_s3_key = f"async/{task_id}/input/{file_id}/{video.filename}"
                self._upload_file(video.path, source_s3_key, video.file_size, video.filename)

                # Extract and upload metadata
                metadata = self._extract_metadata(video)
                metadata_s3_key = f"async/{task_id}/input/{file_id}/metadata.json"
                self._upload_metadata(metadata, metadata_s3_key)

                files_data.append(
                    {
                        "file_id": file_id,
                        "original_uuid": video.uuid,
                        "filename": video.filename,
                        "source_s3_key": source_s3_key,
                        "metadata_s3_key": metadata_s3_key,
                        "source_size_bytes": video.file_size,
                    }
                )

                logger.info(f"Uploaded {video.filename} to s3://{self.s3_bucket}/{source_s3_key}")

        except Exception as e:
            logger.exception(f"Failed to upload files for task {task_id}")
            # Clean up uploaded files
            self._cleanup_task_files(task_id)
            return AsyncTaskResult(
                task_id=task_id,
                status="ERROR",
                file_count=0,
                message="Failed to upload files",
                error_message=str(e),
            )

        # Submit task to API Gateway
        try:
            self._submit_task(
                task_id=task_id,
                user_id=user_id,
                quality_preset=quality_preset,
                files=files_data,
            )

            return AsyncTaskResult(
                task_id=task_id,
                status="PENDING",
                file_count=len(files_data),
                message=f"Task submitted successfully. {len(skipped_candidates)} files skipped (iCloud only).",
                api_url=self.api_url,
            )

        except Exception as e:
            logger.exception(f"Failed to submit task {task_id}")
            # Clean up uploaded files
            self._cleanup_task_files(task_id)
            return AsyncTaskResult(
                task_id=task_id,
                status="ERROR",
                file_count=0,
                message="Failed to submit task",
                error_message=str(e),
            )

    def _check_file_available(self, path: Path) -> bool:
        """Check if file is available locally.

        Args:
            path: Path to the file

        Returns:
            True if file exists and is readable
        """
        try:
            if not path.exists() or not path.is_file():
                return False
            size = path.stat().st_size
            return size > 0
        except (OSError, PermissionError):
            return False

    def _get_machine_id(self) -> str:
        """Get machine identifier for user_id.

        Returns:
            Machine ID string
        """
        import hashlib
        import platform

        # Create a hash of machine-specific info
        machine_info = f"{platform.node()}-{platform.machine()}"
        return hashlib.sha256(machine_info.encode()).hexdigest()[:32]

    def _extract_metadata(self, video) -> VideoMetadata:
        """Extract metadata from video.

        Args:
            video: Video object from candidate

        Returns:
            VideoMetadata object
        """
        try:
            metadata = self.metadata_manager.extract_metadata(video.path)
            metadata.albums = video.albums

            # Use Photos metadata as fallback
            if metadata.capture_date is None and video.capture_date:
                metadata.capture_date = video.capture_date
            if metadata.creation_date is None and video.creation_date:
                metadata.creation_date = video.creation_date
            if metadata.location is None and video.location:
                metadata.location = video.location

            return metadata
        except Exception as e:
            logger.warning(f"Failed to extract metadata: {e}")
            return VideoMetadata(
                capture_date=video.capture_date,
                creation_date=video.creation_date,
                albums=video.albums,
                location=video.location,
            )

    def _upload_file(
        self, local_path: Path, s3_key: str, total_size: int, display_name: str | None = None
    ) -> None:
        """Upload file to S3 with progress tracking.

        Args:
            local_path: Local file path
            s3_key: S3 key for upload
            total_size: Total file size in bytes
            display_name: Name to display in progress (defaults to local_path.name)
        """
        filename = display_name or local_path.name

        # Track cumulative bytes transferred
        # boto3 Callback receives incremental bytes, not cumulative
        bytes_uploaded = 0

        def progress_callback(bytes_amount):
            nonlocal bytes_uploaded
            bytes_uploaded += bytes_amount
            if self.progress_callback:
                self._report_progress(
                    filename,
                    int((bytes_uploaded / total_size) * 100),
                    bytes_uploaded,
                    total_size,
                )

        self.s3_client.upload_file(
            str(local_path),
            self.s3_bucket,
            s3_key,
            Callback=progress_callback,
        )

    def _upload_metadata(self, metadata: VideoMetadata, s3_key: str) -> None:
        """Upload metadata JSON to S3.

        Args:
            metadata: VideoMetadata object
            s3_key: S3 key for upload
        """
        metadata_dict = {
            "capture_date": metadata.capture_date.isoformat() if metadata.capture_date else None,
            "creation_date": metadata.creation_date.isoformat() if metadata.creation_date else None,
            "location": list(metadata.location) if metadata.location else None,
            "albums": metadata.albums or [],
        }

        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=json.dumps(metadata_dict, indent=2),
            ContentType="application/json",
        )

    def _submit_task(
        self,
        task_id: str,
        user_id: str,
        quality_preset: str,
        files: list[dict],
    ) -> dict:
        """Submit task to API Gateway.

        Args:
            task_id: Task ID
            user_id: User ID
            quality_preset: Quality preset
            files: List of file data

        Returns:
            API response
        """
        import requests
        from requests_aws4auth import AWS4Auth

        # Get credentials for signing
        credentials = self.session.get_credentials()
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            self.region,
            "execute-api",
            session_token=credentials.token,
        )

        # Prepare request body
        body = {
            "task_id": task_id,
            "user_id": user_id,
            "quality_preset": quality_preset,
            "files": files,
        }

        # Submit to API Gateway
        url = f"{self.api_url}/tasks"
        response = requests.post(url, json=body, auth=auth, timeout=30)
        response.raise_for_status()

        return response.json()

    def _cleanup_task_files(self, task_id: str) -> None:
        """Clean up uploaded files for a failed task.

        Args:
            task_id: Task ID
        """
        try:
            prefix = f"async/{task_id}/"
            paginator = self.s3_client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
                if "Contents" in page:
                    objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                    if objects:
                        self.s3_client.delete_objects(
                            Bucket=self.s3_bucket, Delete={"Objects": objects}
                        )
                        logger.info(f"Cleaned up {len(objects)} files for task {task_id}")
        except Exception as e:
            logger.warning(f"Failed to clean up files for task {task_id}: {e}")

    def _report_progress(
        self, filename: str, progress_percent: int, uploaded_bytes: int, total_bytes: int
    ) -> None:
        """Report upload progress.

        Args:
            filename: File being uploaded
            progress_percent: Progress percentage
            uploaded_bytes: Bytes uploaded
            total_bytes: Total bytes
        """
        if self.progress_callback:
            self.progress_callback(
                UploadProgress(
                    filename=filename,
                    progress_percent=progress_percent,
                    uploaded_bytes=uploaded_bytes,
                    total_bytes=total_bytes,
                )
            )
