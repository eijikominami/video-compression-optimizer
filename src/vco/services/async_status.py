"""Async status service for checking task status.

This service handles:
1. List active tasks
2. Get task details
3. Display progress information

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import boto3

logger = logging.getLogger(__name__)


@dataclass
class TaskSummary:
    """Summary of a task for list display."""

    task_id: str
    status: str
    file_count: int
    completed_count: int
    failed_count: int
    progress_percentage: int
    created_at: datetime
    quality_preset: str


@dataclass
class FileDetail:
    """Detail of a file in a task."""

    file_id: str
    filename: str
    status: str
    progress_percentage: int
    error_message: str | None = None
    ssim_score: float | None = None
    original_size_bytes: int | None = None
    output_size_bytes: int | None = None
    output_s3_key: str | None = None
    compression_ratio: float | None = None
    space_saved_bytes: int | None = None
    space_saved_percent: float | None = None


@dataclass
class TaskDetail:
    """Detailed task information."""

    task_id: str
    status: str
    quality_preset: str
    files: list[FileDetail]
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    progress_percentage: int = 0
    current_step: str | None = None
    estimated_completion_time: datetime | None = None
    error_message: str | None = None
    execution_arn: str | None = None


class StatusCommand:
    """Handler for status command.

    Retrieves task status from API Gateway.

    Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
    """

    def __init__(
        self,
        api_url: str,
        region: str = "ap-northeast-1",
        profile_name: str | None = None,
    ):
        """Initialize StatusCommand.

        Args:
            api_url: API Gateway URL for async workflow
            region: AWS region
            profile_name: AWS profile name (optional)
        """
        self.api_url = api_url.rstrip("/")
        self.region = region
        self.profile_name = profile_name

        # Initialize AWS session for signing
        self.session = boto3.Session(profile_name=profile_name, region_name=region)

    def list_tasks(
        self,
        status_filter: str | None = None,
        limit: int = 20,
        user_id: str | None = None,
    ) -> list[TaskSummary]:
        """List tasks for the user.

        Args:
            status_filter: Filter by status (optional)
            limit: Maximum number of tasks to return
            user_id: User identifier (defaults to machine ID)

        Returns:
            List of TaskSummary objects

        Requirements: 2.1
        """
        if user_id is None:
            user_id = self._get_machine_id()

        try:
            response = self._call_api(
                method="GET",
                path="/tasks",
                params={
                    "user_id": user_id,
                    "status": status_filter,
                    "limit": str(limit),
                },
            )

            tasks = []
            for item in response.get("tasks", []):
                tasks.append(
                    TaskSummary(
                        task_id=item["task_id"],
                        status=item["status"],
                        file_count=item.get("file_count", 0),
                        completed_count=item.get("completed_count", 0),
                        failed_count=item.get("failed_count", 0),
                        progress_percentage=item.get("progress_percentage", 0),
                        created_at=datetime.fromisoformat(item["created_at"]),
                        quality_preset=item.get("quality_preset", "balanced"),
                    )
                )

            return tasks

        except Exception as e:
            logger.warning(f"Failed to list tasks: {e}")
            raise RuntimeError(f"Failed to list tasks: {e}") from e

    def get_task_detail(self, task_id: str, user_id: str | None = None) -> TaskDetail:
        """Get detailed task information.

        Args:
            task_id: Task ID
            user_id: User identifier (defaults to machine ID)

        Returns:
            TaskDetail object

        Requirements: 2.2, 2.3, 2.4, 2.5
        """
        if user_id is None:
            user_id = self._get_machine_id()

        try:
            response = self._call_api(
                method="GET",
                path=f"/tasks/{task_id}",
                params={"user_id": user_id},
            )

            # Parse files
            files = []
            for file_data in response.get("files", []):
                quality_result = file_data.get("quality_result", {})
                files.append(
                    FileDetail(
                        file_id=file_data["file_id"],
                        filename=file_data["filename"],
                        status=file_data["status"],
                        progress_percentage=file_data.get("conversion_progress_percentage", 0),
                        error_message=file_data.get("error_message"),
                        ssim_score=quality_result.get("ssim_score"),
                        original_size_bytes=quality_result.get("original_size"),
                        output_size_bytes=file_data.get("output_size_bytes")
                        or quality_result.get("converted_size"),
                        output_s3_key=file_data.get("output_s3_key"),
                        compression_ratio=quality_result.get("compression_ratio"),
                        space_saved_bytes=quality_result.get("space_saved_bytes"),
                        space_saved_percent=quality_result.get("space_saved_percent"),
                    )
                )

            # Parse timestamps
            created_at = datetime.fromisoformat(response["created_at"])
            updated_at = datetime.fromisoformat(response["updated_at"])
            started_at = (
                datetime.fromisoformat(response["started_at"])
                if response.get("started_at")
                else None
            )
            completed_at = (
                datetime.fromisoformat(response["completed_at"])
                if response.get("completed_at")
                else None
            )
            estimated_completion = (
                datetime.fromisoformat(response["estimated_completion_time"])
                if response.get("estimated_completion_time")
                else None
            )

            return TaskDetail(
                task_id=response["task_id"],
                status=response["status"],
                quality_preset=response.get("quality_preset", "balanced"),
                files=files,
                created_at=created_at,
                updated_at=updated_at,
                started_at=started_at,
                completed_at=completed_at,
                progress_percentage=response.get("progress_percentage", 0),
                current_step=response.get("current_step"),
                estimated_completion_time=estimated_completion,
                error_message=response.get("error_message"),
                execution_arn=response.get("execution_arn"),
            )

        except Exception as e:
            # Extract user-friendly message
            error_msg = str(e)
            if "404" in error_msg:
                error_msg = f"Task not found: {task_id}"
            elif "403" in error_msg or "Forbidden" in error_msg:
                error_msg = "Access denied. Check your AWS credentials."
            elif "ExpiredToken" in error_msg:
                error_msg = "AWS credentials have expired. Please refresh your credentials."

            logger.warning(f"Failed to get task detail for {task_id}: {error_msg}")
            raise RuntimeError(f"Failed to get task detail: {error_msg}") from e

    def _call_api(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Call API Gateway with AWS Signature V4.

        Args:
            method: HTTP method
            path: API path
            params: Query parameters
            body: Request body

        Returns:
            API response as dict
        """
        import requests  # type: ignore[import-untyped]
        from requests_aws4auth import AWS4Auth  # type: ignore[import-untyped]

        # Get credentials for signing
        credentials = self.session.get_credentials()
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            self.region,
            "execute-api",
            session_token=credentials.token,
        )

        url = f"{self.api_url}{path}"

        # Extract user_id from params for header
        user_id = None
        if params:
            user_id = params.pop("user_id", None)
            params = {k: v for k, v in params.items() if v is not None}

        # Set headers with X-User-Id
        headers = {"Content-Type": "application/json"}
        if user_id:
            headers["X-User-Id"] = user_id

        response = requests.request(
            method=method,
            url=url,
            params=params if params else None,
            json=body,
            auth=auth,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()

        result: dict[str, Any] = response.json()
        return result

    def update_file_status_to_downloaded(
        self,
        task_id: str,
        file_id: str,
        user_id: str | None = None,
    ) -> bool:
        """Update file status to DOWNLOADED after successful download.

        Args:
            task_id: Task ID
            file_id: File ID
            user_id: User identifier (defaults to machine ID)

        Returns:
            True if update succeeded, False otherwise

        Requirements: 4.4
        """
        if user_id is None:
            user_id = self._get_machine_id()

        try:
            self._call_api(
                method="POST",
                path=f"/tasks/{task_id}/download-status",
                params={"user_id": user_id},
                body={"file_id": file_id},
            )
            logger.info(f"Updated file status to DOWNLOADED: {task_id}:{file_id}")
            return True
        except Exception as e:
            logger.warning(f"Failed to update file status for {task_id}:{file_id}: {e}")
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
