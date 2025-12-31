"""Async cancel service for cancelling tasks.

This service handles:
1. Cancel running tasks
2. Stop Step Functions execution
3. Clean up S3 files

Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
"""

import logging
from dataclasses import dataclass

import boto3

logger = logging.getLogger(__name__)


@dataclass
class CancelResult:
    """Result of task cancellation."""

    task_id: str
    success: bool
    previous_status: str
    message: str
    s3_files_deleted: bool = False
    mediaconvert_cancelled: bool = False
    error_message: str | None = None


class CancelCommand:
    """Handler for cancel command.

    Cancels running tasks via API Gateway.

    Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
    """

    def __init__(
        self,
        api_url: str,
        region: str = "ap-northeast-1",
        profile_name: str | None = None,
    ):
        """Initialize CancelCommand.

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

    def cancel(self, task_id: str, user_id: str | None = None) -> CancelResult:
        """Cancel a running task.

        1. Stop Step Functions execution
        2. Cancel MediaConvert jobs (if running)
        3. Clean up S3 files
        4. Update task status to CANCELLED

        Args:
            task_id: Task ID to cancel
            user_id: User identifier (defaults to machine ID)

        Returns:
            CancelResult with cancellation details

        Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
        """
        if user_id is None:
            user_id = self._get_machine_id()

        try:
            response = self._call_api(
                method="POST",
                path=f"/tasks/{task_id}/cancel",
                body={"user_id": user_id},
            )

            return CancelResult(
                task_id=task_id,
                success=response.get("success", True),
                previous_status=response.get("previous_status", "UNKNOWN"),
                message=response.get("message", "Task cancelled successfully"),
                s3_files_deleted=response.get("s3_files_deleted", False),
                mediaconvert_cancelled=response.get("mediaconvert_cancelled", False),
            )

        except Exception as e:
            logger.exception(f"Failed to cancel task {task_id}")
            return CancelResult(
                task_id=task_id,
                success=False,
                previous_status="UNKNOWN",
                message="Failed to cancel task",
                error_message=str(e),
            )

    def _call_api(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        body: dict | None = None,
    ) -> dict:
        """Call API Gateway with AWS Signature V4.

        Args:
            method: HTTP method
            path: API path
            params: Query parameters
            body: Request body

        Returns:
            API response as dict
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

        url = f"{self.api_url}{path}"

        # Extract user_id from body for header
        user_id = None
        if body:
            user_id = body.pop("user_id", None)

        # Set headers with X-User-Id
        headers = {"Content-Type": "application/json"}
        if user_id:
            headers["X-User-Id"] = user_id

        response = requests.request(
            method=method,
            url=url,
            params=params,
            json=body if body else None,
            auth=auth,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()

        return response.json()

    def _get_machine_id(self) -> str:
        """Get machine identifier for user_id.

        Returns:
            Machine ID string
        """
        import hashlib
        import platform

        machine_info = f"{platform.node()}-{platform.machine()}"
        return hashlib.sha256(machine_info.encode()).hexdigest()[:32]
