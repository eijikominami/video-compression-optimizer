"""Quality checker client for Lambda-based video quality validation.

This module provides a client interface to trigger and retrieve results from
the quality-checker Lambda function deployed in AWS.
"""

import json
import time
import uuid
from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


@dataclass
class QualityResult:
    """Quality check result from Lambda function."""

    job_id: str
    original_s3_key: str
    converted_s3_key: str
    status: str  # 'passed', 'failed', 'pending', 'error'
    ssim_score: float | None
    original_size: int
    converted_size: int
    compression_ratio: float
    space_saved_bytes: int
    space_saved_percent: float
    playback_verified: bool
    failure_reason: str | None = None
    converted_metadata: dict | None = None
    metadata_embedded: bool = False
    metadata_embed_error: str | None = None
    timestamp: str | None = None
    result_s3_key: str | None = None

    @property
    def is_acceptable(self) -> bool:
        """Check if the quality result meets acceptance criteria."""
        return self.status == "passed"


class QualityChecker:
    """Client for Lambda-based video quality validation.

    This class provides methods to:
    1. Trigger quality checks via Lambda invocation
    2. Retrieve quality check results from S3
    3. Poll for completion of async quality checks
    """

    SSIM_THRESHOLD = 0.95

    def __init__(
        self,
        region: str,
        s3_bucket: str,
        lambda_function_name: str,
        profile_name: str | None = None,
    ):
        """Initialize QualityChecker.

        Args:
            region: AWS region
            s3_bucket: S3 bucket for video files and results
            lambda_function_name: Name of the quality-checker Lambda function
            profile_name: AWS profile name (optional)
        """
        self.region = region
        self.s3_bucket = s3_bucket
        self.lambda_function_name = lambda_function_name

        # Create boto3 session
        session_kwargs = {"region_name": region}
        if profile_name:
            session_kwargs["profile_name"] = profile_name
        self.session = boto3.Session(**session_kwargs)

        # Create clients
        # Lambda client with extended timeout for long-running quality checks
        # Lambda function can run up to 15 minutes, so we set read_timeout to 900 seconds
        lambda_config = Config(
            read_timeout=900,  # 15 minutes to match Lambda max timeout
            connect_timeout=10,
            retries={"max_attempts": 0},  # Don't retry on timeout
        )
        self.lambda_client = self.session.client("lambda", config=lambda_config)
        self.s3 = self.session.client("s3")

    def trigger_quality_check(
        self,
        original_s3_key: str,
        converted_s3_key: str,
        job_id: str | None = None,
        metadata_s3_key: str | None = None,
    ) -> str:
        """Trigger a quality check via Lambda invocation.

        Args:
            original_s3_key: S3 key of the original video
            converted_s3_key: S3 key of the converted video
            job_id: Optional job ID (generated if not provided)
            metadata_s3_key: Optional S3 key of metadata JSON for embedding

        Returns:
            Job ID for tracking the quality check
        """
        if job_id is None:
            job_id = f"qc_{uuid.uuid4().hex[:12]}"

        payload = {
            "job_id": job_id,
            "original_s3_key": original_s3_key,
            "converted_s3_key": converted_s3_key,
        }

        if metadata_s3_key:
            payload["metadata_s3_key"] = metadata_s3_key

        response = self.lambda_client.invoke(
            FunctionName=self.lambda_function_name,
            InvocationType="Event",  # Async invocation
            Payload=json.dumps(payload),
        )

        # Check for invocation errors
        if response.get("StatusCode") not in (200, 202):
            raise RuntimeError(f"Lambda invocation failed with status {response.get('StatusCode')}")

        return job_id

    def trigger_quality_check_sync(
        self,
        original_s3_key: str,
        converted_s3_key: str,
        job_id: str | None = None,
        metadata_s3_key: str | None = None,
    ) -> QualityResult:
        """Trigger a quality check and wait for result (synchronous).

        Args:
            original_s3_key: S3 key of the original video
            converted_s3_key: S3 key of the converted video
            job_id: Optional job ID (generated if not provided)
            metadata_s3_key: Optional S3 key of metadata JSON for embedding

        Returns:
            QualityResult with check results
        """
        if job_id is None:
            job_id = f"qc_{uuid.uuid4().hex[:12]}"

        payload = {
            "job_id": job_id,
            "original_s3_key": original_s3_key,
            "converted_s3_key": converted_s3_key,
        }

        if metadata_s3_key:
            payload["metadata_s3_key"] = metadata_s3_key

        response = self.lambda_client.invoke(
            FunctionName=self.lambda_function_name,
            InvocationType="RequestResponse",  # Sync invocation
            Payload=json.dumps(payload),
        )

        # Parse response
        response_payload = json.loads(response["Payload"].read())

        if response_payload.get("statusCode") != 200:
            error_msg = response_payload.get("body", {}).get("error", "Unknown error")
            return QualityResult(
                job_id=job_id,
                original_s3_key=original_s3_key,
                converted_s3_key=converted_s3_key,
                status="error",
                ssim_score=None,
                original_size=0,
                converted_size=0,
                compression_ratio=0.0,
                space_saved_bytes=0,
                space_saved_percent=0.0,
                playback_verified=False,
                failure_reason=error_msg,
            )

        body = response_payload.get("body", {})
        return self._parse_result(body)

    def get_quality_result(self, job_id: str) -> QualityResult | None:
        """Get quality check result from S3.

        Args:
            job_id: Job ID of the quality check

        Returns:
            QualityResult if found, None if not yet available
        """
        result_key = f"results/{job_id}.json"

        try:
            response = self.s3.get_object(Bucket=self.s3_bucket, Key=result_key)
            result_data = json.loads(response["Body"].read())
            result_data["result_s3_key"] = result_key
            return self._parse_result(result_data)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def wait_for_result(
        self, job_id: str, poll_interval: int = 5, timeout: int = 1800
    ) -> QualityResult:
        """Wait for a quality check to complete.

        Args:
            job_id: Job ID of the quality check
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait

        Returns:
            QualityResult with final status

        Raises:
            TimeoutError: If check doesn't complete within timeout
        """
        start_time = time.time()

        while True:
            result = self.get_quality_result(job_id)

            if result is not None:
                return result

            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise TimeoutError(
                    f"Quality check {job_id} did not complete within {timeout} seconds"
                )

            time.sleep(poll_interval)

    def delete_result(self, job_id: str) -> bool:
        """Delete a quality check result from S3.

        Args:
            job_id: Job ID of the quality check

        Returns:
            True if successful
        """
        result_key = f"results/{job_id}.json"

        try:
            self.s3.delete_object(Bucket=self.s3_bucket, Key=result_key)
            return True
        except ClientError:
            return False

    def list_results(self, prefix: str = "results/") -> list[str]:
        """List all quality check result job IDs.

        Args:
            prefix: S3 prefix for results (default: "results/")

        Returns:
            List of job IDs
        """
        job_ids = []

        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".json"):
                    # Extract job_id from key (results/job_id.json)
                    job_id = key[len(prefix) : -5]  # Remove prefix and .json
                    job_ids.append(job_id)

        return job_ids

    def _parse_result(self, data: dict) -> QualityResult:
        """Parse quality result from dictionary.

        Args:
            data: Result data dictionary

        Returns:
            QualityResult object
        """
        return QualityResult(
            job_id=data.get("job_id", ""),
            original_s3_key=data.get("original_s3_key", ""),
            converted_s3_key=data.get("converted_s3_key", ""),
            status=data.get("status", "unknown"),
            ssim_score=data.get("ssim_score"),
            original_size=data.get("original_size", 0),
            converted_size=data.get("converted_size", 0),
            compression_ratio=data.get("compression_ratio", 0.0),
            space_saved_bytes=data.get("space_saved_bytes", 0),
            space_saved_percent=data.get("space_saved_percent", 0.0),
            playback_verified=data.get("playback_verified", False),
            failure_reason=data.get("failure_reason"),
            converted_metadata=data.get("converted_metadata"),
            metadata_embedded=data.get("metadata_embedded", False),
            metadata_embed_error=data.get("metadata_embed_error"),
            timestamp=data.get("timestamp"),
            result_s3_key=data.get("result_s3_key"),
        )

    @staticmethod
    def is_quality_acceptable(
        ssim_score: float | None,
        original_size: int,
        converted_size: int,
        ssim_threshold: float = SSIM_THRESHOLD,
    ) -> tuple[bool, str | None]:
        """Check if quality metrics meet acceptance criteria.

        Args:
            ssim_score: SSIM score (0.0 to 1.0)
            original_size: Original file size in bytes
            converted_size: Converted file size in bytes
            ssim_threshold: Minimum acceptable SSIM score

        Returns:
            Tuple of (is_acceptable, rejection_reason)
        """
        # Check file size reduction
        if converted_size >= original_size:
            return False, "Converted file is not smaller than original"

        # Check SSIM score
        if ssim_score is None:
            return False, "SSIM score not available"

        if ssim_score < ssim_threshold:
            return False, f"SSIM score {ssim_score:.4f} is below threshold {ssim_threshold}"

        return True, None
