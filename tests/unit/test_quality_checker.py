"""Unit tests for QualityChecker.

Tests quality check triggering, result parsing, and acceptance criteria.
Target coverage: 70%+ (エラーハンドリング)
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from vco.quality.checker import QualityChecker, QualityResult


class TestQualityResult:
    """Tests for QualityResult dataclass."""

    def test_default_values(self):
        """Test QualityResult with required values."""
        result = QualityResult(
            job_id="test-job",
            original_s3_key="original.mp4",
            converted_s3_key="converted.mp4",
            status="passed",
            ssim_score=0.98,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
        )

        assert result.job_id == "test-job"
        assert result.status == "passed"
        assert result.failure_reason is None
        assert result.converted_metadata is None
        assert result.timestamp is None
        assert result.result_s3_key is None

    def test_is_acceptable_passed(self):
        """Test is_acceptable returns True for passed status."""
        result = QualityResult(
            job_id="test",
            original_s3_key="orig.mp4",
            converted_s3_key="conv.mp4",
            status="passed",
            ssim_score=0.98,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
        )

        assert result.is_acceptable is True

    def test_is_acceptable_failed(self):
        """Test is_acceptable returns False for failed status."""
        result = QualityResult(
            job_id="test",
            original_s3_key="orig.mp4",
            converted_s3_key="conv.mp4",
            status="failed",
            ssim_score=0.80,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
            failure_reason="SSIM too low",
        )

        assert result.is_acceptable is False

    def test_is_acceptable_error(self):
        """Test is_acceptable returns False for error status."""
        result = QualityResult(
            job_id="test",
            original_s3_key="orig.mp4",
            converted_s3_key="conv.mp4",
            status="error",
            ssim_score=None,
            original_size=0,
            converted_size=0,
            compression_ratio=0.0,
            space_saved_bytes=0,
            space_saved_percent=0.0,
            playback_verified=False,
            failure_reason="Processing error",
        )

        assert result.is_acceptable is False

    def test_is_acceptable_pending(self):
        """Test is_acceptable returns False for pending status."""
        result = QualityResult(
            job_id="test",
            original_s3_key="orig.mp4",
            converted_s3_key="conv.mp4",
            status="pending",
            ssim_score=None,
            original_size=0,
            converted_size=0,
            compression_ratio=0.0,
            space_saved_bytes=0,
            space_saved_percent=0.0,
            playback_verified=False,
        )

        assert result.is_acceptable is False


class TestQualityCheckerInit:
    """Tests for QualityChecker initialization."""

    @patch("boto3.Session")
    def test_init_with_profile(self, mock_session):
        """Test initialization with AWS profile."""
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1",
            s3_bucket="test-bucket",
            lambda_function_name="test-function",
            profile_name="test-profile",
        )

        assert checker.region == "ap-northeast-1"
        assert checker.s3_bucket == "test-bucket"
        assert checker.lambda_function_name == "test-function"
        mock_session.assert_called_once_with(
            region_name="ap-northeast-1", profile_name="test-profile"
        )

    @patch("boto3.Session")
    def test_init_without_profile(self, mock_session):
        """Test initialization without AWS profile."""
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        _ = QualityChecker(region="us-east-1", s3_bucket="bucket", lambda_function_name="function")

        mock_session.assert_called_once_with(region_name="us-east-1")


class TestQualityCheckerTrigger:
    """Tests for QualityChecker trigger methods."""

    @patch("boto3.Session")
    def test_trigger_quality_check_success(self, mock_session):
        """Test trigger_quality_check returns job ID."""
        mock_lambda = MagicMock()
        mock_lambda.invoke.return_value = {"StatusCode": 202}

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_lambda
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.lambda_client = mock_lambda

        job_id = checker.trigger_quality_check(
            original_s3_key="original.mp4", converted_s3_key="converted.mp4"
        )

        assert job_id.startswith("qc_")
        mock_lambda.invoke.assert_called_once()

    @patch("boto3.Session")
    def test_trigger_quality_check_with_custom_job_id(self, mock_session):
        """Test trigger_quality_check with custom job ID."""
        mock_lambda = MagicMock()
        mock_lambda.invoke.return_value = {"StatusCode": 202}

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_lambda
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.lambda_client = mock_lambda

        job_id = checker.trigger_quality_check(
            original_s3_key="original.mp4", converted_s3_key="converted.mp4", job_id="custom-job-id"
        )

        assert job_id == "custom-job-id"

    @patch("boto3.Session")
    def test_trigger_quality_check_failure(self, mock_session):
        """Test trigger_quality_check raises error on failure."""
        mock_lambda = MagicMock()
        mock_lambda.invoke.return_value = {"StatusCode": 500}

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_lambda
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.lambda_client = mock_lambda

        with pytest.raises(RuntimeError, match="Lambda invocation failed"):
            checker.trigger_quality_check(
                original_s3_key="original.mp4", converted_s3_key="converted.mp4"
            )


class TestQualityCheckerSync:
    """Tests for QualityChecker synchronous methods."""

    @patch("boto3.Session")
    def test_trigger_quality_check_sync_success(self, mock_session):
        """Test trigger_quality_check_sync returns QualityResult."""
        response_body = {
            "statusCode": 200,
            "body": {
                "job_id": "test-job",
                "original_s3_key": "original.mp4",
                "converted_s3_key": "converted.mp4",
                "status": "passed",
                "ssim_score": 0.98,
                "original_size": 1000000,
                "converted_size": 500000,
                "compression_ratio": 2.0,
                "space_saved_bytes": 500000,
                "space_saved_percent": 50.0,
                "playback_verified": True,
            },
        }

        mock_payload = MagicMock()
        mock_payload.read.return_value = json.dumps(response_body)

        mock_lambda = MagicMock()
        mock_lambda.invoke.return_value = {"Payload": mock_payload}

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_lambda
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.lambda_client = mock_lambda

        result = checker.trigger_quality_check_sync(
            original_s3_key="original.mp4", converted_s3_key="converted.mp4"
        )

        assert isinstance(result, QualityResult)
        assert result.status == "passed"
        assert result.ssim_score == 0.98

    @patch("boto3.Session")
    def test_trigger_quality_check_sync_error(self, mock_session):
        """Test trigger_quality_check_sync handles error response."""
        response_body = {"statusCode": 500, "body": {"error": "Processing failed"}}

        mock_payload = MagicMock()
        mock_payload.read.return_value = json.dumps(response_body)

        mock_lambda = MagicMock()
        mock_lambda.invoke.return_value = {"Payload": mock_payload}

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_lambda
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.lambda_client = mock_lambda

        result = checker.trigger_quality_check_sync(
            original_s3_key="original.mp4", converted_s3_key="converted.mp4"
        )

        assert result.status == "error"
        assert result.failure_reason == "Processing failed"


class TestQualityCheckerResults:
    """Tests for QualityChecker result retrieval methods."""

    @patch("boto3.Session")
    def test_get_quality_result_found(self, mock_session):
        """Test get_quality_result returns result when found."""
        result_data = {
            "job_id": "test-job",
            "original_s3_key": "original.mp4",
            "converted_s3_key": "converted.mp4",
            "status": "passed",
            "ssim_score": 0.98,
            "original_size": 1000000,
            "converted_size": 500000,
            "compression_ratio": 2.0,
            "space_saved_bytes": 500000,
            "space_saved_percent": 50.0,
            "playback_verified": True,
        }

        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(result_data)

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": mock_body}

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.s3 = mock_s3

        result = checker.get_quality_result("test-job")

        assert result is not None
        assert result.job_id == "test-job"
        assert result.status == "passed"

    @patch("boto3.Session")
    def test_get_quality_result_not_found(self, mock_session):
        """Test get_quality_result returns None when not found."""
        mock_s3 = MagicMock()
        error_response = {"Error": {"Code": "NoSuchKey"}}
        mock_s3.get_object.side_effect = ClientError(error_response, "GetObject")

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.s3 = mock_s3

        result = checker.get_quality_result("nonexistent-job")

        assert result is None

    @patch("boto3.Session")
    def test_delete_result_success(self, mock_session):
        """Test delete_result returns True on success."""
        mock_s3 = MagicMock()
        mock_s3.delete_object.return_value = {}

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.s3 = mock_s3

        result = checker.delete_result("test-job")

        assert result is True
        mock_s3.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="results/test-job.json"
        )

    @patch("boto3.Session")
    def test_delete_result_failure(self, mock_session):
        """Test delete_result returns False on failure."""
        mock_s3 = MagicMock()
        error_response = {"Error": {"Code": "AccessDenied"}}
        mock_s3.delete_object.side_effect = ClientError(error_response, "DeleteObject")

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.s3 = mock_s3

        result = checker.delete_result("test-job")

        assert result is False


class TestQualityCheckerListResults:
    """Tests for QualityChecker list_results method."""

    @patch("boto3.Session")
    def test_list_results(self, mock_session):
        """Test list_results returns job IDs."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "results/job1.json"},
                    {"Key": "results/job2.json"},
                    {"Key": "results/job3.json"},
                ]
            }
        ]

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.s3 = mock_s3

        job_ids = checker.list_results()

        assert job_ids == ["job1", "job2", "job3"]

    @patch("boto3.Session")
    def test_list_results_empty(self, mock_session):
        """Test list_results returns empty list when no results."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]

        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator

        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )
        checker.s3 = mock_s3

        job_ids = checker.list_results()

        assert job_ids == []


class TestIsQualityAcceptable:
    """Tests for is_quality_acceptable static method."""

    def test_acceptable_quality(self):
        """Test acceptable quality returns True."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.98, original_size=1000000, converted_size=500000
        )

        assert is_acceptable is True
        assert reason is None

    def test_ssim_at_threshold(self):
        """Test SSIM exactly at threshold is acceptable."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.95, original_size=1000000, converted_size=500000
        )

        assert is_acceptable is True
        assert reason is None

    def test_ssim_below_threshold(self):
        """Test SSIM below threshold is rejected."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.90, original_size=1000000, converted_size=500000
        )

        assert is_acceptable is False
        assert "SSIM score" in reason
        assert "below threshold" in reason

    def test_ssim_none(self):
        """Test None SSIM is rejected."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=None, original_size=1000000, converted_size=500000
        )

        assert is_acceptable is False
        assert "not available" in reason

    def test_converted_larger_than_original(self):
        """Test converted file larger than original is rejected."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.98, original_size=500000, converted_size=600000
        )

        assert is_acceptable is False
        assert "not smaller" in reason

    def test_converted_equal_to_original(self):
        """Test converted file equal to original is rejected."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.98, original_size=500000, converted_size=500000
        )

        assert is_acceptable is False
        assert "not smaller" in reason

    def test_custom_ssim_threshold(self):
        """Test custom SSIM threshold."""
        # Should pass with lower threshold
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.90, original_size=1000000, converted_size=500000, ssim_threshold=0.85
        )

        assert is_acceptable is True
        assert reason is None

        # Should fail with higher threshold
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.90, original_size=1000000, converted_size=500000, ssim_threshold=0.95
        )

        assert is_acceptable is False


class TestQualityCheckerParseResult:
    """Tests for QualityChecker _parse_result method."""

    @patch("boto3.Session")
    def test_parse_result_complete(self, mock_session):
        """Test _parse_result with complete data."""
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )

        data = {
            "job_id": "test-job",
            "original_s3_key": "original.mp4",
            "converted_s3_key": "converted.mp4",
            "status": "passed",
            "ssim_score": 0.98,
            "original_size": 1000000,
            "converted_size": 500000,
            "compression_ratio": 2.0,
            "space_saved_bytes": 500000,
            "space_saved_percent": 50.0,
            "playback_verified": True,
            "failure_reason": None,
            "converted_metadata": {"codec": "hevc"},
            "timestamp": "2024-06-15T10:30:00Z",
            "result_s3_key": "results/test-job.json",
        }

        result = checker._parse_result(data)

        assert result.job_id == "test-job"
        assert result.status == "passed"
        assert result.ssim_score == 0.98
        assert result.converted_metadata == {"codec": "hevc"}
        assert result.timestamp == "2024-06-15T10:30:00Z"

    @patch("boto3.Session")
    def test_parse_result_minimal(self, mock_session):
        """Test _parse_result with minimal data."""
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        checker = QualityChecker(
            region="ap-northeast-1", s3_bucket="test-bucket", lambda_function_name="test-function"
        )

        data = {}

        result = checker._parse_result(data)

        assert result.job_id == ""
        assert result.status == "unknown"
        assert result.ssim_score is None
        assert result.original_size == 0
        assert result.playback_verified is False
