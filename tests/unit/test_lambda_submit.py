"""Unit tests for async-task-submit Lambda function.

Tests: Task 5.5 - Submit Lambda unit tests
Requirements: 1.1, 1.2, 1.3, 3.3, 3.5
"""

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Load the Lambda module directly using importlib
_lambda_path = os.path.join(os.path.dirname(__file__), "../../sam-app/async-task-submit/app.py")
_spec = importlib.util.spec_from_file_location("submit_app", _lambda_path)
submit_app = importlib.util.module_from_spec(_spec)
sys.modules["submit_app"] = submit_app
_spec.loader.exec_module(submit_app)


class TestValidateRequest:
    """Tests for validate_request function."""

    def test_valid_request_minimal(self):
        """Valid request with minimal required fields."""
        body = {
            "user_id": "user-123",
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is True
        assert error is None
        assert error_code is None

    def test_valid_request_with_preset(self):
        """Valid request with quality preset."""
        body = {
            "user_id": "user-123",
            "quality_preset": "high+",
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is True
        assert error is None

    def test_missing_user_id(self):
        """Missing user_id returns error."""
        body = {"files": [{"filename": "video.mov", "original_uuid": "ABC-123"}]}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "user_id" in error
        assert error_code == "MISSING_FIELD"

    def test_missing_files(self):
        """Missing files returns error."""
        body = {"user_id": "user-123"}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "files" in error
        assert error_code == "MISSING_FIELD"

    def test_empty_files_list(self):
        """Empty files list returns error."""
        body = {"user_id": "user-123", "files": []}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "At least one file" in error
        assert error_code == "INVALID_REQUEST"

    def test_files_not_list(self):
        """files not a list returns error."""
        body = {"user_id": "user-123", "files": "not-a-list"}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "must be a list" in error
        assert error_code == "INVALID_FORMAT"

    def test_file_not_dict(self):
        """File entry not a dict returns error."""
        body = {"user_id": "user-123", "files": ["not-a-dict"]}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "must be an object" in error
        assert error_code == "INVALID_FORMAT"

    def test_file_missing_filename(self):
        """File missing filename returns error."""
        body = {"user_id": "user-123", "files": [{"original_uuid": "ABC-123"}]}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "missing filename" in error
        assert error_code == "MISSING_FIELD"

    def test_file_missing_original_uuid(self):
        """File missing original_uuid returns error."""
        body = {"user_id": "user-123", "files": [{"filename": "video.mov"}]}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "missing original_uuid" in error
        assert error_code == "MISSING_FIELD"

    def test_invalid_quality_preset(self):
        """Invalid quality preset returns error."""
        body = {
            "user_id": "user-123",
            "quality_preset": "invalid",
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "Invalid quality_preset" in error
        assert error_code == "INVALID_REQUEST"

    @pytest.mark.parametrize("preset", ["balanced", "high", "compression", "balanced+", "high+"])
    def test_valid_quality_presets(self, preset):
        """All valid quality presets are accepted."""
        body = {
            "user_id": "user-123",
            "quality_preset": preset,
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is True
        assert error is None

    def test_multiple_files(self):
        """Multiple files are validated."""
        body = {
            "user_id": "user-123",
            "files": [
                {"filename": "video1.mov", "original_uuid": "ABC-123"},
                {"filename": "video2.mp4", "original_uuid": "DEF-456"},
                {"filename": "video3.avi", "original_uuid": "GHI-789"},
            ],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is True
        assert error is None


class TestValidateRequestEnhanced:
    """Enhanced validation tests for Task 4.1 (Requirements: 3.3, 3.5)."""

    def test_empty_user_id(self):
        """Empty user_id returns error."""
        body = {
            "user_id": "",
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "non-empty string" in error
        assert error_code == "INVALID_FORMAT"

    def test_whitespace_user_id(self):
        """Whitespace-only user_id returns error."""
        body = {
            "user_id": "   ",
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "non-empty string" in error

    def test_empty_filename(self):
        """Empty filename returns error."""
        body = {
            "user_id": "user-123",
            "files": [{"filename": "", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "non-empty string" in error
        assert error_code == "INVALID_FORMAT"

    def test_filename_too_long(self):
        """Filename exceeding max length returns error."""
        body = {
            "user_id": "user-123",
            "files": [{"filename": "a" * 300, "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "exceeds" in error
        assert error_code == "INVALID_FORMAT"

    def test_empty_original_uuid(self):
        """Empty original_uuid returns error."""
        body = {
            "user_id": "user-123",
            "files": [{"filename": "video.mov", "original_uuid": ""}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "non-empty string" in error

    def test_invalid_task_id_format(self):
        """Invalid task_id format returns error."""
        body = {
            "user_id": "user-123",
            "task_id": "not-a-uuid",
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "valid UUID" in error
        assert error_code == "INVALID_FORMAT"

    def test_valid_task_id_format(self):
        """Valid task_id format is accepted."""
        body = {
            "user_id": "user-123",
            "task_id": "12345678-1234-1234-1234-123456789abc",
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is True

    def test_invalid_file_id_format(self):
        """Invalid file_id format returns error."""
        body = {
            "user_id": "user-123",
            "files": [{"filename": "video.mov", "original_uuid": "ABC-123", "file_id": "invalid"}],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "valid UUID" in error

    def test_valid_file_id_format(self):
        """Valid file_id format is accepted."""
        body = {
            "user_id": "user-123",
            "files": [
                {
                    "filename": "video.mov",
                    "original_uuid": "ABC-123",
                    "file_id": "12345678-1234-1234-1234-123456789abc",
                }
            ],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is True

    def test_negative_source_size_bytes(self):
        """Negative source_size_bytes returns error."""
        body = {
            "user_id": "user-123",
            "files": [
                {"filename": "video.mov", "original_uuid": "ABC-123", "source_size_bytes": -100}
            ],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "non-negative integer" in error

    def test_valid_source_size_bytes(self):
        """Valid source_size_bytes is accepted."""
        body = {
            "user_id": "user-123",
            "files": [
                {"filename": "video.mov", "original_uuid": "ABC-123", "source_size_bytes": 1000000}
            ],
        }
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is True

    def test_too_many_files(self):
        """Too many files returns error."""
        files = [{"filename": f"video{i}.mov", "original_uuid": f"UUID-{i}"} for i in range(101)]
        body = {"user_id": "user-123", "files": files}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is False
        assert "Maximum" in error
        assert error_code == "INVALID_REQUEST"

    def test_max_files_allowed(self):
        """Maximum allowed files is accepted."""
        files = [{"filename": f"video{i}.mov", "original_uuid": f"UUID-{i}"} for i in range(100)]
        body = {"user_id": "user-123", "files": files}
        is_valid, error, error_code = submit_app.validate_request(body)
        assert is_valid is True


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv(
            "STATE_MACHINE_ARN", "arn:aws:states:us-east-1:123456789012:stateMachine:test"
        )
        monkeypatch.setenv("PRESIGNED_URL_EXPIRATION", "3600")
        monkeypatch.setenv("TTL_DAYS", "90")

    @patch.object(submit_app, "get_dynamodb_table")
    @patch.object(submit_app, "get_s3_client")
    @patch.object(submit_app, "get_sfn_client")
    def test_successful_submission(self, mock_sfn, mock_s3, mock_dynamodb):
        """Successful task submission."""
        # Mock DynamoDB
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table

        # Mock S3
        mock_s3_client = MagicMock()
        mock_s3_client.generate_presigned_url.return_value = "https://presigned-url"
        mock_s3.return_value = mock_s3_client

        # Mock Step Functions
        mock_sfn_client = MagicMock()
        mock_sfn_client.start_execution.return_value = {
            "executionArn": "arn:aws:states:us-east-1:123456789012:execution:test:exec-123"
        }
        mock_sfn.return_value = mock_sfn_client

        event = {
            "body": json.dumps(
                {
                    "user_id": "user-123",
                    "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
                }
            )
        }

        response = submit_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "task_id" in body
        assert body["status"] == "PENDING"
        assert len(body["upload_urls"]) == 1
        assert "upload_url" in body["upload_urls"][0]

    @patch.object(submit_app, "get_dynamodb_table")
    @patch.object(submit_app, "get_s3_client")
    @patch.object(submit_app, "get_sfn_client")
    def test_submission_with_dict_body(self, mock_sfn, mock_s3, mock_dynamodb):
        """Submission with body as dict (not JSON string)."""
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table

        mock_s3_client = MagicMock()
        mock_s3_client.generate_presigned_url.return_value = "https://presigned-url"
        mock_s3.return_value = mock_s3_client

        mock_sfn_client = MagicMock()
        mock_sfn_client.start_execution.return_value = {
            "executionArn": "arn:aws:states:us-east-1:123456789012:execution:test:exec-123"
        }
        mock_sfn.return_value = mock_sfn_client

        # Body as dict, not JSON string
        event = {
            "body": {
                "user_id": "user-123",
                "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
            }
        }

        response = submit_app.lambda_handler(event, None)
        assert response["statusCode"] == 200

    def test_validation_error(self):
        """Validation error returns 400."""
        event = {"body": json.dumps({"user_id": "user-123", "files": []})}

        response = submit_app.lambda_handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "error" in body

    @patch.object(submit_app, "get_dynamodb_table")
    @patch.object(submit_app, "get_s3_client")
    @patch.object(submit_app, "get_sfn_client")
    def test_dynamodb_error(self, mock_sfn, mock_s3, mock_dynamodb):
        """DynamoDB error returns 500."""
        from botocore.exceptions import ClientError

        mock_table = MagicMock()
        mock_table.put_item.side_effect = ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "Test error"}},
            "PutItem",
        )
        mock_dynamodb.return_value = mock_table

        mock_s3_client = MagicMock()
        mock_s3_client.generate_presigned_url.return_value = "https://presigned-url"
        mock_s3.return_value = mock_s3_client

        event = {
            "body": json.dumps(
                {
                    "user_id": "user-123",
                    "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
                }
            )
        }

        response = submit_app.lambda_handler(event, None)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body

    @patch.object(submit_app, "get_dynamodb_table")
    @patch.object(submit_app, "get_s3_client")
    @patch.object(submit_app, "get_sfn_client")
    def test_multiple_files_submission(self, mock_sfn, mock_s3, mock_dynamodb):
        """Multiple files generate multiple upload URLs."""
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table

        mock_s3_client = MagicMock()
        mock_s3_client.generate_presigned_url.return_value = "https://presigned-url"
        mock_s3.return_value = mock_s3_client

        mock_sfn_client = MagicMock()
        mock_sfn_client.start_execution.return_value = {
            "executionArn": "arn:aws:states:us-east-1:123456789012:execution:test:exec-123"
        }
        mock_sfn.return_value = mock_sfn_client

        event = {
            "body": json.dumps(
                {
                    "user_id": "user-123",
                    "files": [
                        {"filename": "video1.mov", "original_uuid": "ABC-123"},
                        {"filename": "video2.mp4", "original_uuid": "DEF-456"},
                    ],
                }
            )
        }

        response = submit_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert len(body["upload_urls"]) == 2

    @patch.object(submit_app, "get_dynamodb_table")
    @patch.object(submit_app, "get_s3_client")
    @patch.object(submit_app, "get_sfn_client")
    def test_quality_preset_passed_through(self, mock_sfn, mock_s3, mock_dynamodb):
        """Quality preset is included in response."""
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table

        mock_s3_client = MagicMock()
        mock_s3_client.generate_presigned_url.return_value = "https://presigned-url"
        mock_s3.return_value = mock_s3_client

        mock_sfn_client = MagicMock()
        mock_sfn_client.start_execution.return_value = {
            "executionArn": "arn:aws:states:us-east-1:123456789012:execution:test:exec-123"
        }
        mock_sfn.return_value = mock_sfn_client

        event = {
            "body": json.dumps(
                {
                    "user_id": "user-123",
                    "quality_preset": "high+",
                    "files": [{"filename": "video.mov", "original_uuid": "ABC-123"}],
                }
            )
        }

        response = submit_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["quality_preset"] == "high+"
