"""Unit tests for async-task-status Lambda function.

Tests: Task 5.5 - Status Lambda unit tests
Requirements: 2.1, 2.2, 2.3
"""

import importlib.util
import json
import os
import sys
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

# Load the Lambda module directly using importlib
_lambda_path = os.path.join(os.path.dirname(__file__), "../../sam-app/async-task-status/app.py")
_spec = importlib.util.spec_from_file_location("status_app", _lambda_path)
status_app = importlib.util.module_from_spec(_spec)
sys.modules["status_app"] = status_app
_spec.loader.exec_module(status_app)


class TestGetTask:
    """Tests for get_task function."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

    @patch.object(status_app, "get_dynamodb_table")
    def test_get_task_success(self, mock_dynamodb):
        """Successfully get task by ID."""
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "task_id": "task-123",
                "user_id": "user-123",
                "status": "PENDING",
            }
        }
        mock_dynamodb.return_value = mock_table

        result = status_app.get_task("task-123", "user-123")

        assert result is not None
        assert result["task_id"] == "task-123"

    @patch.object(status_app, "get_dynamodb_table")
    def test_get_task_not_found(self, mock_dynamodb):
        """Task not found returns None."""
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_dynamodb.return_value = mock_table

        result = status_app.get_task("nonexistent", "user-123")

        assert result is None

    @patch.object(status_app, "get_dynamodb_table")
    def test_get_task_wrong_user(self, mock_dynamodb):
        """Task owned by different user returns None."""
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "task_id": "task-123",
                "user_id": "other-user",
                "status": "PENDING",
            }
        }
        mock_dynamodb.return_value = mock_table

        result = status_app.get_task("task-123", "user-123")

        assert result is None


class TestListTasks:
    """Tests for list_tasks function."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

    @patch.object(status_app, "get_dynamodb_table")
    def test_list_tasks_no_filter(self, mock_dynamodb):
        """List tasks without status filter."""
        mock_table = MagicMock()
        mock_table.query.return_value = {
            "Items": [
                {"task_id": "task-1", "status": "PENDING"},
                {"task_id": "task-2", "status": "COMPLETED"},
            ]
        }
        mock_dynamodb.return_value = mock_table

        result = status_app.list_tasks("user-123")

        assert len(result) == 2
        mock_table.query.assert_called_once()
        # Should use GSI1 for user-based query
        call_kwargs = mock_table.query.call_args[1]
        assert call_kwargs["IndexName"] == "GSI1-UserTasks"

    @patch.object(status_app, "get_dynamodb_table")
    def test_list_tasks_with_status_filter(self, mock_dynamodb):
        """List tasks with status filter."""
        mock_table = MagicMock()
        mock_table.query.return_value = {"Items": [{"task_id": "task-1", "status": "PENDING"}]}
        mock_dynamodb.return_value = mock_table

        result = status_app.list_tasks("user-123", status_filter="PENDING")

        assert len(result) == 1
        # Should use GSI2 for status-based query
        call_kwargs = mock_table.query.call_args[1]
        assert call_kwargs["IndexName"] == "GSI2-StatusTasks"

    @patch.object(status_app, "get_dynamodb_table")
    def test_list_tasks_empty(self, mock_dynamodb):
        """List tasks returns empty list when no tasks."""
        mock_table = MagicMock()
        mock_table.query.return_value = {"Items": []}
        mock_dynamodb.return_value = mock_table

        result = status_app.list_tasks("user-123")

        assert result == []


class TestFormatTaskResponse:
    """Tests for format_task_response function."""

    def test_format_basic_task(self):
        """Format basic task response."""
        task = {
            "task_id": "task-123",
            "status": "PENDING",
            "quality_preset": "balanced",
            "progress_percentage": 0,
            "current_step": "pending",
            "files": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        result = status_app.format_task_response(task)

        assert result["task_id"] == "task-123"
        assert result["status"] == "PENDING"
        assert result["progress_percentage"] == 0

    @patch.object(status_app, "generate_download_url")
    def test_format_completed_task_with_download_urls(self, mock_generate_url):
        """Format completed task includes download URLs."""
        mock_generate_url.return_value = "https://download-url"

        task = {
            "task_id": "task-123",
            "status": "COMPLETED",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "status": "COMPLETED",
                    "output_s3_key": "output/task-123/file-1/converted.mp4",
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        result = status_app.format_task_response(task, include_download_urls=True)

        assert result["files"][0]["download_url"] == "https://download-url"

    def test_format_task_without_download_urls(self):
        """Format task without download URLs when not requested."""
        task = {
            "task_id": "task-123",
            "status": "COMPLETED",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "status": "COMPLETED",
                    "output_s3_key": "output/task-123/file-1/converted.mp4",
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        result = status_app.format_task_response(task, include_download_urls=False)

        assert "download_url" not in result["files"][0]


class TestFormatTaskSummary:
    """Tests for format_task_summary function."""

    def test_format_summary_counts(self):
        """Format summary includes correct counts."""
        task = {
            "task_id": "task-123",
            "status": "PARTIALLY_COMPLETED",
            "quality_preset": "balanced",
            "progress_percentage": 50,
            "files": [
                {"file_id": "file-1", "status": "COMPLETED"},
                {"file_id": "file-2", "status": "FAILED"},
                {"file_id": "file-3", "status": "PROCESSING"},
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        result = status_app.format_task_summary(task)

        assert result["file_count"] == 3
        assert result["completed_count"] == 1
        assert result["failed_count"] == 1


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

    def test_missing_user_id_header(self):
        """Missing X-User-Id header returns 401."""
        event = {"headers": {}}

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 401
        body = json.loads(response["body"])
        assert "X-User-Id" in body["error"]

    @patch.object(status_app, "get_task")
    def test_get_single_task_success(self, mock_get_task):
        """Get single task successfully."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "status": "PENDING",
            "quality_preset": "balanced",
            "progress_percentage": 0,
            "files": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {"task_id": "task-123"},
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["task_id"] == "task-123"

    @patch.object(status_app, "get_task")
    def test_get_single_task_not_found(self, mock_get_task):
        """Get single task not found returns 404."""
        mock_get_task.return_value = None

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {"task_id": "nonexistent"},
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 404

    @patch.object(status_app, "list_tasks")
    def test_list_tasks_success(self, mock_list_tasks):
        """List tasks successfully."""
        mock_list_tasks.return_value = [
            {
                "task_id": "task-1",
                "status": "PENDING",
                "quality_preset": "balanced",
                "progress_percentage": 0,
                "files": [],
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        ]

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": None,
            "queryStringParameters": None,
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert "tasks" in body
        assert len(body["tasks"]) == 1

    @patch.object(status_app, "list_tasks")
    def test_list_tasks_with_status_filter(self, mock_list_tasks):
        """List tasks with status filter."""
        mock_list_tasks.return_value = []

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": None,
            "queryStringParameters": {"status": "PENDING"},
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        mock_list_tasks.assert_called_with("user-123", "PENDING", 20)

    @patch.object(status_app, "list_tasks")
    def test_list_tasks_limit_capped(self, mock_list_tasks):
        """List tasks limit is capped at 100."""
        mock_list_tasks.return_value = []

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": None,
            "queryStringParameters": {"limit": "500"},
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        mock_list_tasks.assert_called_with("user-123", None, 100)

    @patch.object(status_app, "list_tasks")
    def test_lowercase_user_id_header(self, mock_list_tasks):
        """Lowercase x-user-id header is accepted."""
        mock_list_tasks.return_value = []

        event = {
            "headers": {"x-user-id": "user-123"},
            "pathParameters": None,
            "queryStringParameters": None,
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 200


class TestDecimalEncoder:
    """Tests for DecimalEncoder JSON encoder."""

    def test_decimal_whole_number(self):
        """Decimal whole number converts to int."""
        data = {"value": Decimal("42")}
        result = json.dumps(data, cls=status_app.DecimalEncoder)
        assert '"value": 42' in result

    def test_decimal_float(self):
        """Decimal with decimals converts to float."""
        data = {"value": Decimal("3.14")}
        result = json.dumps(data, cls=status_app.DecimalEncoder)
        assert '"value": 3.14' in result


class TestResponseValidation:
    """Tests for API response validation (Requirement 3.4)."""

    def test_validate_task_detail_response_valid(self):
        """Valid task detail response passes validation."""
        response = {
            "task_id": "task-123",
            "status": "PENDING",
            "quality_preset": "balanced",
            "progress_percentage": 0,
            "current_step": "pending",
            "files": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is True
        assert error is None

    def test_validate_task_detail_response_missing_field(self):
        """Missing required field fails validation."""
        response = {
            "task_id": "task-123",
            "status": "PENDING",
            # Missing quality_preset, progress_percentage, etc.
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is False
        assert "Missing required field" in error

    def test_validate_task_detail_response_invalid_status(self):
        """Invalid task status fails validation."""
        response = {
            "task_id": "task-123",
            "status": "INVALID_STATUS",
            "quality_preset": "balanced",
            "progress_percentage": 0,
            "current_step": "pending",
            "files": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is False
        assert "Invalid task status" in error

    def test_validate_task_detail_response_invalid_current_step(self):
        """Invalid current_step fails validation."""
        response = {
            "task_id": "task-123",
            "status": "PENDING",
            "quality_preset": "balanced",
            "progress_percentage": 0,
            "current_step": "invalid_step",
            "files": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is False
        assert "Invalid current_step" in error

    def test_validate_task_detail_response_invalid_progress(self):
        """Invalid progress_percentage fails validation."""
        response = {
            "task_id": "task-123",
            "status": "PENDING",
            "quality_preset": "balanced",
            "progress_percentage": 150,  # Invalid: > 100
            "current_step": "pending",
            "files": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is False
        assert "Invalid progress_percentage" in error

    def test_validate_task_detail_response_negative_progress(self):
        """Negative progress_percentage fails validation."""
        response = {
            "task_id": "task-123",
            "status": "PENDING",
            "quality_preset": "balanced",
            "progress_percentage": -10,
            "current_step": "pending",
            "files": [],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is False
        assert "Invalid progress_percentage" in error

    def test_validate_task_detail_response_with_valid_files(self):
        """Task with valid files passes validation."""
        response = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "quality_preset": "balanced",
            "progress_percentage": 50,
            "current_step": "converting",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
                {"file_id": "file-2", "filename": "video2.mov", "status": "CONVERTING"},
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is True
        assert error is None

    def test_validate_task_detail_response_invalid_file_status(self):
        """File with invalid status fails validation."""
        response = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "quality_preset": "balanced",
            "progress_percentage": 50,
            "current_step": "converting",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "INVALID"},
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is False
        assert "invalid status" in error

    def test_validate_task_detail_response_file_missing_field(self):
        """File missing required field fails validation."""
        response = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "quality_preset": "balanced",
            "progress_percentage": 50,
            "current_step": "converting",
            "files": [
                {"file_id": "file-1", "status": "COMPLETED"},  # Missing filename
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_detail_response(response)

        assert is_valid is False
        assert "missing required field" in error

    def test_validate_file_response_valid(self):
        """Valid file response passes validation."""
        file = {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"}

        is_valid, error = status_app.validate_file_response(file, 0)

        assert is_valid is True
        assert error is None

    def test_validate_file_response_all_statuses(self):
        """All valid file statuses pass validation."""
        for status in ["PENDING", "CONVERTING", "VERIFYING", "COMPLETED", "FAILED"]:
            file = {"file_id": "file-1", "filename": "video.mov", "status": status}
            is_valid, error = status_app.validate_file_response(file, 0)
            assert is_valid is True, f"Status {status} should be valid"

    def test_validate_task_summary_response_valid(self):
        """Valid task summary response passes validation."""
        response = {
            "task_id": "task-123",
            "status": "PENDING",
            "quality_preset": "balanced",
            "progress_percentage": 0,
            "file_count": 3,
            "completed_count": 0,
            "failed_count": 0,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_summary_response(response)

        assert is_valid is True
        assert error is None

    def test_validate_task_summary_response_missing_field(self):
        """Missing required field fails validation."""
        response = {
            "task_id": "task-123",
            "status": "PENDING",
            # Missing other required fields
        }

        is_valid, error = status_app.validate_task_summary_response(response)

        assert is_valid is False
        assert "Missing required field" in error

    def test_validate_task_summary_response_invalid_count(self):
        """Negative count fails validation."""
        response = {
            "task_id": "task-123",
            "status": "PENDING",
            "quality_preset": "balanced",
            "progress_percentage": 0,
            "file_count": -1,  # Invalid
            "completed_count": 0,
            "failed_count": 0,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        is_valid, error = status_app.validate_task_summary_response(response)

        assert is_valid is False
        assert "Invalid file_count" in error

    def test_validate_task_summary_all_statuses(self):
        """All valid task statuses pass validation."""
        valid_statuses = [
            "PENDING",
            "UPLOADING",
            "CONVERTING",
            "VERIFYING",
            "COMPLETED",
            "PARTIALLY_COMPLETED",
            "FAILED",
            "CANCELLED",
        ]
        for status in valid_statuses:
            response = {
                "task_id": "task-123",
                "status": status,
                "quality_preset": "balanced",
                "progress_percentage": 50,
                "file_count": 3,
                "completed_count": 1,
                "failed_count": 0,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
            is_valid, error = status_app.validate_task_summary_response(response)
            assert is_valid is True, f"Status {status} should be valid"


class TestResponseSchemaConstants:
    """Tests for API response schema constants."""

    def test_valid_task_statuses_defined(self):
        """VALID_TASK_STATUSES contains all expected statuses."""
        expected = [
            "PENDING",
            "UPLOADING",
            "CONVERTING",
            "VERIFYING",
            "COMPLETED",
            "PARTIALLY_COMPLETED",
            "FAILED",
            "CANCELLED",
        ]
        assert status_app.VALID_TASK_STATUSES == expected

    def test_valid_file_statuses_defined(self):
        """VALID_FILE_STATUSES contains all expected statuses."""
        expected = [
            "PENDING",
            "CONVERTING",
            "VERIFYING",
            "COMPLETED",
            "DOWNLOADED",
            "REMOVED",
            "FAILED",
        ]
        assert status_app.VALID_FILE_STATUSES == expected

    def test_valid_current_steps_defined(self):
        """VALID_CURRENT_STEPS contains all expected steps."""
        expected = ["pending", "converting", "verifying", "completed"]
        assert status_app.VALID_CURRENT_STEPS == expected

    def test_task_detail_required_fields(self):
        """TASK_DETAIL_REQUIRED_FIELDS contains expected fields."""
        assert "task_id" in status_app.TASK_DETAIL_REQUIRED_FIELDS
        assert "status" in status_app.TASK_DETAIL_REQUIRED_FIELDS
        assert "files" in status_app.TASK_DETAIL_REQUIRED_FIELDS
        assert "progress_percentage" in status_app.TASK_DETAIL_REQUIRED_FIELDS

    def test_file_required_fields(self):
        """FILE_REQUIRED_FIELDS contains expected fields."""
        assert "file_id" in status_app.FILE_REQUIRED_FIELDS
        assert "filename" in status_app.FILE_REQUIRED_FIELDS
        assert "status" in status_app.FILE_REQUIRED_FIELDS


class TestUpdateFileStatusToDownloaded:
    """Tests for update_file_status_to_downloaded function (used by cleanup_file)."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

    @patch.object(status_app, "get_task")
    @patch.object(status_app, "get_dynamodb_table")
    def test_update_file_status_to_downloaded(self, mock_dynamodb, mock_get_task):
        """Update file status to DOWNLOADED after successful download."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
            ],
        }
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table

        result = status_app.update_file_status_to_downloaded("task-123", "file-1", "user-123")

        assert result is not None
        assert result["file_id"] == "file-1"
        assert result["status"] == "DOWNLOADED"
        assert result["downloaded_at"] is not None
        mock_table.update_item.assert_called_once()

    @patch.object(status_app, "get_task")
    def test_update_file_status_to_downloaded_task_not_found(self, mock_get_task):
        """Update file status fails when task not found."""
        mock_get_task.return_value = None

        result = status_app.update_file_status_to_downloaded("nonexistent", "file-1", "user-123")

        assert result is None

    @patch.object(status_app, "get_task")
    def test_update_file_status_to_downloaded_file_not_found(self, mock_get_task):
        """Update file status fails when file not found."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
            ],
        }

        result = status_app.update_file_status_to_downloaded(
            "task-123", "nonexistent-file", "user-123"
        )

        assert result is None


class TestDownloadStatusInResponse:
    """Tests for download status fields in API response (Requirement 8.4).

    Note: download_status field was removed. File status is now tracked via
    FileStatus enum (COMPLETED -> DOWNLOADED transition).
    """

    def test_format_task_response_includes_download_available(self):
        """format_task_response includes download_available field."""
        task = {
            "task_id": "task-123",
            "status": "COMPLETED",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    "download_available": True,
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert "files" in response
        assert len(response["files"]) == 1
        assert response["files"][0]["download_available"] is True

    def test_format_task_response_download_available_defaults_true(self):
        """download_available defaults to True when not set."""
        task = {
            "task_id": "task-123",
            "status": "COMPLETED",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    # No download_available
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert response["files"][0]["download_available"] is True

    def test_format_task_response_download_available_false(self):
        """download_available can be False (file expired)."""
        task = {
            "task_id": "task-123",
            "status": "COMPLETED",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    "download_available": False,  # File expired from S3
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert response["files"][0]["download_available"] is False

    def test_format_task_response_downloaded_status(self):
        """Files with DOWNLOADED status are properly formatted."""
        task = {
            "task_id": "task-123",
            "status": "COMPLETED",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "DOWNLOADED",  # Already downloaded
                    "downloaded_at": "2024-01-01T12:00:00Z",
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert response["files"][0]["status"] == "DOWNLOADED"


class TestCleanupFileEndpoint:
    """Tests for cleanup file endpoint (Requirement 10.1-10.5)."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        # Also patch the module-level variable
        monkeypatch.setattr(status_app, "S3_BUCKET", "test-bucket")

    @patch.object(status_app, "get_task")
    @patch.object(status_app, "get_dynamodb_table")
    @patch.object(status_app, "get_s3_client")
    def test_cleanup_file_downloaded_success(self, mock_s3, mock_dynamodb, mock_get_task):
        """Cleanup file with action=downloaded updates status and deletes S3."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    "output_s3_key": "output/task-123/file-1/converted.mp4",
                },
            ],
        }
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table
        mock_s3_client = MagicMock()
        mock_s3.return_value = mock_s3_client

        result = status_app.cleanup_file("task-123", "file-1", "user-123", "downloaded")

        assert result["file_id"] == "file-1"
        assert result["status"] == "DOWNLOADED"
        assert result["s3_deleted"] is True
        assert result["completed_at"] is not None
        mock_table.update_item.assert_called_once()
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="output/task-123/file-1/converted.mp4"
        )

    @patch.object(status_app, "get_task")
    @patch.object(status_app, "get_dynamodb_table")
    @patch.object(status_app, "get_s3_client")
    def test_cleanup_file_removed_success(self, mock_s3, mock_dynamodb, mock_get_task):
        """Cleanup file with action=removed updates status to REMOVED."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    "output_s3_key": "output/task-123/file-1/converted.mp4",
                },
            ],
        }
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table
        mock_s3_client = MagicMock()
        mock_s3.return_value = mock_s3_client

        result = status_app.cleanup_file("task-123", "file-1", "user-123", "removed")

        assert result["file_id"] == "file-1"
        assert result["status"] == "REMOVED"
        assert result["s3_deleted"] is True

    @patch.object(status_app, "get_task")
    def test_cleanup_file_task_not_found(self, mock_get_task):
        """Cleanup file raises ValueError when task not found."""
        mock_get_task.return_value = None

        with pytest.raises(ValueError, match="Task not found"):
            status_app.cleanup_file("nonexistent", "file-1", "user-123", "downloaded")

    @patch.object(status_app, "get_task")
    def test_cleanup_file_file_not_found(self, mock_get_task):
        """Cleanup file raises ValueError when file not found."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
            ],
        }

        with pytest.raises(ValueError, match="File not found"):
            status_app.cleanup_file("task-123", "nonexistent-file", "user-123", "downloaded")

    def test_cleanup_file_invalid_action(self):
        """Cleanup file raises ValueError for invalid action."""
        with pytest.raises(ValueError, match="Invalid action"):
            status_app.cleanup_file("task-123", "file-1", "user-123", "invalid")

    @patch.object(status_app, "get_task")
    @patch.object(status_app, "get_dynamodb_table")
    @patch.object(status_app, "get_s3_client")
    def test_cleanup_file_s3_delete_failure_returns_success(
        self, mock_s3, mock_dynamodb, mock_get_task
    ):
        """S3 deletion failure still returns success (status is source of truth)."""
        from botocore.exceptions import ClientError

        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    "output_s3_key": "output/task-123/file-1/converted.mp4",
                },
            ],
        }
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table
        mock_s3_client = MagicMock()
        mock_s3_client.delete_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "DeleteObject"
        )
        mock_s3.return_value = mock_s3_client

        result = status_app.cleanup_file("task-123", "file-1", "user-123", "downloaded")

        # Should still succeed, but s3_deleted is False
        assert result["file_id"] == "file-1"
        assert result["status"] == "DOWNLOADED"
        assert result["s3_deleted"] is False

    @patch.object(status_app, "get_task")
    @patch.object(status_app, "get_dynamodb_table")
    def test_cleanup_file_status_update_failure_raises_error(self, mock_dynamodb, mock_get_task):
        """Status update failure raises RuntimeError (S3 deletion skipped)."""
        from botocore.exceptions import ClientError

        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    "output_s3_key": "output/task-123/file-1/converted.mp4",
                },
            ],
        }
        mock_table = MagicMock()
        mock_table.update_item.side_effect = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "Error"}}, "UpdateItem"
        )
        mock_dynamodb.return_value = mock_table

        with pytest.raises(RuntimeError, match="Failed to update file status"):
            status_app.cleanup_file("task-123", "file-1", "user-123", "downloaded")

    @patch.object(status_app, "get_task")
    @patch.object(status_app, "get_dynamodb_table")
    def test_cleanup_file_no_s3_key(self, mock_dynamodb, mock_get_task):
        """Cleanup file handles missing S3 key gracefully."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    # No output_s3_key
                },
            ],
        }
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table

        result = status_app.cleanup_file("task-123", "file-1", "user-123", "downloaded")

        assert result["file_id"] == "file-1"
        assert result["status"] == "DOWNLOADED"
        assert result["s3_deleted"] is False

    @patch.object(status_app, "cleanup_file")
    def test_handler_cleanup_endpoint_success(self, mock_cleanup):
        """Handler processes cleanup endpoint successfully."""
        mock_cleanup.return_value = {
            "file_id": "file-1",
            "status": "DOWNLOADED",
            "s3_deleted": True,
            "completed_at": "2024-01-01T00:00:00Z",
        }

        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/files/{file_id}/cleanup",
            "pathParameters": {"task_id": "task-123", "file_id": "file-1"},
            "body": json.dumps({"action": "downloaded"}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["file_id"] == "file-1"
        assert body["status"] == "DOWNLOADED"
        assert body["s3_deleted"] is True

    def test_handler_cleanup_missing_action(self):
        """Handler returns 400 when action is missing."""
        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/files/{file_id}/cleanup",
            "pathParameters": {"task_id": "task-123", "file_id": "file-1"},
            "body": json.dumps({}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "action" in body["error"]

    def test_handler_cleanup_invalid_action(self):
        """Handler returns 400 for invalid action."""
        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/files/{file_id}/cleanup",
            "pathParameters": {"task_id": "task-123", "file_id": "file-1"},
            "body": json.dumps({"action": "invalid"}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "Invalid action" in body["error"]

    @patch.object(status_app, "cleanup_file")
    def test_handler_cleanup_not_found(self, mock_cleanup):
        """Handler returns 404 when task/file not found."""
        mock_cleanup.side_effect = ValueError("Task not found")

        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/files/{file_id}/cleanup",
            "pathParameters": {"task_id": "task-123", "file_id": "file-1"},
            "body": json.dumps({"action": "downloaded"}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 404

    @patch.object(status_app, "cleanup_file")
    def test_handler_cleanup_status_update_error(self, mock_cleanup):
        """Handler returns 500 when status update fails."""
        mock_cleanup.side_effect = RuntimeError("Failed to update file status")

        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/files/{file_id}/cleanup",
            "pathParameters": {"task_id": "task-123", "file_id": "file-1"},
            "body": json.dumps({"action": "downloaded"}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 500


class TestValidFileStatusesIncludesRemoved:
    """Test that VALID_FILE_STATUSES includes REMOVED status."""

    def test_removed_status_is_valid(self):
        """REMOVED is a valid file status."""
        assert "REMOVED" in status_app.VALID_FILE_STATUSES

    def test_validate_file_response_removed_status(self):
        """File with REMOVED status passes validation."""
        file = {"file_id": "file-1", "filename": "video.mov", "status": "REMOVED"}
        is_valid, error = status_app.validate_file_response(file, 0)
        assert is_valid is True
