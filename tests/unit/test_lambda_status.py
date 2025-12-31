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
        expected = ["PENDING", "CONVERTING", "VERIFYING", "COMPLETED", "FAILED"]
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


class TestDownloadStatusEndpoint:
    """Tests for download-status endpoint (Requirement 8.3)."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

    @patch.object(status_app, "get_task")
    @patch.object(status_app, "get_dynamodb_table")
    def test_update_download_status_completed(self, mock_dynamodb, mock_get_task):
        """Update download status to completed."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
            ],
        }
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table

        result = status_app.update_download_status("task-123", "file-1", "completed", "user-123")

        assert result is not None
        assert result["file_id"] == "file-1"
        assert result["downloaded_at"] is not None
        mock_table.update_item.assert_called_once()

    @patch.object(status_app, "get_task")
    @patch.object(status_app, "get_dynamodb_table")
    def test_update_download_status_started(self, mock_dynamodb, mock_get_task):
        """Update download status to started."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
            ],
        }
        mock_table = MagicMock()
        mock_dynamodb.return_value = mock_table

        result = status_app.update_download_status("task-123", "file-1", "started", "user-123")

        assert result is not None
        assert result["file_id"] == "file-1"
        assert result["downloaded_at"] is None  # Not set for started
        mock_table.update_item.assert_called_once()

    @patch.object(status_app, "get_task")
    def test_update_download_status_task_not_found(self, mock_get_task):
        """Update download status fails when task not found."""
        mock_get_task.return_value = None

        result = status_app.update_download_status("nonexistent", "file-1", "completed", "user-123")

        assert result is None

    @patch.object(status_app, "get_task")
    def test_update_download_status_file_not_found(self, mock_get_task):
        """Update download status fails when file not found."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
            ],
        }

        result = status_app.update_download_status(
            "task-123", "nonexistent-file", "completed", "user-123"
        )

        assert result is None

    @patch.object(status_app, "get_task")
    def test_update_download_status_invalid_action(self, mock_get_task):
        """Update download status fails with invalid action."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
            ],
        }

        result = status_app.update_download_status("task-123", "file-1", "invalid", "user-123")

        assert result is None

    @patch.object(status_app, "update_download_status")
    def test_handler_download_status_endpoint(self, mock_update):
        """Handler processes download-status endpoint."""
        mock_update.return_value = {
            "file_id": "file-1",
            "downloaded_at": "2024-01-01T00:00:00Z",
        }

        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/download-status",
            "pathParameters": {"task_id": "task-123"},
            "body": json.dumps({"file_id": "file-1", "action": "completed"}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["file_id"] == "file-1"
        assert body["downloaded_at"] is not None

    def test_handler_download_status_missing_file_id(self):
        """Handler returns 400 when file_id is missing."""
        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/download-status",
            "pathParameters": {"task_id": "task-123"},
            "body": json.dumps({"action": "completed"}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "file_id" in body["error"]

    def test_handler_download_status_invalid_action(self):
        """Handler returns 400 when action is invalid."""
        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/download-status",
            "pathParameters": {"task_id": "task-123"},
            "body": json.dumps({"file_id": "file-1", "action": "invalid"}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "action" in body["error"].lower()

    @patch.object(status_app, "update_download_status")
    def test_handler_download_status_not_found(self, mock_update):
        """Handler returns 404 when task/file not found."""
        mock_update.return_value = None

        event = {
            "headers": {"X-User-Id": "user-123"},
            "httpMethod": "POST",
            "resource": "/tasks/{task_id}/download-status",
            "pathParameters": {"task_id": "task-123"},
            "body": json.dumps({"file_id": "file-1", "action": "completed"}),
        }

        response = status_app.lambda_handler(event, None)

        assert response["statusCode"] == 404


class TestDownloadStatusInResponse:
    """Tests for download status fields in API response (Requirement 8.4)."""

    def test_format_task_response_includes_download_fields(self):
        """format_task_response includes downloaded_at and download_available."""
        task = {
            "task_id": "task-123",
            "status": "COMPLETED",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "status": "COMPLETED",
                    "downloaded_at": "2024-01-01T12:00:00Z",
                    "download_available": True,
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert "files" in response
        assert len(response["files"]) == 1
        assert response["files"][0]["downloaded_at"] == "2024-01-01T12:00:00Z"
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
                    # No downloaded_at or download_available
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert response["files"][0]["downloaded_at"] is None
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
                    "downloaded_at": "2024-01-01T12:00:00Z",
                    "download_available": False,  # File expired from S3
                }
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert response["files"][0]["download_available"] is False
