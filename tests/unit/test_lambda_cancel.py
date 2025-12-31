"""Unit tests for async-task-cancel Lambda function.

Tests: Task 5.5 - Cancel Lambda unit tests
Requirements: 3.1, 3.2, 3.3, 3.4, 3.5
"""

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Load the Lambda module directly using importlib
_lambda_path = os.path.join(os.path.dirname(__file__), "../../sam-app/async-task-cancel/app.py")
_spec = importlib.util.spec_from_file_location("cancel_app", _lambda_path)
cancel_app = importlib.util.module_from_spec(_spec)
sys.modules["cancel_app"] = cancel_app
_spec.loader.exec_module(cancel_app)


class TestGetTask:
    """Tests for get_task function."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

    @patch.object(cancel_app, "get_dynamodb_table")
    def test_get_task_success(self, mock_dynamodb):
        """Successfully get task by ID."""
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "task_id": "task-123",
                "user_id": "user-123",
                "status": "PROCESSING",
            }
        }
        mock_dynamodb.return_value = mock_table

        result = cancel_app.get_task("task-123", "user-123")

        assert result is not None
        assert result["task_id"] == "task-123"

    @patch.object(cancel_app, "get_dynamodb_table")
    def test_get_task_wrong_user(self, mock_dynamodb):
        """Task owned by different user returns None."""
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "task_id": "task-123",
                "user_id": "other-user",
                "status": "PROCESSING",
            }
        }
        mock_dynamodb.return_value = mock_table

        result = cancel_app.get_task("task-123", "user-123")

        assert result is None


class TestStopStepFunctionsExecution:
    """Tests for stop_step_functions_execution function."""

    @patch.object(cancel_app, "get_sfn_client")
    def test_stop_execution_success(self, mock_sfn):
        """Successfully stop execution."""
        mock_client = MagicMock()
        mock_sfn.return_value = mock_client

        result = cancel_app.stop_step_functions_execution(
            "arn:aws:states:us-east-1:123:execution:test:exec-1"
        )

        assert result is True
        mock_client.stop_execution.assert_called_once()

    def test_stop_execution_no_arn(self):
        """No execution ARN returns True."""
        result = cancel_app.stop_step_functions_execution("")

        assert result is True

    @patch.object(cancel_app, "get_sfn_client")
    def test_stop_execution_already_completed(self, mock_sfn):
        """Execution already completed returns True."""
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.stop_execution.side_effect = ClientError(
            {"Error": {"Code": "ExecutionDoesNotExist", "Message": "Not found"}},
            "StopExecution",
        )
        mock_sfn.return_value = mock_client

        result = cancel_app.stop_step_functions_execution(
            "arn:aws:states:us-east-1:123:execution:test:exec-1"
        )

        assert result is True


class TestCancelMediaConvertJobs:
    """Tests for cancel_mediaconvert_jobs function."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("MEDIACONVERT_ENDPOINT", "https://mediaconvert.us-east-1.amazonaws.com")

    @patch.object(cancel_app, "get_mediaconvert_client")
    def test_cancel_running_job(self, mock_mc):
        """Cancel running MediaConvert job."""
        mock_client = MagicMock()
        mock_client.get_job.return_value = {"Job": {"Status": "PROGRESSING"}}
        mock_mc.return_value = mock_client

        files = [{"file_id": "file-1", "mediaconvert_job_id": "job-123"}]
        result = cancel_app.cancel_mediaconvert_jobs(files)

        assert "job-123" in result
        mock_client.cancel_job.assert_called_once_with(Id="job-123")

    @patch.object(cancel_app, "get_mediaconvert_client")
    def test_skip_completed_job(self, mock_mc):
        """Skip already completed job."""
        mock_client = MagicMock()
        mock_client.get_job.return_value = {"Job": {"Status": "COMPLETE"}}
        mock_mc.return_value = mock_client

        files = [{"file_id": "file-1", "mediaconvert_job_id": "job-123"}]
        result = cancel_app.cancel_mediaconvert_jobs(files)

        assert result == []
        mock_client.cancel_job.assert_not_called()

    @patch.object(cancel_app, "get_mediaconvert_client")
    def test_skip_file_without_job_id(self, mock_mc):
        """Skip file without MediaConvert job ID."""
        mock_client = MagicMock()
        mock_mc.return_value = mock_client

        files = [{"file_id": "file-1"}]  # No mediaconvert_job_id
        result = cancel_app.cancel_mediaconvert_jobs(files)

        assert result == []
        mock_client.get_job.assert_not_called()


class TestCleanupS3Files:
    """Tests for cleanup_s3_files function."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

    @patch.object(cancel_app, "get_s3_client")
    def test_cleanup_files(self, mock_s3):
        """Clean up S3 files."""
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_client.get_paginator.return_value = mock_paginator
        mock_s3.return_value = mock_client

        files = [
            {
                "file_id": "file-1",
                "source_s3_key": "input/task-123/file-1/video.mov",
                "output_s3_key": "output/task-123/file-1/converted.mp4",
            }
        ]
        result = cancel_app.cleanup_s3_files("task-123", files)

        assert result >= 0
        mock_client.delete_objects.assert_called()

    @patch.object(cancel_app, "get_s3_client")
    def test_cleanup_empty_files(self, mock_s3):
        """Clean up with no files."""
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_client.get_paginator.return_value = mock_paginator
        mock_s3.return_value = mock_client

        result = cancel_app.cleanup_s3_files("task-123", [])

        assert result == 0


class TestLambdaHandler:
    """Tests for lambda_handler function."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set up environment variables."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("MEDIACONVERT_ENDPOINT", "https://mediaconvert.us-east-1.amazonaws.com")

    def test_missing_user_id_header(self):
        """Missing X-User-Id header returns 401."""
        event = {"headers": {}}

        response = cancel_app.lambda_handler(event, None)

        assert response["statusCode"] == 401

    def test_missing_task_id(self):
        """Missing task_id returns 400."""
        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {},
        }

        response = cancel_app.lambda_handler(event, None)

        assert response["statusCode"] == 400

    @patch.object(cancel_app, "get_task")
    def test_task_not_found(self, mock_get_task):
        """Task not found returns 404."""
        mock_get_task.return_value = None

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {"task_id": "nonexistent"},
        }

        response = cancel_app.lambda_handler(event, None)

        assert response["statusCode"] == 404

    @patch.object(cancel_app, "get_task")
    def test_cancel_completed_task(self, mock_get_task):
        """Cannot cancel completed task."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "status": "COMPLETED",
        }

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {"task_id": "task-123"},
        }

        response = cancel_app.lambda_handler(event, None)

        assert response["statusCode"] == 400
        body = json.loads(response["body"])
        assert "cannot be cancelled" in body["error"]

    @patch.object(cancel_app, "get_task")
    def test_cancel_already_cancelled_task(self, mock_get_task):
        """Cannot cancel already cancelled task."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "status": "CANCELLED",
        }

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {"task_id": "task-123"},
        }

        response = cancel_app.lambda_handler(event, None)

        assert response["statusCode"] == 400

    @pytest.mark.parametrize("status", ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED", "CANCELLED"])
    @patch.object(cancel_app, "get_task")
    def test_cannot_cancel_terminal_statuses(self, mock_get_task, status):
        """Cannot cancel tasks in terminal statuses."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "status": status,
        }

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {"task_id": "task-123"},
        }

        response = cancel_app.lambda_handler(event, None)

        assert response["statusCode"] == 400

    @patch.object(cancel_app, "update_task_cancelled")
    @patch.object(cancel_app, "cleanup_s3_files")
    @patch.object(cancel_app, "cancel_mediaconvert_jobs")
    @patch.object(cancel_app, "stop_step_functions_execution")
    @patch.object(cancel_app, "get_task")
    def test_successful_cancellation(
        self, mock_get_task, mock_stop_sfn, mock_cancel_mc, mock_cleanup, mock_update
    ):
        """Successful task cancellation."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "status": "PROCESSING",
            "execution_arn": "arn:aws:states:us-east-1:123:execution:test:exec-1",
            "files": [{"file_id": "file-1", "mediaconvert_job_id": "job-123"}],
        }
        mock_stop_sfn.return_value = True
        mock_cancel_mc.return_value = ["job-123"]
        mock_cleanup.return_value = 5

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {"task_id": "task-123"},
        }

        response = cancel_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["status"] == "CANCELLED"
        assert body["sfn_stopped"] is True
        assert "job-123" in body["cancelled_jobs"]
        assert body["deleted_files"] == 5

    @pytest.mark.parametrize(
        "status", ["PENDING", "UPLOADING", "PROCESSING", "CONVERTING", "VERIFYING"]
    )
    @patch.object(cancel_app, "update_task_cancelled")
    @patch.object(cancel_app, "cleanup_s3_files")
    @patch.object(cancel_app, "cancel_mediaconvert_jobs")
    @patch.object(cancel_app, "stop_step_functions_execution")
    @patch.object(cancel_app, "get_task")
    def test_can_cancel_non_terminal_statuses(
        self, mock_get_task, mock_stop_sfn, mock_cancel_mc, mock_cleanup, mock_update, status
    ):
        """Can cancel tasks in non-terminal statuses."""
        mock_get_task.return_value = {
            "task_id": "task-123",
            "user_id": "user-123",
            "status": status,
            "files": [],
        }
        mock_stop_sfn.return_value = True
        mock_cancel_mc.return_value = []
        mock_cleanup.return_value = 0

        event = {
            "headers": {"X-User-Id": "user-123"},
            "pathParameters": {"task_id": "task-123"},
        }

        response = cancel_app.lambda_handler(event, None)

        assert response["statusCode"] == 200
