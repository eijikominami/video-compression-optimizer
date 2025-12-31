"""Unit tests for CancelCommand.

Tests: Task 11.4 - CancelCommand unit tests
Requirements: 3.1, 3.2, 3.3
"""

from unittest.mock import MagicMock, patch

import pytest

from vco.services.async_cancel import CancelCommand


class TestCancelCommandInit:
    """Tests for CancelCommand initialization."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            assert cmd.api_url == "https://api.example.com"
            assert cmd.region == "ap-northeast-1"

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped from API URL."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com/")
            assert cmd.api_url == "https://api.example.com"

    def test_init_with_custom_region(self):
        """Test initialization with custom region."""
        with patch("boto3.Session"):
            cmd = CancelCommand(
                api_url="https://api.example.com",
                region="us-west-2",
            )
            assert cmd.region == "us-west-2"


class TestCancelSuccess:
    """Tests for successful cancellation."""

    def test_cancel_success(self):
        """Test successful task cancellation."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "success": True,
                    "previous_status": "CONVERTING",
                    "message": "Task cancelled successfully",
                    "s3_files_deleted": True,
                    "mediaconvert_cancelled": True,
                }
            )

            result = cmd.cancel("task-1")

            assert result.success is True
            assert result.task_id == "task-1"
            assert result.previous_status == "CONVERTING"
            assert result.s3_files_deleted is True
            assert result.mediaconvert_cancelled is True

    def test_cancel_pending_task(self):
        """Test cancelling a pending task."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "success": True,
                    "previous_status": "PENDING",
                    "message": "Task cancelled before processing started",
                    "s3_files_deleted": True,
                    "mediaconvert_cancelled": False,
                }
            )

            result = cmd.cancel("task-1")

            assert result.success is True
            assert result.previous_status == "PENDING"
            assert result.mediaconvert_cancelled is False

    def test_cancel_uploading_task(self):
        """Test cancelling a task during upload."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "success": True,
                    "previous_status": "UPLOADING",
                    "message": "Task cancelled during upload",
                    "s3_files_deleted": True,
                    "mediaconvert_cancelled": False,
                }
            )

            result = cmd.cancel("task-1")

            assert result.success is True
            assert result.previous_status == "UPLOADING"


class TestCancelAlreadyCancelled:
    """Tests for cancelling already cancelled tasks."""

    def test_cancel_already_cancelled(self):
        """Test cancelling an already cancelled task."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "success": False,
                    "previous_status": "CANCELLED",
                    "message": "Task is already cancelled",
                }
            )

            result = cmd.cancel("task-1")

            assert result.success is False
            assert result.previous_status == "CANCELLED"
            assert "already cancelled" in result.message


class TestCancelCompletedTask:
    """Tests for cancelling completed tasks."""

    def test_cancel_completed_task(self):
        """Test cancelling a completed task (should fail)."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "success": False,
                    "previous_status": "COMPLETED",
                    "message": "Cannot cancel completed task",
                }
            )

            result = cmd.cancel("task-1")

            assert result.success is False
            assert result.previous_status == "COMPLETED"

    def test_cancel_failed_task(self):
        """Test cancelling a failed task."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "success": False,
                    "previous_status": "FAILED",
                    "message": "Cannot cancel failed task",
                }
            )

            result = cmd.cancel("task-1")

            assert result.success is False
            assert result.previous_status == "FAILED"


class TestCancelTaskNotFound:
    """Tests for cancelling non-existent tasks."""

    def test_cancel_task_not_found(self):
        """Test cancelling a non-existent task."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(side_effect=Exception("Task not found"))

            result = cmd.cancel("nonexistent-task")

            assert result.success is False
            assert result.error_message is not None
            assert "Task not found" in result.error_message


class TestCancelApiError:
    """Tests for API error handling."""

    def test_cancel_api_error(self):
        """Test handling of API error."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(side_effect=Exception("API error"))

            result = cmd.cancel("task-1")

            assert result.success is False
            assert result.error_message is not None
            assert "API error" in result.error_message

    def test_cancel_timeout_error(self):
        """Test handling of timeout error."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(side_effect=Exception("Connection timeout"))

            result = cmd.cancel("task-1")

            assert result.success is False
            assert "timeout" in result.error_message.lower()


class TestCancelAllStates:
    """Tests for cancelling tasks in various states."""

    @pytest.mark.parametrize(
        "status,can_cancel",
        [
            ("PENDING", True),
            ("UPLOADING", True),
            ("CONVERTING", True),
            ("VERIFYING", True),
            ("COMPLETED", False),
            ("PARTIALLY_COMPLETED", False),
            ("FAILED", False),
            ("CANCELLED", False),
        ],
    )
    def test_cancel_by_status(self, status, can_cancel):
        """Test cancellation behavior for each status."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "success": can_cancel,
                    "previous_status": status,
                    "message": "Cancelled" if can_cancel else f"Cannot cancel {status} task",
                }
            )

            result = cmd.cancel("task-1")

            assert result.success == can_cancel
            assert result.previous_status == status


class TestHelperMethods:
    """Tests for helper methods."""

    def test_get_machine_id_returns_string(self):
        """Test _get_machine_id returns valid string."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            machine_id = cmd._get_machine_id()
            assert isinstance(machine_id, str)
            assert len(machine_id) == 32

    def test_get_machine_id_consistent(self):
        """Test _get_machine_id returns consistent value."""
        with patch("boto3.Session"):
            cmd = CancelCommand(api_url="https://api.example.com")
            id1 = cmd._get_machine_id()
            id2 = cmd._get_machine_id()
            assert id1 == id2
