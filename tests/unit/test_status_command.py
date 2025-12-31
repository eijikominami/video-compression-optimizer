"""Unit tests for StatusCommand.

Tests: Task 10.4 - StatusCommand unit tests
Requirements: 2.1, 2.2, 2.3
"""

from unittest.mock import MagicMock, patch

import pytest

from vco.services.async_status import StatusCommand


class TestStatusCommandInit:
    """Tests for StatusCommand initialization."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            assert cmd.api_url == "https://api.example.com"
            assert cmd.region == "ap-northeast-1"

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped from API URL."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com/")
            assert cmd.api_url == "https://api.example.com"

    def test_init_with_custom_region(self):
        """Test initialization with custom region."""
        with patch("boto3.Session"):
            cmd = StatusCommand(
                api_url="https://api.example.com",
                region="us-west-2",
            )
            assert cmd.region == "us-west-2"


class TestListTasks:
    """Tests for list_tasks method."""

    def test_list_tasks_success(self):
        """Test successful task listing."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "tasks": [
                        {
                            "task_id": "task-1",
                            "status": "CONVERTING",
                            "file_count": 3,
                            "completed_count": 1,
                            "failed_count": 0,
                            "progress_percentage": 33,
                            "created_at": "2024-01-01T10:00:00",
                            "quality_preset": "balanced",
                        },
                        {
                            "task_id": "task-2",
                            "status": "COMPLETED",
                            "file_count": 2,
                            "completed_count": 2,
                            "failed_count": 0,
                            "progress_percentage": 100,
                            "created_at": "2024-01-01T09:00:00",
                            "quality_preset": "quality",
                        },
                    ]
                }
            )

            tasks = cmd.list_tasks()

            assert len(tasks) == 2
            assert tasks[0].task_id == "task-1"
            assert tasks[0].status == "CONVERTING"
            assert tasks[0].progress_percentage == 33
            assert tasks[1].task_id == "task-2"
            assert tasks[1].status == "COMPLETED"

    def test_list_tasks_empty(self):
        """Test listing with no tasks."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(return_value={"tasks": []})

            tasks = cmd.list_tasks()

            assert len(tasks) == 0

    def test_list_tasks_with_filter(self):
        """Test listing with status filter."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(return_value={"tasks": []})

            cmd.list_tasks(status_filter="CONVERTING")

            cmd._call_api.assert_called_once()
            call_args = cmd._call_api.call_args
            assert call_args[1]["params"]["status"] == "CONVERTING"

    def test_list_tasks_with_limit(self):
        """Test listing with custom limit."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(return_value={"tasks": []})

            cmd.list_tasks(limit=5)

            cmd._call_api.assert_called_once()
            call_args = cmd._call_api.call_args
            assert call_args[1]["params"]["limit"] == "5"

    def test_list_tasks_api_error(self):
        """Test handling of API error."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(side_effect=Exception("API error"))

            with pytest.raises(RuntimeError) as exc_info:
                cmd.list_tasks()

            assert "Failed to list tasks" in str(exc_info.value)


class TestGetTaskDetail:
    """Tests for get_task_detail method."""

    def test_get_task_detail_success(self):
        """Test successful task detail retrieval."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "task_id": "task-1",
                    "status": "CONVERTING",
                    "quality_preset": "balanced",
                    "created_at": "2024-01-01T10:00:00",
                    "updated_at": "2024-01-01T10:05:00",
                    "started_at": "2024-01-01T10:01:00",
                    "progress_percentage": 50,
                    "current_step": "Converting file 2 of 3",
                    "files": [
                        {
                            "file_id": "file-1",
                            "filename": "video1.mov",
                            "status": "COMPLETED",
                            "conversion_progress_percentage": 100,
                            "quality_result": {"ssim_score": 0.95},
                            "output_size_bytes": 1024000,
                        },
                        {
                            "file_id": "file-2",
                            "filename": "video2.mov",
                            "status": "CONVERTING",
                            "conversion_progress_percentage": 50,
                        },
                    ],
                }
            )

            detail = cmd.get_task_detail("task-1")

            assert detail.task_id == "task-1"
            assert detail.status == "CONVERTING"
            assert detail.progress_percentage == 50
            assert detail.current_step == "Converting file 2 of 3"
            assert len(detail.files) == 2
            assert detail.files[0].filename == "video1.mov"
            assert detail.files[0].ssim_score == 0.95
            assert detail.files[1].status == "CONVERTING"

    def test_get_task_detail_not_found(self):
        """Test handling of task not found."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(side_effect=Exception("Task not found"))

            with pytest.raises(RuntimeError) as exc_info:
                cmd.get_task_detail("nonexistent-task")

            assert "Failed to get task detail" in str(exc_info.value)

    def test_get_task_detail_completed(self):
        """Test completed task detail."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "task_id": "task-1",
                    "status": "COMPLETED",
                    "quality_preset": "balanced",
                    "created_at": "2024-01-01T10:00:00",
                    "updated_at": "2024-01-01T10:30:00",
                    "started_at": "2024-01-01T10:01:00",
                    "completed_at": "2024-01-01T10:30:00",
                    "progress_percentage": 100,
                    "files": [],
                }
            )

            detail = cmd.get_task_detail("task-1")

            assert detail.status == "COMPLETED"
            assert detail.completed_at is not None
            assert detail.progress_percentage == 100

    def test_get_task_detail_failed(self):
        """Test failed task detail with error message."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "task_id": "task-1",
                    "status": "FAILED",
                    "quality_preset": "balanced",
                    "created_at": "2024-01-01T10:00:00",
                    "updated_at": "2024-01-01T10:05:00",
                    "error_message": "MediaConvert job failed",
                    "files": [
                        {
                            "file_id": "file-1",
                            "filename": "video1.mov",
                            "status": "FAILED",
                            "conversion_progress_percentage": 0,
                            "error_message": "Unsupported codec",
                        },
                    ],
                }
            )

            detail = cmd.get_task_detail("task-1")

            assert detail.status == "FAILED"
            assert detail.error_message == "MediaConvert job failed"
            assert detail.files[0].error_message == "Unsupported codec"


class TestStatusDisplay:
    """Tests for status display across all states."""

    @pytest.mark.parametrize(
        "status",
        [
            "PENDING",
            "UPLOADING",
            "CONVERTING",
            "VERIFYING",
            "COMPLETED",
            "PARTIALLY_COMPLETED",
            "FAILED",
            "CANCELLED",
        ],
    )
    def test_all_status_values(self, status):
        """Test that all status values are handled correctly."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            cmd._call_api = MagicMock(
                return_value={
                    "task_id": "task-1",
                    "status": status,
                    "quality_preset": "balanced",
                    "created_at": "2024-01-01T10:00:00",
                    "updated_at": "2024-01-01T10:05:00",
                    "progress_percentage": 50,
                    "files": [],
                }
            )

            detail = cmd.get_task_detail("task-1")

            assert detail.status == status


class TestHelperMethods:
    """Tests for helper methods."""

    def test_get_machine_id_returns_string(self):
        """Test _get_machine_id returns valid string."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            machine_id = cmd._get_machine_id()
            assert isinstance(machine_id, str)
            assert len(machine_id) == 32

    def test_get_machine_id_consistent(self):
        """Test _get_machine_id returns consistent value."""
        with patch("boto3.Session"):
            cmd = StatusCommand(api_url="https://api.example.com")
            id1 = cmd._get_machine_id()
            id2 = cmd._get_machine_id()
            assert id1 == id2
