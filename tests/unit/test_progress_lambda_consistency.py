"""Tests for progress calculation consistency between Lambda and CLI.

Property 10: Progress Calculation Consistency
For any list of file statuses, the progress calculated by Lambda and CLI
SHALL be identical when using the same input data.

Requirements: 6.3, 6.4
"""

import importlib.util
import os
import sys

from vco.utils.progress import (
    PROGRESS_CONVERTING_MIDPOINT,
    PROGRESS_VERIFYING,
    calculate_progress,
    calculate_progress_simple,
)

# Load the Lambda module directly using importlib
_lambda_path = os.path.join(os.path.dirname(__file__), "../../sam-app/async-task-status/app.py")
_spec = importlib.util.spec_from_file_location("status_app", _lambda_path)
status_app = importlib.util.module_from_spec(_spec)
sys.modules["status_app"] = status_app
_spec.loader.exec_module(status_app)


class TestProgressCalculationConsistency:
    """Tests for progress calculation consistency between Lambda and CLI."""

    def test_empty_files_consistency(self):
        """Empty file list produces same result in Lambda and CLI."""
        files = []

        cli_progress, cli_step = calculate_progress(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress
        assert cli_step == lambda_step

    def test_single_pending_consistency(self):
        """Single PENDING file produces same result."""
        files = [{"status": "PENDING"}]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress
        assert cli_step == lambda_step

    def test_single_completed_consistency(self):
        """Single COMPLETED file produces same result."""
        files = [{"status": "COMPLETED"}]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress
        assert cli_step == lambda_step

    def test_single_failed_consistency(self):
        """Single FAILED file produces same result."""
        files = [{"status": "FAILED"}]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress
        assert cli_step == lambda_step

    def test_single_verifying_consistency(self):
        """Single VERIFYING file produces same result."""
        files = [{"status": "VERIFYING"}]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress
        assert cli_step == lambda_step
        assert cli_progress == PROGRESS_VERIFYING

    def test_single_converting_consistency(self):
        """Single CONVERTING file produces same result (simple version)."""
        files = [{"status": "CONVERTING"}]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress
        assert cli_step == lambda_step
        assert cli_progress == PROGRESS_CONVERTING_MIDPOINT

    def test_mixed_statuses_consistency(self):
        """Mixed statuses produce same result."""
        files = [
            {"status": "COMPLETED"},
            {"status": "VERIFYING"},
            {"status": "PENDING"},
        ]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress
        assert cli_step == lambda_step

    def test_all_completed_consistency(self):
        """All COMPLETED files produce same result."""
        files = [
            {"status": "COMPLETED"},
            {"status": "COMPLETED"},
            {"status": "COMPLETED"},
        ]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress == 100
        assert cli_step == lambda_step == "completed"

    def test_completed_and_failed_consistency(self):
        """COMPLETED and FAILED files produce same result."""
        files = [
            {"status": "COMPLETED"},
            {"status": "FAILED"},
        ]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        assert cli_progress == lambda_progress == 100
        assert cli_step == lambda_step == "completed"

    def test_converting_with_job_id_consistency(self):
        """CONVERTING with job_id produces same result (simple version ignores job_id)."""
        files = [{"status": "CONVERTING", "mediaconvert_job_id": "job-123"}]

        cli_progress, cli_step = calculate_progress_simple(files)
        lambda_progress, lambda_step = status_app.calculate_progress_simple(files)

        # Simple version should use midpoint regardless of job_id
        assert cli_progress == lambda_progress == PROGRESS_CONVERTING_MIDPOINT
        assert cli_step == lambda_step == "converting"


class TestProgressResponseInclusion:
    """Tests for progress_percentage inclusion in API responses."""

    def test_format_task_response_includes_progress(self):
        """format_task_response includes progress_percentage."""
        task = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "quality_preset": "balanced",
            "files": [
                {"file_id": "f1", "filename": "v1.mov", "status": "COMPLETED"},
                {"file_id": "f2", "filename": "v2.mov", "status": "CONVERTING"},
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert "progress_percentage" in response
        assert isinstance(response["progress_percentage"], int)
        assert 0 <= response["progress_percentage"] <= 100

    def test_format_task_response_includes_current_step(self):
        """format_task_response includes current_step."""
        task = {
            "task_id": "task-123",
            "status": "VERIFYING",
            "quality_preset": "balanced",
            "files": [{"file_id": "f1", "filename": "v1.mov", "status": "VERIFYING"}],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        assert "current_step" in response
        assert response["current_step"] in ["pending", "converting", "verifying", "completed"]

    def test_format_task_summary_includes_progress(self):
        """format_task_summary includes progress_percentage."""
        task = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "quality_preset": "balanced",
            "files": [
                {"file_id": "f1", "status": "COMPLETED"},
                {"file_id": "f2", "status": "PENDING"},
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_summary(task)

        assert "progress_percentage" in response
        assert isinstance(response["progress_percentage"], int)
        assert 0 <= response["progress_percentage"] <= 100

    def test_progress_calculated_from_files_not_stored(self):
        """Progress is calculated from files, not from stored value."""
        # Task has stored progress_percentage but files indicate different progress
        task = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "quality_preset": "balanced",
            "progress_percentage": 99,  # Stored value (should be ignored)
            "files": [
                {"file_id": "f1", "filename": "v1.mov", "status": "PENDING"},
                {"file_id": "f2", "filename": "v2.mov", "status": "PENDING"},
            ],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        }

        response = status_app.format_task_response(task)

        # Progress should be calculated from files (0%), not stored value (99%)
        assert response["progress_percentage"] == 0
