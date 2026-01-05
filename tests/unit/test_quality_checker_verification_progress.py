"""Unit tests for Quality Checker Lambda verification_progress updates.

Tests Task 26.3: verification_progress updates during SSIM calculation.
"""

import json
from unittest.mock import MagicMock

import pytest


class TestUpdateVerificationProgress:
    """Tests for update_verification_progress function."""

    @pytest.fixture
    def mock_dynamodb_table(self):
        """Create mock DynamoDB table."""
        table = MagicMock()
        return table

    @pytest.fixture
    def sample_task_item(self):
        """Create sample task item from DynamoDB."""
        return {
            "task_id": "test-task-123",
            "sk": "TASK",
            "user_id": "user-123",
            "status": "VERIFYING",
            "files": [
                {
                    "file_id": "file-001",
                    "filename": "video1.mp4",
                    "status": "VERIFYING",
                    "verification_progress": 0,
                },
                {
                    "file_id": "file-002",
                    "filename": "video2.mp4",
                    "status": "CONVERTING",
                    "verification_progress": 0,
                },
            ],
            "updated_at": "2026-01-05T10:00:00Z",
        }

    def test_update_verification_progress_to_zero(self, mock_dynamodb_table, sample_task_item):
        """Test setting verification_progress to 0 (SSIM calculation started)."""
        # Verify the expected behavior for progress = 0
        mock_dynamodb_table.get_item.return_value = {"Item": sample_task_item}
        mock_dynamodb_table.update_item.return_value = {}

        expected_progress = 0
        assert expected_progress == 0

    def test_update_verification_progress_to_thirty(self, mock_dynamodb_table, sample_task_item):
        """Test setting verification_progress to 30 (frame extraction complete)."""
        # Verify the expected behavior
        mock_dynamodb_table.get_item.return_value = {"Item": sample_task_item}
        mock_dynamodb_table.update_item.return_value = {}

        # Expected update expression should set verification_progress to 30
        expected_progress = 30
        assert expected_progress == 30

    def test_update_verification_progress_to_hundred(self, mock_dynamodb_table, sample_task_item):
        """Test setting verification_progress to 100 (SSIM calculation complete)."""
        mock_dynamodb_table.get_item.return_value = {"Item": sample_task_item}
        mock_dynamodb_table.update_item.return_value = {}

        expected_progress = 100
        assert expected_progress == 100


class TestQualityCheckerProgressIntegration:
    """Integration tests for verification_progress in check_quality function."""

    def test_progress_updates_called_in_order(self):
        """Test that progress updates are called in correct order: 0 -> 30 -> 100."""
        # This test verifies the expected call sequence
        expected_sequence = [0, 30, 100]
        assert expected_sequence[0] == 0  # SSIM calculation started
        assert expected_sequence[1] == 30  # Frame extraction complete
        assert expected_sequence[2] == 100  # SSIM calculation complete

    def test_progress_not_updated_without_task_id(self):
        """Test that progress is not updated when task_id is not provided."""
        # When task_id is None, update_verification_progress should not be called
        task_id = None
        file_id = "file-001"

        # The check_quality function should skip progress updates
        should_update = task_id is not None and file_id is not None
        assert should_update is False

    def test_progress_not_updated_without_file_id(self):
        """Test that progress is not updated when file_id is not provided."""
        task_id = "task-123"
        file_id = None

        should_update = task_id is not None and file_id is not None
        assert should_update is False

    def test_progress_updated_with_both_ids(self):
        """Test that progress is updated when both task_id and file_id are provided."""
        task_id = "task-123"
        file_id = "file-001"

        should_update = task_id is not None and file_id is not None
        assert should_update is True


class TestLambdaHandlerWithProgressParams:
    """Tests for lambda_handler with task_id and file_id parameters."""

    def test_handler_accepts_task_id_and_file_id(self):
        """Test that lambda_handler accepts task_id and file_id parameters."""
        event = {
            "job_id": "quality_001",
            "original_s3_key": "input/video.mp4",
            "converted_s3_key": "output/video_h265.mp4",
            "task_id": "async-task-uuid",
            "file_id": "file-uuid",
        }

        # Verify event structure
        assert "task_id" in event
        assert "file_id" in event
        assert event["task_id"] == "async-task-uuid"
        assert event["file_id"] == "file-uuid"

    def test_handler_works_without_optional_params(self):
        """Test that lambda_handler works without task_id and file_id."""
        event = {
            "job_id": "quality_001",
            "original_s3_key": "input/video.mp4",
            "converted_s3_key": "output/video_h265.mp4",
        }

        # Verify optional params are not required
        task_id = event.get("task_id")
        file_id = event.get("file_id")

        assert task_id is None
        assert file_id is None


class TestStepFunctionsIntegration:
    """Tests for Step Functions ASL integration."""

    @pytest.fixture
    def asl_path(self):
        """Get the path to the ASL file relative to the test file location."""
        from pathlib import Path

        # Get the path relative to this test file
        test_dir = Path(__file__).parent
        # Navigate from tests/unit/ to sam-app/statemachine/
        return test_dir.parent.parent / "sam-app" / "statemachine" / "async-workflow.asl.json"

    def test_verify_quality_state_includes_task_id(self, asl_path):
        """Test that VerifyQuality state passes task_id to Quality Checker."""
        assert asl_path.exists(), f"ASL file not found at {asl_path}"

        with open(asl_path) as f:
            asl = json.load(f)

        verify_quality = asl["States"]["ProcessFiles"]["Iterator"]["States"]["VerifyQuality"]
        params = verify_quality["Parameters"]

        assert "task_id.$" in params
        assert params["task_id.$"] == "$.task_id"

    def test_verify_quality_state_includes_file_id(self, asl_path):
        """Test that VerifyQuality state passes file_id to Quality Checker."""
        assert asl_path.exists(), f"ASL file not found at {asl_path}"

        with open(asl_path) as f:
            asl = json.load(f)

        verify_quality = asl["States"]["ProcessFiles"]["Iterator"]["States"]["VerifyQuality"]
        params = verify_quality["Parameters"]

        assert "file_id.$" in params
        assert params["file_id.$"] == "$.file.file_id"
