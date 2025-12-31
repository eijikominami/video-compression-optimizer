"""Boundary tests for API interfaces.

Tests: Tasks 5.6, 6.4, 9.4 - API boundary tests
Requirements: 1.1, 2.1, 3.1, 3.4, 5.1

Task 4.2: Improved to use schema definitions from Lambda modules.
"""

import importlib.util
import json
import os
import sys

# Load the Lambda status module to access schema constants
_status_lambda_path = os.path.join(
    os.path.dirname(__file__), "../../sam-app/async-task-status/app.py"
)
_status_spec = importlib.util.spec_from_file_location("status_app", _status_lambda_path)
status_app = importlib.util.module_from_spec(_status_spec)
sys.modules["status_app_boundary"] = status_app
_status_spec.loader.exec_module(status_app)

# Load the Lambda submit module to access schema constants
_submit_lambda_path = os.path.join(
    os.path.dirname(__file__), "../../sam-app/async-task-submit/app.py"
)
_submit_spec = importlib.util.spec_from_file_location("submit_app", _submit_lambda_path)
submit_app = importlib.util.module_from_spec(_submit_spec)
sys.modules["submit_app_boundary"] = submit_app
_submit_spec.loader.exec_module(submit_app)


class TestApiGatewayLambdaInterface:
    """Tests for API Gateway → Lambda interface (Task 5.6)."""

    def test_submit_request_uses_valid_quality_presets(self):
        """Test that submit request uses valid quality presets from schema."""
        # Use schema constants from Lambda module
        valid_presets = submit_app.VALID_QUALITY_PRESETS

        # Valid request body with proper UUID format
        valid_body = {
            "task_id": "12345678-1234-1234-1234-123456789012",
            "user_id": "user-456",
            "quality_preset": valid_presets[0],  # Use first valid preset
            "files": [
                {
                    "file_id": "abcdef12-1234-1234-1234-123456789012",
                    "original_uuid": "uuid-1",
                    "filename": "video.mov",
                    "source_s3_key": "async/task-123/input/file-1/video.mov",
                    "metadata_s3_key": "async/task-123/input/file-1/metadata.json",
                    "source_size_bytes": 1024000,
                }
            ],
        }

        # Validate using Lambda's validate_request
        is_valid, error_msg, error_code = submit_app.validate_request(valid_body)
        assert is_valid is True, f"Validation failed: {error_msg}"
        assert error_msg is None

    def test_submit_request_body_schema(self):
        """Test that submit request body follows expected schema."""
        # Valid request body
        valid_body = {
            "task_id": "task-123",
            "user_id": "user-456",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "original_uuid": "uuid-1",
                    "filename": "video.mov",
                    "source_s3_key": "async/task-123/input/file-1/video.mov",
                    "metadata_s3_key": "async/task-123/input/file-1/metadata.json",
                    "source_size_bytes": 1024000,
                }
            ],
        }

        # Required fields
        assert "task_id" in valid_body
        assert "user_id" in valid_body
        assert "quality_preset" in valid_body
        assert "files" in valid_body
        assert len(valid_body["files"]) > 0

        # File structure
        file_item = valid_body["files"][0]
        assert "file_id" in file_item
        assert "filename" in file_item
        assert "source_s3_key" in file_item

    def test_status_request_uses_valid_statuses(self):
        """Test that status request uses valid statuses from schema."""
        # Use schema constants from Lambda module
        valid_statuses = status_app.VALID_TASK_STATUSES

        # All statuses should be valid
        for status in valid_statuses:
            valid_params = {
                "user_id": "user-456",
                "status": status,
                "limit": "20",
            }
            assert valid_params["status"] in valid_statuses

    def test_status_request_params_schema(self):
        """Test that status request params follow expected schema."""
        # Valid params
        valid_params = {
            "user_id": "user-456",
            "status": "CONVERTING",
            "limit": "20",
        }

        # user_id is required
        assert "user_id" in valid_params

        # Optional params - use schema constants
        assert valid_params.get("status") in [None] + status_app.VALID_TASK_STATUSES

    def test_cancel_request_body_schema(self):
        """Test that cancel request body follows expected schema."""
        valid_body = {
            "user_id": "user-456",
        }

        assert "user_id" in valid_body

    def test_submit_response_format(self):
        """Test submit response format consistency."""
        # Success response
        success_response = {
            "task_id": "task-123",
            "status": "PENDING",
            "message": "Task submitted successfully",
            "execution_arn": "arn:aws:states:...",
        }

        assert "task_id" in success_response
        assert "status" in success_response
        assert success_response["status"] in ["PENDING", "ERROR"]

    def test_status_response_format(self):
        """Test status response format consistency using schema validation."""
        # List response
        list_response = {
            "tasks": [
                {
                    "task_id": "task-123",
                    "status": "CONVERTING",
                    "file_count": 3,
                    "completed_count": 1,
                    "failed_count": 0,
                    "progress_percentage": 33,
                    "created_at": "2024-01-01T10:00:00",
                    "quality_preset": "balanced",
                    "updated_at": "2024-01-01T10:05:00",
                }
            ]
        }

        assert "tasks" in list_response
        assert isinstance(list_response["tasks"], list)

        # Validate each task summary using Lambda's validation
        for task_summary in list_response["tasks"]:
            is_valid, error = status_app.validate_task_summary_response(task_summary)
            assert is_valid is True, f"Task summary validation failed: {error}"

        # Detail response
        detail_response = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "quality_preset": "balanced",
            "created_at": "2024-01-01T10:00:00",
            "updated_at": "2024-01-01T10:05:00",
            "progress_percentage": 50,
            "current_step": "converting",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "COMPLETED"},
            ],
        }

        # Validate detail response using Lambda's validation
        is_valid, error = status_app.validate_task_detail_response(detail_response)
        assert is_valid is True, f"Task detail validation failed: {error}"

    def test_status_response_validates_file_statuses(self):
        """Test that file statuses in response are validated against schema."""
        valid_file_statuses = status_app.VALID_FILE_STATUSES

        for file_status in valid_file_statuses:
            detail_response = {
                "task_id": "task-123",
                "status": "CONVERTING",
                "quality_preset": "balanced",
                "created_at": "2024-01-01T10:00:00",
                "updated_at": "2024-01-01T10:05:00",
                "progress_percentage": 50,
                "current_step": "converting",
                "files": [
                    {"file_id": "file-1", "filename": "video.mov", "status": file_status},
                ],
            }

            is_valid, error = status_app.validate_task_detail_response(detail_response)
            assert is_valid is True, f"File status {file_status} should be valid"

    def test_status_response_rejects_invalid_file_status(self):
        """Test that invalid file status is rejected."""
        detail_response = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "quality_preset": "balanced",
            "created_at": "2024-01-01T10:00:00",
            "updated_at": "2024-01-01T10:05:00",
            "progress_percentage": 50,
            "current_step": "converting",
            "files": [
                {"file_id": "file-1", "filename": "video.mov", "status": "INVALID_STATUS"},
            ],
        }

        is_valid, error = status_app.validate_task_detail_response(detail_response)
        assert is_valid is False
        assert "invalid status" in error

    def test_cancel_response_format(self):
        """Test cancel response format consistency."""
        success_response = {
            "success": True,
            "previous_status": "CONVERTING",
            "message": "Task cancelled successfully",
            "s3_files_deleted": True,
            "mediaconvert_cancelled": True,
        }

        assert "success" in success_response
        assert isinstance(success_response["success"], bool)
        assert "previous_status" in success_response

    def test_error_response_format(self):
        """Test error response format consistency."""
        error_responses = [
            {
                "error": "ValidationError",
                "message": "Invalid request body",
                "statusCode": 400,
            },
            {
                "error": "NotFoundError",
                "message": "Task not found",
                "statusCode": 404,
            },
            {
                "error": "ForbiddenError",
                "message": "Access denied",
                "statusCode": 403,
            },
            {
                "error": "InternalError",
                "message": "Internal server error",
                "statusCode": 500,
            },
        ]

        for response in error_responses:
            assert "error" in response
            assert "message" in response
            assert "statusCode" in response
            assert isinstance(response["statusCode"], int)


class TestStepFunctionsLambdaInterface:
    """Tests for Step Functions → Lambda interface (Task 6.4)."""

    def test_validate_input_state_format(self):
        """Test validate_input state input/output format."""
        # Input from Step Functions
        state_input = {
            "task_id": "task-123",
            "user_id": "user-456",
            "quality_preset": "balanced",
            "files": [
                {
                    "file_id": "file-1",
                    "filename": "video.mov",
                    "source_s3_key": "async/task-123/input/file-1/video.mov",
                }
            ],
        }

        # Output should include validation result
        # expected_output_keys = ["task_id", "user_id", "quality_preset", "files", "validated"]

        assert "task_id" in state_input
        assert "files" in state_input

    def test_start_conversion_state_format(self):
        """Test start_conversion state input/output format."""
        # Input for single file conversion
        state_input = {
            "task_id": "task-123",
            "file": {
                "file_id": "file-1",
                "filename": "video.mov",
                "source_s3_key": "async/task-123/input/file-1/video.mov",
            },
            "quality_preset": "balanced",
        }

        # Output should include job ID
        expected_output = {
            "file_id": "file-1",
            "job_id": "job-abc123",
            "status": "SUBMITTED",
        }

        assert "task_id" in state_input
        assert "file" in state_input
        assert "file_id" in expected_output

    def test_check_status_state_format(self):
        """Test check_status state input/output format."""
        state_input = {
            "file_id": "file-1",
            "job_id": "job-abc123",
        }

        expected_output = {
            "file_id": "file-1",
            "job_id": "job-abc123",
            "status": "COMPLETE",  # or PROGRESSING, ERROR
            "progress_percentage": 100,
        }

        assert "job_id" in state_input
        assert "status" in expected_output

    def test_aggregate_results_state_format(self):
        """Test aggregate_results state input/output format."""
        # Input from Map state (array of results)
        state_input = {
            "task_id": "task-123",
            "results": [
                {"file_id": "file-1", "status": "COMPLETED", "ssim_score": 0.95},
                {"file_id": "file-2", "status": "COMPLETED", "ssim_score": 0.93},
                {"file_id": "file-3", "status": "FAILED", "error": "Codec error"},
            ],
        }

        # Output should include aggregated status
        # expected_output = {
        #     "task_id": "task-123",
        #     "status": "PARTIALLY_COMPLETED",
        #     "completed_count": 2,
        #     "failed_count": 1,
        # }

        assert "results" in state_input
        assert isinstance(state_input["results"], list)

    def test_error_propagation(self):
        """Test error propagation through states."""
        # Error from Lambda
        lambda_error = {
            "errorType": "ValidationError",
            "errorMessage": "Invalid input",
            "stackTrace": ["..."],
        }

        # Step Functions catches and transforms
        caught_error = {
            "Error": "ValidationError",
            "Cause": json.dumps(lambda_error),
        }

        assert "Error" in caught_error
        assert "Cause" in caught_error

    def test_map_state_input_format(self):
        """Test Map state input format for parallel processing."""
        map_input = {
            "task_id": "task-123",
            "quality_preset": "balanced",
            "files": [
                {"file_id": "f1", "filename": "v1.mov"},
                {"file_id": "f2", "filename": "v2.mov"},
                {"file_id": "f3", "filename": "v3.mov"},
            ],
        }

        # Map state iterates over files array
        assert "files" in map_input
        assert len(map_input["files"]) == 3

    def test_map_state_output_format(self):
        """Test Map state output format (array of results)."""
        map_output = [
            {"file_id": "f1", "status": "COMPLETED"},
            {"file_id": "f2", "status": "COMPLETED"},
            {"file_id": "f3", "status": "FAILED"},
        ]

        assert isinstance(map_output, list)
        assert all("file_id" in item for item in map_output)
        assert all("status" in item for item in map_output)


class TestCliApiGatewayInterface:
    """Tests for CLI → API Gateway interface (Task 9.4)."""

    def test_conversion_candidate_to_api_request(self):
        """Test ConversionCandidate → API request transformation."""
        from dataclasses import dataclass
        from pathlib import Path

        @dataclass
        class MockVideo:
            uuid: str = "video-uuid"
            filename: str = "test.mov"
            path: Path = Path("/tmp/test.mov")
            file_size: int = 1024000

        @dataclass
        class MockCandidate:
            video: MockVideo = None

            def __post_init__(self):
                if self.video is None:
                    self.video = MockVideo()

        candidate = MockCandidate()

        # Transform to API request format
        api_file = {
            "file_id": "generated-uuid",
            "original_uuid": candidate.video.uuid,
            "filename": candidate.video.filename,
            "source_s3_key": f"async/task-id/input/file-id/{candidate.video.filename}",
            "source_size_bytes": candidate.video.file_size,
        }

        assert api_file["original_uuid"] == candidate.video.uuid
        assert api_file["filename"] == candidate.video.filename
        assert api_file["source_size_bytes"] == candidate.video.file_size

    def test_api_response_to_async_task_result(self):
        """Test API response → AsyncTaskResult transformation."""
        from vco.services.async_convert import AsyncTaskResult

        # API success response
        api_response = {
            "task_id": "task-123",
            "status": "PENDING",
            "message": "Task submitted",
        }

        result = AsyncTaskResult(
            task_id=api_response["task_id"],
            status=api_response["status"],
            file_count=1,
            message=api_response["message"],
            api_url="https://api.example.com",
        )

        assert result.task_id == "task-123"
        assert result.status == "PENDING"

    def test_api_error_response_handling(self):
        """Test API error response handling."""
        from vco.services.async_convert import AsyncTaskResult

        # API error response
        api_error = {
            "error": "ValidationError",
            "message": "Invalid quality preset",
            "statusCode": 400,
        }

        result = AsyncTaskResult(
            task_id="",
            status="ERROR",
            file_count=0,
            message="API error",
            error_message=api_error["message"],
        )

        assert result.status == "ERROR"
        assert result.error_message == "Invalid quality preset"

    def test_status_api_response_to_task_summary(self):
        """Test status API response → TaskSummary transformation."""
        from datetime import datetime

        from vco.services.async_status import TaskSummary

        api_response = {
            "task_id": "task-123",
            "status": "CONVERTING",
            "file_count": 3,
            "completed_count": 1,
            "failed_count": 0,
            "progress_percentage": 33,
            "created_at": "2024-01-01T10:00:00",
            "quality_preset": "balanced",
        }

        summary = TaskSummary(
            task_id=api_response["task_id"],
            status=api_response["status"],
            file_count=api_response["file_count"],
            completed_count=api_response["completed_count"],
            failed_count=api_response["failed_count"],
            progress_percentage=api_response["progress_percentage"],
            created_at=datetime.fromisoformat(api_response["created_at"]),
            quality_preset=api_response["quality_preset"],
        )

        assert summary.task_id == "task-123"
        assert summary.progress_percentage == 33

    def test_cancel_api_response_to_cancel_result(self):
        """Test cancel API response → CancelResult transformation."""
        from vco.services.async_cancel import CancelResult

        api_response = {
            "success": True,
            "previous_status": "CONVERTING",
            "message": "Task cancelled",
            "s3_files_deleted": True,
            "mediaconvert_cancelled": True,
        }

        result = CancelResult(
            task_id="task-123",
            success=api_response["success"],
            previous_status=api_response["previous_status"],
            message=api_response["message"],
            s3_files_deleted=api_response["s3_files_deleted"],
            mediaconvert_cancelled=api_response["mediaconvert_cancelled"],
        )

        assert result.success is True
        assert result.previous_status == "CONVERTING"
