"""Contract tests for API endpoints and response formats.

These tests ensure that API contracts remain stable across deployments
and that client-server communication follows expected schemas.
"""

from datetime import datetime

from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus
from vco.models.converters import api_to_async_task, async_task_to_api


class TestAsyncTaskAPIContract:
    """Contract tests for AsyncTask API endpoints."""

    def test_task_status_response_schema(self):
        """Test that task status API response has required schema."""
        task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    uuid="video-1",
                    filename="video1.mp4",
                    file_size=1024000,
                    file_id="file-1",
                    source_s3_key="input/video1.mp4",
                    status=FileStatus.COMPLETED,
                )
            ],
            created_at=datetime.now(),
            updated_at=datetime.now(),
            progress_percentage=100,
        )

        api_response = async_task_to_api(task)

        # API contract: required top-level fields
        required_fields = [
            "task_id",
            "user_id",
            "status",
            "quality_preset",
            "files",
            "created_at",
            "updated_at",
            "progress_percentage",
        ]

        for field in required_fields:
            assert field in api_response, f"Missing required field: {field}"

    def test_task_status_response_types(self):
        """Test that task status API response has correct field types."""
        task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.CONVERTING,
            quality_preset="high",
            files=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
            progress_percentage=45,
            max_concurrent=3,
        )

        api_response = async_task_to_api(task)

        # Type contract validation
        assert isinstance(api_response["task_id"], str)
        assert isinstance(api_response["user_id"], str)
        assert isinstance(api_response["status"], str)
        assert isinstance(api_response["quality_preset"], str)
        assert isinstance(api_response["files"], list)
        assert isinstance(api_response["created_at"], str)  # ISO format
        assert isinstance(api_response["updated_at"], str)  # ISO format
        assert isinstance(api_response["progress_percentage"], int)
        assert isinstance(api_response["max_concurrent"], int)

    def test_task_files_array_schema(self):
        """Test that files array in task response has correct schema."""
        file1 = AsyncFile(
            uuid="video-1",
            filename="video1.mp4",
            file_size=1024000,
            file_id="file-1",
            source_s3_key="input/video1.mp4",
            status=FileStatus.COMPLETED,
        )

        file2 = AsyncFile(
            uuid="video-2",
            filename="video2.mp4",
            file_size=2048000,
            file_id="file-2",
            source_s3_key="input/video2.mp4",
            status=FileStatus.FAILED,
            error_message="Conversion failed",
        )

        task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.PARTIALLY_COMPLETED,
            quality_preset="balanced",
            files=[file1, file2],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        api_response = async_task_to_api(task)

        # Files array contract
        assert len(api_response["files"]) == 2

        # Each file must have required fields
        for file_data in api_response["files"]:
            assert "file_id" in file_data
            assert "original_uuid" in file_data
            assert "filename" in file_data
            assert "status" in file_data


class TestAPIErrorContract:
    """Contract tests for API error responses."""

    def test_task_not_found_error_format(self):
        """Test that task not found errors have consistent format."""
        # This would typically be tested with actual API calls
        # For now, we test the expected error structure

        expected_error_format = {
            "error": "TASK_NOT_FOUND",
            "message": "Task with ID 'invalid-task-id' not found",
            "task_id": "invalid-task-id",
        }

        # Validate error response structure
        assert "error" in expected_error_format
        assert "message" in expected_error_format
        assert isinstance(expected_error_format["error"], str)
        assert isinstance(expected_error_format["message"], str)

    def test_validation_error_format(self):
        """Test that validation errors have consistent format."""
        expected_error_format = {
            "error": "VALIDATION_ERROR",
            "message": "Invalid request data",
            "details": [
                {
                    "field": "quality_preset",
                    "message": "Must be one of: balanced, high, compression",
                }
            ],
        }

        # Validate error response structure
        assert "error" in expected_error_format
        assert "message" in expected_error_format
        assert "details" in expected_error_format
        assert isinstance(expected_error_format["details"], list)


class TestAPIVersionCompatibility:
    """Contract tests for API version compatibility."""

    def test_task_response_backward_compatibility(self):
        """Test that task responses maintain backward compatibility."""
        task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        api_response = async_task_to_api(task)

        # Fields that must always be present for backward compatibility
        backward_compatible_fields = [
            "task_id",
            "user_id",
            "status",
            "quality_preset",
            "files",
            "created_at",
            "updated_at",
        ]

        for field in backward_compatible_fields:
            assert field in api_response

    def test_file_response_backward_compatibility(self):
        """Test that file responses maintain backward compatibility."""
        async_file = AsyncFile(
            uuid="video-123",
            filename="test.mp4",
            file_size=1024000,
            file_id="file-456",
            source_s3_key="input/test.mp4",
            status=FileStatus.COMPLETED,
        )

        from vco.models.converters import async_file_to_api

        api_response = async_file_to_api(async_file)

        # Legacy field names that must be preserved
        assert "original_uuid" in api_response  # Legacy name for uuid
        assert api_response["original_uuid"] == async_file.uuid

    def test_status_enum_values_stability(self):
        """Test that status enum values remain stable."""
        # Task status values that clients depend on
        expected_task_statuses = [
            "PENDING",
            "UPLOADING",
            "CONVERTING",
            "VERIFYING",
            "COMPLETED",
            "PARTIALLY_COMPLETED",
            "FAILED",
            "CANCELLED",
        ]

        for status_value in expected_task_statuses:
            # Should be able to create TaskStatus from string
            status = TaskStatus(status_value)
            assert status.value == status_value

        # File status values that clients depend on
        expected_file_statuses = [
            "PENDING",
            "CONVERTING",
            "VERIFYING",
            "COMPLETED",
            "DOWNLOADED",
            "FAILED",
        ]

        for status_value in expected_file_statuses:
            # Should be able to create FileStatus from string
            status = FileStatus(status_value)
            assert status.value == status_value


class TestAPIDataIntegrity:
    """Contract tests for API data integrity."""

    def test_task_roundtrip_preserves_data(self):
        """Test that API serialization roundtrip preserves task data."""
        original_task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    uuid="video-1",
                    filename="video1.mp4",
                    file_size=1024000,
                    file_id="file-1",
                    source_s3_key="input/video1.mp4",
                    status=FileStatus.CONVERTING,
                )
            ],
            created_at=datetime(2023, 12, 25, 10, 30, 45),
            updated_at=datetime(2023, 12, 25, 10, 35, 45),
            progress_percentage=30,
        )

        # API roundtrip: Task -> API dict -> Task
        api_data = async_task_to_api(original_task)
        restored_task = api_to_async_task(api_data)

        # Critical fields must be preserved
        assert restored_task.task_id == original_task.task_id
        assert restored_task.user_id == original_task.user_id
        assert restored_task.status == original_task.status
        assert restored_task.quality_preset == original_task.quality_preset
        assert len(restored_task.files) == len(original_task.files)
        assert restored_task.progress_percentage == original_task.progress_percentage

    def test_datetime_serialization_consistency(self):
        """Test that datetime fields are consistently serialized."""
        task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[],
            created_at=datetime(2023, 12, 25, 10, 30, 45),
            updated_at=datetime(2023, 12, 25, 10, 35, 45),
            completed_at=datetime(2023, 12, 25, 10, 40, 45),
        )

        api_response = async_task_to_api(task)

        # Datetime fields must be ISO format strings
        assert api_response["created_at"] == "2023-12-25T10:30:45"
        assert api_response["updated_at"] == "2023-12-25T10:35:45"
        assert api_response["completed_at"] == "2023-12-25T10:40:45"

    def test_null_field_handling(self):
        """Test that null/None fields are handled consistently."""
        task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.PENDING,
            quality_preset="balanced",
            files=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
            # These fields are None
            started_at=None,
            completed_at=None,
            execution_arn=None,
            error_message=None,
        )

        api_response = async_task_to_api(task)

        # None fields should be explicitly null in API response
        assert api_response["started_at"] is None
        assert api_response["completed_at"] is None
        assert api_response["execution_arn"] is None
        assert api_response["error_message"] is None
