"""Unit tests for data model converters.

Tests the conversion functions between CLI models and API formats.

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5
"""

from datetime import datetime

import pytest

from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus
from vco.models.converters import (
    api_to_async_file,
    api_to_async_task,
    async_file_to_api,
    async_task_to_api,
)


class TestAsyncFileConversion:
    """Tests for AsyncFile conversion functions."""

    def test_async_file_to_api_basic(self):
        """Test basic AsyncFile to API conversion."""
        file = AsyncFile(
            file_id="file-123",
            original_uuid="uuid-456",
            filename="test.mov",
            source_s3_key="async/task-1/input/file-123/test.mov",
        )

        result = async_file_to_api(file)

        assert result["file_id"] == "file-123"
        assert result["original_uuid"] == "uuid-456"
        assert result["filename"] == "test.mov"
        assert result["source_s3_key"] == "async/task-1/input/file-123/test.mov"
        assert result["status"] == "PENDING"
        assert result["retry_count"] == 0
        assert result["checksum_algorithm"] == "ETag"

    def test_async_file_to_api_with_all_fields(self):
        """Test AsyncFile to API conversion with all fields populated."""
        file = AsyncFile(
            file_id="file-123",
            original_uuid="uuid-456",
            filename="test.mov",
            source_s3_key="async/task-1/input/file-123/test.mov",
            output_s3_key="output/task-1/file-123/test_h265.mp4",
            metadata_s3_key="async/task-1/input/file-123/metadata.json",
            status=FileStatus.COMPLETED,
            mediaconvert_job_id="job-789",
            quality_result={"ssim_score": 0.97, "compression_ratio": 2.5},
            error_code=None,
            error_message=None,
            retry_count=1,
            preset_attempts=["balanced", "high"],
            source_size_bytes=1000000,
            output_size_bytes=400000,
            output_checksum="abc123",
            checksum_algorithm="SHA256",
        )

        result = async_file_to_api(file)

        assert result["file_id"] == "file-123"
        assert result["output_s3_key"] == "output/task-1/file-123/test_h265.mp4"
        assert result["status"] == "COMPLETED"
        assert result["mediaconvert_job_id"] == "job-789"
        assert result["quality_result"]["ssim_score"] == 0.97
        assert result["retry_count"] == 1
        assert result["preset_attempts"] == ["balanced", "high"]
        assert result["source_size_bytes"] == 1000000
        assert result["output_size_bytes"] == 400000
        assert result["output_checksum"] == "abc123"
        assert result["checksum_algorithm"] == "SHA256"

    def test_api_to_async_file_basic(self):
        """Test basic API to AsyncFile conversion."""
        data = {
            "file_id": "file-123",
            "filename": "test.mov",
        }

        result = api_to_async_file(data)

        assert result.file_id == "file-123"
        assert result.filename == "test.mov"
        assert result.original_uuid == ""
        assert result.source_s3_key == ""
        assert result.status == FileStatus.PENDING
        assert result.retry_count == 0

    def test_api_to_async_file_with_all_fields(self):
        """Test API to AsyncFile conversion with all fields."""
        data = {
            "file_id": "file-123",
            "original_uuid": "uuid-456",
            "filename": "test.mov",
            "source_s3_key": "async/task-1/input/file-123/test.mov",
            "output_s3_key": "output/task-1/file-123/test_h265.mp4",
            "metadata_s3_key": "async/task-1/input/file-123/metadata.json",
            "status": "COMPLETED",
            "mediaconvert_job_id": "job-789",
            "quality_result": {"ssim_score": 0.97},
            "error_code": None,
            "error_message": None,
            "retry_count": 2,
            "preset_attempts": ["balanced"],
            "source_size_bytes": 1000000,
            "output_size_bytes": 400000,
            "output_checksum": "abc123",
            "checksum_algorithm": "SHA256",
        }

        result = api_to_async_file(data)

        assert result.file_id == "file-123"
        assert result.original_uuid == "uuid-456"
        assert result.output_s3_key == "output/task-1/file-123/test_h265.mp4"
        assert result.status == FileStatus.COMPLETED
        assert result.mediaconvert_job_id == "job-789"
        assert result.quality_result["ssim_score"] == 0.97
        assert result.retry_count == 2
        assert result.preset_attempts == ["balanced"]
        assert result.checksum_algorithm == "SHA256"

    def test_async_file_roundtrip(self):
        """Test AsyncFile -> API -> AsyncFile roundtrip preserves data."""
        original = AsyncFile(
            file_id="file-123",
            original_uuid="uuid-456",
            filename="test.mov",
            source_s3_key="async/task-1/input/file-123/test.mov",
            output_s3_key="output/task-1/file-123/test_h265.mp4",
            metadata_s3_key="async/task-1/input/file-123/metadata.json",
            status=FileStatus.COMPLETED,
            mediaconvert_job_id="job-789",
            quality_result={"ssim_score": 0.97},
            retry_count=1,
            preset_attempts=["balanced", "high"],
            source_size_bytes=1000000,
            output_size_bytes=400000,
            output_checksum="abc123",
            checksum_algorithm="SHA256",
        )

        api_data = async_file_to_api(original)
        restored = api_to_async_file(api_data)

        assert restored.file_id == original.file_id
        assert restored.original_uuid == original.original_uuid
        assert restored.filename == original.filename
        assert restored.source_s3_key == original.source_s3_key
        assert restored.output_s3_key == original.output_s3_key
        assert restored.metadata_s3_key == original.metadata_s3_key
        assert restored.status == original.status
        assert restored.mediaconvert_job_id == original.mediaconvert_job_id
        assert restored.quality_result == original.quality_result
        assert restored.retry_count == original.retry_count
        assert restored.preset_attempts == original.preset_attempts
        assert restored.source_size_bytes == original.source_size_bytes
        assert restored.output_size_bytes == original.output_size_bytes
        assert restored.output_checksum == original.output_checksum
        assert restored.checksum_algorithm == original.checksum_algorithm


class TestAsyncTaskConversion:
    """Tests for AsyncTask conversion functions."""

    def test_async_task_to_api_basic(self):
        """Test basic AsyncTask to API conversion."""
        now = datetime.now()
        task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.PENDING,
            quality_preset="balanced",
            files=[],
            created_at=now,
            updated_at=now,
        )

        result = async_task_to_api(task)

        assert result["task_id"] == "task-123"
        assert result["user_id"] == "user-456"
        assert result["status"] == "PENDING"
        assert result["quality_preset"] == "balanced"
        assert result["files"] == []
        assert result["progress_percentage"] == 0
        assert result["max_concurrent"] == 5

    def test_async_task_to_api_with_files(self):
        """Test AsyncTask to API conversion with files."""
        now = datetime.now()
        file = AsyncFile(
            file_id="file-123",
            original_uuid="uuid-456",
            filename="test.mov",
            source_s3_key="async/task-1/input/file-123/test.mov",
            status=FileStatus.COMPLETED,
        )
        task = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[file],
            created_at=now,
            updated_at=now,
            completed_at=now,
            progress_percentage=100,
            current_step="completed",
        )

        result = async_task_to_api(task)

        assert result["task_id"] == "task-123"
        assert result["status"] == "COMPLETED"
        assert len(result["files"]) == 1
        assert result["files"][0]["file_id"] == "file-123"
        assert result["files"][0]["status"] == "COMPLETED"
        assert result["progress_percentage"] == 100
        assert result["current_step"] == "completed"

    def test_api_to_async_task_basic(self):
        """Test basic API to AsyncTask conversion."""
        now = datetime.now()
        data = {
            "task_id": "task-123",
            "user_id": "user-456",
            "status": "PENDING",
            "quality_preset": "balanced",
            "files": [],
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        result = api_to_async_task(data)

        assert result.task_id == "task-123"
        assert result.user_id == "user-456"
        assert result.status == TaskStatus.PENDING
        assert result.quality_preset == "balanced"
        assert result.files == []
        assert result.progress_percentage == 0
        assert result.max_concurrent == 5

    def test_api_to_async_task_with_files(self):
        """Test API to AsyncTask conversion with files."""
        now = datetime.now()
        data = {
            "task_id": "task-123",
            "user_id": "user-456",
            "status": "COMPLETED",
            "quality_preset": "high",
            "files": [
                {
                    "file_id": "file-123",
                    "filename": "test.mov",
                    "status": "COMPLETED",
                }
            ],
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "completed_at": now.isoformat(),
            "progress_percentage": 100,
            "current_step": "completed",
        }

        result = api_to_async_task(data)

        assert result.task_id == "task-123"
        assert result.status == TaskStatus.COMPLETED
        assert len(result.files) == 1
        assert result.files[0].file_id == "file-123"
        assert result.files[0].status == FileStatus.COMPLETED
        assert result.progress_percentage == 100
        assert result.current_step == "completed"

    def test_async_task_roundtrip(self):
        """Test AsyncTask -> API -> AsyncTask roundtrip preserves data."""
        now = datetime.now()
        file = AsyncFile(
            file_id="file-123",
            original_uuid="uuid-456",
            filename="test.mov",
            source_s3_key="async/task-1/input/file-123/test.mov",
            status=FileStatus.COMPLETED,
        )
        original = AsyncTask(
            task_id="task-123",
            user_id="user-456",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[file],
            created_at=now,
            updated_at=now,
            started_at=now,
            completed_at=now,
            execution_arn="arn:aws:states:...",
            progress_percentage=100,
            current_step="completed",
            max_concurrent=10,
        )

        api_data = async_task_to_api(original)
        restored = api_to_async_task(api_data)

        assert restored.task_id == original.task_id
        assert restored.user_id == original.user_id
        assert restored.status == original.status
        assert restored.quality_preset == original.quality_preset
        assert len(restored.files) == len(original.files)
        assert restored.files[0].file_id == original.files[0].file_id
        assert restored.execution_arn == original.execution_arn
        assert restored.progress_percentage == original.progress_percentage
        assert restored.current_step == original.current_step
        assert restored.max_concurrent == original.max_concurrent


class TestEdgeCases:
    """Tests for edge cases in conversion."""

    def test_api_to_async_file_missing_optional_fields(self):
        """Test conversion handles missing optional fields gracefully."""
        data = {
            "file_id": "file-123",
            "filename": "test.mov",
            # All other fields missing
        }

        result = api_to_async_file(data)

        assert result.file_id == "file-123"
        assert result.filename == "test.mov"
        assert result.original_uuid == ""
        assert result.source_s3_key == ""
        assert result.output_s3_key is None
        assert result.metadata_s3_key is None
        assert result.status == FileStatus.PENDING
        assert result.mediaconvert_job_id is None
        assert result.quality_result is None
        assert result.error_code is None
        assert result.error_message is None
        assert result.retry_count == 0
        assert result.preset_attempts == []
        assert result.source_size_bytes is None
        assert result.output_size_bytes is None
        assert result.output_checksum is None
        assert result.checksum_algorithm == "ETag"

    def test_async_file_to_api_none_values(self):
        """Test conversion handles None values correctly."""
        file = AsyncFile(
            file_id="file-123",
            original_uuid="uuid-456",
            filename="test.mov",
            source_s3_key="async/task-1/input/file-123/test.mov",
            output_s3_key=None,
            metadata_s3_key=None,
            quality_result=None,
            error_code=None,
            error_message=None,
        )

        result = async_file_to_api(file)

        assert result["output_s3_key"] is None
        assert result["metadata_s3_key"] is None
        assert result["quality_result"] is None
        assert result["error_code"] is None
        assert result["error_message"] is None

    def test_api_to_async_file_invalid_status(self):
        """Test conversion raises error for invalid status."""
        data = {
            "file_id": "file-123",
            "filename": "test.mov",
            "status": "INVALID_STATUS",
        }

        with pytest.raises(ValueError):
            api_to_async_file(data)

    def test_api_to_async_task_invalid_status(self):
        """Test conversion raises error for invalid task status."""
        now = datetime.now()
        data = {
            "task_id": "task-123",
            "user_id": "user-456",
            "status": "INVALID_STATUS",
            "quality_preset": "balanced",
            "files": [],
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        with pytest.raises(ValueError):
            api_to_async_task(data)
