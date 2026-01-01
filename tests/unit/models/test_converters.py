"""Tests for data model converters."""

from datetime import datetime
from pathlib import Path

from vco.models.async_task import AsyncFile, FileStatus
from vco.models.converters import (
    async_file_to_conversion_result,
    video_info_to_async_file,
    video_info_to_conversion_result,
)
from vco.models.types import VideoInfo


class TestVideoInfoToAsyncFile:
    """Test cases for video_info_to_async_file converter."""

    def test_conversion_with_all_fields(self):
        """Test conversion with all VideoInfo fields."""
        video = VideoInfo(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            capture_date=datetime(2023, 12, 25, 10, 30, 45),
            location=(35.6762, 139.6503),
            path=Path("/path/to/video.mp4"),
            codec="h264",
            resolution=(1920, 1080),
            bitrate=5000000,
            duration=120.5,
            frame_rate=30.0,
            creation_date=datetime(2023, 12, 25, 10, 30, 45),
            albums=["Vacation", "Family"],
            is_in_icloud=False,
            is_local=True,
        )

        result = video_info_to_async_file(video, "file-id-123")

        # Check base fields are copied
        assert result.uuid == video.uuid
        assert result.filename == video.filename
        assert result.file_size == video.file_size
        assert result.capture_date == video.capture_date
        assert result.location == video.location

        # Check AsyncFile specific fields
        assert result.file_id == "file-id-123"
        assert result.source_s3_key == ""
        assert result.status == FileStatus.PENDING
        assert result.source_size_bytes == video.file_size

    def test_conversion_generates_file_id(self):
        """Test conversion generates UUID when file_id not provided."""
        video = VideoInfo(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            path=Path("/path/to/video.mp4"),
            codec="h264",
            resolution=(1920, 1080),
            bitrate=5000000,
            duration=120.5,
            frame_rate=30.0,
            creation_date=datetime.now(),
        )

        result = video_info_to_async_file(video)

        # Should generate a UUID
        assert result.file_id is not None
        assert len(result.file_id) == 36  # UUID format
        assert "-" in result.file_id


class TestAsyncFileToConversionResult:
    """Test cases for async_file_to_conversion_result converter."""

    def test_conversion_completed_file(self):
        """Test conversion of completed AsyncFile."""
        async_file = AsyncFile(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            capture_date=datetime(2023, 12, 25, 10, 30, 45),
            location=(35.6762, 139.6503),
            file_id="file-id-123",
            source_s3_key="input/test_video.mp4",
            status=FileStatus.COMPLETED,
            mediaconvert_job_id="job-123",
        )

        original_path = Path("/path/to/original.mp4")
        result = async_file_to_conversion_result(async_file, original_path)

        # Check base fields are copied
        assert result.uuid == async_file.uuid
        assert result.filename == async_file.filename
        assert result.file_size == async_file.file_size
        assert result.capture_date == async_file.capture_date
        assert result.location == async_file.location

        # Check ConversionResult specific fields
        assert result.success is True  # COMPLETED status
        assert result.original_path == original_path
        assert result.converted_path is None  # Will be set after download
        assert result.mediaconvert_job_id == async_file.mediaconvert_job_id

    def test_conversion_failed_file(self):
        """Test conversion of failed AsyncFile."""
        async_file = AsyncFile(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            file_id="file-id-123",
            source_s3_key="input/test_video.mp4",
            status=FileStatus.FAILED,
            error_message="Conversion failed",
        )

        original_path = Path("/path/to/original.mp4")
        result = async_file_to_conversion_result(async_file, original_path)

        # Check failure is reflected
        assert result.success is False  # FAILED status
        assert result.error_message == "Conversion failed"


class TestVideoInfoToConversionResult:
    """Test cases for video_info_to_conversion_result converter."""

    def test_conversion_success(self):
        """Test conversion with success=True."""
        video = VideoInfo(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            capture_date=datetime(2023, 12, 25, 10, 30, 45),
            location=(35.6762, 139.6503),
            path=Path("/path/to/video.mp4"),
            codec="h264",
            resolution=(1920, 1080),
            bitrate=5000000,
            duration=120.5,
            frame_rate=30.0,
            creation_date=datetime.now(),
        )

        result = video_info_to_conversion_result(video, success=True)

        # Check base fields are copied
        assert result.uuid == video.uuid
        assert result.filename == video.filename
        assert result.file_size == video.file_size
        assert result.capture_date == video.capture_date
        assert result.location == video.location

        # Check ConversionResult specific fields
        assert result.success is True
        assert result.original_path == video.path
        assert result.error_message is None

    def test_conversion_failure(self):
        """Test conversion with success=False and error message."""
        video = VideoInfo(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            path=Path("/path/to/video.mp4"),
            codec="h264",
            resolution=(1920, 1080),
            bitrate=5000000,
            duration=120.5,
            frame_rate=30.0,
            creation_date=datetime.now(),
        )

        error_msg = "File not found"
        result = video_info_to_conversion_result(video, success=False, error_message=error_msg)

        # Check failure is reflected
        assert result.success is False
        assert result.error_message == error_msg
