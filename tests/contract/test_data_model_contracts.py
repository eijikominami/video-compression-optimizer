"""Contract tests for data model schemas and API interfaces.

These tests ensure that data structures remain consistent across
different components of the distributed system.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from vco.models.async_task import AsyncFile, FileStatus
from vco.models.base import BaseVideoMetadata
from vco.models.converters import (
    api_to_async_file,
    async_file_to_api,
    video_info_to_async_file,
)
from vco.models.types import VideoInfo


class TestBaseVideoMetadataContract:
    """Contract tests for BaseVideoMetadata schema."""

    def test_base_metadata_required_fields(self):
        """Test that BaseVideoMetadata has all required fields."""
        metadata = BaseVideoMetadata(uuid="test-123", filename="test.mp4", file_size=1024)

        data = metadata.to_dict()

        # Required fields must be present
        assert "uuid" in data
        assert "filename" in data
        assert "file_size" in data
        assert "capture_date" in data  # Can be None
        assert "location" in data  # Can be None

    def test_base_metadata_field_types(self):
        """Test that BaseVideoMetadata fields have correct types."""
        metadata = BaseVideoMetadata(
            uuid="test-123",
            filename="test.mp4",
            file_size=1024,
            capture_date=datetime(2023, 12, 25, 10, 30, 45),
            location=(35.6762, 139.6503),
        )

        data = metadata.to_dict()

        # Type validation
        assert isinstance(data["uuid"], str)
        assert isinstance(data["filename"], str)
        assert isinstance(data["file_size"], int)
        assert isinstance(data["capture_date"], str)  # ISO format
        assert isinstance(data["location"], list)
        assert len(data["location"]) == 2

    def test_base_metadata_roundtrip_contract(self):
        """Test that BaseVideoMetadata roundtrip preserves data."""
        original = BaseVideoMetadata(
            uuid="test-123",
            filename="test.mp4",
            file_size=1024,
            capture_date=datetime(2023, 12, 25, 10, 30, 45),
            location=(35.6762, 139.6503),
        )

        # Roundtrip: object -> dict -> object
        data = original.to_dict()
        restored = BaseVideoMetadata.from_dict(data)

        # All fields must be preserved
        assert restored.uuid == original.uuid
        assert restored.filename == original.filename
        assert restored.file_size == original.file_size
        assert restored.capture_date == original.capture_date
        assert restored.location == original.location


class TestAsyncFileAPIContract:
    """Contract tests for AsyncFile API interface."""

    def test_async_file_api_response_schema(self):
        """Test AsyncFile API response has required schema."""
        async_file = AsyncFile(
            uuid="video-123",
            filename="test.mp4",
            file_size=1024000,
            file_id="file-456",
            source_s3_key="input/test.mp4",
            status=FileStatus.COMPLETED,
        )

        api_response = async_file_to_api(async_file)

        # API contract: required fields
        required_fields = [
            "file_id",
            "original_uuid",
            "filename",
            "source_s3_key",
            "status",
            "retry_count",
            "preset_attempts",
        ]

        for field in required_fields:
            assert field in api_response, f"Missing required field: {field}"

    def test_async_file_api_field_types(self):
        """Test AsyncFile API response field types."""
        async_file = AsyncFile(
            uuid="video-123",
            filename="test.mp4",
            file_size=1024000,
            file_id="file-456",
            source_s3_key="input/test.mp4",
            status=FileStatus.COMPLETED,
            retry_count=2,
            preset_attempts=["balanced", "high"],
        )

        api_response = async_file_to_api(async_file)

        # Type contract validation
        assert isinstance(api_response["file_id"], str)
        assert isinstance(api_response["original_uuid"], str)
        assert isinstance(api_response["filename"], str)
        assert isinstance(api_response["status"], str)
        assert isinstance(api_response["retry_count"], int)
        assert isinstance(api_response["preset_attempts"], list)

    def test_async_file_api_roundtrip_contract(self):
        """Test AsyncFile API roundtrip preserves essential data."""
        original = AsyncFile(
            uuid="video-123",
            filename="test.mp4",
            file_size=1024000,
            file_id="file-456",
            source_s3_key="input/test.mp4",
            status=FileStatus.COMPLETED,
        )

        # API roundtrip: AsyncFile -> API dict -> AsyncFile
        api_data = async_file_to_api(original)
        restored = api_to_async_file(api_data)

        # Essential fields must be preserved
        assert restored.uuid == original.uuid
        assert restored.filename == original.filename
        assert restored.file_size == original.file_size
        assert restored.file_id == original.file_id
        assert restored.source_s3_key == original.source_s3_key
        assert restored.status == original.status


class TestModelConversionContract:
    """Contract tests for model-to-model conversions."""

    def test_video_info_to_async_file_contract(self):
        """Test VideoInfo -> AsyncFile conversion preserves base fields."""
        video = VideoInfo(
            uuid="video-123",
            filename="test.mp4",
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

        async_file = video_info_to_async_file(video, "file-456")

        # Base fields must be preserved
        assert async_file.uuid == video.uuid
        assert async_file.filename == video.filename
        assert async_file.file_size == video.file_size
        assert async_file.capture_date == video.capture_date
        assert async_file.location == video.location

        # AsyncFile specific fields must be set
        assert async_file.file_id == "file-456"
        assert async_file.status == FileStatus.PENDING

    def test_conversion_preserves_data_integrity(self):
        """Test that conversions don't corrupt data."""
        video = VideoInfo(
            uuid="video-123",
            filename="test.mp4",
            file_size=1024000,
            path=Path("/path/to/video.mp4"),
            codec="h264",
            resolution=(1920, 1080),
            bitrate=5000000,
            duration=120.5,
            frame_rate=30.0,
            creation_date=datetime.now(),
        )

        # Multi-step conversion
        async_file = video_info_to_async_file(video)
        api_data = async_file_to_api(async_file)
        restored_async_file = api_to_async_file(api_data)

        # Core data must survive the conversion chain
        assert restored_async_file.uuid == video.uuid
        assert restored_async_file.filename == video.filename
        assert restored_async_file.file_size == video.file_size


class TestErrorResponseContract:
    """Contract tests for error response formats."""

    def test_validation_error_format(self):
        """Test that validation errors have consistent format."""
        from vco.models.validators import validate_base_metadata

        invalid_metadata = BaseVideoMetadata(
            uuid="",  # Invalid: empty string
            filename="test.mp4",
            file_size=1024,
        )

        with pytest.raises(ValueError) as exc_info:
            validate_base_metadata(invalid_metadata)

        error_message = str(exc_info.value)

        # Error message contract
        assert "uuid" in error_message
        assert "required" in error_message
        assert "empty" in error_message

    def test_conversion_error_handling(self):
        """Test that conversion errors are handled consistently."""
        invalid_api_data = {
            "file_id": "file-123",
            # Missing required fields: filename, etc.
        }

        with pytest.raises((KeyError, TypeError)):
            api_to_async_file(invalid_api_data)


class TestSchemaCompatibility:
    """Contract tests for schema compatibility."""

    def test_json_serialization_compatibility(self):
        """Test that models can be JSON serialized/deserialized."""
        async_file = AsyncFile(
            uuid="video-123",
            filename="test.mp4",
            file_size=1024000,
            file_id="file-456",
            source_s3_key="input/test.mp4",
            status=FileStatus.COMPLETED,
        )

        # JSON roundtrip
        api_data = async_file_to_api(async_file)
        json_str = json.dumps(api_data)
        parsed_data = json.loads(json_str)
        restored = api_to_async_file(parsed_data)

        # Data must survive JSON serialization
        assert restored.uuid == async_file.uuid
        assert restored.filename == async_file.filename
        assert restored.status == async_file.status

    def test_backward_compatibility_original_uuid(self):
        """Test backward compatibility with original_uuid field."""
        # API data with old field name
        api_data = {
            "file_id": "file-123",
            "original_uuid": "video-456",  # Old field name
            "filename": "test.mp4",
            "source_s3_key": "input/test.mp4",
            "status": "COMPLETED",
        }

        async_file = api_to_async_file(api_data)

        # Should map to new uuid field
        assert async_file.uuid == "video-456"
        assert async_file.original_uuid == "video-456"  # Property should work

    def test_api_response_includes_original_uuid(self):
        """Test that API responses include original_uuid for compatibility."""
        async_file = AsyncFile(
            uuid="video-123",
            filename="test.mp4",
            file_size=1024000,
            file_id="file-456",
            source_s3_key="input/test.mp4",
        )

        api_response = async_file_to_api(async_file)

        # Both uuid and original_uuid should be present
        assert "original_uuid" in api_response
        assert api_response["original_uuid"] == async_file.uuid
