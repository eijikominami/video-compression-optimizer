"""Tests for base data models."""

from datetime import datetime

import pytest

from vco.models.base import BaseVideoMetadata


class TestBaseVideoMetadata:
    """Test cases for BaseVideoMetadata class."""

    def test_roundtrip_conversion_full(self):
        """Test to_dict -> from_dict roundtrip with all fields."""
        original = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            capture_date=datetime(2023, 12, 25, 10, 30, 45),
            location=(35.6762, 139.6503),
        )

        # Convert to dict and back
        data_dict = original.to_dict()
        restored = BaseVideoMetadata.from_dict(data_dict)

        # Should be identical
        assert restored.uuid == original.uuid
        assert restored.filename == original.filename
        assert restored.file_size == original.file_size
        assert restored.capture_date == original.capture_date
        assert restored.location == original.location

    def test_roundtrip_conversion_minimal(self):
        """Test to_dict -> from_dict roundtrip with minimal fields."""
        original = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
        )

        # Convert to dict and back
        data_dict = original.to_dict()
        restored = BaseVideoMetadata.from_dict(data_dict)

        # Should be identical
        assert restored.uuid == original.uuid
        assert restored.filename == original.filename
        assert restored.file_size == original.file_size
        assert restored.capture_date is None
        assert restored.location is None

    def test_to_dict_format(self):
        """Test to_dict produces expected format."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            capture_date=datetime(2023, 12, 25, 10, 30, 45),
            location=(35.6762, 139.6503),
        )

        result = metadata.to_dict()

        expected = {
            "uuid": "test-uuid-123",
            "filename": "test_video.mp4",
            "file_size": 1024000,
            "capture_date": "2023-12-25T10:30:45",
            "location": [35.6762, 139.6503],
        }

        assert result == expected

    def test_to_dict_with_none_values(self):
        """Test to_dict handles None values correctly."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024000,
            capture_date=None,
            location=None,
        )

        result = metadata.to_dict()

        expected = {
            "uuid": "test-uuid-123",
            "filename": "test_video.mp4",
            "file_size": 1024000,
            "capture_date": None,
            "location": None,
        }

        assert result == expected

    def test_from_dict_missing_required_fields(self):
        """Test from_dict raises error for missing required fields."""
        # Missing uuid
        with pytest.raises(KeyError):
            BaseVideoMetadata.from_dict(
                {
                    "filename": "test_video.mp4",
                    "file_size": 1024000,
                }
            )

        # Missing filename
        with pytest.raises(KeyError):
            BaseVideoMetadata.from_dict(
                {
                    "uuid": "test-uuid-123",
                    "file_size": 1024000,
                }
            )

        # Missing file_size
        with pytest.raises(KeyError):
            BaseVideoMetadata.from_dict(
                {
                    "uuid": "test-uuid-123",
                    "filename": "test_video.mp4",
                }
            )

    def test_from_dict_invalid_datetime(self):
        """Test from_dict raises error for invalid datetime format."""
        with pytest.raises(ValueError):
            BaseVideoMetadata.from_dict(
                {
                    "uuid": "test-uuid-123",
                    "filename": "test_video.mp4",
                    "file_size": 1024000,
                    "capture_date": "invalid-datetime",
                }
            )

    def test_from_dict_invalid_location(self):
        """Test from_dict handles invalid location gracefully."""
        # tuple() converts string to tuple of characters
        metadata = BaseVideoMetadata.from_dict(
            {
                "uuid": "test-uuid-123",
                "filename": "test_video.mp4",
                "file_size": 1024000,
                "location": "ab",  # Will become ('a', 'b')
            }
        )

        # Should create object but location will be invalid characters
        assert metadata.uuid == "test-uuid-123"
        assert metadata.location == ("a", "b")  # Characters, not coordinates
