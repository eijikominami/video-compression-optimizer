"""Tests for data model validators."""

from datetime import datetime

import pytest

from vco.models.base import BaseVideoMetadata
from vco.models.validators import validate_base_metadata


class TestValidateBaseMetadata:
    """Test cases for validate_base_metadata function."""

    def test_valid_metadata(self):
        """Test validation passes for valid metadata."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024,
            capture_date=datetime.now(),
            location=(35.6762, 139.6503),  # Tokyo coordinates
        )

        # Should not raise any exception
        validate_base_metadata(metadata)

    def test_valid_metadata_minimal(self):
        """Test validation passes for minimal valid metadata."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024,
        )

        # Should not raise any exception
        validate_base_metadata(metadata)

    def test_empty_uuid(self):
        """Test validation fails for empty uuid."""
        metadata = BaseVideoMetadata(
            uuid="",
            filename="test_video.mp4",
            file_size=1024,
        )

        with pytest.raises(ValueError, match="uuid is required and cannot be empty"):
            validate_base_metadata(metadata)

    def test_empty_filename(self):
        """Test validation fails for empty filename."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="",
            file_size=1024,
        )

        with pytest.raises(ValueError, match="filename is required and cannot be empty"):
            validate_base_metadata(metadata)

    def test_zero_file_size(self):
        """Test validation fails for zero file size."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=0,
        )

        with pytest.raises(ValueError, match="file_size must be positive"):
            validate_base_metadata(metadata)

    def test_negative_file_size(self):
        """Test validation fails for negative file size."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=-100,
        )

        with pytest.raises(ValueError, match="file_size must be positive"):
            validate_base_metadata(metadata)

    def test_invalid_location_format(self):
        """Test validation fails for invalid location format."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024,
            location=(35.6762,),  # Only one coordinate
        )

        with pytest.raises(ValueError, match="location must be \\(latitude, longitude\\)"):
            validate_base_metadata(metadata)

    def test_invalid_latitude_range(self):
        """Test validation fails for latitude out of range."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024,
            location=(91.0, 139.6503),  # Latitude > 90
        )

        with pytest.raises(ValueError, match="latitude must be between -90 and 90"):
            validate_base_metadata(metadata)

    def test_invalid_longitude_range(self):
        """Test validation fails for longitude out of range."""
        metadata = BaseVideoMetadata(
            uuid="test-uuid-123",
            filename="test_video.mp4",
            file_size=1024,
            location=(35.6762, 181.0),  # Longitude > 180
        )

        with pytest.raises(ValueError, match="longitude must be between -180 and 180"):
            validate_base_metadata(metadata)

    def test_valid_location_edge_cases(self):
        """Test validation passes for edge case coordinates."""
        # Test all edge cases
        edge_cases = [
            (90.0, 180.0),  # Max values
            (-90.0, -180.0),  # Min values
            (0.0, 0.0),  # Zero values
        ]

        for lat, lon in edge_cases:
            metadata = BaseVideoMetadata(
                uuid="test-uuid-123",
                filename="test_video.mp4",
                file_size=1024,
                location=(lat, lon),
            )

            # Should not raise any exception
            validate_base_metadata(metadata)
