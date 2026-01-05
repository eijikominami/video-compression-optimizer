"""Unit tests for metadata verification models."""

from datetime import datetime

import pytest

from vco.models.metadata import (
    ExtractedMetadata,
    FieldVerificationResult,
    GPSLocation,
    OriginalMetadata,
    ProcessingTimeProximityWarning,
    VerificationResult,
)


class TestGPSLocation:
    """Tests for GPSLocation dataclass."""

    def test_from_iso6709_basic(self):
        """Test parsing basic ISO6709 format."""
        result = GPSLocation.from_iso6709("+35.6762+139.6503/")
        assert result is not None
        assert result.latitude == pytest.approx(35.6762)
        assert result.longitude == pytest.approx(139.6503)
        assert result.altitude is None

    def test_from_iso6709_with_altitude(self):
        """Test parsing ISO6709 format with altitude."""
        result = GPSLocation.from_iso6709("+35.6762+139.6503+100.5/")
        assert result is not None
        assert result.latitude == pytest.approx(35.6762)
        assert result.longitude == pytest.approx(139.6503)
        assert result.altitude == pytest.approx(100.5)

    def test_from_iso6709_negative_coordinates(self):
        """Test parsing ISO6709 format with negative coordinates."""
        result = GPSLocation.from_iso6709("-33.8688+151.2093/")
        assert result is not None
        assert result.latitude == pytest.approx(-33.8688)
        assert result.longitude == pytest.approx(151.2093)

    def test_from_iso6709_negative_longitude(self):
        """Test parsing ISO6709 format with negative longitude."""
        result = GPSLocation.from_iso6709("+40.7128-74.0060/")
        assert result is not None
        assert result.latitude == pytest.approx(40.7128)
        assert result.longitude == pytest.approx(-74.0060)

    def test_from_iso6709_without_trailing_slash(self):
        """Test parsing ISO6709 format without trailing slash."""
        result = GPSLocation.from_iso6709("+35.6762+139.6503")
        assert result is not None
        assert result.latitude == pytest.approx(35.6762)
        assert result.longitude == pytest.approx(139.6503)

    def test_from_iso6709_empty_string(self):
        """Test parsing empty string returns None."""
        assert GPSLocation.from_iso6709("") is None

    def test_from_iso6709_none(self):
        """Test parsing None returns None."""
        assert GPSLocation.from_iso6709(None) is None

    def test_from_iso6709_invalid_format(self):
        """Test parsing invalid format returns None."""
        assert GPSLocation.from_iso6709("invalid") is None
        assert GPSLocation.from_iso6709("35.6762, 139.6503") is None

    def test_from_iso6709_out_of_range_latitude(self):
        """Test parsing out of range latitude returns None."""
        assert GPSLocation.from_iso6709("+91.0000+139.6503/") is None
        assert GPSLocation.from_iso6709("-91.0000+139.6503/") is None

    def test_from_iso6709_out_of_range_longitude(self):
        """Test parsing out of range longitude returns None."""
        assert GPSLocation.from_iso6709("+35.6762+181.0000/") is None
        assert GPSLocation.from_iso6709("+35.6762-181.0000/") is None

    def test_distance_to_same_location(self):
        """Test distance to same location is zero."""
        loc = GPSLocation(latitude=35.6762, longitude=139.6503)
        assert loc.distance_to(loc) == 0.0

    def test_distance_to_different_location(self):
        """Test distance to different location."""
        loc1 = GPSLocation(latitude=35.6762, longitude=139.6503)
        loc2 = GPSLocation(latitude=35.6763, longitude=139.6504)
        distance = loc1.distance_to(loc2)
        assert distance == pytest.approx(0.0001, abs=0.00001)

    def test_distance_to_uses_max_difference(self):
        """Test distance uses maximum of lat/lon differences."""
        loc1 = GPSLocation(latitude=35.0, longitude=139.0)
        loc2 = GPSLocation(latitude=35.1, longitude=139.05)
        distance = loc1.distance_to(loc2)
        assert distance == pytest.approx(0.1)  # Max of 0.1 and 0.05

    def test_to_dict(self):
        """Test conversion to dictionary."""
        loc = GPSLocation(latitude=35.6762, longitude=139.6503, altitude=100.0)
        result = loc.to_dict()
        assert result == {
            "latitude": 35.6762,
            "longitude": 139.6503,
            "altitude": 100.0,
        }

    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {"latitude": 35.6762, "longitude": 139.6503, "altitude": 100.0}
        loc = GPSLocation.from_dict(data)
        assert loc is not None
        assert loc.latitude == 35.6762
        assert loc.longitude == 139.6503
        assert loc.altitude == 100.0

    def test_from_dict_none(self):
        """Test creation from None returns None."""
        assert GPSLocation.from_dict(None) is None

    def test_str_representation(self):
        """Test string representation."""
        loc = GPSLocation(latitude=35.6762, longitude=139.6503)
        assert str(loc) == "35.6762°N, 139.6503°E"

    def test_str_representation_negative(self):
        """Test string representation with negative coordinates."""
        loc = GPSLocation(latitude=-33.8688, longitude=-151.2093)
        assert str(loc) == "33.8688°S, 151.2093°W"


class TestExtractedMetadata:
    """Tests for ExtractedMetadata dataclass."""

    def test_default_values(self):
        """Test default values."""
        metadata = ExtractedMetadata()
        assert metadata.capture_date is None
        assert metadata.gps_location is None
        assert metadata.duration is None
        assert metadata.codec is None
        assert metadata.resolution is None
        assert metadata.extraction_errors == []

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metadata = ExtractedMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=GPSLocation(35.6762, 139.6503),
            duration=120.5,
            codec="hevc",
            resolution=(1920, 1080),
        )
        result = metadata.to_dict()
        assert result["capture_date"] == "2024-01-15T14:30:00"
        assert result["gps_location"]["latitude"] == 35.6762
        assert result["duration"] == 120.5
        assert result["codec"] == "hevc"
        assert result["resolution"] == [1920, 1080]


class TestFieldVerificationResult:
    """Tests for FieldVerificationResult dataclass."""

    def test_matching_field(self):
        """Test matching field result."""
        result = FieldVerificationResult(
            field_name="capture_date",
            matches=True,
            original_value=datetime(2024, 1, 15),
            converted_value=datetime(2024, 1, 15),
        )
        assert result.matches is True
        assert result.error_message is None
        assert result.skipped is False

    def test_mismatched_field(self):
        """Test mismatched field result."""
        result = FieldVerificationResult(
            field_name="capture_date",
            matches=False,
            original_value=datetime(2024, 1, 15),
            converted_value=datetime(2024, 1, 16),
            error_message="Dates differ by more than 1 second",
        )
        assert result.matches is False
        assert result.error_message is not None

    def test_skipped_field(self):
        """Test skipped field result."""
        result = FieldVerificationResult(
            field_name="gps_location",
            matches=True,
            original_value=None,
            converted_value=None,
            skipped=True,
        )
        assert result.skipped is True


class TestProcessingTimeProximityWarning:
    """Tests for ProcessingTimeProximityWarning dataclass."""

    def test_message_generation(self):
        """Test warning message generation."""
        warning = ProcessingTimeProximityWarning(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            processing_time=datetime(2024, 1, 15, 14, 0, 0),
            difference_hours=0.5,
        )
        message = warning.message
        assert "2024-01-15 14:30:00" in message
        assert "2024-01-15 14:00:00" in message
        assert "0.5 hours" in message
        assert "file creation time" in message

    def test_to_dict(self):
        """Test conversion to dictionary."""
        warning = ProcessingTimeProximityWarning(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            processing_time=datetime(2024, 1, 15, 14, 0, 0),
            difference_hours=0.5,
        )
        result = warning.to_dict()
        assert result["capture_date"] == "2024-01-15T14:30:00"
        assert result["processing_time"] == "2024-01-15T14:00:00"
        assert result["difference_hours"] == 0.5
        assert "message" in result


class TestVerificationResult:
    """Tests for VerificationResult dataclass."""

    def test_successful_verification(self):
        """Test successful verification result."""
        result = VerificationResult(
            success=True,
            filename="test.mp4",
            capture_date=FieldVerificationResult(
                "capture_date", True, datetime(2024, 1, 15), datetime(2024, 1, 15)
            ),
            gps_location=FieldVerificationResult(
                "gps_location", True, GPSLocation(35.0, 139.0), GPSLocation(35.0, 139.0)
            ),
            album_info=FieldVerificationResult("album_info", True, ["Album1"], ["Album1"]),
        )
        assert result.success is True
        assert result.has_mismatch is False
        assert result.has_warning is False
        assert result.mismatched_fields == []

    def test_failed_verification(self):
        """Test failed verification result."""
        result = VerificationResult(
            success=False,
            filename="test.mp4",
            capture_date=FieldVerificationResult(
                "capture_date", False, datetime(2024, 1, 15), datetime(2024, 1, 16)
            ),
            gps_location=FieldVerificationResult(
                "gps_location", False, GPSLocation(35.0, 139.0), None
            ),
            album_info=FieldVerificationResult("album_info", True, ["Album1"], ["Album1"]),
        )
        assert result.success is False
        assert result.has_mismatch is True
        assert result.mismatched_fields == ["capture_date", "gps_location"]

    def test_verification_with_warning(self):
        """Test verification result with processing time warning."""
        warning = ProcessingTimeProximityWarning(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            processing_time=datetime(2024, 1, 15, 14, 0, 0),
            difference_hours=0.5,
        )
        result = VerificationResult(
            success=True,
            filename="test.mp4",
            capture_date=FieldVerificationResult(
                "capture_date", True, datetime(2024, 1, 15), datetime(2024, 1, 15)
            ),
            gps_location=FieldVerificationResult(
                "gps_location", True, GPSLocation(35.0, 139.0), GPSLocation(35.0, 139.0)
            ),
            album_info=FieldVerificationResult("album_info", True, ["Album1"], ["Album1"]),
            processing_time_warning=warning,
        )
        assert result.has_warning is True
        assert result.success is True  # Warning doesn't affect success


class TestOriginalMetadata:
    """Tests for OriginalMetadata dataclass."""

    def test_from_dict_basic(self):
        """Test creation from basic dictionary."""
        data = {
            "capture_date": "2024-01-15T14:30:00",
            "gps_location": {"latitude": 35.6762, "longitude": 139.6503},
            "album_names": ["Travel", "2024"],
            "original_uuid": "abc-123",
            "filename": "test.mp4",
        }
        metadata = OriginalMetadata.from_dict(data)
        assert metadata.capture_date == datetime(2024, 1, 15, 14, 30, 0)
        assert metadata.gps_location.latitude == 35.6762
        assert metadata.album_names == ["Travel", "2024"]
        assert metadata.original_uuid == "abc-123"
        assert metadata.filename == "test.mp4"

    def test_from_dict_with_location_tuple(self):
        """Test creation from dictionary with location as tuple."""
        data = {
            "capture_date": "2024-01-15T14:30:00",
            "location": [35.6762, 139.6503],
            "albums": ["Travel"],
            "uuid": "abc-123",
            "filename": "test.mp4",
        }
        metadata = OriginalMetadata.from_dict(data)
        assert metadata.gps_location.latitude == 35.6762
        assert metadata.gps_location.longitude == 139.6503
        assert metadata.album_names == ["Travel"]
        assert metadata.original_uuid == "abc-123"

    def test_from_dict_with_datetime_object(self):
        """Test creation from dictionary with datetime object."""
        data = {
            "capture_date": datetime(2024, 1, 15, 14, 30, 0),
            "album_names": [],
            "original_uuid": "abc-123",
            "filename": "test.mp4",
        }
        metadata = OriginalMetadata.from_dict(data)
        assert metadata.capture_date == datetime(2024, 1, 15, 14, 30, 0)

    def test_from_dict_missing_optional_fields(self):
        """Test creation from dictionary with missing optional fields."""
        data = {
            "original_uuid": "abc-123",
            "filename": "test.mp4",
        }
        metadata = OriginalMetadata.from_dict(data)
        assert metadata.capture_date is None
        assert metadata.gps_location is None
        assert metadata.album_names == []

    def test_to_dict(self):
        """Test conversion to dictionary."""
        metadata = OriginalMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=GPSLocation(35.6762, 139.6503),
            album_names=["Travel", "2024"],
            original_uuid="abc-123",
            filename="test.mp4",
        )
        result = metadata.to_dict()
        assert result["capture_date"] == "2024-01-15T14:30:00"
        assert result["gps_location"]["latitude"] == 35.6762
        assert result["album_names"] == ["Travel", "2024"]
        assert result["original_uuid"] == "abc-123"
        assert result["filename"] == "test.mp4"
