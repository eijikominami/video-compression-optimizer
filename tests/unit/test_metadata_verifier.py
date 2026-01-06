"""Unit tests for MetadataVerifier."""

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from vco.metadata.verifier import MetadataVerifier
from vco.models.metadata import (
    ExtractedMetadata,
    GPSLocation,
    OriginalMetadata,
)


class TestMetadataVerifier:
    """Tests for MetadataVerifier class."""

    def test_verify_all_matching(self):
        """Test verification with all metadata matching."""
        original = OriginalMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=GPSLocation(35.6762, 139.6503),
            album_names=["Travel", "2024"],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=GPSLocation(35.6762, 139.6503),
        )

        verifier = MetadataVerifier()
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(
                Path("test.mp4"),
                original,
                processing_time=datetime(2020, 1, 1),  # Far from capture date
            )

        assert result.success is True
        assert result.has_mismatch is False
        assert result.capture_date.matches is True
        assert result.gps_location.matches is True
        assert result.album_info.matches is True

    def test_verify_capture_date_mismatch(self):
        """Test verification with capture date mismatch."""
        original = OriginalMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=None,
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 5),  # 5 seconds diff
        )

        verifier = MetadataVerifier(tolerance_seconds=1.0)
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(Path("test.mp4"), original)

        assert result.success is False
        assert result.capture_date.matches is False
        assert "5.0 seconds" in result.capture_date.error_message

    def test_verify_capture_date_within_tolerance(self):
        """Test verification with capture date within tolerance."""
        original = OriginalMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=None,
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0, 500000),  # 0.5 seconds diff
        )

        verifier = MetadataVerifier(tolerance_seconds=1.0)
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(Path("test.mp4"), original)

        assert result.capture_date.matches is True

    def test_verify_gps_missing_in_converted(self):
        """Test verification when GPS is missing in converted video."""
        original = OriginalMetadata(
            capture_date=None,
            gps_location=GPSLocation(35.6762, 139.6503),
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            gps_location=None,  # Missing GPS
        )

        verifier = MetadataVerifier()
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(Path("test.mp4"), original)

        assert result.success is False
        assert result.gps_location.matches is False
        assert "missing" in result.gps_location.error_message.lower()

    def test_verify_gps_within_tolerance(self):
        """Test verification with GPS within tolerance."""
        original = OriginalMetadata(
            capture_date=None,
            gps_location=GPSLocation(35.6762, 139.6503),
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            gps_location=GPSLocation(35.67625, 139.65035),  # Very close
        )

        verifier = MetadataVerifier(tolerance_degrees=0.0001)
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(Path("test.mp4"), original)

        assert result.gps_location.matches is True

    def test_verify_gps_outside_tolerance(self):
        """Test verification with GPS outside tolerance."""
        original = OriginalMetadata(
            capture_date=None,
            gps_location=GPSLocation(35.6762, 139.6503),
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            gps_location=GPSLocation(35.6772, 139.6513),  # 0.001 degrees diff
        )

        verifier = MetadataVerifier(tolerance_degrees=0.0001)
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(Path("test.mp4"), original)

        assert result.gps_location.matches is False

    def test_verify_both_gps_none(self):
        """Test verification when both GPS are None."""
        original = OriginalMetadata(
            capture_date=None,
            gps_location=None,
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            gps_location=None,
        )

        verifier = MetadataVerifier()
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(Path("test.mp4"), original)

        assert result.gps_location.matches is True

    def test_verify_processing_time_proximity_warning(self):
        """Test processing time proximity warning."""
        original = OriginalMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=None,
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
        )

        verifier = MetadataVerifier(processing_time_tolerance_hours=1.0)
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(
                Path("test.mp4"),
                original,
                processing_time=datetime(2024, 1, 15, 14, 0, 0),  # 30 min before
            )

        assert result.has_warning is True
        assert result.processing_time_warning is not None
        assert result.processing_time_warning.difference_hours == pytest.approx(0.5)
        # Warning should not affect success
        assert result.success is True

    def test_verify_no_processing_time_warning(self):
        """Test no warning when capture date is far from processing time."""
        original = OriginalMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=None,
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
        )

        verifier = MetadataVerifier(processing_time_tolerance_hours=1.0)
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(
                Path("test.mp4"),
                original,
                processing_time=datetime(2024, 1, 15, 10, 0, 0),  # 4.5 hours before
            )

        assert result.has_warning is False
        assert result.processing_time_warning is None

    def test_verify_mismatched_fields_list(self):
        """Test mismatched_fields property."""
        original = OriginalMetadata(
            capture_date=datetime(2024, 1, 15, 14, 30, 0),
            gps_location=GPSLocation(35.6762, 139.6503),
            album_names=[],
            original_uuid="abc-123",
            filename="test.mp4",
        )

        extracted = ExtractedMetadata(
            capture_date=datetime(2024, 1, 16, 14, 30, 0),  # Different day
            gps_location=None,  # Missing
        )

        verifier = MetadataVerifier()
        with patch.object(verifier.extractor, "extract", return_value=extracted):
            result = verifier.verify(Path("test.mp4"), original)

        assert "capture_date" in result.mismatched_fields
        assert "gps_location" in result.mismatched_fields
        assert "album_info" not in result.mismatched_fields


class TestCompareCaptureDate:
    """Tests for _compare_capture_date method."""

    def test_both_none(self):
        """Test comparison when both dates are None."""
        verifier = MetadataVerifier()
        result = verifier._compare_capture_date(None, None)
        assert result.matches is True

    def test_original_none(self):
        """Test comparison when original is None."""
        verifier = MetadataVerifier()
        result = verifier._compare_capture_date(None, datetime(2024, 1, 15))
        assert result.matches is False

    def test_converted_none(self):
        """Test comparison when converted is None."""
        verifier = MetadataVerifier()
        result = verifier._compare_capture_date(datetime(2024, 1, 15), None)
        assert result.matches is False

    def test_exact_match(self):
        """Test exact date match."""
        verifier = MetadataVerifier()
        dt = datetime(2024, 1, 15, 14, 30, 0)
        result = verifier._compare_capture_date(dt, dt)
        assert result.matches is True

    def test_within_tolerance(self):
        """Test dates within tolerance."""
        verifier = MetadataVerifier(tolerance_seconds=1.0)
        dt1 = datetime(2024, 1, 15, 14, 30, 0)
        dt2 = datetime(2024, 1, 15, 14, 30, 0, 999999)  # ~1 second diff
        result = verifier._compare_capture_date(dt1, dt2)
        assert result.matches is True

    def test_outside_tolerance(self):
        """Test dates outside tolerance."""
        verifier = MetadataVerifier(tolerance_seconds=1.0)
        dt1 = datetime(2024, 1, 15, 14, 30, 0)
        dt2 = datetime(2024, 1, 15, 14, 30, 2)  # 2 seconds diff
        result = verifier._compare_capture_date(dt1, dt2)
        assert result.matches is False


class TestCompareGPSLocation:
    """Tests for _compare_gps_location method."""

    def test_both_none(self):
        """Test comparison when both locations are None."""
        verifier = MetadataVerifier()
        result = verifier._compare_gps_location(None, None)
        assert result.matches is True

    def test_original_has_gps_converted_none(self):
        """Test when original has GPS but converted doesn't."""
        verifier = MetadataVerifier()
        result = verifier._compare_gps_location(GPSLocation(35.6762, 139.6503), None)
        assert result.matches is False
        assert "missing" in result.error_message.lower()

    def test_original_none_converted_has_gps(self):
        """Test when original doesn't have GPS but converted does."""
        verifier = MetadataVerifier()
        result = verifier._compare_gps_location(None, GPSLocation(35.6762, 139.6503))
        # This is considered a match (converted added GPS)
        assert result.matches is True

    def test_within_tolerance(self):
        """Test locations within tolerance."""
        verifier = MetadataVerifier(tolerance_degrees=0.0001)
        loc1 = GPSLocation(35.6762, 139.6503)
        loc2 = GPSLocation(35.67625, 139.65035)
        result = verifier._compare_gps_location(loc1, loc2)
        assert result.matches is True

    def test_outside_tolerance(self):
        """Test locations outside tolerance."""
        verifier = MetadataVerifier(tolerance_degrees=0.0001)
        loc1 = GPSLocation(35.6762, 139.6503)
        loc2 = GPSLocation(35.6772, 139.6513)  # 0.001 degrees diff
        result = verifier._compare_gps_location(loc1, loc2)
        assert result.matches is False


class TestCheckProcessingTimeProximity:
    """Tests for _check_processing_time_proximity method."""

    def test_capture_date_none(self):
        """Test when capture date is None."""
        verifier = MetadataVerifier()
        result = verifier._check_processing_time_proximity(None, datetime(2024, 1, 15))
        assert result is None

    def test_within_tolerance(self):
        """Test capture date within tolerance of processing time."""
        verifier = MetadataVerifier(processing_time_tolerance_hours=1.0)
        capture = datetime(2024, 1, 15, 14, 30, 0)
        processing = datetime(2024, 1, 15, 14, 0, 0)  # 30 min before
        result = verifier._check_processing_time_proximity(capture, processing)
        assert result is not None
        assert result.difference_hours == pytest.approx(0.5)

    def test_outside_tolerance(self):
        """Test capture date outside tolerance of processing time."""
        verifier = MetadataVerifier(processing_time_tolerance_hours=1.0)
        capture = datetime(2024, 1, 15, 14, 30, 0)
        processing = datetime(2024, 1, 15, 10, 0, 0)  # 4.5 hours before
        result = verifier._check_processing_time_proximity(capture, processing)
        assert result is None

    def test_negative_difference(self):
        """Test when capture date is before processing time."""
        verifier = MetadataVerifier(processing_time_tolerance_hours=1.0)
        capture = datetime(2024, 1, 15, 13, 30, 0)
        processing = datetime(2024, 1, 15, 14, 0, 0)  # 30 min after capture
        result = verifier._check_processing_time_proximity(capture, processing)
        assert result is not None
        assert result.difference_hours == pytest.approx(-0.5)
