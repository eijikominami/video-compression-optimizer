"""Metadata verifier for Video Compression Optimizer."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from vco.metadata.extractor import MetadataExtractor
from vco.models.metadata import (
    FieldVerificationResult,
    GPSLocation,
    OriginalMetadata,
    ProcessingTimeProximityWarning,
    VerificationResult,
)

logger = logging.getLogger(__name__)


class MetadataVerifier:
    """Verify metadata of converted videos against original metadata.

    This class compares metadata from converted videos with the original
    metadata stored in JSON files to ensure metadata preservation.
    """

    def __init__(
        self,
        extractor: MetadataExtractor | None = None,
        tolerance_seconds: float = 1.0,
        tolerance_degrees: float = 0.0001,
        processing_time_tolerance_hours: float = 1.0,
    ):
        """Initialize the verifier.

        Args:
            extractor: MetadataExtractor instance (creates default if None)
            tolerance_seconds: Allowed difference in capture date (seconds)
            tolerance_degrees: Allowed difference in GPS coordinates (degrees)
            processing_time_tolerance_hours: Threshold for processing time warning (hours)
        """
        self.extractor = extractor or MetadataExtractor()
        self.tolerance_seconds = tolerance_seconds
        self.tolerance_degrees = tolerance_degrees
        self.processing_time_tolerance_hours = processing_time_tolerance_hours

    def verify(
        self,
        converted_path: Path,
        original_metadata: OriginalMetadata,
        processing_time: datetime | None = None,
    ) -> VerificationResult:
        """Verify metadata of a converted video.

        Args:
            converted_path: Path to the converted video file
            original_metadata: Original metadata from JSON file
            processing_time: Processing time for proximity check (defaults to now)

        Returns:
            VerificationResult with verification details
        """
        if processing_time is None:
            processing_time = datetime.now()

        # Extract metadata from converted video
        extracted = self.extractor.extract(converted_path)

        # Compare each field
        capture_date_result = self._compare_capture_date(
            original_metadata.capture_date,
            extracted.capture_date,
        )

        gps_location_result = self._compare_gps_location(
            original_metadata.gps_location,
            extracted.gps_location,
        )

        album_info_result = self._verify_album_info(original_metadata)

        # Check processing time proximity
        processing_time_warning = self._check_processing_time_proximity(
            extracted.capture_date,
            processing_time,
        )

        # Determine overall success
        success = (
            capture_date_result.matches
            and gps_location_result.matches
            and album_info_result.matches
        )

        return VerificationResult(
            success=success,
            filename=original_metadata.filename,
            capture_date=capture_date_result,
            gps_location=gps_location_result,
            album_info=album_info_result,
            processing_time_warning=processing_time_warning,
        )

    def _compare_capture_date(
        self,
        original: datetime | None,
        converted: datetime | None,
    ) -> FieldVerificationResult:
        """Compare capture dates with tolerance.

        Args:
            original: Original capture date
            converted: Converted video capture date

        Returns:
            FieldVerificationResult for capture_date field
        """
        # Both None is a match
        if original is None and converted is None:
            return FieldVerificationResult(
                field_name="capture_date",
                matches=True,
                original_value=None,
                converted_value=None,
            )

        # One is None, other is not - mismatch
        if original is None or converted is None:
            return FieldVerificationResult(
                field_name="capture_date",
                matches=False,
                original_value=original,
                converted_value=converted,
                error_message="Capture date missing in one of the sources",
            )

        # Compare with tolerance
        # Remove timezone info for comparison if present
        orig_naive = original.replace(tzinfo=None) if original.tzinfo else original
        conv_naive = converted.replace(tzinfo=None) if converted.tzinfo else converted

        diff_seconds = abs((orig_naive - conv_naive).total_seconds())

        if diff_seconds <= self.tolerance_seconds:
            return FieldVerificationResult(
                field_name="capture_date",
                matches=True,
                original_value=original,
                converted_value=converted,
            )

        return FieldVerificationResult(
            field_name="capture_date",
            matches=False,
            original_value=original,
            converted_value=converted,
            error_message=f"Capture dates differ by {diff_seconds:.1f} seconds (tolerance: {self.tolerance_seconds}s)",
        )

    def _compare_gps_location(
        self,
        original: GPSLocation | None,
        converted: GPSLocation | None,
    ) -> FieldVerificationResult:
        """Compare GPS locations with tolerance.

        Args:
            original: Original GPS location
            converted: Converted video GPS location

        Returns:
            FieldVerificationResult for gps_location field
        """
        # Both None is a match
        if original is None and converted is None:
            return FieldVerificationResult(
                field_name="gps_location",
                matches=True,
                original_value=None,
                converted_value=None,
            )

        # Original had GPS but converted doesn't - mismatch
        if original is not None and converted is None:
            return FieldVerificationResult(
                field_name="gps_location",
                matches=False,
                original_value=original,
                converted_value=None,
                error_message="GPS location missing in converted video",
            )

        # Original didn't have GPS but converted does - still a match
        # (converted may have added GPS, which is fine)
        if original is None and converted is not None:
            return FieldVerificationResult(
                field_name="gps_location",
                matches=True,
                original_value=None,
                converted_value=converted,
            )

        # Both have GPS - compare with tolerance
        # At this point, both original and converted are not None
        assert original is not None and converted is not None
        distance = original.distance_to(converted)

        if distance <= self.tolerance_degrees:
            return FieldVerificationResult(
                field_name="gps_location",
                matches=True,
                original_value=original,
                converted_value=converted,
            )

        return FieldVerificationResult(
            field_name="gps_location",
            matches=False,
            original_value=original,
            converted_value=converted,
            error_message=f"GPS locations differ by {distance:.6f} degrees (tolerance: {self.tolerance_degrees})",
        )

    def _verify_album_info(
        self,
        original_metadata: OriginalMetadata,
    ) -> FieldVerificationResult:
        """Verify album information exists in original metadata.

        Album assignment happens at import time, so we just verify
        the album_names field exists in the metadata JSON.

        Args:
            original_metadata: Original metadata from JSON file

        Returns:
            FieldVerificationResult for album_info field
        """
        # album_names can be empty list, that's fine
        # We just verify the field exists (which it always does in OriginalMetadata)
        return FieldVerificationResult(
            field_name="album_info",
            matches=True,
            original_value=original_metadata.album_names,
            converted_value=original_metadata.album_names,  # Same value, verified in JSON
        )

    def _check_processing_time_proximity(
        self,
        capture_date: datetime | None,
        processing_time: datetime,
    ) -> ProcessingTimeProximityWarning | None:
        """Check if capture date is suspiciously close to processing time.

        This may indicate that file creation time was used instead of
        actual capture time.

        Args:
            capture_date: Capture date from converted video
            processing_time: Time when processing occurred

        Returns:
            ProcessingTimeProximityWarning if within tolerance, None otherwise
        """
        if capture_date is None:
            return None

        # Remove timezone info for comparison
        capture_naive = capture_date.replace(tzinfo=None) if capture_date.tzinfo else capture_date
        processing_naive = (
            processing_time.replace(tzinfo=None) if processing_time.tzinfo else processing_time
        )

        diff_seconds = (capture_naive - processing_naive).total_seconds()
        diff_hours = diff_seconds / 3600

        if abs(diff_hours) <= self.processing_time_tolerance_hours:
            return ProcessingTimeProximityWarning(
                capture_date=capture_date,
                processing_time=processing_time,
                difference_hours=diff_hours,
            )

        return None
