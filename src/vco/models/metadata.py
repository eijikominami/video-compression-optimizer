"""Metadata verification models for Video Compression Optimizer."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class GPSLocation:
    """GPS location information.

    Attributes:
        latitude: Latitude in degrees (-90 to 90)
        longitude: Longitude in degrees (-180 to 180)
        altitude: Altitude in meters (optional)
    """

    latitude: float
    longitude: float
    altitude: float | None = None

    @classmethod
    def from_iso6709(cls, iso_string: str) -> GPSLocation | None:
        """Parse GPS location from ISO6709 format.

        ISO6709 format examples:
        - "+35.6762+139.6503/"
        - "+35.6762+139.6503+0.000/"
        - "+35.6762-139.6503/"

        Args:
            iso_string: ISO6709 formatted location string

        Returns:
            GPSLocation if parsing succeeds, None otherwise
        """
        if not iso_string:
            return None

        # Pattern: +/-lat+/-lon[+/-alt]/
        # Latitude: +/- followed by digits and optional decimal
        # Longitude: +/- followed by digits and optional decimal
        # Altitude: optional +/- followed by digits and optional decimal
        pattern = r"^([+-]?\d+\.?\d*)([+-]\d+\.?\d*)([+-]\d+\.?\d*)?/?$"
        match = re.match(pattern, iso_string.strip())

        if not match:
            return None

        try:
            latitude = float(match.group(1))
            longitude = float(match.group(2))
            altitude = float(match.group(3)) if match.group(3) else None

            # Validate ranges
            if not (-90 <= latitude <= 90):
                return None
            if not (-180 <= longitude <= 180):
                return None

            return cls(latitude=latitude, longitude=longitude, altitude=altitude)
        except (ValueError, TypeError):
            return None

    def distance_to(self, other: GPSLocation) -> float:
        """Calculate maximum coordinate difference to another location.

        Uses simple degree-based distance (max of lat/lon differences).
        Suitable for small distances where Earth curvature is negligible.

        Args:
            other: Another GPS location

        Returns:
            Maximum difference in degrees
        """
        return max(
            abs(self.latitude - other.latitude),
            abs(self.longitude - other.longitude),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude": self.altitude,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GPSLocation | None:
        """Create instance from dictionary."""
        if not data:
            return None
        return cls(
            latitude=data["latitude"],
            longitude=data["longitude"],
            altitude=data.get("altitude"),
        )

    def __str__(self) -> str:
        """Human-readable string representation."""
        lat_dir = "N" if self.latitude >= 0 else "S"
        lon_dir = "E" if self.longitude >= 0 else "W"
        return f"{abs(self.latitude):.4f}°{lat_dir}, {abs(self.longitude):.4f}°{lon_dir}"


@dataclass
class ExtractedMetadata:
    """Metadata extracted from video file using ffprobe.

    Attributes:
        capture_date: Video capture/creation date
        gps_location: GPS location if available
        duration: Video duration in seconds
        codec: Video codec name
        resolution: Video resolution as (width, height)
        extraction_errors: List of errors during extraction
    """

    capture_date: datetime | None = None
    gps_location: GPSLocation | None = None
    duration: float | None = None
    codec: str | None = None
    resolution: tuple[int, int] | None = None
    extraction_errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "capture_date": self.capture_date.isoformat() if self.capture_date else None,
            "gps_location": self.gps_location.to_dict() if self.gps_location else None,
            "duration": self.duration,
            "codec": self.codec,
            "resolution": list(self.resolution) if self.resolution else None,
            "extraction_errors": self.extraction_errors,
        }


@dataclass
class FieldVerificationResult:
    """Result of verifying a single metadata field.

    Attributes:
        field_name: Name of the field being verified
        matches: Whether the field values match
        original_value: Value from original metadata
        converted_value: Value from converted video
        error_message: Error message if verification failed
        skipped: Whether verification was skipped (e.g., ffprobe failure)
    """

    field_name: str
    matches: bool
    original_value: Any
    converted_value: Any
    error_message: str | None = None
    skipped: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "field_name": self.field_name,
            "matches": self.matches,
            "original_value": str(self.original_value) if self.original_value else None,
            "converted_value": str(self.converted_value) if self.converted_value else None,
            "error_message": self.error_message,
            "skipped": self.skipped,
        }


@dataclass
class ProcessingTimeProximityWarning:
    """Warning when capture date is close to processing time.

    This may indicate that file creation time was used instead of
    actual capture time.

    Attributes:
        capture_date: Capture date from converted video
        processing_time: Time when processing occurred
        difference_hours: Difference in hours between capture and processing
    """

    capture_date: datetime
    processing_time: datetime
    difference_hours: float

    @property
    def message(self) -> str:
        """Generate warning message."""
        return (
            f"Warning: Capture date ({self.capture_date.strftime('%Y-%m-%d %H:%M:%S')}) "
            f"is within {abs(self.difference_hours):.1f} hours of processing time "
            f"({self.processing_time.strftime('%Y-%m-%d %H:%M:%S')}). "
            "This may indicate file creation time instead of actual capture time."
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "capture_date": self.capture_date.isoformat(),
            "processing_time": self.processing_time.isoformat(),
            "difference_hours": self.difference_hours,
            "message": self.message,
        }


@dataclass
class VerificationResult:
    """Result of metadata verification.

    Attributes:
        success: Whether all verifications passed
        filename: Name of the verified file
        capture_date: Capture date verification result
        gps_location: GPS location verification result
        album_info: Album info verification result
        processing_time_warning: Warning if capture date is near processing time
    """

    success: bool
    filename: str
    capture_date: FieldVerificationResult
    gps_location: FieldVerificationResult
    album_info: FieldVerificationResult
    processing_time_warning: ProcessingTimeProximityWarning | None = None

    @property
    def has_mismatch(self) -> bool:
        """Check if any field has a mismatch."""
        return not (
            self.capture_date.matches and self.gps_location.matches and self.album_info.matches
        )

    @property
    def has_warning(self) -> bool:
        """Check if there is a processing time proximity warning."""
        return self.processing_time_warning is not None

    @property
    def mismatched_fields(self) -> list[str]:
        """Get list of field names that have mismatches."""
        fields = []
        if not self.capture_date.matches:
            fields.append("capture_date")
        if not self.gps_location.matches:
            fields.append("gps_location")
        if not self.album_info.matches:
            fields.append("album_info")
        return fields

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "success": self.success,
            "filename": self.filename,
            "capture_date": self.capture_date.to_dict(),
            "gps_location": self.gps_location.to_dict(),
            "album_info": self.album_info.to_dict(),
            "processing_time_warning": (
                self.processing_time_warning.to_dict() if self.processing_time_warning else None
            ),
            "has_mismatch": self.has_mismatch,
            "has_warning": self.has_warning,
            "mismatched_fields": self.mismatched_fields,
        }


@dataclass
class OriginalMetadata:
    """Original video metadata loaded from JSON file.

    Attributes:
        capture_date: Original capture date
        gps_location: Original GPS location
        album_names: List of album names
        original_uuid: UUID of original video in Photos library
        filename: Original filename
    """

    capture_date: datetime | None
    gps_location: GPSLocation | None
    album_names: list[str]
    original_uuid: str
    filename: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "capture_date": self.capture_date.isoformat() if self.capture_date else None,
            "gps_location": self.gps_location.to_dict() if self.gps_location else None,
            "album_names": self.album_names,
            "original_uuid": self.original_uuid,
            "filename": self.filename,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OriginalMetadata:
        """Create instance from dictionary."""
        gps_data = data.get("gps_location") or data.get("location")
        gps_location = None
        if gps_data:
            if isinstance(gps_data, dict):
                gps_location = GPSLocation.from_dict(gps_data)
            elif isinstance(gps_data, (list, tuple)) and len(gps_data) >= 2:
                gps_location = GPSLocation(
                    latitude=gps_data[0],
                    longitude=gps_data[1],
                    altitude=gps_data[2] if len(gps_data) > 2 else None,
                )

        capture_date = None
        capture_date_field = data.get("capture_date") or data.get("creation_date")
        if capture_date_field:
            if isinstance(capture_date_field, datetime):
                capture_date = capture_date_field
            else:
                capture_date = datetime.fromisoformat(capture_date_field)

        return cls(
            capture_date=capture_date,
            gps_location=gps_location,
            album_names=data.get("album_names", data.get("albums", [])),
            original_uuid=data.get("original_uuid", data.get("uuid", "")),
            filename=data.get("filename", data.get("original_filename", "")),
        )
