"""Base data models for video metadata.

This module provides the foundation for all video metadata models
to ensure consistency and type safety across the application.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

__all__ = ["BaseVideoMetadata"]


@dataclass
class BaseVideoMetadata:
    """Base class for all video metadata models.

    Provides common fields and methods that all video metadata
    models should inherit from to ensure consistency.

    Attributes:
        uuid: Unique identifier for the video
        filename: Original filename of the video
        file_size: Size of the video file in bytes
        capture_date: When the video was captured (optional)
        location: GPS coordinates as (latitude, longitude) (optional)
    """

    uuid: str = ""
    filename: str = ""
    file_size: int = 0
    capture_date: datetime | None = None
    location: tuple[float, float] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format.

        Returns:
            Dictionary with all fields explicitly converted
        """
        return {
            "uuid": self.uuid,
            "filename": self.filename,
            "file_size": self.file_size,
            "capture_date": self.capture_date.isoformat() if self.capture_date else None,
            "location": list(self.location) if self.location else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BaseVideoMetadata:
        """Create instance from dictionary.

        Args:
            data: Dictionary containing the data

        Returns:
            New instance with data from dictionary

        Raises:
            KeyError: If required fields are missing
            ValueError: If data format is invalid
        """
        return cls(
            uuid=data["uuid"],
            filename=data["filename"],
            file_size=data["file_size"],
            capture_date=datetime.fromisoformat(data["capture_date"])
            if data.get("capture_date")
            else None,
            location=tuple(data["location"]) if data.get("location") else None,
        )
