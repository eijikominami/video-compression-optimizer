"""Validation functions for data models.

This module provides validation functions for all data models
to ensure data integrity and provide clear error messages.
"""

from vco.models.base import BaseVideoMetadata

__all__ = ["validate_base_metadata"]


def validate_base_metadata(metadata: BaseVideoMetadata) -> None:
    """Validate base video metadata fields.

    Args:
        metadata: The metadata to validate

    Raises:
        ValueError: If any validation fails with specific error message
    """
    if not metadata.uuid:
        raise ValueError("uuid is required and cannot be empty")

    if not metadata.filename:
        raise ValueError("filename is required and cannot be empty")

    if metadata.file_size <= 0:
        raise ValueError("file_size must be positive")

    if metadata.location is not None:
        if len(metadata.location) != 2:
            raise ValueError("location must be (latitude, longitude)")

        lat, lon = metadata.location
        if not (-90 <= lat <= 90):
            raise ValueError("latitude must be between -90 and 90")
        if not (-180 <= lon <= 180):
            raise ValueError("longitude must be between -180 and 180")
