"""Disk space utilities for video conversion.

This module provides disk space checking functionality to ensure
sufficient space is available before starting conversions.
"""

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


class InsufficientDiskSpaceError(Exception):
    """Raised when there is not enough disk space for conversion."""

    def __init__(self, required_bytes: int, available_bytes: int, path: Path):
        self.required_bytes = required_bytes
        self.available_bytes = available_bytes
        self.path = path

        required_gb = required_bytes / (1024**3)
        available_gb = available_bytes / (1024**3)

        super().__init__(
            f"Insufficient disk space at {path}: "
            f"required {required_gb:.2f} GB, available {available_gb:.2f} GB"
        )


def get_available_disk_space(path: Path) -> int:
    """Get available disk space at the given path.

    Args:
        path: Path to check (uses the filesystem containing this path)

    Returns:
        Available space in bytes
    """
    # Ensure path exists or use parent
    check_path = path
    while not check_path.exists() and check_path.parent != check_path:
        check_path = check_path.parent

    if not check_path.exists():
        check_path = Path.home()

    usage = shutil.disk_usage(check_path)
    return usage.free


def check_disk_space(source_file_size: int, target_path: Path, multiplier: float = 2.0) -> bool:
    """Check if there is sufficient disk space for conversion.

    The required space is calculated as source_file_size * multiplier.
    Default multiplier of 2.0 ensures space for both the converted file
    and temporary files during processing.

    Args:
        source_file_size: Size of the source file in bytes
        target_path: Path where converted file will be stored
        multiplier: Multiplier for required space (default 2.0)

    Returns:
        True if sufficient space is available

    Raises:
        InsufficientDiskSpaceError: If not enough space is available
    """
    required_space = int(source_file_size * multiplier)
    available_space = get_available_disk_space(target_path)

    if available_space < required_space:
        raise InsufficientDiskSpaceError(
            required_bytes=required_space, available_bytes=available_space, path=target_path
        )

    return True


def check_batch_disk_space(
    total_source_size: int, target_path: Path, multiplier: float = 2.0
) -> bool:
    """Check if there is sufficient disk space for batch conversion.

    For batch processing, we check if there's enough space for the
    largest expected concurrent operations.

    Args:
        total_source_size: Total size of all source files in bytes
        target_path: Path where converted files will be stored
        multiplier: Multiplier for required space (default 2.0)

    Returns:
        True if sufficient space is available

    Raises:
        InsufficientDiskSpaceError: If not enough space is available
    """
    return check_disk_space(total_source_size, target_path, multiplier)


def format_bytes(size_bytes: int) -> str:
    """Format bytes as human-readable string.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human-readable string (e.g., "1.5 GB")
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes / (1024**2):.1f} MB"
    else:
        return f"{size_bytes / (1024**3):.2f} GB"
