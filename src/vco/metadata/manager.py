"""Metadata manager for video file metadata extraction and application.

This module handles:
1. Extracting metadata from video files (capture_date, creation_date, albums)
2. Applying metadata to converted video files
3. Setting file system dates on video files
"""

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class VideoMetadata:
    """Video metadata for preservation during conversion."""

    capture_date: datetime | None = None
    creation_date: datetime | None = None
    albums: list[str] = field(default_factory=list)
    title: str | None = None
    description: str | None = None
    location: tuple[float, float] | None = None  # (latitude, longitude)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "capture_date": self.capture_date.isoformat() if self.capture_date else None,
            "creation_date": self.creation_date.isoformat() if self.creation_date else None,
            "albums": self.albums,
            "title": self.title,
            "description": self.description,
            "location": list(self.location) if self.location else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "VideoMetadata":
        """Create from dictionary."""
        capture_date = None
        if data.get("capture_date"):
            capture_date = datetime.fromisoformat(data["capture_date"])

        creation_date = None
        if data.get("creation_date"):
            creation_date = datetime.fromisoformat(data["creation_date"])

        location = None
        if data.get("location"):
            location = tuple(data["location"])

        return cls(
            capture_date=capture_date,
            creation_date=creation_date,
            albums=data.get("albums", []),
            title=data.get("title"),
            description=data.get("description"),
            location=location,
        )


class MetadataManager:
    """Manager for video metadata extraction and application."""

    def __init__(self):
        """Initialize MetadataManager."""
        pass

    def extract_metadata(self, video_path: Path) -> VideoMetadata:
        """Extract metadata from a video file using FFprobe.

        Args:
            video_path: Path to the video file

        Returns:
            VideoMetadata with extracted information
        """
        if not video_path.exists():
            raise FileNotFoundError(f"Video file not found: {video_path}")

        # Get file creation/modification times
        stat = video_path.stat()
        creation_date = datetime.fromtimestamp(stat.st_birthtime)

        # Try to extract metadata using FFprobe
        capture_date = None
        title = None
        description = None
        location = None

        try:
            probe_data = self._run_ffprobe(video_path)

            # Extract creation_time from format tags
            format_tags = probe_data.get("format", {}).get("tags", {})

            # Try various date fields
            for date_field in ["creation_time", "date", "com.apple.quicktime.creationdate"]:
                if date_field in format_tags:
                    try:
                        date_str = format_tags[date_field]
                        capture_date = self._parse_date(date_str)
                        break
                    except (ValueError, TypeError):
                        continue

            # Extract title
            title = format_tags.get("title")

            # Extract description/comment
            description = format_tags.get("description") or format_tags.get("comment")

            # Extract location from QuickTime metadata
            location_str = format_tags.get("com.apple.quicktime.location.ISO6709")
            if location_str:
                location = self._parse_location(location_str)

        except Exception:
            # If FFprobe fails, use file system dates only
            pass

        return VideoMetadata(
            capture_date=capture_date,
            creation_date=creation_date,
            albums=[],  # Albums are managed by Photos, not in file metadata
            title=title,
            description=description,
            location=location,
        )

    def apply_metadata(self, video_path: Path, metadata: VideoMetadata) -> bool:
        """Apply metadata to a video file.

        Note: This uses FFmpeg to copy metadata. Some metadata like albums
        cannot be embedded in video files and must be managed separately.

        Args:
            video_path: Path to the video file
            metadata: Metadata to apply

        Returns:
            True if successful
        """
        if not video_path.exists():
            raise FileNotFoundError(f"Video file not found: {video_path}")

        # Build FFmpeg metadata arguments
        metadata_args = []

        if metadata.capture_date:
            date_str = metadata.capture_date.strftime("%Y-%m-%dT%H:%M:%S")
            metadata_args.extend(["-metadata", f"creation_time={date_str}"])

        if metadata.title:
            metadata_args.extend(["-metadata", f"title={metadata.title}"])

        if metadata.description:
            metadata_args.extend(["-metadata", f"description={metadata.description}"])

        if metadata.location:
            lat, lon = metadata.location
            # ISO 6709 format: +DD.DDDD+DDD.DDDD/
            location_str = f"{lat:+.4f}{lon:+.4f}/"
            metadata_args.extend(
                ["-metadata", f"com.apple.quicktime.location.ISO6709={location_str}"]
            )

        if not metadata_args:
            return True  # Nothing to apply

        # Create temporary output file
        temp_path = video_path.with_suffix(".temp.mp4")

        try:
            cmd = [
                "ffmpeg",
                "-i",
                str(video_path),
                "-c",
                "copy",  # Copy streams without re-encoding
                *metadata_args,
                "-y",  # Overwrite output
                str(temp_path),
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

            if result.returncode != 0:
                return False

            # Replace original with temp file
            temp_path.replace(video_path)
            return True

        except Exception:
            # Clean up temp file on error
            if temp_path.exists():
                temp_path.unlink()
            return False

    def set_file_dates(
        self,
        video_path: Path,
        creation_date: datetime | None = None,
        modification_date: datetime | None = None,
    ) -> bool:
        """Set file system dates on a video file.

        Args:
            video_path: Path to the video file
            creation_date: File creation date to set
            modification_date: File modification date to set

        Returns:
            True if successful
        """
        if not video_path.exists():
            raise FileNotFoundError(f"Video file not found: {video_path}")

        try:
            # Set modification time using os.utime
            if modification_date:
                mod_timestamp = modification_date.timestamp()
                os.utime(video_path, (mod_timestamp, mod_timestamp))

            # Set creation time using SetFile on macOS
            if creation_date:
                # Format: MM/DD/YYYY HH:MM:SS
                date_str = creation_date.strftime("%m/%d/%Y %H:%M:%S")

                result = subprocess.run(
                    ["SetFile", "-d", date_str, str(video_path)],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )

                if result.returncode != 0:
                    # SetFile might not be available, try touch
                    return self._set_dates_with_touch(video_path, creation_date)

            return True

        except Exception:
            return False

    def copy_dates_from_original(self, original_path: Path, converted_path: Path) -> bool:
        """Copy file dates from original to converted video.

        Args:
            original_path: Path to the original video
            converted_path: Path to the converted video

        Returns:
            True if successful
        """
        if not original_path.exists():
            raise FileNotFoundError(f"Original file not found: {original_path}")
        if not converted_path.exists():
            raise FileNotFoundError(f"Converted file not found: {converted_path}")

        try:
            # Get original file dates
            stat = original_path.stat()
            creation_date = datetime.fromtimestamp(stat.st_birthtime)
            modification_date = datetime.fromtimestamp(stat.st_mtime)

            return self.set_file_dates(
                converted_path, creation_date=creation_date, modification_date=modification_date
            )

        except Exception:
            return False

    def save_metadata_json(self, metadata: VideoMetadata, output_path: Path) -> bool:
        """Save metadata to a JSON file.

        Args:
            metadata: Metadata to save
            output_path: Path to save JSON file

        Returns:
            True if successful
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(metadata.to_dict(), f, indent=2)
            return True
        except Exception:
            return False

    def load_metadata_json(self, json_path: Path) -> VideoMetadata | None:
        """Load metadata from a JSON file.

        Args:
            json_path: Path to JSON file

        Returns:
            VideoMetadata if successful, None otherwise
        """
        try:
            with open(json_path) as f:
                data = json.load(f)
            return VideoMetadata.from_dict(data)
        except Exception:
            return None

    def _run_ffprobe(self, video_path: Path) -> dict[str, Any]:
        """Run FFprobe to get video metadata.

        Args:
            video_path: Path to video file

        Returns:
            FFprobe output as dictionary
        """
        cmd = [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(video_path),
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode != 0:
            raise RuntimeError(f"FFprobe failed: {result.stderr}")

        data: dict[str, Any] = json.loads(result.stdout)
        return data

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string from various formats.

        Args:
            date_str: Date string to parse

        Returns:
            Parsed datetime
        """
        # Try various formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        raise ValueError(f"Unable to parse date: {date_str}")

    def _parse_location(self, location_str: str) -> tuple[float, float] | None:
        """Parse ISO 6709 location string.

        Format: +DD.DDDD+DDD.DDDD/ or +DD.DDDD-DDD.DDDD/

        Args:
            location_str: ISO 6709 location string

        Returns:
            Tuple of (latitude, longitude) or None
        """
        try:
            # Remove trailing slash
            location_str = location_str.rstrip("/")

            # Find the second +/- sign (start of longitude)
            second_sign_idx = None

            for i in range(1, len(location_str)):
                if location_str[i] in ("+", "-"):
                    second_sign_idx = i
                    break

            if second_sign_idx is None:
                return None

            lat_str = location_str[:second_sign_idx]
            lon_str = location_str[second_sign_idx:]

            return (float(lat_str), float(lon_str))

        except (ValueError, IndexError):
            return None

    def _set_dates_with_touch(self, video_path: Path, date: datetime) -> bool:
        """Set file dates using touch command (fallback).

        Args:
            video_path: Path to video file
            date: Date to set

        Returns:
            True if successful
        """
        try:
            # Format: [[CC]YY]MMDDhhmm[.SS]
            date_str = date.strftime("%Y%m%d%H%M.%S")

            result = subprocess.run(
                ["touch", "-t", date_str, str(video_path)],
                capture_output=True,
                text=True,
                timeout=30,
            )

            return result.returncode == 0

        except Exception:
            return False
