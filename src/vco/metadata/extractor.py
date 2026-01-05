"""Metadata extractor using ffprobe for Video Compression Optimizer."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from vco.models.metadata import ExtractedMetadata, GPSLocation

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Extract metadata from video files using ffprobe.

    This class uses ffprobe to extract metadata from video files,
    including capture date, GPS location, duration, codec, and resolution.
    """

    def __init__(self, ffprobe_path: str = "ffprobe"):
        """Initialize the extractor.

        Args:
            ffprobe_path: Path to ffprobe executable
        """
        self.ffprobe_path = ffprobe_path

    def extract(self, video_path: Path) -> ExtractedMetadata:
        """Extract metadata from a video file.

        Args:
            video_path: Path to the video file

        Returns:
            ExtractedMetadata with extracted values and any errors
        """
        errors: list[str] = []

        try:
            ffprobe_output = self._run_ffprobe(video_path)
        except Exception as e:
            logger.warning(f"ffprobe failed for {video_path}: {e}")
            return ExtractedMetadata(extraction_errors=[str(e)])

        capture_date = self._parse_creation_time(ffprobe_output, errors)
        gps_location = self._parse_gps_location(ffprobe_output, errors)
        duration = self._parse_duration(ffprobe_output, errors)
        codec = self._parse_codec(ffprobe_output, errors)
        resolution = self._parse_resolution(ffprobe_output, errors)

        return ExtractedMetadata(
            capture_date=capture_date,
            gps_location=gps_location,
            duration=duration,
            codec=codec,
            resolution=resolution,
            extraction_errors=errors,
        )

    def _run_ffprobe(self, video_path: Path) -> dict:
        """Run ffprobe and return JSON output.

        Args:
            video_path: Path to the video file

        Returns:
            Parsed JSON output from ffprobe

        Raises:
            RuntimeError: If ffprobe fails
        """
        cmd = [
            self.ffprobe_path,
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(video_path),
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"ffprobe timed out for {video_path}")
        except FileNotFoundError:
            raise RuntimeError(f"ffprobe not found at {self.ffprobe_path}")

        if result.returncode != 0:
            raise RuntimeError(f"ffprobe failed: {result.stderr}")

        try:
            parsed: dict[Any, Any] = json.loads(result.stdout)
            return parsed
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse ffprobe output: {e}")

    def _parse_creation_time(self, ffprobe_output: dict, errors: list[str]) -> datetime | None:
        """Parse creation_time from ffprobe output.

        Looks for creation_time in:
        1. format.tags.creation_time
        2. streams[0].tags.creation_time (video stream)

        Args:
            ffprobe_output: Parsed ffprobe JSON output
            errors: List to append errors to

        Returns:
            Parsed datetime or None
        """
        creation_time_str = None

        # Try format.tags.creation_time first
        format_tags = ffprobe_output.get("format", {}).get("tags", {})
        creation_time_str = format_tags.get("creation_time")

        # Fall back to video stream tags
        if not creation_time_str:
            streams = ffprobe_output.get("streams", [])
            for stream in streams:
                if stream.get("codec_type") == "video":
                    stream_tags = stream.get("tags", {})
                    creation_time_str = stream_tags.get("creation_time")
                    if creation_time_str:
                        break

        if not creation_time_str:
            return None

        try:
            # Handle various datetime formats
            # Common formats: "2024-01-15T14:30:00.000000Z", "2024-01-15 14:30:00"
            creation_time_str = creation_time_str.replace("Z", "+00:00")
            if "T" in creation_time_str:
                # ISO format with potential timezone
                if "+" in creation_time_str or creation_time_str.endswith("Z"):
                    return datetime.fromisoformat(creation_time_str.replace("Z", "+00:00"))
                return datetime.fromisoformat(creation_time_str)
            else:
                # Simple format without T
                return datetime.strptime(creation_time_str, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError) as e:
            errors.append(f"Failed to parse creation_time '{creation_time_str}': {e}")
            return None

    def _parse_gps_location(self, ffprobe_output: dict, errors: list[str]) -> GPSLocation | None:
        """Parse GPS location from ffprobe output.

        Looks for location in format.tags:
        - com.apple.quicktime.location.ISO6709
        - location (fallback)

        Args:
            ffprobe_output: Parsed ffprobe JSON output
            errors: List to append errors to

        Returns:
            GPSLocation or None
        """
        format_tags = ffprobe_output.get("format", {}).get("tags", {})

        # Try Apple QuickTime location first
        location_str = format_tags.get("com.apple.quicktime.location.ISO6709")

        # Fall back to generic location tag
        if not location_str:
            location_str = format_tags.get("location")

        if not location_str:
            return None

        gps = GPSLocation.from_iso6709(location_str)
        if gps is None:
            errors.append(f"Failed to parse GPS location '{location_str}'")

        return gps

    def _parse_duration(self, ffprobe_output: dict, errors: list[str]) -> float | None:
        """Parse duration from ffprobe output.

        Args:
            ffprobe_output: Parsed ffprobe JSON output
            errors: List to append errors to

        Returns:
            Duration in seconds or None
        """
        duration_str = ffprobe_output.get("format", {}).get("duration")
        if not duration_str:
            return None

        try:
            return float(duration_str)
        except (ValueError, TypeError) as e:
            errors.append(f"Failed to parse duration '{duration_str}': {e}")
            return None

    def _parse_codec(self, ffprobe_output: dict, errors: list[str]) -> str | None:
        """Parse video codec from ffprobe output.

        Args:
            ffprobe_output: Parsed ffprobe JSON output
            errors: List to append errors to

        Returns:
            Codec name or None
        """
        streams = ffprobe_output.get("streams", [])
        for stream in streams:
            if stream.get("codec_type") == "video":
                codec_name: str | None = stream.get("codec_name")
                return codec_name
        return None

    def _parse_resolution(self, ffprobe_output: dict, errors: list[str]) -> tuple[int, int] | None:
        """Parse video resolution from ffprobe output.

        Args:
            ffprobe_output: Parsed ffprobe JSON output
            errors: List to append errors to

        Returns:
            Resolution as (width, height) or None
        """
        streams = ffprobe_output.get("streams", [])
        for stream in streams:
            if stream.get("codec_type") == "video":
                width = stream.get("width")
                height = stream.get("height")
                if width and height:
                    try:
                        return (int(width), int(height))
                    except (ValueError, TypeError) as e:
                        errors.append(f"Failed to parse resolution: {e}")
                        return None
        return None
