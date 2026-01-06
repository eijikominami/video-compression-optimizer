"""Metadata embedder using exiftool for Video Compression Optimizer."""

from __future__ import annotations

import logging
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from vco.models.metadata import OriginalMetadata

logger = logging.getLogger(__name__)


@dataclass
class EmbedResult:
    """Result of metadata embedding operation.

    Attributes:
        success: Whether embedding succeeded
        video_path: Path to the video file
        embedded_fields: List of fields that were embedded
        error_message: Error message if embedding failed
        skipped: Whether embedding was skipped (e.g., exiftool not available)
    """

    success: bool
    video_path: Path
    embedded_fields: list[str] = field(default_factory=list)
    error_message: str | None = None
    skipped: bool = False


class MetadataEmbedder:
    """Embed metadata into video files using exiftool.

    Uses exiftool to write Keys:CreationDate and Keys:GPSCoordinates tags,
    which are properly read by Photos app with correct timezone handling.
    """

    def __init__(self):
        """Initialize the embedder."""
        self._exiftool_path: str | None = None

    def _check_exiftool(self) -> bool:
        """Check if exiftool is available.

        Returns:
            True if exiftool is available, False otherwise
        """
        if self._exiftool_path is not None:
            return bool(self._exiftool_path)

        self._exiftool_path = shutil.which("exiftool") or ""
        return bool(self._exiftool_path)

    def _build_exiftool_command(
        self,
        video_path: Path,
        metadata: OriginalMetadata,
    ) -> tuple[list[str], list[str]]:
        """Build exiftool command for embedding metadata.

        Args:
            video_path: Path to the video file
            metadata: Metadata to embed

        Returns:
            Tuple of (command args, embedded field names)
        """
        cmd = ["exiftool", "-overwrite_original"]
        embedded_fields: list[str] = []

        # Add capture date with timezone
        if metadata.capture_date:
            # Format: 2021:11:27 09:40:30+09:00
            date_str = metadata.capture_date.strftime("%Y:%m:%d %H:%M:%S")
            # Add timezone offset if available
            tz_offset = metadata.capture_date.strftime("%z")
            if tz_offset:
                # Format offset as +09:00 instead of +0900
                date_str += f"{tz_offset[:3]}:{tz_offset[3:]}"
            cmd.append(f"-Keys:CreationDate={date_str}")
            embedded_fields.append("capture_date")

        # Add GPS coordinates
        if metadata.gps_location:
            gps_str = f"{metadata.gps_location.latitude}, {metadata.gps_location.longitude}"
            cmd.append(f"-Keys:GPSCoordinates={gps_str}")
            embedded_fields.append("gps_location")

        cmd.append(str(video_path))
        return cmd, embedded_fields

    def embed(
        self,
        video_path: Path,
        metadata: OriginalMetadata,
    ) -> EmbedResult:
        """Embed metadata into a video file using exiftool.

        Args:
            video_path: Path to the video file
            metadata: Metadata to embed

        Returns:
            EmbedResult with embedding details
        """
        # Check if exiftool is available
        if not self._check_exiftool():
            logger.warning("exiftool not found, skipping metadata embedding")
            return EmbedResult(
                success=False,
                video_path=video_path,
                error_message="exiftool not found. Install with: brew install exiftool",
                skipped=True,
            )

        # Check if there's anything to embed
        if not metadata.capture_date and not metadata.gps_location:
            logger.info("No metadata to embed")
            return EmbedResult(
                success=True,
                video_path=video_path,
                skipped=True,
            )

        # Build and run exiftool command
        cmd, embedded_fields = self._build_exiftool_command(video_path, metadata)

        try:
            logger.info(f"Embedding metadata: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                error_msg = result.stderr.strip() or "exiftool failed"
                logger.error(f"exiftool failed: {error_msg}")
                return EmbedResult(
                    success=False,
                    video_path=video_path,
                    error_message=error_msg,
                )

            logger.info(f"Metadata embedded successfully: {embedded_fields}")
            return EmbedResult(
                success=True,
                video_path=video_path,
                embedded_fields=embedded_fields,
            )

        except subprocess.TimeoutExpired:
            logger.error("exiftool timed out")
            return EmbedResult(
                success=False,
                video_path=video_path,
                error_message="exiftool timed out",
            )
        except Exception as e:
            logger.exception(f"Failed to embed metadata: {e}")
            return EmbedResult(
                success=False,
                video_path=video_path,
                error_message=str(e),
            )
