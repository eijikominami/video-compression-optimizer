"""iCloud download service for downloading videos from iCloud.

This service handles downloading videos from iCloud before conversion,
using the SwiftBridge to access PhotoKit APIs.

Requirements: 2.1, 3.1, 3.2, 3.3, 3.4, 5.1, 5.2, 5.3
"""

import logging
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from vco.models.types import VideoInfo
from vco.photos.manager import PhotosAccessError

logger = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    """Result of iCloud download operation.

    Attributes:
        uuid: Video UUID
        filename: Video filename
        success: Whether download succeeded
        local_path: Path to downloaded file (None if failed)
        error_message: Error message if failed
        download_time_seconds: Time taken to download
    """

    uuid: str
    filename: str
    success: bool
    local_path: Path | None
    error_message: str | None
    download_time_seconds: float


@dataclass
class DownloadProgress:
    """Progress information for download.

    Attributes:
        uuid: Video UUID
        filename: Video filename
        progress_percent: Download progress percentage (0-100)
        downloaded_bytes: Bytes downloaded so far
        total_bytes: Total bytes to download
    """

    uuid: str
    filename: str
    progress_percent: float
    downloaded_bytes: int
    total_bytes: int


@dataclass
class DownloadSummary:
    """Summary of download operations.

    Attributes:
        total: Total number of videos to download
        successful: Number of successful downloads
        failed: Number of failed downloads
        skipped: Number of skipped videos (already local)
        results: List of individual download results
    """

    total: int
    successful: int
    failed: int
    skipped: int
    results: list[DownloadResult]


class ICloudDownloadService:
    """Service for downloading videos from iCloud.

    Uses SwiftBridge to download videos from iCloud before conversion.
    Provides progress tracking and error handling.

    Requirements: 5.1, 5.2, 5.3, 5.4
    """

    def __init__(
        self,
        swift_bridge,
        timeout: int = 300,
        progress_callback: Callable[[DownloadProgress], None] | None = None,
    ):
        """Initialize service.

        Args:
            swift_bridge: SwiftBridge instance for Photos access
            timeout: Download timeout in seconds (default: 300)
            progress_callback: Callback for progress updates
        """
        self.swift_bridge = swift_bridge
        self.timeout = timeout
        self.progress_callback = progress_callback

    def download_video(self, video: VideoInfo) -> DownloadResult:
        """Download a single video from iCloud.

        Args:
            video: VideoInfo object for the video to download

        Returns:
            DownloadResult with success status and local path

        Requirements: 5.1, 5.2, 5.3
        """
        start_time = time.time()

        # Check if already local
        if video.is_local and video.path.exists():
            return DownloadResult(
                uuid=video.uuid,
                filename=video.filename,
                success=True,
                local_path=video.path,
                error_message=None,
                download_time_seconds=0.0,
            )

        # Report initial progress
        if self.progress_callback:
            self.progress_callback(
                DownloadProgress(
                    uuid=video.uuid,
                    filename=video.filename,
                    progress_percent=0.0,
                    downloaded_bytes=0,
                    total_bytes=video.file_size,
                )
            )

        try:
            # Download using SwiftBridge
            local_path = self.swift_bridge.download_from_icloud(
                video=video,
                timeout=self.timeout,
            )

            download_time = time.time() - start_time

            # Report completion
            if self.progress_callback:
                self.progress_callback(
                    DownloadProgress(
                        uuid=video.uuid,
                        filename=video.filename,
                        progress_percent=100.0,
                        downloaded_bytes=video.file_size,
                        total_bytes=video.file_size,
                    )
                )

            logger.info(f"Downloaded {video.filename} in {download_time:.1f}s")

            return DownloadResult(
                uuid=video.uuid,
                filename=video.filename,
                success=True,
                local_path=local_path,
                error_message=None,
                download_time_seconds=download_time,
            )

        except PhotosAccessError as e:
            download_time = time.time() - start_time
            error_msg = str(e)

            # Categorize error
            if "timeout" in error_msg.lower():
                error_msg = f"Download timed out after {self.timeout}s"
            elif "network" in error_msg.lower():
                error_msg = "Network connection unavailable"
            elif "not found" in error_msg.lower():
                error_msg = "Video not found in Photos library"

            logger.warning(f"Failed to download {video.filename}: {error_msg}")

            return DownloadResult(
                uuid=video.uuid,
                filename=video.filename,
                success=False,
                local_path=None,
                error_message=error_msg,
                download_time_seconds=download_time,
            )

        except Exception as e:
            download_time = time.time() - start_time
            error_msg = f"Unexpected error: {e}"
            logger.exception(f"Failed to download {video.filename}")

            return DownloadResult(
                uuid=video.uuid,
                filename=video.filename,
                success=False,
                local_path=None,
                error_message=error_msg,
                download_time_seconds=download_time,
            )

    def download_videos(self, videos: list[VideoInfo]) -> DownloadSummary:
        """Download multiple videos from iCloud.

        Args:
            videos: List of VideoInfo objects to download

        Returns:
            DownloadSummary with results for all videos

        Requirements: 3.4, 3.5
        """
        results: list[DownloadResult] = []
        successful = 0
        failed = 0
        skipped = 0

        for i, video in enumerate(videos):
            # Check if already local
            if video.is_local and video.path.exists():
                results.append(
                    DownloadResult(
                        uuid=video.uuid,
                        filename=video.filename,
                        success=True,
                        local_path=video.path,
                        error_message=None,
                        download_time_seconds=0.0,
                    )
                )
                skipped += 1
                continue

            # Download
            result = self.download_video(video)
            results.append(result)

            if result.success:
                successful += 1
            else:
                failed += 1

        return DownloadSummary(
            total=len(videos),
            successful=successful,
            failed=failed,
            skipped=skipped,
            results=results,
        )

    def check_disk_space(self, required_bytes: int) -> tuple[bool, int]:
        """Check if sufficient disk space is available.

        Args:
            required_bytes: Required disk space in bytes

        Returns:
            Tuple of (has_space, available_bytes)

        Requirements: 3.3
        """
        try:
            usage = shutil.disk_usage(Path.home())
            available = usage.free
            # Require 10% margin
            has_space = available > required_bytes * 1.1
            return has_space, available
        except OSError as e:
            logger.warning(f"Failed to check disk space: {e}")
            # Assume space is available if check fails
            return True, 0

    def estimate_download_size(self, videos: list[VideoInfo]) -> int:
        """Estimate total download size for iCloud videos.

        Args:
            videos: List of VideoInfo objects

        Returns:
            Estimated total size in bytes
        """
        total = 0
        for video in videos:
            if not video.is_local:
                total += video.file_size
        return total
