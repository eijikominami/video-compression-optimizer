"""Unit tests for ICloudDownloadService.

Tests cover:
- Single video download (success/failure)
- Multiple video download
- Disk space checking
- Error handling
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vco.models.types import VideoInfo
from vco.photos.manager import PhotosAccessError
from vco.services.icloud_download import (
    DownloadProgress,
    DownloadResult,
    ICloudDownloadService,
)


@pytest.fixture
def mock_swift_bridge():
    """Create a mock SwiftBridge."""
    return MagicMock()


@pytest.fixture
def sample_video():
    """Create a sample VideoInfo for testing."""
    return VideoInfo(
        uuid="test-uuid-123",
        filename="test_video.mov",
        path=Path("/tmp/test_video.mov"),
        codec="h264",
        resolution=(1920, 1080),
        bitrate=10000000,
        duration=60.0,
        frame_rate=30.0,
        file_size=100_000_000,
        capture_date=None,
        creation_date=None,
        albums=[],
        is_in_icloud=True,
        is_local=False,
        location=None,
    )


@pytest.fixture
def local_video():
    """Create a local VideoInfo for testing."""
    return VideoInfo(
        uuid="local-uuid-456",
        filename="local_video.mov",
        path=Path("/tmp/local_video.mov"),
        codec="h264",
        resolution=(1920, 1080),
        bitrate=10000000,
        duration=60.0,
        frame_rate=30.0,
        file_size=50_000_000,
        capture_date=None,
        creation_date=None,
        albums=[],
        is_in_icloud=False,
        is_local=True,
        location=None,
    )


class TestDownloadVideo:
    """Tests for download_video method."""

    def test_download_success(self, mock_swift_bridge, sample_video):
        """Test successful download from iCloud."""
        expected_path = Path("/tmp/downloaded_video.mov")
        mock_swift_bridge.download_from_icloud.return_value = expected_path

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        result = service.download_video(sample_video)

        assert result.success is True
        assert result.uuid == sample_video.uuid
        assert result.filename == sample_video.filename
        assert result.local_path == expected_path
        assert result.error_message is None
        assert result.download_time_seconds >= 0

        mock_swift_bridge.download_from_icloud.assert_called_once_with(
            video=sample_video,
            timeout=300,
        )

    def test_download_already_local(self, mock_swift_bridge, local_video):
        """Test that local videos are not re-downloaded."""
        with patch.object(Path, "exists", return_value=True):
            service = ICloudDownloadService(
                swift_bridge=mock_swift_bridge,
                timeout=300,
            )

            result = service.download_video(local_video)

            assert result.success is True
            assert result.local_path == local_video.path
            assert result.download_time_seconds == 0.0

            # Should not call download
            mock_swift_bridge.download_from_icloud.assert_not_called()

    def test_download_timeout_error(self, mock_swift_bridge, sample_video):
        """Test handling of timeout error."""
        mock_swift_bridge.download_from_icloud.side_effect = PhotosAccessError(
            "Command 'download' timed out after 300s"
        )

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        result = service.download_video(sample_video)

        assert result.success is False
        assert result.local_path is None
        assert "timed out" in result.error_message.lower()

    def test_download_network_error(self, mock_swift_bridge, sample_video):
        """Test handling of network error."""
        mock_swift_bridge.download_from_icloud.side_effect = PhotosAccessError(
            "Network connection unavailable"
        )

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        result = service.download_video(sample_video)

        assert result.success is False
        assert result.local_path is None
        assert "network" in result.error_message.lower()

    def test_download_not_found_error(self, mock_swift_bridge, sample_video):
        """Test handling of not found error."""
        mock_swift_bridge.download_from_icloud.side_effect = PhotosAccessError(
            "Video not found in Photos library"
        )

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        result = service.download_video(sample_video)

        assert result.success is False
        assert result.local_path is None
        assert "not found" in result.error_message.lower()

    def test_download_with_progress_callback(self, mock_swift_bridge, sample_video):
        """Test that progress callback is called."""
        expected_path = Path("/tmp/downloaded_video.mov")
        mock_swift_bridge.download_from_icloud.return_value = expected_path

        progress_updates = []

        def progress_callback(progress: DownloadProgress):
            progress_updates.append(progress)

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
            progress_callback=progress_callback,
        )

        result = service.download_video(sample_video)

        assert result.success is True
        assert len(progress_updates) >= 2  # At least start and end

        # Check first progress (0%)
        assert progress_updates[0].progress_percent == 0.0
        assert progress_updates[0].uuid == sample_video.uuid

        # Check last progress (100%)
        assert progress_updates[-1].progress_percent == 100.0


class TestDownloadVideos:
    """Tests for download_videos method."""

    def test_download_multiple_videos(self, mock_swift_bridge, sample_video, local_video):
        """Test downloading multiple videos."""
        expected_path = Path("/tmp/downloaded_video.mov")
        mock_swift_bridge.download_from_icloud.return_value = expected_path

        with patch.object(Path, "exists", return_value=True):
            service = ICloudDownloadService(
                swift_bridge=mock_swift_bridge,
                timeout=300,
            )

            summary = service.download_videos([sample_video, local_video])

            assert summary.total == 2
            assert summary.successful == 1  # iCloud video
            assert summary.skipped == 1  # Local video
            assert summary.failed == 0
            assert len(summary.results) == 2

    def test_download_with_failures(self, mock_swift_bridge, sample_video):
        """Test that failures don't stop other downloads."""
        video2 = VideoInfo(
            uuid="test-uuid-456",
            filename="test_video2.mov",
            path=Path("/tmp/test_video2.mov"),
            codec="h264",
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=100_000_000,
            capture_date=None,
            creation_date=None,
            albums=[],
            is_in_icloud=True,
            is_local=False,
            location=None,
        )

        # First call fails, second succeeds
        mock_swift_bridge.download_from_icloud.side_effect = [
            PhotosAccessError("Network error"),
            Path("/tmp/downloaded_video2.mov"),
        ]

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        summary = service.download_videos([sample_video, video2])

        assert summary.total == 2
        assert summary.successful == 1
        assert summary.failed == 1
        assert len(summary.results) == 2


class TestCheckDiskSpace:
    """Tests for check_disk_space method."""

    def test_sufficient_disk_space(self, mock_swift_bridge):
        """Test when disk space is sufficient."""
        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        with patch("shutil.disk_usage") as mock_usage:
            # 10GB free
            mock_usage.return_value = MagicMock(free=10_000_000_000)

            # Request 1GB
            has_space, available = service.check_disk_space(1_000_000_000)

            assert has_space is True
            assert available == 10_000_000_000

    def test_insufficient_disk_space(self, mock_swift_bridge):
        """Test when disk space is insufficient."""
        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        with patch("shutil.disk_usage") as mock_usage:
            # 500MB free
            mock_usage.return_value = MagicMock(free=500_000_000)

            # Request 1GB (with 10% margin = 1.1GB needed)
            has_space, available = service.check_disk_space(1_000_000_000)

            assert has_space is False
            assert available == 500_000_000


class TestEstimateDownloadSize:
    """Tests for estimate_download_size method."""

    def test_estimate_icloud_only(self, mock_swift_bridge, sample_video, local_video):
        """Test that only iCloud videos are counted."""
        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        total = service.estimate_download_size([sample_video, local_video])

        # Only sample_video (iCloud) should be counted
        assert total == sample_video.file_size


class TestDownloadResultDataclass:
    """Tests for DownloadResult dataclass."""

    def test_download_result_creation(self):
        """Test DownloadResult can be created with all fields."""
        result = DownloadResult(
            uuid="test-uuid",
            filename="test.mov",
            success=True,
            local_path=Path("/tmp/test.mov"),
            error_message=None,
            download_time_seconds=5.5,
        )

        assert result.uuid == "test-uuid"
        assert result.filename == "test.mov"
        assert result.success is True
        assert result.local_path == Path("/tmp/test.mov")
        assert result.error_message is None
        assert result.download_time_seconds == 5.5


class TestDownloadProgressDataclass:
    """Tests for DownloadProgress dataclass."""

    def test_download_progress_creation(self):
        """Test DownloadProgress can be created with all fields."""
        progress = DownloadProgress(
            uuid="test-uuid",
            filename="test.mov",
            progress_percent=50.0,
            downloaded_bytes=50_000_000,
            total_bytes=100_000_000,
        )

        assert progress.uuid == "test-uuid"
        assert progress.filename == "test.mov"
        assert progress.progress_percent == 50.0
        assert progress.downloaded_bytes == 50_000_000
        assert progress.total_bytes == 100_000_000
