"""Unit tests for iCloud parallel download progress display.

Tests cover:
- Dynamic progress callback assignment
- Progress callback updates during download
- Multiple concurrent downloads with separate progress tracking
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vco.models.types import VideoInfo
from vco.services.icloud_download import (
    DownloadProgress,
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


class TestDynamicProgressCallback:
    """Tests for dynamic progress callback assignment."""

    def test_progress_callback_can_be_set_after_init(self, mock_swift_bridge, sample_video):
        """Test that progress_callback can be set after initialization."""
        expected_path = Path("/tmp/downloaded_video.mov")
        mock_swift_bridge.download_from_icloud.return_value = expected_path

        # Initialize without progress callback
        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        progress_updates = []

        def progress_callback(progress: DownloadProgress):
            progress_updates.append(progress)

        # Set progress callback dynamically (like CLI does)
        service.progress_callback = progress_callback

        result = service.download_video(sample_video)

        assert result.success is True
        # Progress callback should have been called
        assert len(progress_updates) >= 2  # At least start (0%) and end (100%)
        assert progress_updates[0].progress_percent == 0.0
        assert progress_updates[-1].progress_percent == 100.0

    def test_progress_callback_can_be_changed_between_downloads(
        self, mock_swift_bridge, sample_video
    ):
        """Test that progress_callback can be changed between downloads."""
        expected_path = Path("/tmp/downloaded_video.mov")
        mock_swift_bridge.download_from_icloud.return_value = expected_path

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        # First download with callback 1
        updates_1 = []
        service.progress_callback = lambda p: updates_1.append(p)
        service.download_video(sample_video)

        # Second download with callback 2
        updates_2 = []
        service.progress_callback = lambda p: updates_2.append(p)
        service.download_video(sample_video)

        # Both callbacks should have received updates
        assert len(updates_1) >= 2
        assert len(updates_2) >= 2

        # Updates should be separate
        assert updates_1 is not updates_2

    def test_progress_callback_receives_correct_video_info(
        self, mock_swift_bridge, sample_video
    ):
        """Test that progress callback receives correct video information."""
        expected_path = Path("/tmp/downloaded_video.mov")
        mock_swift_bridge.download_from_icloud.return_value = expected_path

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        progress_updates = []
        service.progress_callback = lambda p: progress_updates.append(p)

        service.download_video(sample_video)

        # All updates should have correct video info
        for update in progress_updates:
            assert update.uuid == sample_video.uuid
            assert update.filename == sample_video.filename
            assert update.total_bytes == sample_video.file_size


class TestProgressCallbackIntegration:
    """Tests for progress callback integration with parallel downloads."""

    def test_multiple_videos_with_separate_callbacks(self, mock_swift_bridge):
        """Test that multiple videos can have separate progress tracking."""
        video1 = VideoInfo(
            uuid="video-1",
            filename="video1.mov",
            path=Path("/tmp/video1.mov"),
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

        video2 = VideoInfo(
            uuid="video-2",
            filename="video2.mov",
            path=Path("/tmp/video2.mov"),
            codec="h264",
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=200_000_000,
            capture_date=None,
            creation_date=None,
            albums=[],
            is_in_icloud=True,
            is_local=False,
            location=None,
        )

        mock_swift_bridge.download_from_icloud.return_value = Path("/tmp/downloaded.mov")

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        # Track updates per video
        updates_by_video: dict[str, list[DownloadProgress]] = {}

        def create_callback(video_uuid: str):
            def callback(progress: DownloadProgress):
                if video_uuid not in updates_by_video:
                    updates_by_video[video_uuid] = []
                updates_by_video[video_uuid].append(progress)

            return callback

        # Download video1 with its callback
        service.progress_callback = create_callback("video-1")
        service.download_video(video1)

        # Download video2 with its callback
        service.progress_callback = create_callback("video-2")
        service.download_video(video2)

        # Both videos should have progress updates
        assert "video-1" in updates_by_video
        assert "video-2" in updates_by_video

        # Each video's updates should have correct file size
        for update in updates_by_video["video-1"]:
            assert update.total_bytes == video1.file_size

        for update in updates_by_video["video-2"]:
            assert update.total_bytes == video2.file_size

    def test_swift_bridge_progress_callback_updates_service_callback(
        self, mock_swift_bridge, sample_video
    ):
        """Test that SwiftBridge progress callback triggers service callback."""
        expected_path = Path("/tmp/downloaded_video.mov")

        # Capture the progress callback passed to SwiftBridge
        captured_swift_callback = None

        def capture_callback(**kwargs):
            nonlocal captured_swift_callback
            captured_swift_callback = kwargs.get("progress_callback")
            return expected_path

        mock_swift_bridge.download_from_icloud.side_effect = capture_callback

        service = ICloudDownloadService(
            swift_bridge=mock_swift_bridge,
            timeout=300,
        )

        progress_updates = []
        service.progress_callback = lambda p: progress_updates.append(p)

        service.download_video(sample_video)

        # SwiftBridge should have received a progress callback
        assert captured_swift_callback is not None

        # Simulate SwiftBridge calling the callback with progress
        captured_swift_callback(50)  # 50%

        # Service callback should have been called
        # (initial 0%, then 50% from swift callback, then 100% at end)
        assert any(u.progress_percent == 50.0 for u in progress_updates)
