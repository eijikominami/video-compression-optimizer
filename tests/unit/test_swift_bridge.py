"""Unit tests for SwiftBridge."""

import json
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vco.photos.manager import PhotosAccessError
from vco.photos.swift_bridge import SwiftBridge


class TestSwiftBridgeBinaryDetection:
    """Tests for binary detection logic."""

    def test_find_binary_in_package_bin(self, tmp_path: Path) -> None:
        """Test finding binary in package bin directory."""
        # Create mock binary
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        binary = bin_dir / "vco-photos"
        binary.touch()

        with patch.object(Path, "__new__", return_value=tmp_path):
            bridge = SwiftBridge(binary_path=binary)
            assert bridge._binary_path == binary

    def test_find_binary_not_found(self) -> None:
        """Test error when binary not found."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="")

            # Don't provide binary_path to trigger auto-detection
            with patch.object(
                SwiftBridge, "_find_binary", side_effect=PhotosAccessError("vco-photos binary not found")
            ):
                with pytest.raises(PhotosAccessError, match="vco-photos binary not found"):
                    SwiftBridge()


class TestSwiftBridgeCommandExecution:
    """Tests for command execution."""

    @pytest.fixture
    def bridge(self, tmp_path: Path) -> SwiftBridge:
        """Create a SwiftBridge with mock binary."""
        binary = tmp_path / "vco-photos"
        binary.touch()
        binary.chmod(0o755)
        return SwiftBridge(binary_path=binary)

    def test_execute_command_success(self, bridge: SwiftBridge) -> None:
        """Test successful command execution."""
        mock_response = {"success": True, "data": []}

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(mock_response),
                stderr="",
            )

            result = bridge._execute_command("scan")
            assert result["success"] is True
            assert result["data"] == []

    def test_execute_command_error_response(self, bridge: SwiftBridge) -> None:
        """Test command with error response."""
        mock_response = {
            "success": False,
            "error": {"type": "authorization_denied", "message": "Access denied"},
        }

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stdout=json.dumps(mock_response),
                stderr="",
            )

            with pytest.raises(PhotosAccessError, match="authorization_denied"):
                bridge._execute_command("scan")

    def test_execute_command_timeout(self, bridge: SwiftBridge) -> None:
        """Test command timeout handling."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="vco-photos", timeout=60)

            with pytest.raises(PhotosAccessError, match="timed out"):
                bridge._execute_command("scan", timeout=60)

    def test_execute_command_invalid_json(self, bridge: SwiftBridge) -> None:
        """Test handling of invalid JSON response."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="not valid json",
                stderr="",
            )

            with pytest.raises(PhotosAccessError, match="Invalid JSON"):
                bridge._execute_command("scan")

    def test_execute_command_empty_response(self, bridge: SwiftBridge) -> None:
        """Test handling of empty response."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="",
                stderr="",
            )

            with pytest.raises(PhotosAccessError, match="Empty response"):
                bridge._execute_command("scan")


class TestSwiftBridgeVideoInfoParsing:
    """Tests for VideoInfo parsing."""

    @pytest.fixture
    def bridge(self, tmp_path: Path) -> SwiftBridge:
        """Create a SwiftBridge with mock binary."""
        binary = tmp_path / "vco-photos"
        binary.touch()
        binary.chmod(0o755)
        return SwiftBridge(binary_path=binary)

    def test_parse_video_info_complete(self, bridge: SwiftBridge) -> None:
        """Test parsing complete video info."""
        data = {
            "uuid": "ABC123",
            "filename": "test.mov",
            "path": "/path/to/test.mov",
            "codec": "hevc",
            "resolution": [1920, 1080],
            "bitrate": 5000000,
            "duration": 120.5,
            "frame_rate": 30.0,
            "file_size": 100000000,
            "capture_date": "2024-01-15T10:30:00Z",
            "creation_date": "2024-01-15T10:30:00Z",
            "albums": ["Vacation", "2024"],
            "is_in_icloud": False,
            "is_local": True,
            "location": [35.6762, 139.6503],
        }

        video = bridge._parse_video_info(data)

        assert video.uuid == "ABC123"
        assert video.filename == "test.mov"
        assert video.path == Path("/path/to/test.mov")
        assert video.codec == "hevc"
        assert video.resolution == (1920, 1080)
        assert video.bitrate == 5000000
        assert video.duration == 120.5
        assert video.frame_rate == 30.0
        assert video.file_size == 100000000
        assert video.albums == ["Vacation", "2024"]
        assert video.is_in_icloud is False
        assert video.is_local is True
        assert video.location == (35.6762, 139.6503)

    def test_parse_video_info_minimal(self, bridge: SwiftBridge) -> None:
        """Test parsing minimal video info."""
        data = {"uuid": "ABC123", "filename": "test.mov"}

        video = bridge._parse_video_info(data)

        assert video.uuid == "ABC123"
        assert video.filename == "test.mov"
        assert video.codec == "unknown"
        assert video.resolution == (0, 0)
        assert video.bitrate == 0
        assert video.location is None

    def test_parse_video_info_null_location(self, bridge: SwiftBridge) -> None:
        """Test parsing video info with null location."""
        data = {"uuid": "ABC123", "filename": "test.mov", "location": None}

        video = bridge._parse_video_info(data)
        assert video.location is None


class TestSwiftBridgePhotosInterface:
    """Tests for PhotosAccessManager compatible interface."""

    @pytest.fixture
    def bridge(self, tmp_path: Path) -> SwiftBridge:
        """Create a SwiftBridge with mock binary."""
        binary = tmp_path / "vco-photos"
        binary.touch()
        binary.chmod(0o755)
        return SwiftBridge(binary_path=binary)

    def test_get_all_videos(self, bridge: SwiftBridge) -> None:
        """Test get_all_videos method."""
        mock_response = {
            "success": True,
            "data": [
                {"uuid": "ABC123", "filename": "video1.mov"},
                {"uuid": "DEF456", "filename": "video2.mov"},
            ],
        }

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(mock_response),
                stderr="",
            )

            videos = bridge.get_all_videos()

            assert len(videos) == 2
            assert videos[0].uuid == "ABC123"
            assert videos[1].uuid == "DEF456"

    def test_get_videos_by_date_range(self, bridge: SwiftBridge) -> None:
        """Test get_videos_by_date_range method."""
        mock_response = {
            "success": True,
            "data": [{"uuid": "ABC123", "filename": "video1.mov"}],
        }

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(mock_response),
                stderr="",
            )

            from_date = datetime(2024, 1, 1)
            to_date = datetime(2024, 12, 31)

            videos = bridge.get_videos_by_date_range(from_date, to_date)

            assert len(videos) == 1
            # Verify args were passed correctly
            call_args = mock_run.call_args
            input_json = json.loads(call_args.kwargs["input"])
            assert "from_date" in input_json["args"]
            assert "to_date" in input_json["args"]

    def test_import_video(self, bridge: SwiftBridge, tmp_path: Path) -> None:
        """Test import_video method."""
        video_file = tmp_path / "test.mov"
        video_file.touch()

        mock_response = {"success": True, "data": "NEW-UUID-123"}

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(mock_response),
                stderr="",
            )

            uuid = bridge.import_video(video_file, album_name="Test Album")

            assert uuid == "NEW-UUID-123"

    def test_import_video_file_not_found(self, bridge: SwiftBridge) -> None:
        """Test import_video with non-existent file."""
        with pytest.raises(PhotosAccessError, match="not found"):
            bridge.import_video(Path("/nonexistent/video.mov"))

    def test_delete_video(self, bridge: SwiftBridge) -> None:
        """Test delete_video method."""
        mock_response = {"success": True, "data": True}

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(mock_response),
                stderr="",
            )

            result = bridge.delete_video("ABC123")

            assert result is True

    def test_download_from_icloud(self, bridge: SwiftBridge) -> None:
        """Test download_from_icloud method."""
        from vco.models.types import VideoInfo

        video = VideoInfo(
            uuid="ABC123",
            filename="test.mov",
            path=Path("/icloud/test.mov"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=5000000,
            duration=120.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=None,
            creation_date=datetime.now(),
            albums=[],
            is_in_icloud=True,
            is_local=False,
            location=None,
        )

        mock_response = {"success": True, "data": "/tmp/downloaded/test.mov"}

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout=json.dumps(mock_response),
                stderr="",
            )

            path = bridge.download_from_icloud(video)

            assert path == Path("/tmp/downloaded/test.mov")

    def test_get_photos_app_link_returns_empty(self, bridge: SwiftBridge) -> None:
        """Test that get_photos_app_link returns empty string."""
        from vco.models.types import VideoInfo

        video = VideoInfo(
            uuid="ABC123",
            filename="test.mov",
            path=Path("/test.mov"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=5000000,
            duration=120.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=None,
            creation_date=datetime.now(),
            albums=[],
            is_in_icloud=False,
            is_local=True,
            location=None,
        )

        result = bridge.get_photos_app_link(video)
        assert result == ""
