"""Unit tests for MetadataEmbedder."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vco.metadata.embedder import EmbedResult, MetadataEmbedder
from vco.models.metadata import GPSLocation, OriginalMetadata


class TestMetadataEmbedder:
    """Tests for MetadataEmbedder class."""

    @pytest.fixture
    def embedder(self):
        """Create a MetadataEmbedder instance."""
        return MetadataEmbedder()

    @pytest.fixture
    def sample_metadata(self):
        """Create sample metadata for testing."""
        jst = timezone(timedelta(hours=9))
        return OriginalMetadata(
            capture_date=datetime(2021, 11, 27, 9, 40, 30, tzinfo=jst),
            gps_location=GPSLocation(latitude=35.6762, longitude=139.6503),
            album_names=["Travel", "2021"],
            original_uuid="test-uuid",
            filename="MVI_0425.MOV",
        )

    @pytest.fixture
    def metadata_no_gps(self):
        """Create metadata without GPS."""
        jst = timezone(timedelta(hours=9))
        return OriginalMetadata(
            capture_date=datetime(2021, 11, 27, 9, 40, 30, tzinfo=jst),
            gps_location=None,
            album_names=[],
            original_uuid="test-uuid",
            filename="MVI_0425.MOV",
        )

    @pytest.fixture
    def metadata_no_date(self):
        """Create metadata without capture date."""
        return OriginalMetadata(
            capture_date=None,
            gps_location=GPSLocation(latitude=35.6762, longitude=139.6503),
            album_names=[],
            original_uuid="test-uuid",
            filename="MVI_0425.MOV",
        )

    @pytest.fixture
    def empty_metadata(self):
        """Create empty metadata."""
        return OriginalMetadata(
            capture_date=None,
            gps_location=None,
            album_names=[],
            original_uuid="test-uuid",
            filename="MVI_0425.MOV",
        )

    def test_check_exiftool_available(self, embedder):
        """Test _check_exiftool when exiftool is available."""
        with patch("shutil.which", return_value="/usr/local/bin/exiftool"):
            assert embedder._check_exiftool() is True
            assert embedder._exiftool_path == "/usr/local/bin/exiftool"

    def test_check_exiftool_not_available(self, embedder):
        """Test _check_exiftool when exiftool is not available."""
        with patch("shutil.which", return_value=None):
            assert embedder._check_exiftool() is False
            assert embedder._exiftool_path == ""

    def test_build_command_with_all_metadata(self, embedder, sample_metadata, tmp_path):
        """Test command building with capture date and GPS."""
        video_path = tmp_path / "test.mp4"
        cmd, fields = embedder._build_exiftool_command(video_path, sample_metadata)

        assert cmd[0] == "exiftool"
        assert "-overwrite_original" in cmd
        assert "-Keys:CreationDate=2021:11:27 09:40:30+09:00" in cmd
        assert "-Keys:GPSCoordinates=35.6762, 139.6503" in cmd
        assert str(video_path) in cmd
        assert "capture_date" in fields
        assert "gps_location" in fields

    def test_build_command_with_date_only(self, embedder, metadata_no_gps, tmp_path):
        """Test command building with capture date only."""
        video_path = tmp_path / "test.mp4"
        cmd, fields = embedder._build_exiftool_command(video_path, metadata_no_gps)

        assert "-Keys:CreationDate=2021:11:27 09:40:30+09:00" in cmd
        assert not any("GPSCoordinates" in arg for arg in cmd)
        assert "capture_date" in fields
        assert "gps_location" not in fields

    def test_build_command_with_gps_only(self, embedder, metadata_no_date, tmp_path):
        """Test command building with GPS only."""
        video_path = tmp_path / "test.mp4"
        cmd, fields = embedder._build_exiftool_command(video_path, metadata_no_date)

        assert not any("CreationDate" in arg for arg in cmd)
        assert "-Keys:GPSCoordinates=35.6762, 139.6503" in cmd
        assert "capture_date" not in fields
        assert "gps_location" in fields

    def test_embed_exiftool_not_available(self, embedder, sample_metadata, tmp_path):
        """Test embed when exiftool is not available."""
        video_path = tmp_path / "test.mp4"
        video_path.touch()

        with patch("shutil.which", return_value=None):
            result = embedder.embed(video_path, sample_metadata)

        assert result.success is False
        assert result.skipped is True
        assert "exiftool not found" in result.error_message

    def test_embed_empty_metadata(self, embedder, empty_metadata, tmp_path):
        """Test embed with empty metadata."""
        video_path = tmp_path / "test.mp4"
        video_path.touch()

        with patch("shutil.which", return_value="/usr/local/bin/exiftool"):
            result = embedder.embed(video_path, empty_metadata)

        assert result.success is True
        assert result.skipped is True
        assert result.embedded_fields == []

    def test_embed_success(self, embedder, sample_metadata, tmp_path):
        """Test successful metadata embedding."""
        video_path = tmp_path / "test.mp4"
        video_path.touch()

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""

        with patch("shutil.which", return_value="/usr/local/bin/exiftool"):
            with patch("subprocess.run", return_value=mock_result) as mock_run:
                result = embedder.embed(video_path, sample_metadata)

        assert result.success is True
        assert result.skipped is False
        assert "capture_date" in result.embedded_fields
        assert "gps_location" in result.embedded_fields
        mock_run.assert_called_once()

    def test_embed_exiftool_failure(self, embedder, sample_metadata, tmp_path):
        """Test embed when exiftool fails."""
        video_path = tmp_path / "test.mp4"
        video_path.touch()

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Error writing file"

        with patch("shutil.which", return_value="/usr/local/bin/exiftool"):
            with patch("subprocess.run", return_value=mock_result):
                result = embedder.embed(video_path, sample_metadata)

        assert result.success is False
        assert result.skipped is False
        assert "Error writing file" in result.error_message

    def test_embed_timeout(self, embedder, sample_metadata, tmp_path):
        """Test embed when exiftool times out."""
        import subprocess

        video_path = tmp_path / "test.mp4"
        video_path.touch()

        with patch("shutil.which", return_value="/usr/local/bin/exiftool"):
            with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("exiftool", 60)):
                result = embedder.embed(video_path, sample_metadata)

        assert result.success is False
        assert "timed out" in result.error_message


class TestEmbedResult:
    """Tests for EmbedResult dataclass."""

    def test_embed_result_defaults(self):
        """Test EmbedResult default values."""
        result = EmbedResult(success=True, video_path=Path("/test.mp4"))
        assert result.embedded_fields == []
        assert result.error_message is None
        assert result.skipped is False

    def test_embed_result_with_fields(self):
        """Test EmbedResult with embedded fields."""
        result = EmbedResult(
            success=True,
            video_path=Path("/test.mp4"),
            embedded_fields=["capture_date", "gps_location"],
        )
        assert len(result.embedded_fields) == 2
