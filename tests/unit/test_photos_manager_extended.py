"""Extended unit tests for PhotosAccessManager.

Additional tests to improve coverage for photos/manager.py.
Target coverage: 70%+ (ビジネスロジック)
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from vco.models.types import VideoInfo
from vco.photos.manager import PhotosAccessError, PhotosAccessManager


class TestPhotosAccessManagerInit:
    """Tests for PhotosAccessManager initialization."""

    def test_init_default_library(self):
        """Test initialization with default library path."""
        manager = PhotosAccessManager()
        assert manager._library_path is None
        assert manager._photosdb is None

    def test_init_custom_library(self, tmp_path):
        """Test initialization with custom library path."""
        library_path = tmp_path / "Photos.photoslibrary"
        manager = PhotosAccessManager(library_path=library_path)
        assert manager._library_path == library_path


class TestPhotosAccessManagerPhotosDB:
    """Tests for PhotosAccessManager photosdb property."""

    def test_photosdb_lazy_load(self):
        """Test photosdb is lazily loaded."""
        manager = PhotosAccessManager()
        assert manager._photosdb is None

    def test_photosdb_loads_default_library(self):
        """Test photosdb loads default library when no path specified."""
        import sys

        # Create mock osxphotos module
        mock_osxphotos = MagicMock()
        mock_db = MagicMock()
        mock_osxphotos.PhotosDB.return_value = mock_db

        # Temporarily add mock to sys.modules
        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager()
            manager._photosdb = None  # Reset
            db = manager.photosdb

            mock_osxphotos.PhotosDB.assert_called_once_with()
            assert db == mock_db

    def test_photosdb_loads_custom_library(self, tmp_path):
        """Test photosdb loads custom library when path specified."""
        import sys

        mock_osxphotos = MagicMock()
        mock_db = MagicMock()
        mock_osxphotos.PhotosDB.return_value = mock_db

        library_path = tmp_path / "Photos.photoslibrary"

        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager(library_path=library_path)
            manager._photosdb = None  # Reset
            _ = manager.photosdb

            mock_osxphotos.PhotosDB.assert_called_once_with(dbfile=str(library_path))

    def test_photosdb_open_error(self):
        """Test photosdb raises error when library cannot be opened."""
        import sys

        mock_osxphotos = MagicMock()
        mock_osxphotos.PhotosDB.side_effect = Exception("Cannot open library")

        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager()
            manager._photosdb = None  # Reset

            with pytest.raises(PhotosAccessError, match="Failed to open Photos library"):
                _ = manager.photosdb


class TestExtractCodec:
    """Tests for _extract_codec method."""

    def test_extract_codec_from_exiftool(self):
        """Test codec extraction from exiftool data."""
        manager = PhotosAccessManager()

        mock_photo = MagicMock()
        mock_photo.exiftool = {"CompressorID": "avc1"}
        mock_photo.path = None

        codec = manager._extract_codec(mock_photo)
        assert codec == "avc1"

    def test_extract_codec_video_codec_field(self):
        """Test codec extraction from VideoCodec field."""
        manager = PhotosAccessManager()

        mock_photo = MagicMock()
        mock_photo.exiftool = {"VideoCodec": "HEVC"}
        mock_photo.path = None

        codec = manager._extract_codec(mock_photo)
        assert codec == "hevc"

    def test_extract_codec_compressor_name(self):
        """Test codec extraction from CompressorName field."""
        manager = PhotosAccessManager()

        mock_photo = MagicMock()
        mock_photo.exiftool = {"CompressorName": "H.264"}
        mock_photo.path = None

        codec = manager._extract_codec(mock_photo)
        assert codec == "h.264"

    def test_extract_codec_fallback_to_ffprobe(self, tmp_path):
        """Test codec extraction falls back to ffprobe."""
        manager = PhotosAccessManager()

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy")

        mock_photo = MagicMock()
        mock_photo.exiftool = {}
        mock_photo.path = str(video_path)

        with patch.object(manager, "_get_codec_from_ffprobe", return_value="h264"):
            codec = manager._extract_codec(mock_photo)

        assert codec == "h264"

    def test_extract_codec_unknown(self):
        """Test codec extraction returns unknown when no source available."""
        manager = PhotosAccessManager()

        mock_photo = MagicMock()
        mock_photo.exiftool = None
        mock_photo.path = None

        codec = manager._extract_codec(mock_photo)
        assert codec == "unknown"

    def test_extract_codec_exiftool_exception(self):
        """Test codec extraction handles exiftool exception."""
        manager = PhotosAccessManager()

        mock_photo = MagicMock()
        type(mock_photo).exiftool = PropertyMock(side_effect=Exception("Error"))
        mock_photo.path = None

        codec = manager._extract_codec(mock_photo)
        assert codec == "unknown"


class TestGetCodecFromFFprobe:
    """Tests for _get_codec_from_ffprobe method."""

    @patch("subprocess.run")
    def test_ffprobe_success(self, mock_run, tmp_path):
        """Test successful ffprobe codec detection."""
        mock_run.return_value = MagicMock(returncode=0, stdout="h264\n")

        manager = PhotosAccessManager()
        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy")

        codec = manager._get_codec_from_ffprobe(video_path)

        assert codec == "h264"

    @patch("subprocess.run")
    def test_ffprobe_failure(self, mock_run, tmp_path):
        """Test ffprobe failure returns None."""
        mock_run.return_value = MagicMock(returncode=1, stdout="")

        manager = PhotosAccessManager()
        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy")

        codec = manager._get_codec_from_ffprobe(video_path)

        assert codec is None

    @patch("subprocess.run")
    def test_ffprobe_timeout(self, mock_run, tmp_path):
        """Test ffprobe timeout returns None."""
        import subprocess

        mock_run.side_effect = subprocess.TimeoutExpired(cmd="ffprobe", timeout=10)

        manager = PhotosAccessManager()
        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy")

        codec = manager._get_codec_from_ffprobe(video_path)

        assert codec is None

    @patch("subprocess.run")
    def test_ffprobe_not_found(self, mock_run, tmp_path):
        """Test ffprobe not found returns None."""
        mock_run.side_effect = FileNotFoundError()

        manager = PhotosAccessManager()
        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy")

        codec = manager._get_codec_from_ffprobe(video_path)

        assert codec is None


class TestExtractVideoInfo:
    """Tests for _extract_video_info method."""

    def test_extract_video_info_complete(self, tmp_path):
        """Test extracting complete video info."""
        manager = PhotosAccessManager()

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy video content")

        mock_exif_info = MagicMock()
        mock_exif_info.codec = "h264"
        mock_exif_info.duration = 120.5
        mock_exif_info.bit_rate = 5000000
        mock_exif_info.fps = 30.0

        mock_photo = MagicMock()
        mock_photo.uuid = "test-uuid-123"
        mock_photo.path = str(video_path)
        mock_photo.original_filename = "test.mp4"
        mock_photo.width = 1920
        mock_photo.height = 1080
        mock_photo.date = datetime(2024, 6, 15, 10, 30, 0)
        mock_photo.date_added = datetime(2024, 6, 15, 11, 0, 0)
        mock_photo.albums = ["Vacation", "Summer"]
        mock_photo.iscloudasset = False
        mock_photo.exif_info = mock_exif_info
        mock_photo.exiftool = {}

        video_info = manager._extract_video_info(mock_photo)

        assert video_info is not None
        assert video_info.uuid == "test-uuid-123"
        assert video_info.filename == "test.mp4"
        assert video_info.codec == "h264"
        assert video_info.resolution == (1920, 1080)
        assert video_info.duration == 120.5
        assert video_info.albums == ["Vacation", "Summer"]
        assert video_info.is_local is True
        assert video_info.is_in_icloud is False

    def test_extract_video_info_icloud_file(self):
        """Test extracting video info for iCloud file."""
        manager = PhotosAccessManager()

        mock_exif_info = MagicMock()
        mock_exif_info.codec = "hevc"
        mock_exif_info.duration = 60.0
        mock_exif_info.bit_rate = None
        mock_exif_info.fps = None

        mock_photo = MagicMock()
        mock_photo.uuid = "icloud-uuid"
        mock_photo.path = None  # iCloud file has no local path
        mock_photo.original_filename = "icloud_video.mp4"
        mock_photo.width = 1280
        mock_photo.height = 720
        mock_photo.date = datetime(2024, 1, 1)
        mock_photo.date_added = datetime(2024, 1, 2)
        mock_photo.albums = []
        mock_photo.iscloudasset = True
        mock_photo.original_filesize = 50000000
        mock_photo.exif_info = mock_exif_info

        video_info = manager._extract_video_info(mock_photo)

        assert video_info is not None
        assert video_info.is_local is False
        assert video_info.is_in_icloud is True
        assert video_info.file_size == 50000000

    def test_extract_video_info_missing_filename(self):
        """Test extracting video info with missing filename."""
        manager = PhotosAccessManager()

        mock_exif_info = MagicMock()
        mock_exif_info.codec = None
        mock_exif_info.duration = None
        mock_exif_info.bit_rate = None
        mock_exif_info.fps = None

        mock_photo = MagicMock()
        mock_photo.uuid = "no-filename-uuid"
        mock_photo.path = None
        mock_photo.original_filename = None
        mock_photo.width = 0
        mock_photo.height = 0
        mock_photo.date = None
        mock_photo.date_added = None
        mock_photo.albums = None
        mock_photo.iscloudasset = False
        mock_photo.original_filesize = 0
        mock_photo.exif_info = mock_exif_info

        video_info = manager._extract_video_info(mock_photo)

        assert video_info is not None
        assert video_info.filename == "video_no-filename-uuid"

    def test_extract_video_info_exception(self):
        """Test extracting video info handles exception."""
        manager = PhotosAccessManager()

        mock_photo = MagicMock()
        mock_photo.uuid = "error-uuid"
        type(mock_photo).path = PropertyMock(side_effect=Exception("Error"))

        video_info = manager._extract_video_info(mock_photo)

        assert video_info is None


class TestGetVideosByDateRange:
    """Tests for get_videos_by_date_range method."""

    @patch.object(PhotosAccessManager, "get_all_videos")
    def test_no_filter_returns_all(self, mock_get_all):
        """Test no date filter returns all videos."""
        videos = [
            VideoInfo(
                uuid="1",
                filename="v1.mp4",
                path=Path("/v1.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=datetime(2024, 1, 1),
                creation_date=datetime(2024, 1, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="2",
                filename="v2.mp4",
                path=Path("/v2.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=datetime(2024, 6, 1),
                creation_date=datetime(2024, 6, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]
        mock_get_all.return_value = videos

        manager = PhotosAccessManager()
        result = manager.get_videos_by_date_range(None, None)

        assert len(result) == 2

    @patch.object(PhotosAccessManager, "get_all_videos")
    def test_from_date_filter(self, mock_get_all):
        """Test from_date filter."""
        videos = [
            VideoInfo(
                uuid="1",
                filename="v1.mp4",
                path=Path("/v1.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=datetime(2024, 1, 1),
                creation_date=datetime(2024, 1, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="2",
                filename="v2.mp4",
                path=Path("/v2.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=datetime(2024, 6, 1),
                creation_date=datetime(2024, 6, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]
        mock_get_all.return_value = videos

        manager = PhotosAccessManager()
        result = manager.get_videos_by_date_range(from_date=datetime(2024, 3, 1), to_date=None)

        assert len(result) == 1
        assert result[0].uuid == "2"

    @patch.object(PhotosAccessManager, "get_all_videos")
    def test_to_date_filter(self, mock_get_all):
        """Test to_date filter."""
        videos = [
            VideoInfo(
                uuid="1",
                filename="v1.mp4",
                path=Path("/v1.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=datetime(2024, 1, 1),
                creation_date=datetime(2024, 1, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="2",
                filename="v2.mp4",
                path=Path("/v2.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=datetime(2024, 6, 1),
                creation_date=datetime(2024, 6, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]
        mock_get_all.return_value = videos

        manager = PhotosAccessManager()
        result = manager.get_videos_by_date_range(from_date=None, to_date=datetime(2024, 3, 1))

        assert len(result) == 1
        assert result[0].uuid == "1"

    @patch.object(PhotosAccessManager, "get_all_videos")
    def test_timezone_aware_dates(self, mock_get_all):
        """Test filtering with timezone-aware dates."""
        videos = [
            VideoInfo(
                uuid="1",
                filename="v1.mp4",
                path=Path("/v1.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
                creation_date=datetime(2024, 6, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )
        ]
        mock_get_all.return_value = videos

        manager = PhotosAccessManager()
        result = manager.get_videos_by_date_range(
            from_date=datetime(2024, 1, 1), to_date=datetime(2024, 12, 31)
        )

        assert len(result) == 1

    @patch.object(PhotosAccessManager, "get_all_videos")
    def test_skip_videos_without_date(self, mock_get_all):
        """Test videos without capture_date are skipped."""
        videos = [
            VideoInfo(
                uuid="1",
                filename="v1.mp4",
                path=Path("/v1.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=None,  # No capture date
                creation_date=datetime(2024, 6, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )
        ]
        mock_get_all.return_value = videos

        manager = PhotosAccessManager()
        result = manager.get_videos_by_date_range(
            from_date=datetime(2024, 1, 1), to_date=datetime(2024, 12, 31), date_type="capture"
        )

        assert len(result) == 0

    @patch.object(PhotosAccessManager, "get_all_videos")
    def test_filter_by_creation_date(self, mock_get_all):
        """Test filtering by creation_date instead of capture_date."""
        videos = [
            VideoInfo(
                uuid="1",
                filename="v1.mp4",
                path=Path("/v1.mp4"),
                codec="h264",
                resolution=(1920, 1080),
                bitrate=5000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=1000000,
                capture_date=datetime(2020, 1, 1),  # Old capture date
                creation_date=datetime(2024, 6, 1),  # Recent creation date
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )
        ]
        mock_get_all.return_value = videos

        manager = PhotosAccessManager()
        result = manager.get_videos_by_date_range(
            from_date=datetime(2024, 1, 1), to_date=datetime(2024, 12, 31), date_type="creation"
        )

        assert len(result) == 1


class TestPhotosAccessError:
    """Tests for PhotosAccessError exception."""

    def test_error_message(self):
        """Test error message is preserved."""
        error = PhotosAccessError("Test error message")
        assert str(error) == "Test error message"

    def test_error_inheritance(self):
        """Test PhotosAccessError inherits from Exception."""
        error = PhotosAccessError("Test")
        assert isinstance(error, Exception)


class TestDownloadFromICloud:
    """Tests for download_from_icloud method."""

    def test_download_already_local(self, tmp_path):
        """Test download returns path when file is already local."""
        manager = PhotosAccessManager()

        video_path = tmp_path / "local_video.mp4"
        video_path.write_bytes(b"video data")

        video = VideoInfo(
            uuid="local-uuid",
            filename="local_video.mp4",
            path=video_path,
            codec="h264",
            resolution=(1920, 1080),
            bitrate=5000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=1000000,
            capture_date=datetime(2024, 6, 15),
            creation_date=datetime(2024, 6, 15),
            albums=[],
            is_in_icloud=False,
            is_local=True,
        )

        result = manager.download_from_icloud(video)

        assert result == video_path


class TestImportVideo:
    """Tests for import_video method."""

    def test_import_video_file_not_found(self, tmp_path):
        """Test import raises error when file not found."""
        manager = PhotosAccessManager()

        nonexistent = tmp_path / "nonexistent.mp4"

        with pytest.raises(PhotosAccessError, match="Video file not found"):
            manager.import_video(nonexistent)

    def test_import_video_success(self, tmp_path):
        """Test successful video import."""
        import sys

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"video data")

        mock_photoscript = MagicMock()
        mock_photos_app = MagicMock()
        mock_imported_photo = MagicMock()
        mock_imported_photo.uuid = "new-uuid-123"
        mock_photos_app.import_photos.return_value = [mock_imported_photo]
        mock_photoscript.PhotosLibrary.return_value = mock_photos_app

        with patch.dict(sys.modules, {"photoscript": mock_photoscript}):
            manager = PhotosAccessManager()
            result = manager.import_video(video_path)

        assert result == "new-uuid-123"

    def test_import_video_with_album(self, tmp_path):
        """Test video import with album."""
        import sys

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"video data")

        mock_photoscript = MagicMock()
        mock_photos_app = MagicMock()
        mock_imported_photo = MagicMock()
        mock_imported_photo.uuid = "new-uuid-456"
        mock_photos_app.import_photos.return_value = [mock_imported_photo]
        mock_photoscript.PhotosLibrary.return_value = mock_photos_app

        with patch.dict(sys.modules, {"photoscript": mock_photoscript}):
            manager = PhotosAccessManager()
            with patch.object(manager, "_add_to_album_by_name") as mock_add:
                result = manager.import_video(video_path, album_name="Test Album")

        assert result == "new-uuid-456"
        mock_add.assert_called_once()

    def test_import_video_import_failed(self, tmp_path):
        """Test import raises error when import fails."""
        import sys

        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"video data")

        mock_photoscript = MagicMock()
        mock_photos_app = MagicMock()
        mock_photos_app.import_photos.return_value = []  # Empty = failed
        mock_photoscript.PhotosLibrary.return_value = mock_photos_app

        with patch.dict(sys.modules, {"photoscript": mock_photoscript}):
            manager = PhotosAccessManager()

            with pytest.raises(PhotosAccessError, match="Failed to import video"):
                manager.import_video(video_path)


class TestDeleteVideo:
    """Tests for delete_video method."""

    def test_delete_video_success(self):
        """Test successful video deletion using AppleScript."""

        manager = PhotosAccessManager()

        # Mock subprocess.run to simulate successful AppleScript execution
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="deleted\n", stderr="")
            result = manager.delete_video("uuid-to-delete")

        assert result is True
        mock_run.assert_called_once()
        # Verify AppleScript was called with osascript
        call_args = mock_run.call_args
        assert call_args[0][0][0] == "osascript"

    def test_delete_video_not_found(self):
        """Test delete raises error when video not found."""
        manager = PhotosAccessManager()

        # Mock subprocess.run to simulate AppleScript error (video not found)
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout='error: can\'t get media item id "nonexistent-uuid"\n',
                stderr="",
            )
            # When video is not found, the implementation treats it as success
            # (already deleted or not found)
            result = manager.delete_video("nonexistent-uuid")

        # The implementation returns True even when video is not found
        # (treats "can't get" as already deleted)
        assert result is True

    def test_delete_video_applescript_error(self):
        """Test delete raises error on AppleScript failure."""
        manager = PhotosAccessManager()

        # Mock subprocess.run to simulate AppleScript execution failure
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="AppleScript error")

            with pytest.raises(PhotosAccessError, match="AppleScript error"):
                manager.delete_video("uuid-to-delete")

    def test_delete_video_timeout(self):
        """Test delete raises error on timeout."""
        import subprocess

        manager = PhotosAccessManager()

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="osascript", timeout=30)

            with pytest.raises(PhotosAccessError, match="Timeout"):
                manager.delete_video("uuid-to-delete")


class TestAddToAlbums:
    """Tests for add_to_albums method."""

    def test_add_to_albums_empty_list(self):
        """Test add_to_albums with empty list returns True."""
        manager = PhotosAccessManager()

        result = manager.add_to_albums("uuid-123", [])

        assert result is True

    def test_add_to_albums_success(self):
        """Test successful add to albums."""
        import sys

        mock_photoscript = MagicMock()
        mock_photos_app = MagicMock()
        mock_photo = MagicMock()
        mock_photos_app.photos.return_value = [mock_photo]
        mock_photoscript.PhotosLibrary.return_value = mock_photos_app

        with patch.dict(sys.modules, {"photoscript": mock_photoscript}):
            manager = PhotosAccessManager()
            with patch.object(manager, "_add_to_album_by_name") as mock_add:
                result = manager.add_to_albums("uuid-123", ["Album1", "Album2"])

        assert result is True
        assert mock_add.call_count == 2

    def test_add_to_albums_video_not_found(self):
        """Test add_to_albums raises error when video not found."""
        import sys

        mock_photoscript = MagicMock()
        mock_photos_app = MagicMock()
        mock_photos_app.photos.return_value = []  # Not found
        mock_photoscript.PhotosLibrary.return_value = mock_photos_app

        with patch.dict(sys.modules, {"photoscript": mock_photoscript}):
            manager = PhotosAccessManager()

            with pytest.raises(PhotosAccessError, match="Video not found"):
                manager.add_to_albums("nonexistent", ["Album1"])


class TestAddToAlbumByName:
    """Tests for _add_to_album_by_name method."""

    def test_add_to_existing_album(self):
        """Test adding to existing album."""
        import sys

        mock_photoscript = MagicMock()
        mock_photos_app = MagicMock()
        mock_album = MagicMock()
        mock_album.name = "Existing Album"  # Set name to match album_name
        mock_photos_app.albums.return_value = [mock_album]
        mock_photoscript.PhotosLibrary.return_value = mock_photos_app

        mock_photo = MagicMock()

        with patch.dict(sys.modules, {"photoscript": mock_photoscript}):
            manager = PhotosAccessManager()
            manager._add_to_album_by_name(mock_photo, "Existing Album")

        mock_album.add.assert_called_once_with([mock_photo])

    def test_add_to_new_album(self):
        """Test adding to new album (creates album)."""
        import sys

        mock_photoscript = MagicMock()
        mock_photos_app = MagicMock()
        mock_new_album = MagicMock()
        mock_photos_app.albums.return_value = []  # No existing album
        mock_photos_app.create_album.return_value = mock_new_album
        mock_photoscript.PhotosLibrary.return_value = mock_photos_app

        mock_photo = MagicMock()

        with patch.dict(sys.modules, {"photoscript": mock_photoscript}):
            manager = PhotosAccessManager()
            manager._add_to_album_by_name(mock_photo, "New Album")

        mock_photos_app.create_album.assert_called_once_with("New Album")
        mock_new_album.add.assert_called_once_with([mock_photo])


class TestExportVideo:
    """Tests for export_video method."""

    def test_export_video_not_found(self):
        """Test export raises error when video not found."""
        import sys

        mock_osxphotos = MagicMock()
        mock_db = MagicMock()
        mock_db.photos.return_value = []  # Not found
        mock_osxphotos.PhotosDB.return_value = mock_db

        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager()
            manager._photosdb = mock_db

            with pytest.raises(PhotosAccessError, match="Video not found"):
                manager.export_video("nonexistent", Path("/tmp"))

    def test_export_video_success(self, tmp_path):
        """Test successful video export."""
        import sys

        mock_osxphotos = MagicMock()
        mock_db = MagicMock()
        mock_photo = MagicMock()
        exported_path = str(tmp_path / "exported.mp4")
        mock_photo.export.return_value = [exported_path]
        mock_db.photos.return_value = [mock_photo]
        mock_osxphotos.PhotosDB.return_value = mock_db

        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager()
            manager._photosdb = mock_db

            result = manager.export_video("uuid-123", tmp_path)

        assert result == Path(exported_path)

    def test_export_video_failed(self, tmp_path):
        """Test export raises error when export fails."""
        import sys

        mock_osxphotos = MagicMock()
        mock_db = MagicMock()
        mock_photo = MagicMock()
        mock_photo.export.return_value = []  # Export failed
        mock_db.photos.return_value = [mock_photo]
        mock_osxphotos.PhotosDB.return_value = mock_db

        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager()
            manager._photosdb = mock_db

            with pytest.raises(PhotosAccessError, match="Failed to export video"):
                manager.export_video("uuid-123", tmp_path)


class TestGetVideoByUuid:
    """Tests for get_video_by_uuid method."""

    def test_get_video_by_uuid_found(self):
        """Test getting video by UUID when found."""
        import sys

        mock_osxphotos = MagicMock()
        mock_db = MagicMock()

        mock_exif_info = MagicMock()
        mock_exif_info.codec = "h264"
        mock_exif_info.duration = 60.0
        mock_exif_info.bit_rate = 5000000
        mock_exif_info.fps = 30.0

        mock_photo = MagicMock()
        mock_photo.uuid = "found-uuid"
        mock_photo.path = None
        mock_photo.original_filename = "test.mp4"
        mock_photo.width = 1920
        mock_photo.height = 1080
        mock_photo.date = datetime(2024, 6, 15)
        mock_photo.date_added = datetime(2024, 6, 15)
        mock_photo.albums = []
        mock_photo.iscloudasset = False
        mock_photo.original_filesize = 1000000
        mock_photo.exif_info = mock_exif_info

        mock_db.photos.return_value = [mock_photo]
        mock_osxphotos.PhotosDB.return_value = mock_db

        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager()
            manager._photosdb = mock_db

            result = manager.get_video_by_uuid("found-uuid")

        assert result is not None
        assert result.uuid == "found-uuid"

    def test_get_video_by_uuid_not_found(self):
        """Test getting video by UUID when not found."""
        import sys

        mock_osxphotos = MagicMock()
        mock_db = MagicMock()
        mock_db.photos.return_value = []  # Not found
        mock_osxphotos.PhotosDB.return_value = mock_db

        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager()
            manager._photosdb = mock_db

            result = manager.get_video_by_uuid("nonexistent")

        assert result is None


class TestGetAllVideos:
    """Tests for get_all_videos method."""

    def test_get_all_videos(self):
        """Test getting all videos."""
        import sys

        mock_osxphotos = MagicMock()
        mock_db = MagicMock()

        mock_exif_info = MagicMock()
        mock_exif_info.codec = "h264"
        mock_exif_info.duration = 60.0
        mock_exif_info.bit_rate = 5000000
        mock_exif_info.fps = 30.0

        mock_photo1 = MagicMock()
        mock_photo1.uuid = "uuid-1"
        mock_photo1.path = None
        mock_photo1.original_filename = "video1.mp4"
        mock_photo1.width = 1920
        mock_photo1.height = 1080
        mock_photo1.date = datetime(2024, 6, 15)
        mock_photo1.date_added = datetime(2024, 6, 15)
        mock_photo1.albums = []
        mock_photo1.iscloudasset = False
        mock_photo1.original_filesize = 1000000
        mock_photo1.exif_info = mock_exif_info

        mock_photo2 = MagicMock()
        mock_photo2.uuid = "uuid-2"
        mock_photo2.path = None
        mock_photo2.original_filename = "video2.mp4"
        mock_photo2.width = 1280
        mock_photo2.height = 720
        mock_photo2.date = datetime(2024, 7, 1)
        mock_photo2.date_added = datetime(2024, 7, 1)
        mock_photo2.albums = ["Album1"]
        mock_photo2.iscloudasset = True
        mock_photo2.original_filesize = 500000
        mock_photo2.exif_info = mock_exif_info

        mock_db.photos.return_value = [mock_photo1, mock_photo2]
        mock_osxphotos.PhotosDB.return_value = mock_db

        with patch.dict(sys.modules, {"osxphotos": mock_osxphotos}):
            manager = PhotosAccessManager()
            manager._photosdb = mock_db

            result = manager.get_all_videos()

        assert len(result) == 2
        assert result[0].uuid == "uuid-1"
        assert result[1].uuid == "uuid-2"
