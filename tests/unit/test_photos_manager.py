"""Unit tests for PhotosAccessManager.

Tests for video extraction from Photos library.
Validates: Requirements 1.1, 1.2, 1.3
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

from vco.models.types import VideoInfo
from vco.photos.manager import PhotosAccessManager


class MockExifInfo:
    """Mock for osxphotos ExifInfo object."""

    def __init__(
        self,
        codec: str | None = None,
        duration: float | None = None,
        bit_rate: float | None = None,
        fps: float | None = None,
    ):
        self.codec = codec
        self.duration = duration
        self.bit_rate = bit_rate
        self.fps = fps


class MockPhotoInfo:
    """Mock for osxphotos PhotoInfo object."""

    def __init__(
        self,
        uuid: str = "test-uuid-123",
        path: str | None = "/tmp/test_video.mov",
        width: int = 1920,
        height: int = 1080,
        date: datetime | None = None,
        date_added: datetime | None = None,
        original_filename: str = "test_video.mov",
        original_filesize: int = 100000000,
        iscloudasset: bool = False,
        albums: list | None = None,
        exif_info: MockExifInfo | None = None,
        exiftool: dict | None = None,
        location: tuple[float, float] | None = None,
    ):
        self.uuid = uuid
        self._path = path
        self.width = width
        self.height = height
        self.date = date or datetime(2020, 7, 15, 14, 30, 0)
        self.date_added = date_added or datetime(2020, 7, 15, 14, 30, 0)
        self.original_filename = original_filename
        self.original_filesize = original_filesize
        self.iscloudasset = iscloudasset
        self.albums = albums or []
        self._exif_info = exif_info
        self._exiftool = exiftool
        self.location = location

    @property
    def path(self):
        return self._path

    @property
    def exif_info(self):
        return self._exif_info

    @property
    def exiftool(self):
        return self._exiftool


class TestExtractVideoInfo:
    """Tests for _extract_video_info method."""

    def test_extract_basic_video_info(self):
        """Test extraction of basic video information."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-uuid-123",
            path="/tmp/test_video.mov",
            width=1920,
            height=1080,
            original_filename="test_video.mov",
            original_filesize=100000000,
            exif_info=MockExifInfo(
                codec="avc1",
                duration=120.5,
                bit_rate=25000000,
                fps=30.0,
            ),
        )

        # Mock path.exists() to return False (file doesn't exist in test)
        with patch.object(Path, "exists", return_value=False):
            result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.uuid == "test-uuid-123"
        assert result.filename == "test_video.mov"
        assert result.resolution == (1920, 1080)
        assert result.codec == "avc1"
        assert result.duration == 120.5
        assert result.bitrate == 25000000
        assert result.frame_rate == 30.0

    def test_extract_video_info_with_timezone_aware_date(self):
        """Test extraction with timezone-aware datetime."""
        manager = PhotosAccessManager()

        tz_aware_date = datetime(2020, 7, 15, 14, 30, 0, tzinfo=timezone.utc)

        mock_photo = MockPhotoInfo(
            uuid="test-uuid-tz",
            date=tz_aware_date,
            date_added=tz_aware_date,
            exif_info=MockExifInfo(codec="avc1", duration=60.0),
        )

        with patch.object(Path, "exists", return_value=False):
            result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.capture_date == tz_aware_date
        assert result.creation_date == tz_aware_date

    def test_extract_video_info_without_exif_info(self):
        """Test extraction when exif_info is None."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-uuid-no-exif",
            exif_info=None,
        )

        with patch.object(Path, "exists", return_value=False):
            result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.codec == "unknown"
        assert result.duration == 0.0
        assert result.bitrate == 0
        assert result.frame_rate == 0.0

    def test_extract_video_info_with_partial_exif_info(self):
        """Test extraction when exif_info has partial data."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-uuid-partial",
            exif_info=MockExifInfo(
                codec="hevc",
                duration=None,  # Missing duration
                bit_rate=None,  # Missing bitrate
                fps=29.97,
            ),
        )

        with patch.object(Path, "exists", return_value=False):
            result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.codec == "hevc"
        assert result.duration == 0.0  # Default when None
        assert result.bitrate == 0  # Default when None
        assert result.frame_rate == 29.97

    def test_extract_video_info_icloud_video(self):
        """Test extraction for iCloud video (path is None)."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-uuid-icloud",
            path=None,  # iCloud video without local path
            original_filesize=500000000,
            iscloudasset=True,
            exif_info=MockExifInfo(codec="avc1", duration=300.0),
        )

        result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.uuid == "test-uuid-icloud"
        assert result.is_in_icloud is True
        assert result.is_local is False
        assert result.file_size == 500000000  # From original_filesize
        assert result.path == Path("/icloud/test-uuid-icloud")  # Placeholder path

    def test_extract_video_info_with_albums(self):
        """Test extraction with album information."""
        manager = PhotosAccessManager()

        # osxphotos returns albums as a list of strings (album names directly)
        mock_photo = MockPhotoInfo(
            uuid="test-uuid-albums",
            albums=["Vacation 2020", "Family", "Travel"],
            exif_info=MockExifInfo(codec="avc1", duration=60.0),
        )

        with patch.object(Path, "exists", return_value=False):
            result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert "Vacation 2020" in result.albums
        assert "Family" in result.albums
        assert "Travel" in result.albums
        assert len(result.albums) == 3

    def test_extract_video_info_with_zero_dimensions(self):
        """Test extraction when width/height are zero or None."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-uuid-zero-dim",
            width=0,
            height=None,
            exif_info=MockExifInfo(codec="avc1", duration=60.0),
        )
        # Override height to None
        mock_photo.height = None

        with patch.object(Path, "exists", return_value=False):
            result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.resolution == (0, 0)

    def test_extract_video_info_missing_original_filename(self):
        """Test extraction when original_filename is None."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-uuid-no-filename",
            path="/tmp/some_video.mov",
            original_filename=None,
            exif_info=MockExifInfo(codec="avc1", duration=60.0),
        )

        # Mock both exists() and stat() for the path
        with patch.object(Path, "exists", return_value=True):
            with patch.object(Path, "stat") as mock_stat:
                mock_stat.return_value.st_size = 100000000
                result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.filename == "some_video.mov"  # Falls back to path.name

    def test_extract_video_info_no_filename_no_path(self):
        """Test extraction when both original_filename and path are None."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-uuid-no-name",
            path=None,
            original_filename=None,
            exif_info=MockExifInfo(codec="avc1", duration=60.0),
        )

        result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.filename == "video_test-uuid-no-name"  # Falls back to uuid

    def test_extract_video_info_exception_handling(self):
        """Test that exceptions are handled gracefully."""
        manager = PhotosAccessManager()

        # Create a mock that raises an exception when accessing properties
        mock_photo = MagicMock()
        mock_photo.uuid = "test-uuid-error"
        mock_photo.path = None
        type(mock_photo).width = PropertyMock(side_effect=Exception("Test error"))

        result = manager._extract_video_info(mock_photo)

        # Should return None and not raise
        assert result is None


class TestDateRangeFiltering:
    """Tests for date range filtering with timezone handling."""

    def test_filter_with_naive_dates(self):
        """Test filtering with naive (no timezone) dates."""
        manager = PhotosAccessManager()

        # Create videos spanning multiple years (2015-2024)
        videos = [
            VideoInfo(
                uuid="video1",
                filename="video1.mov",
                path=Path("/tmp/video1.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2015, 6, 15),
                creation_date=datetime(2015, 6, 15),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="video2",
                filename="video2.mov",
                path=Path("/tmp/video2.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2020, 3, 20),
                creation_date=datetime(2020, 3, 20),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="video3",
                filename="video3.mov",
                path=Path("/tmp/video3.mov"),
                codec="hevc",
                resolution=(3840, 2160),
                bitrate=50000000,
                duration=180.0,
                frame_rate=60.0,
                file_size=500000000,
                capture_date=datetime(2024, 11, 10),
                creation_date=datetime(2024, 11, 10),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]

        # Mock get_all_videos to return our test videos
        with patch.object(manager, "get_all_videos", return_value=videos):
            # Filter for 2015 only
            result = manager.get_videos_by_date_range(
                from_date=datetime(2015, 1, 1),
                to_date=datetime(2015, 12, 31),
            )

        assert len(result) == 1
        assert result[0].uuid == "video1"

    def test_filter_wide_date_range(self):
        """Test filtering with a wide date range (2018-2025)."""
        manager = PhotosAccessManager()

        videos = [
            VideoInfo(
                uuid="video_2017",
                filename="video_2017.mov",
                path=Path("/tmp/video_2017.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2017, 12, 31),
                creation_date=datetime(2017, 12, 31),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="video_2020",
                filename="video_2020.mov",
                path=Path("/tmp/video_2020.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=120.0,
                frame_rate=30.0,
                file_size=200000000,
                capture_date=datetime(2020, 6, 15),
                creation_date=datetime(2020, 6, 15),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="video_2024",
                filename="video_2024.mov",
                path=Path("/tmp/video_2024.mov"),
                codec="hevc",
                resolution=(3840, 2160),
                bitrate=50000000,
                duration=300.0,
                frame_rate=60.0,
                file_size=800000000,
                capture_date=datetime(2024, 8, 20),
                creation_date=datetime(2024, 8, 20),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="video_2025",
                filename="video_2025.mov",
                path=Path("/tmp/video_2025.mov"),
                codec="hevc",
                resolution=(3840, 2160),
                bitrate=50000000,
                duration=60.0,
                frame_rate=60.0,
                file_size=300000000,
                capture_date=datetime(2025, 1, 5),
                creation_date=datetime(2025, 1, 5),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]

        with patch.object(manager, "get_all_videos", return_value=videos):
            # Filter for 2018-2025
            result = manager.get_videos_by_date_range(
                from_date=datetime(2018, 1, 1),
                to_date=datetime(2025, 12, 31),
            )

        # Should include 2020, 2024, 2025 but not 2017
        assert len(result) == 3
        uuids = [v.uuid for v in result]
        assert "video_2017" not in uuids
        assert "video_2020" in uuids
        assert "video_2024" in uuids
        assert "video_2025" in uuids

    def test_filter_with_timezone_aware_video_dates(self):
        """Test filtering when video dates have timezone info."""
        manager = PhotosAccessManager()

        # Create videos with timezone-aware dates (like Photos library returns)
        from zoneinfo import ZoneInfo

        jst = ZoneInfo("Asia/Tokyo")

        videos = [
            VideoInfo(
                uuid="video1",
                filename="video1.mov",
                path=Path("/tmp/video1.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2022, 6, 15, 12, 0, 0, tzinfo=jst),
                creation_date=datetime(2022, 6, 15, 12, 0, 0, tzinfo=jst),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="video2",
                filename="video2.mov",
                path=Path("/tmp/video2.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2024, 3, 20, 12, 0, 0, tzinfo=jst),
                creation_date=datetime(2024, 3, 20, 12, 0, 0, tzinfo=jst),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]

        # Filter with naive dates (like CLI provides)
        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(
                from_date=datetime(2022, 1, 1),
                to_date=datetime(2023, 12, 31),
            )

        assert len(result) == 1
        assert result[0].uuid == "video1"

    def test_filter_with_no_capture_date(self):
        """Test filtering excludes videos without capture_date."""
        manager = PhotosAccessManager()

        videos = [
            VideoInfo(
                uuid="video1",
                filename="video1.mov",
                path=Path("/tmp/video1.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=None,  # No capture date
                creation_date=datetime(2023, 6, 15),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(
                from_date=datetime(2020, 1, 1),
                to_date=datetime(2025, 12, 31),
                date_type="capture",
            )

        assert len(result) == 0

    def test_filter_by_creation_date(self):
        """Test filtering by creation_date instead of capture_date."""
        manager = PhotosAccessManager()

        videos = [
            VideoInfo(
                uuid="video1",
                filename="video1.mov",
                path=Path("/tmp/video1.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2010, 1, 1),  # Old capture date
                creation_date=datetime(2023, 6, 15),  # Recent creation date
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]

        with patch.object(manager, "get_all_videos", return_value=videos):
            # Filter by capture_date - should not match
            result_capture = manager.get_videos_by_date_range(
                from_date=datetime(2020, 1, 1),
                to_date=datetime(2025, 12, 31),
                date_type="capture",
            )

            # Filter by creation_date - should match
            result_creation = manager.get_videos_by_date_range(
                from_date=datetime(2020, 1, 1),
                to_date=datetime(2025, 12, 31),
                date_type="creation",
            )

        assert len(result_capture) == 0
        assert len(result_creation) == 1

    def test_filter_boundary_dates(self):
        """Test filtering at exact boundary dates."""
        manager = PhotosAccessManager()

        videos = [
            VideoInfo(
                uuid="video_start",
                filename="video_start.mov",
                path=Path("/tmp/video_start.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2020, 1, 1, 0, 0, 0),  # Exact start
                creation_date=datetime(2020, 1, 1, 0, 0, 0),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="video_end",
                filename="video_end.mov",
                path=Path("/tmp/video_end.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2020, 12, 31, 23, 59, 59),  # Exact end
                creation_date=datetime(2020, 12, 31, 23, 59, 59),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
            VideoInfo(
                uuid="video_outside",
                filename="video_outside.mov",
                path=Path("/tmp/video_outside.mov"),
                codec="avc1",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime(2021, 1, 1, 0, 0, 1),  # Just outside
                creation_date=datetime(2021, 1, 1, 0, 0, 1),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            ),
        ]

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(
                from_date=datetime(2020, 1, 1),
                to_date=datetime(2020, 12, 31, 23, 59, 59),
            )

        assert len(result) == 2
        uuids = [v.uuid for v in result]
        assert "video_start" in uuids
        assert "video_end" in uuids
        assert "video_outside" not in uuids


class TestCodecExtraction:
    """Tests for codec extraction from various sources."""

    def test_codec_from_exif_info(self):
        """Test codec extraction from exif_info."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-codec-exif",
            exif_info=MockExifInfo(codec="AVC1", duration=60.0),
        )

        with patch.object(Path, "exists", return_value=False):
            result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.codec == "avc1"  # Should be lowercase

    def test_codec_fallback_to_exiftool(self):
        """Test codec extraction falls back to exiftool when exif_info has no codec."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-codec-exiftool",
            path="/tmp/test.mov",
            exif_info=MockExifInfo(codec=None, duration=60.0),
            exiftool={"CompressorID": "H264"},
        )

        # Mock both exists() and stat() for the path
        with patch.object(Path, "exists", return_value=True):
            with patch.object(Path, "stat") as mock_stat:
                mock_stat.return_value.st_size = 100000000
                result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.codec == "h264"

    def test_codec_unknown_when_no_source(self):
        """Test codec is 'unknown' when no source provides it."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-codec-unknown",
            path=None,  # No local file
            exif_info=MockExifInfo(codec=None, duration=60.0),
            exiftool=None,
        )

        result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.codec == "unknown"


class TestFileSizeExtraction:
    """Tests for file size extraction."""

    def test_file_size_from_local_file(self):
        """Test file size extraction from local file."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-size-local",
            path="/tmp/test.mov",
            original_filesize=100000000,
            exif_info=MockExifInfo(codec="avc1", duration=60.0),
        )

        with patch.object(Path, "exists", return_value=True):
            with patch.object(Path, "stat") as mock_stat:
                mock_stat.return_value.st_size = 150000000  # Different from original_filesize
                result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.file_size == 150000000  # Should use actual file size

    def test_file_size_from_metadata_for_icloud(self):
        """Test file size extraction from metadata for iCloud files."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-size-icloud",
            path=None,  # iCloud file
            original_filesize=200000000,
            iscloudasset=True,
            exif_info=MockExifInfo(codec="avc1", duration=60.0),
        )

        result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.file_size == 200000000  # Should use original_filesize

    def test_file_size_zero_when_unavailable(self):
        """Test file size is 0 when not available."""
        manager = PhotosAccessManager()

        mock_photo = MockPhotoInfo(
            uuid="test-size-zero",
            path=None,
            original_filesize=0,
            exif_info=MockExifInfo(codec="avc1", duration=60.0),
        )
        # Override original_filesize to None
        mock_photo.original_filesize = None

        result = manager._extract_video_info(mock_photo)

        assert result is not None
        assert result.file_size == 0
