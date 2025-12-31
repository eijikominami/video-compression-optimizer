"""Unit tests for MetadataManager.

Tests metadata extraction, application, and file operations.
Target coverage: 50%+ (ファイル I/O)
"""

import json
from datetime import datetime
from unittest.mock import patch

import pytest

from vco.metadata.manager import MetadataManager, VideoMetadata


class TestVideoMetadata:
    """Tests for VideoMetadata dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        metadata = VideoMetadata()
        assert metadata.capture_date is None
        assert metadata.creation_date is None
        assert metadata.albums == []
        assert metadata.title is None
        assert metadata.description is None
        assert metadata.location is None

    def test_custom_values(self):
        """Test custom values are set correctly."""
        capture = datetime(2024, 6, 15, 10, 30, 0)
        creation = datetime(2024, 6, 15, 11, 0, 0)

        metadata = VideoMetadata(
            capture_date=capture,
            creation_date=creation,
            albums=["Vacation", "Summer 2024"],
            title="Beach Video",
            description="A day at the beach",
            location=(35.6762, 139.6503),
        )

        assert metadata.capture_date == capture
        assert metadata.creation_date == creation
        assert metadata.albums == ["Vacation", "Summer 2024"]
        assert metadata.title == "Beach Video"
        assert metadata.description == "A day at the beach"
        assert metadata.location == (35.6762, 139.6503)

    def test_to_dict_with_all_values(self):
        """Test to_dict with all values set."""
        capture = datetime(2024, 6, 15, 10, 30, 0)
        creation = datetime(2024, 6, 15, 11, 0, 0)

        metadata = VideoMetadata(
            capture_date=capture,
            creation_date=creation,
            albums=["Album1"],
            title="Test",
            description="Desc",
            location=(35.0, 139.0),
        )

        result = metadata.to_dict()

        assert result["capture_date"] == "2024-06-15T10:30:00"
        assert result["creation_date"] == "2024-06-15T11:00:00"
        assert result["albums"] == ["Album1"]
        assert result["title"] == "Test"
        assert result["description"] == "Desc"
        assert result["location"] == [35.0, 139.0]

    def test_to_dict_with_none_values(self):
        """Test to_dict with None values."""
        metadata = VideoMetadata()
        result = metadata.to_dict()

        assert result["capture_date"] is None
        assert result["creation_date"] is None
        assert result["albums"] == []
        assert result["title"] is None
        assert result["description"] is None
        assert result["location"] is None

    def test_from_dict_with_all_values(self):
        """Test from_dict with all values."""
        data = {
            "capture_date": "2024-06-15T10:30:00",
            "creation_date": "2024-06-15T11:00:00",
            "albums": ["Album1", "Album2"],
            "title": "Test Title",
            "description": "Test Description",
            "location": [35.6762, 139.6503],
        }

        metadata = VideoMetadata.from_dict(data)

        assert metadata.capture_date == datetime(2024, 6, 15, 10, 30, 0)
        assert metadata.creation_date == datetime(2024, 6, 15, 11, 0, 0)
        assert metadata.albums == ["Album1", "Album2"]
        assert metadata.title == "Test Title"
        assert metadata.description == "Test Description"
        assert metadata.location == (35.6762, 139.6503)

    def test_from_dict_with_empty_values(self):
        """Test from_dict with empty/missing values."""
        data = {}

        metadata = VideoMetadata.from_dict(data)

        assert metadata.capture_date is None
        assert metadata.creation_date is None
        assert metadata.albums == []
        assert metadata.title is None
        assert metadata.description is None
        assert metadata.location is None

    def test_roundtrip(self):
        """Test to_dict -> from_dict roundtrip preserves data."""
        original = VideoMetadata(
            capture_date=datetime(2024, 1, 15, 12, 0, 0),
            creation_date=datetime(2024, 1, 15, 13, 0, 0),
            albums=["Test Album"],
            title="Test",
            description="Description",
            location=(40.7128, -74.0060),
        )

        data = original.to_dict()
        restored = VideoMetadata.from_dict(data)

        assert restored.capture_date == original.capture_date
        assert restored.creation_date == original.creation_date
        assert restored.albums == original.albums
        assert restored.title == original.title
        assert restored.description == original.description
        assert restored.location == original.location


class TestMetadataManager:
    """Tests for MetadataManager class."""

    def test_init(self):
        """Test MetadataManager initialization."""
        manager = MetadataManager()
        assert manager is not None

    def test_extract_metadata_file_not_found(self, tmp_path):
        """Test extract_metadata raises error for non-existent file."""
        manager = MetadataManager()

        with pytest.raises(FileNotFoundError):
            manager.extract_metadata(tmp_path / "nonexistent.mp4")

    def test_extract_metadata_from_file(self, tmp_path):
        """Test extract_metadata from existing file."""
        # Create a dummy video file
        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy video content")

        manager = MetadataManager()

        # Mock ffprobe to avoid actual execution
        with patch.object(manager, "_run_ffprobe", return_value={"format": {"tags": {}}}):
            metadata = manager.extract_metadata(video_path)

        assert isinstance(metadata, VideoMetadata)
        assert metadata.creation_date is not None  # From file stat
        assert metadata.albums == []

    def test_extract_metadata_with_ffprobe_data(self, tmp_path):
        """Test extract_metadata with FFprobe data."""
        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy video content")

        manager = MetadataManager()

        ffprobe_data = {
            "format": {
                "tags": {
                    "creation_time": "2024-06-15T10:30:00Z",
                    "title": "Test Video",
                    "description": "A test video",
                }
            }
        }

        with patch.object(manager, "_run_ffprobe", return_value=ffprobe_data):
            metadata = manager.extract_metadata(video_path)

        assert metadata.capture_date == datetime(2024, 6, 15, 10, 30, 0)
        assert metadata.title == "Test Video"
        assert metadata.description == "A test video"

    def test_save_metadata_json(self, tmp_path):
        """Test save_metadata_json creates JSON file."""
        manager = MetadataManager()
        metadata = VideoMetadata(
            capture_date=datetime(2024, 6, 15, 10, 30, 0), albums=["Test Album"]
        )

        output_path = tmp_path / "metadata.json"
        result = manager.save_metadata_json(metadata, output_path)

        assert result is True
        assert output_path.exists()

        # Verify content
        saved_data = json.loads(output_path.read_text())
        assert saved_data["capture_date"] == "2024-06-15T10:30:00"
        assert saved_data["albums"] == ["Test Album"]

    def test_save_metadata_json_creates_parent_dirs(self, tmp_path):
        """Test save_metadata_json creates parent directories."""
        manager = MetadataManager()
        metadata = VideoMetadata()

        output_path = tmp_path / "nested" / "dir" / "metadata.json"
        result = manager.save_metadata_json(metadata, output_path)

        assert result is True
        assert output_path.exists()

    def test_load_metadata_json(self, tmp_path):
        """Test load_metadata_json loads JSON file."""
        manager = MetadataManager()

        # Create JSON file
        json_path = tmp_path / "metadata.json"
        data = {
            "capture_date": "2024-06-15T10:30:00",
            "creation_date": None,
            "albums": ["Album1"],
            "title": "Test",
            "description": None,
            "location": None,
        }
        json_path.write_text(json.dumps(data))

        metadata = manager.load_metadata_json(json_path)

        assert metadata is not None
        assert metadata.capture_date == datetime(2024, 6, 15, 10, 30, 0)
        assert metadata.albums == ["Album1"]
        assert metadata.title == "Test"

    def test_load_metadata_json_nonexistent_file(self, tmp_path):
        """Test load_metadata_json returns None for non-existent file."""
        manager = MetadataManager()

        result = manager.load_metadata_json(tmp_path / "nonexistent.json")

        assert result is None

    def test_load_metadata_json_invalid_json(self, tmp_path):
        """Test load_metadata_json returns None for invalid JSON."""
        manager = MetadataManager()

        json_path = tmp_path / "invalid.json"
        json_path.write_text("invalid json {{{")

        result = manager.load_metadata_json(json_path)

        assert result is None

    def test_set_file_dates_file_not_found(self, tmp_path):
        """Test set_file_dates raises error for non-existent file."""
        manager = MetadataManager()

        with pytest.raises(FileNotFoundError):
            manager.set_file_dates(tmp_path / "nonexistent.mp4")

    def test_set_file_dates_modification_date(self, tmp_path):
        """Test set_file_dates sets modification date."""
        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy content")

        manager = MetadataManager()
        mod_date = datetime(2024, 6, 15, 10, 30, 0)

        result = manager.set_file_dates(video_path, modification_date=mod_date)

        assert result is True
        # Verify modification time was set
        stat = video_path.stat()
        assert abs(stat.st_mtime - mod_date.timestamp()) < 1

    def test_copy_dates_from_original_file_not_found(self, tmp_path):
        """Test copy_dates_from_original raises error for non-existent files."""
        manager = MetadataManager()

        with pytest.raises(FileNotFoundError):
            manager.copy_dates_from_original(tmp_path / "original.mp4", tmp_path / "converted.mp4")

    def test_copy_dates_from_original(self, tmp_path):
        """Test copy_dates_from_original copies dates."""
        original_path = tmp_path / "original.mp4"
        converted_path = tmp_path / "converted.mp4"

        original_path.write_bytes(b"original content")
        converted_path.write_bytes(b"converted content")

        manager = MetadataManager()

        # Mock set_file_dates to avoid platform-specific issues
        with patch.object(manager, "set_file_dates", return_value=True) as mock_set:
            result = manager.copy_dates_from_original(original_path, converted_path)

        assert result is True
        mock_set.assert_called_once()

    def test_apply_metadata_file_not_found(self, tmp_path):
        """Test apply_metadata raises error for non-existent file."""
        manager = MetadataManager()
        metadata = VideoMetadata()

        with pytest.raises(FileNotFoundError):
            manager.apply_metadata(tmp_path / "nonexistent.mp4", metadata)

    def test_apply_metadata_empty_metadata(self, tmp_path):
        """Test apply_metadata with empty metadata returns True."""
        video_path = tmp_path / "test.mp4"
        video_path.write_bytes(b"dummy content")

        manager = MetadataManager()
        metadata = VideoMetadata()  # All None/empty

        result = manager.apply_metadata(video_path, metadata)

        assert result is True  # Nothing to apply


class TestMetadataManagerParsing:
    """Tests for MetadataManager parsing methods."""

    def test_parse_date_iso_format_with_z(self):
        """Test _parse_date with ISO format ending in Z."""
        manager = MetadataManager()

        result = manager._parse_date("2024-06-15T10:30:00Z")

        assert result == datetime(2024, 6, 15, 10, 30, 0)

    def test_parse_date_iso_format_with_milliseconds(self):
        """Test _parse_date with ISO format including milliseconds."""
        manager = MetadataManager()

        result = manager._parse_date("2024-06-15T10:30:00.123456Z")

        assert result.year == 2024
        assert result.month == 6
        assert result.day == 15

    def test_parse_date_simple_format(self):
        """Test _parse_date with simple date format."""
        manager = MetadataManager()

        result = manager._parse_date("2024-06-15")

        assert result == datetime(2024, 6, 15)

    def test_parse_date_invalid_format(self):
        """Test _parse_date raises ValueError for invalid format."""
        manager = MetadataManager()

        with pytest.raises(ValueError, match="Unable to parse date"):
            manager._parse_date("invalid-date")

    def test_parse_location_valid(self):
        """Test _parse_location with valid ISO 6709 format."""
        manager = MetadataManager()

        result = manager._parse_location("+35.6762+139.6503/")

        assert result is not None
        assert abs(result[0] - 35.6762) < 0.0001
        assert abs(result[1] - 139.6503) < 0.0001

    def test_parse_location_negative_longitude(self):
        """Test _parse_location with negative longitude."""
        manager = MetadataManager()

        result = manager._parse_location("+40.7128-074.0060/")

        assert result is not None
        assert abs(result[0] - 40.7128) < 0.0001
        assert abs(result[1] - (-74.0060)) < 0.0001

    def test_parse_location_negative_latitude(self):
        """Test _parse_location with negative latitude."""
        manager = MetadataManager()

        result = manager._parse_location("-33.8688+151.2093/")

        assert result is not None
        assert abs(result[0] - (-33.8688)) < 0.0001
        assert abs(result[1] - 151.2093) < 0.0001

    def test_parse_location_invalid(self):
        """Test _parse_location returns None for invalid format."""
        manager = MetadataManager()

        result = manager._parse_location("invalid")

        assert result is None

    def test_parse_location_empty(self):
        """Test _parse_location returns None for empty string."""
        manager = MetadataManager()

        result = manager._parse_location("")

        assert result is None


class TestMetadataJsonRoundtrip:
    """Tests for metadata JSON file roundtrip."""

    def test_save_and_load_roundtrip(self, tmp_path):
        """Test save and load preserves all metadata."""
        manager = MetadataManager()

        original = VideoMetadata(
            capture_date=datetime(2024, 6, 15, 10, 30, 0),
            creation_date=datetime(2024, 6, 15, 11, 0, 0),
            albums=["Album1", "Album2"],
            title="Test Video",
            description="A test description",
            location=(35.6762, 139.6503),
        )

        json_path = tmp_path / "metadata.json"

        # Save
        save_result = manager.save_metadata_json(original, json_path)
        assert save_result is True

        # Load
        loaded = manager.load_metadata_json(json_path)

        assert loaded is not None
        assert loaded.capture_date == original.capture_date
        assert loaded.creation_date == original.creation_date
        assert loaded.albums == original.albums
        assert loaded.title == original.title
        assert loaded.description == original.description
        assert loaded.location == original.location
