"""Property-based tests for metadata roundtrip preservation.

**Property 7: Metadata Roundtrip Preservation**
**Validates: Requirements 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6**

For any video conversion, metadata extracted from the original video
(capture_date, creation_date, albums) is preserved and applied to the
converted video. After import to Apple Photos, the converted video has
the same capture_date, creation_date, and album membership as the original.
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.metadata.manager import MetadataManager, VideoMetadata

# Strategy for generating valid datetime objects
datetime_strategy = st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2030, 12, 31))

# Strategy for album names
album_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N", "P", "S")), min_size=1, max_size=50
).filter(lambda x: x.strip() != "")

# Strategy for location coordinates
location_strategy = st.tuples(
    st.floats(min_value=-90.0, max_value=90.0, allow_nan=False),
    st.floats(min_value=-180.0, max_value=180.0, allow_nan=False),
)


class TestVideoMetadataDataclass:
    """Test VideoMetadata dataclass functionality."""

    def test_default_values(self):
        """VideoMetadata should have sensible defaults."""
        metadata = VideoMetadata()

        assert metadata.capture_date is None
        assert metadata.creation_date is None
        assert metadata.albums == []
        assert metadata.title is None
        assert metadata.description is None
        assert metadata.location is None

    @given(
        capture_date=st.one_of(st.none(), datetime_strategy),
        creation_date=st.one_of(st.none(), datetime_strategy),
        albums=st.lists(album_name_strategy, max_size=10),
        title=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
        description=st.one_of(st.none(), st.text(min_size=1, max_size=500)),
    )
    @settings(max_examples=100)
    def test_metadata_creation_with_values(
        self, capture_date, creation_date, albums, title, description
    ):
        """VideoMetadata should store all provided values."""
        metadata = VideoMetadata(
            capture_date=capture_date,
            creation_date=creation_date,
            albums=albums,
            title=title,
            description=description,
        )

        assert metadata.capture_date == capture_date
        assert metadata.creation_date == creation_date
        assert metadata.albums == albums
        assert metadata.title == title
        assert metadata.description == description


class TestMetadataRoundtrip:
    """Test metadata roundtrip through JSON serialization."""

    # Property 7: Metadata Roundtrip Preservation
    # **Validates: Requirements 8.1.1, 8.1.2, 8.1.3, 8.1.4, 8.1.5, 8.1.6**

    @given(
        capture_date=st.one_of(st.none(), datetime_strategy),
        creation_date=st.one_of(st.none(), datetime_strategy),
        albums=st.lists(album_name_strategy, max_size=10),
    )
    @settings(max_examples=100)
    def test_metadata_json_roundtrip(self, capture_date, creation_date, albums):
        """Metadata should survive JSON serialization roundtrip."""
        original = VideoMetadata(
            capture_date=capture_date, creation_date=creation_date, albums=albums
        )

        # Convert to dict and back
        data = original.to_dict()
        restored = VideoMetadata.from_dict(data)

        # Verify all fields match
        assert restored.capture_date == original.capture_date
        assert restored.creation_date == original.creation_date
        assert restored.albums == original.albums

    @given(
        capture_date=datetime_strategy,
        creation_date=datetime_strategy,
        albums=st.lists(album_name_strategy, min_size=1, max_size=5),
        title=st.text(min_size=1, max_size=50),
        description=st.text(min_size=1, max_size=200),
    )
    @settings(max_examples=100)
    def test_full_metadata_json_roundtrip(
        self, capture_date, creation_date, albums, title, description
    ):
        """Full metadata with all fields should survive roundtrip."""
        original = VideoMetadata(
            capture_date=capture_date,
            creation_date=creation_date,
            albums=albums,
            title=title,
            description=description,
        )

        # Convert to dict and back
        data = original.to_dict()
        restored = VideoMetadata.from_dict(data)

        # Verify all fields match
        assert restored.capture_date == original.capture_date
        assert restored.creation_date == original.creation_date
        assert restored.albums == original.albums
        assert restored.title == original.title
        assert restored.description == original.description

    @given(location=location_strategy)
    @settings(max_examples=50)
    def test_location_roundtrip(self, location):
        """Location coordinates should survive roundtrip."""
        original = VideoMetadata(location=location)

        data = original.to_dict()
        restored = VideoMetadata.from_dict(data)

        assert restored.location is not None
        assert abs(restored.location[0] - location[0]) < 0.0001
        assert abs(restored.location[1] - location[1]) < 0.0001


class TestMetadataFileRoundtrip:
    """Test metadata roundtrip through file save/load."""

    @given(
        capture_date=st.one_of(st.none(), datetime_strategy),
        creation_date=st.one_of(st.none(), datetime_strategy),
        albums=st.lists(album_name_strategy, max_size=5),
    )
    @settings(max_examples=50)
    def test_metadata_file_roundtrip(self, capture_date, creation_date, albums):
        """Metadata should survive file save/load roundtrip."""
        manager = MetadataManager()

        original = VideoMetadata(
            capture_date=capture_date, creation_date=creation_date, albums=albums
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "metadata.json"

            # Save and load
            assert manager.save_metadata_json(original, json_path)
            restored = manager.load_metadata_json(json_path)

            # Verify
            assert restored is not None
            assert restored.capture_date == original.capture_date
            assert restored.creation_date == original.creation_date
            assert restored.albums == original.albums

    def test_load_nonexistent_file_returns_none(self):
        """Loading nonexistent file should return None."""
        manager = MetadataManager()

        result = manager.load_metadata_json(Path("/nonexistent/path.json"))

        assert result is None

    def test_load_invalid_json_returns_none(self):
        """Loading invalid JSON should return None."""
        manager = MetadataManager()

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "invalid.json"
            json_path.write_text("not valid json {{{")

            result = manager.load_metadata_json(json_path)

            assert result is None


class TestMetadataToDictFormat:
    """Test to_dict output format."""

    def test_to_dict_with_all_none(self):
        """to_dict should handle all None values."""
        metadata = VideoMetadata()
        data = metadata.to_dict()

        assert data["capture_date"] is None
        assert data["creation_date"] is None
        assert data["albums"] == []
        assert data["title"] is None
        assert data["description"] is None
        assert data["location"] is None

    def test_to_dict_datetime_format(self):
        """to_dict should format datetime as ISO string."""
        dt = datetime(2024, 6, 15, 14, 30, 45)
        metadata = VideoMetadata(capture_date=dt)
        data = metadata.to_dict()

        assert data["capture_date"] == "2024-06-15T14:30:45"

    def test_to_dict_location_format(self):
        """to_dict should format location as list."""
        metadata = VideoMetadata(location=(35.6762, 139.6503))
        data = metadata.to_dict()

        assert data["location"] == [35.6762, 139.6503]


class TestFromDictParsing:
    """Test from_dict parsing."""

    def test_from_dict_empty(self):
        """from_dict should handle empty dict."""
        metadata = VideoMetadata.from_dict({})

        assert metadata.capture_date is None
        assert metadata.creation_date is None
        assert metadata.albums == []

    def test_from_dict_iso_datetime(self):
        """from_dict should parse ISO datetime strings."""
        data = {"capture_date": "2024-06-15T14:30:45", "creation_date": "2024-06-15T10:00:00"}

        metadata = VideoMetadata.from_dict(data)

        assert metadata.capture_date == datetime(2024, 6, 15, 14, 30, 45)
        assert metadata.creation_date == datetime(2024, 6, 15, 10, 0, 0)

    def test_from_dict_location_tuple(self):
        """from_dict should convert location list to tuple."""
        data = {"location": [35.6762, 139.6503]}

        metadata = VideoMetadata.from_dict(data)

        assert metadata.location == (35.6762, 139.6503)


class TestMetadataPreservationProperties:
    """Test properties related to metadata preservation."""

    @given(
        capture_date=datetime_strategy,
        creation_date=datetime_strategy,
        albums=st.lists(album_name_strategy, min_size=1, max_size=5),
    )
    @settings(max_examples=50)
    def test_capture_date_preserved(self, capture_date, creation_date, albums):
        """Capture date should be preserved through roundtrip."""
        original = VideoMetadata(
            capture_date=capture_date, creation_date=creation_date, albums=albums
        )

        # Simulate roundtrip
        data = original.to_dict()
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        restored = VideoMetadata.from_dict(restored_data)

        assert restored.capture_date == original.capture_date

    @given(
        capture_date=datetime_strategy,
        creation_date=datetime_strategy,
        albums=st.lists(album_name_strategy, min_size=1, max_size=5),
    )
    @settings(max_examples=50)
    def test_creation_date_preserved(self, capture_date, creation_date, albums):
        """Creation date should be preserved through roundtrip."""
        original = VideoMetadata(
            capture_date=capture_date, creation_date=creation_date, albums=albums
        )

        # Simulate roundtrip
        data = original.to_dict()
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        restored = VideoMetadata.from_dict(restored_data)

        assert restored.creation_date == original.creation_date

    @given(albums=st.lists(album_name_strategy, min_size=1, max_size=10))
    @settings(max_examples=50)
    def test_albums_preserved(self, albums):
        """Album list should be preserved through roundtrip."""
        original = VideoMetadata(albums=albums)

        # Simulate roundtrip
        data = original.to_dict()
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        restored = VideoMetadata.from_dict(restored_data)

        assert restored.albums == original.albums
        assert len(restored.albums) == len(original.albums)

    @given(albums=st.lists(album_name_strategy, min_size=1, max_size=10))
    @settings(max_examples=50)
    def test_album_order_preserved(self, albums):
        """Album order should be preserved through roundtrip."""
        original = VideoMetadata(albums=albums)

        # Simulate roundtrip
        data = original.to_dict()
        restored = VideoMetadata.from_dict(data)

        for i, album in enumerate(original.albums):
            assert restored.albums[i] == album
