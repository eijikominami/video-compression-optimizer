"""Unit tests for MetadataExtractor."""

from unittest.mock import MagicMock, patch

import pytest

from vco.metadata.extractor import MetadataExtractor


class TestMetadataExtractor:
    """Tests for MetadataExtractor class."""

    def test_extract_with_full_metadata(self):
        """Test extraction with all metadata present."""
        ffprobe_output = {
            "format": {
                "duration": "120.5",
                "tags": {
                    "creation_time": "2024-01-15T14:30:00.000000Z",
                    "com.apple.quicktime.location.ISO6709": "+35.6762+139.6503/",
                },
            },
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "hevc",
                    "width": 1920,
                    "height": 1080,
                    "tags": {},
                }
            ],
        }

        extractor = MetadataExtractor()
        with patch.object(extractor, "_run_ffprobe", return_value=ffprobe_output):
            result = extractor.extract(MagicMock())

        assert result.capture_date is not None
        assert result.capture_date.year == 2024
        assert result.capture_date.month == 1
        assert result.capture_date.day == 15
        assert result.gps_location is not None
        assert result.gps_location.latitude == pytest.approx(35.6762)
        assert result.gps_location.longitude == pytest.approx(139.6503)
        assert result.duration == pytest.approx(120.5)
        assert result.codec == "hevc"
        assert result.resolution == (1920, 1080)
        assert result.extraction_errors == []

    def test_extract_with_missing_metadata(self):
        """Test extraction with missing metadata."""
        ffprobe_output = {
            "format": {},
            "streams": [],
        }

        extractor = MetadataExtractor()
        with patch.object(extractor, "_run_ffprobe", return_value=ffprobe_output):
            result = extractor.extract(MagicMock())

        assert result.capture_date is None
        assert result.gps_location is None
        assert result.duration is None
        assert result.codec is None
        assert result.resolution is None
        assert result.extraction_errors == []

    def test_extract_with_ffprobe_failure(self):
        """Test extraction when ffprobe fails."""
        extractor = MetadataExtractor()
        with patch.object(extractor, "_run_ffprobe", side_effect=RuntimeError("ffprobe failed")):
            result = extractor.extract(MagicMock())

        assert result.capture_date is None
        assert result.gps_location is None
        assert len(result.extraction_errors) == 1
        assert "ffprobe failed" in result.extraction_errors[0]

    def test_parse_creation_time_from_format_tags(self):
        """Test parsing creation_time from format.tags."""
        ffprobe_output = {
            "format": {
                "tags": {
                    "creation_time": "2024-01-15T14:30:00.000000Z",
                },
            },
            "streams": [],
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_creation_time(ffprobe_output, errors)

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert errors == []

    def test_parse_creation_time_from_stream_tags(self):
        """Test parsing creation_time from stream tags as fallback."""
        ffprobe_output = {
            "format": {"tags": {}},
            "streams": [
                {
                    "codec_type": "video",
                    "tags": {
                        "creation_time": "2024-01-15T14:30:00.000000Z",
                    },
                }
            ],
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_creation_time(ffprobe_output, errors)

        assert result is not None
        assert result.year == 2024

    def test_parse_creation_time_simple_format(self):
        """Test parsing creation_time in simple format."""
        ffprobe_output = {
            "format": {
                "tags": {
                    "creation_time": "2024-01-15 14:30:00",
                },
            },
            "streams": [],
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_creation_time(ffprobe_output, errors)

        assert result is not None
        assert result.year == 2024

    def test_parse_creation_time_invalid_format(self):
        """Test parsing invalid creation_time format."""
        ffprobe_output = {
            "format": {
                "tags": {
                    "creation_time": "invalid-date",
                },
            },
            "streams": [],
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_creation_time(ffprobe_output, errors)

        assert result is None
        assert len(errors) == 1
        assert "Failed to parse creation_time" in errors[0]

    def test_parse_gps_location_apple_format(self):
        """Test parsing GPS from Apple QuickTime format."""
        ffprobe_output = {
            "format": {
                "tags": {
                    "com.apple.quicktime.location.ISO6709": "+35.6762+139.6503/",
                },
            },
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_gps_location(ffprobe_output, errors)

        assert result is not None
        assert result.latitude == pytest.approx(35.6762)
        assert result.longitude == pytest.approx(139.6503)

    def test_parse_gps_location_generic_format(self):
        """Test parsing GPS from generic location tag."""
        ffprobe_output = {
            "format": {
                "tags": {
                    "location": "+35.6762+139.6503/",
                },
            },
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_gps_location(ffprobe_output, errors)

        assert result is not None
        assert result.latitude == pytest.approx(35.6762)

    def test_parse_gps_location_invalid_format(self):
        """Test parsing invalid GPS format."""
        ffprobe_output = {
            "format": {
                "tags": {
                    "com.apple.quicktime.location.ISO6709": "invalid",
                },
            },
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_gps_location(ffprobe_output, errors)

        assert result is None
        assert len(errors) == 1
        assert "Failed to parse GPS location" in errors[0]

    def test_parse_duration(self):
        """Test parsing duration."""
        ffprobe_output = {
            "format": {
                "duration": "120.5",
            },
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_duration(ffprobe_output, errors)

        assert result == pytest.approx(120.5)

    def test_parse_duration_invalid(self):
        """Test parsing invalid duration."""
        ffprobe_output = {
            "format": {
                "duration": "invalid",
            },
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_duration(ffprobe_output, errors)

        assert result is None
        assert len(errors) == 1

    def test_parse_codec(self):
        """Test parsing video codec."""
        ffprobe_output = {
            "streams": [
                {
                    "codec_type": "audio",
                    "codec_name": "aac",
                },
                {
                    "codec_type": "video",
                    "codec_name": "hevc",
                },
            ],
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_codec(ffprobe_output, errors)

        assert result == "hevc"

    def test_parse_resolution(self):
        """Test parsing video resolution."""
        ffprobe_output = {
            "streams": [
                {
                    "codec_type": "video",
                    "width": 1920,
                    "height": 1080,
                },
            ],
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_resolution(ffprobe_output, errors)

        assert result == (1920, 1080)

    def test_parse_resolution_missing(self):
        """Test parsing resolution when missing."""
        ffprobe_output = {
            "streams": [
                {
                    "codec_type": "video",
                },
            ],
        }

        extractor = MetadataExtractor()
        errors = []
        result = extractor._parse_resolution(ffprobe_output, errors)

        assert result is None
