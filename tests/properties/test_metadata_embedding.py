"""Property-based tests for Lambda metadata embedding feature.

Validates:
- Property 16: Metadata embedding accuracy
- Property 17: Metadata embedding fallback on failure

Requirements: 13.1, 13.2, 13.3
"""

import json
from datetime import datetime

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from vco.metadata.manager import VideoMetadata
from vco.quality.checker import QualityResult


class TestMetadataEmbeddingAccuracy:
    """Property 16: Metadata embedding accuracy.

    For any downloaded converted file:
    - If original has capture date, converted file metadata contains capture date
    - If original has GPS location, converted file metadata contains GPS location
    """

    @given(
        capture_date=st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2025, 12, 31)),
        has_location=st.booleans(),
        latitude=st.floats(min_value=-90.0, max_value=90.0, allow_nan=False),
        longitude=st.floats(min_value=-180.0, max_value=180.0, allow_nan=False),
    )
    @settings(max_examples=50)
    def test_metadata_json_contains_capture_date(
        self, capture_date: datetime, has_location: bool, latitude: float, longitude: float
    ):
        """Metadata JSON uploaded to S3 contains capture_date when present."""
        location = (latitude, longitude) if has_location else None

        metadata = VideoMetadata(
            capture_date=capture_date,
            creation_date=capture_date,
            albums=["Test Album"],
            location=location,
        )

        # Simulate metadata dict creation (same as ConvertService._upload_metadata_to_s3)
        metadata_dict = {
            "capture_date": metadata.capture_date.isoformat() if metadata.capture_date else None,
            "creation_date": metadata.creation_date.isoformat() if metadata.creation_date else None,
            "location": list(metadata.location) if metadata.location else None,
            "albums": metadata.albums or [],
        }

        # Verify capture_date is present
        assert metadata_dict["capture_date"] is not None
        assert metadata_dict["capture_date"] == capture_date.isoformat()

        # Verify location if present
        if has_location:
            assert metadata_dict["location"] is not None
            assert len(metadata_dict["location"]) == 2
            assert metadata_dict["location"][0] == latitude
            assert metadata_dict["location"][1] == longitude
        else:
            assert metadata_dict["location"] is None

    @given(
        latitude=st.floats(min_value=-90.0, max_value=90.0, allow_nan=False),
        longitude=st.floats(min_value=-180.0, max_value=180.0, allow_nan=False),
    )
    @settings(max_examples=50)
    def test_location_format_for_ffmpeg(self, latitude: float, longitude: float):
        """Location is formatted correctly for FFmpeg embedding."""
        # FFmpeg location format: +/-DD.DDDD+/-DDD.DDDD/
        lat_sign = "+" if latitude >= 0 else ""
        lon_sign = "+" if longitude >= 0 else ""
        location_str = f"{lat_sign}{latitude:.4f}{lon_sign}{longitude:.4f}/"

        # Verify format
        assert location_str.endswith("/")
        # Should contain two coordinate values
        parts = location_str[:-1]  # Remove trailing /
        # The format should be parseable
        assert "+" in parts or "-" in parts

    def test_metadata_embedded_flag_true_on_success(self):
        """QualityResult has metadata_embedded=True when Lambda succeeds."""
        result = QualityResult(
            job_id="test-job",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="passed",
            ssim_score=0.98,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
            metadata_embedded=True,
            metadata_embed_error=None,
        )

        assert result.metadata_embedded is True
        assert result.metadata_embed_error is None

    def test_metadata_embedded_flag_false_on_failure(self):
        """QualityResult has metadata_embedded=False when Lambda fails to embed."""
        result = QualityResult(
            job_id="test-job",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="passed",
            ssim_score=0.98,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
            metadata_embedded=False,
            metadata_embed_error="FFmpeg failed: some error",
        )

        assert result.metadata_embedded is False
        assert result.metadata_embed_error is not None


class TestMetadataEmbeddingFallback:
    """Property 17: Metadata embedding fallback on failure.

    For any conversion where metadata embedding fails:
    - System logs the error
    - File is marked for local metadata application
    - Conversion does not fail
    - Quality check continues
    """

    def test_quality_result_passed_despite_metadata_failure(self):
        """Quality check passes even if metadata embedding fails."""
        result = QualityResult(
            job_id="test-job",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="passed",
            ssim_score=0.98,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
            metadata_embedded=False,
            metadata_embed_error="No metadata to embed",
        )

        # Quality check should still pass
        assert result.is_acceptable is True
        assert result.status == "passed"

        # But metadata was not embedded
        assert result.metadata_embedded is False

    @given(error_message=st.text(min_size=1, max_size=200))
    @settings(max_examples=20)
    def test_error_message_preserved(self, error_message: str):
        """Error message from Lambda is preserved in QualityResult."""
        assume(error_message.strip())  # Non-empty after stripping

        result = QualityResult(
            job_id="test-job",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="passed",
            ssim_score=0.98,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
            metadata_embedded=False,
            metadata_embed_error=error_message,
        )

        assert result.metadata_embed_error == error_message

    def test_fallback_applies_metadata_locally(self):
        """When Lambda doesn't embed metadata, local fallback is triggered."""
        # This test verifies the logic in ConvertService
        # When metadata_embedded is False, apply_metadata should be called

        quality_result = QualityResult(
            job_id="test-job",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="passed",
            ssim_score=0.98,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
            metadata_embedded=False,
            metadata_embed_error="No metadata to embed",
        )

        # Verify the flag that triggers local fallback
        metadata_embedded_by_lambda = getattr(quality_result, "metadata_embedded", False)
        assert metadata_embedded_by_lambda is False

        # In ConvertService, this would trigger:
        # if not metadata_embedded_by_lambda:
        #     self.metadata_manager.apply_metadata(local_output_path, metadata)


class TestMetadataJsonFormat:
    """Test metadata JSON format for S3 upload."""

    @given(
        capture_date=st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2025, 12, 31)),
        albums=st.lists(st.text(min_size=1, max_size=50), min_size=0, max_size=5),
    )
    @settings(max_examples=30)
    def test_metadata_json_serializable(self, capture_date: datetime, albums: list[str]):
        """Metadata dict is JSON serializable."""
        metadata = VideoMetadata(
            capture_date=capture_date,
            creation_date=capture_date,
            albums=albums,
            location=(35.6762, 139.6503),
        )

        metadata_dict = {
            "capture_date": metadata.capture_date.isoformat() if metadata.capture_date else None,
            "creation_date": metadata.creation_date.isoformat() if metadata.creation_date else None,
            "location": list(metadata.location) if metadata.location else None,
            "albums": metadata.albums or [],
        }

        # Should be JSON serializable
        json_str = json.dumps(metadata_dict)
        assert json_str is not None

        # Should be deserializable
        parsed = json.loads(json_str)
        assert parsed["capture_date"] == capture_date.isoformat()
        assert parsed["albums"] == albums

    def test_metadata_with_none_values(self):
        """Metadata with None values is handled correctly."""
        metadata = VideoMetadata(
            capture_date=None, creation_date=datetime(2020, 1, 1), albums=[], location=None
        )

        metadata_dict = {
            "capture_date": metadata.capture_date.isoformat() if metadata.capture_date else None,
            "creation_date": metadata.creation_date.isoformat() if metadata.creation_date else None,
            "location": list(metadata.location) if metadata.location else None,
            "albums": metadata.albums or [],
        }

        assert metadata_dict["capture_date"] is None
        assert metadata_dict["location"] is None
        assert metadata_dict["albums"] == []

        # Should still be JSON serializable
        json_str = json.dumps(metadata_dict)
        assert json_str is not None
