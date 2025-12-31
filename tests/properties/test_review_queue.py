"""Property-based tests for review queue auto-registration.

Feature: video-compression-optimizer, Property 15: Review queue auto-registration
Validates: Requirements 12.1, 12.2, 12.3
"""

import json
from datetime import datetime
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from vco.metadata.manager import VideoMetadata
from vco.quality.checker import QualityResult
from vco.services.convert import BatchConversionResult, ConversionResult
from vco.services.review import ReviewService


def create_quality_result(
    ssim_score: float = 0.97,
    original_size: int = 100_000_000,
    converted_size: int = 50_000_000,
) -> QualityResult:
    """Create a QualityResult object with specified parameters."""
    compression_ratio = original_size / converted_size if converted_size > 0 else 0
    space_saved_bytes = original_size - converted_size
    space_saved_percent = (space_saved_bytes / original_size * 100) if original_size > 0 else 0

    return QualityResult(
        job_id=f"quality_{ssim_score}",
        original_s3_key="input/test.mp4",
        converted_s3_key="output/test_h265.mp4",
        status="passed" if ssim_score >= 0.95 and converted_size < original_size else "failed",
        ssim_score=ssim_score,
        original_size=original_size,
        converted_size=converted_size,
        compression_ratio=compression_ratio,
        space_saved_bytes=space_saved_bytes,
        space_saved_percent=space_saved_percent,
        playback_verified=True,
    )


def create_metadata(
    capture_date: datetime | None = None,
    albums: list[str] | None = None,
    location: tuple[float, float] | None = None,
) -> VideoMetadata:
    """Create a VideoMetadata object with specified parameters."""
    return VideoMetadata(
        capture_date=capture_date or datetime(2020, 7, 15, 14, 30, 0),
        creation_date=datetime(2020, 7, 15, 14, 30, 0),
        albums=albums or ["Vacation 2020"],
        location=location,
    )


def create_conversion_result(
    uuid: str = "test-uuid-123",
    filename: str = "test_video.mov",
    success: bool = True,
    quality_result: QualityResult | None = None,
    metadata: VideoMetadata | None = None,
) -> ConversionResult:
    """Create a ConversionResult object with specified parameters."""
    return ConversionResult(
        uuid=uuid,
        filename=filename,
        success=success,
        original_path=Path(f"/tmp/original/{filename}"),
        converted_path=Path(f"/tmp/converted/{filename.replace('.mov', '_h265.mp4')}")
        if success
        else None,
        quality_result=quality_result or create_quality_result() if success else None,
        metadata=metadata or create_metadata() if success else None,
        error_message=None if success else "Conversion failed",
    )


class TestReviewQueueAutoRegistration:
    """Property tests for review queue auto-registration.

    Property 15: Review queue auto-registration
    - Successful conversions are automatically added to review queue
    - Added items include quality metrics (SSIM, compression ratio, space saved)
    - Added items include metadata (capture date, albums, location)

    Validates: Requirements 12.1, 12.2, 12.3
    """

    def test_successful_conversion_added_to_queue(self, tmp_path):
        """Successful conversions are automatically added to review queue.

        Requirement 12.1: Auto-registration of successful conversions
        """
        queue_path = tmp_path / "review_queue.json"
        review_service = ReviewService(queue_path=queue_path)
        conversion_result = create_conversion_result(success=True)

        review_item = review_service.add_to_queue(conversion_result)

        assert review_item is not None
        assert review_item.original_uuid == conversion_result.uuid
        assert review_item.status == "pending_review"

    def test_failed_conversion_not_added_to_queue(self, tmp_path):
        """Failed conversions are not added to review queue.

        Requirement 12.1: Only successful conversions are added
        """
        queue_path = tmp_path / "review_queue.json"
        review_service = ReviewService(queue_path=queue_path)
        conversion_result = create_conversion_result(success=False)

        review_item = review_service.add_to_queue(conversion_result)

        assert review_item is None

        # Verify queue is empty
        pending = review_service.get_pending_reviews()
        assert len(pending) == 0

    @given(
        ssim_score=st.floats(min_value=0.95, max_value=1.0, allow_nan=False),
        original_size=st.integers(min_value=1_000_000, max_value=10_000_000_000),
        compression_ratio=st.floats(min_value=1.1, max_value=10.0, allow_nan=False),
    )
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_quality_metrics_included_in_review_item(
        self, tmp_path, ssim_score: float, original_size: int, compression_ratio: float
    ):
        """Review items include quality metrics.

        Requirement 12.2: Include SSIM score, compression ratio, space saved
        """
        queue_path = tmp_path / f"review_queue_{ssim_score}_{original_size}.json"
        converted_size = int(original_size / compression_ratio)
        quality_result = create_quality_result(
            ssim_score=ssim_score,
            original_size=original_size,
            converted_size=converted_size,
        )
        conversion_result = create_conversion_result(
            success=True,
            quality_result=quality_result,
        )

        review_service = ReviewService(queue_path=queue_path)
        review_item = review_service.add_to_queue(conversion_result)

        assert review_item is not None

        # Verify quality metrics are included
        qr = review_item.quality_result
        assert "ssim_score" in qr
        assert qr["ssim_score"] == ssim_score
        assert "original_size" in qr
        assert qr["original_size"] == original_size
        assert "converted_size" in qr
        assert qr["converted_size"] == converted_size
        assert "compression_ratio" in qr
        assert "space_saved_bytes" in qr
        assert "space_saved_percent" in qr

    @given(
        album_count=st.integers(min_value=0, max_value=10),
        has_location=st.booleans(),
    )
    @settings(max_examples=50, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_metadata_included_in_review_item(self, tmp_path, album_count: int, has_location: bool):
        """Review items include metadata.

        Requirement 12.3: Include capture date, albums, location
        """
        queue_path = tmp_path / f"review_queue_{album_count}_{has_location}.json"
        albums = [f"Album {i}" for i in range(album_count)]
        location = (35.6762, 139.6503) if has_location else None
        capture_date = datetime(2020, 7, 15, 14, 30, 0)

        metadata = VideoMetadata(
            capture_date=capture_date,
            creation_date=datetime(2020, 7, 15, 14, 30, 0),
            albums=albums,
            location=location,
        )
        conversion_result = create_conversion_result(
            success=True,
            metadata=metadata,
        )

        review_service = ReviewService(queue_path=queue_path)
        review_item = review_service.add_to_queue(conversion_result)

        assert review_item is not None

        # Verify metadata is included
        md = review_item.metadata
        assert "capture_date" in md
        assert "albums" in md
        assert len(md["albums"]) == album_count

        if has_location:
            assert "location" in md
            assert md["location"] is not None

    @given(count=st.integers(min_value=1, max_value=10))
    @settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_multiple_conversions_added_to_queue(self, tmp_path, count: int):
        """Multiple successful conversions are all added to queue.

        Requirement 12.1: All successful conversions are added
        """
        queue_path = tmp_path / f"review_queue_multi_{count}.json"
        review_service = ReviewService(queue_path=queue_path)

        for i in range(count):
            conversion_result = create_conversion_result(
                uuid=f"test-uuid-{i}",
                filename=f"video_{i}.mov",
                success=True,
            )
            review_service.add_to_queue(conversion_result)

        pending = review_service.get_pending_reviews()
        assert len(pending) == count

    def test_review_item_has_unique_id(self, tmp_path):
        """Each review item has a unique ID when UUIDs are different.

        Note: The ID format is rev_{uuid[:8]}_{timestamp}, so items with
        different UUIDs will have different IDs.
        """
        import time

        queue_path = tmp_path / "review_queue.json"
        review_service = ReviewService(queue_path=queue_path)

        ids = set()
        for i in range(5):
            # Use completely different UUID prefixes to ensure unique IDs
            unique_uuid = f"uuid{i:04d}-{time.time_ns()}"
            conversion_result = create_conversion_result(
                uuid=unique_uuid,
                filename=f"video_{i}.mov",
                success=True,
            )
            review_item = review_service.add_to_queue(conversion_result)
            assert review_item is not None
            # ID should contain the UUID prefix
            assert unique_uuid[:8] in review_item.id
            ids.add(review_item.id)

        # All IDs should be unique
        assert len(ids) == 5

    def test_review_item_persisted_to_file(self, tmp_path):
        """Review items are persisted to the queue file."""
        queue_path = tmp_path / "review_queue.json"
        review_service = ReviewService(queue_path=queue_path)
        conversion_result = create_conversion_result(success=True)

        review_service.add_to_queue(conversion_result)

        # Verify file exists and contains the item
        assert queue_path.exists()

        with open(queue_path) as f:
            data = json.load(f)

        assert "items" in data
        assert len(data["items"]) == 1
        assert data["items"][0]["original_uuid"] == conversion_result.uuid

    def test_conversion_without_converted_path_not_added(self, tmp_path):
        """Conversions without converted_path are not added."""
        queue_path = tmp_path / "review_queue.json"
        review_service = ReviewService(queue_path=queue_path)

        # Create a result that claims success but has no converted_path
        conversion_result = ConversionResult(
            uuid="test-uuid",
            filename="test.mov",
            success=True,
            original_path=Path("/tmp/test.mov"),
            converted_path=None,  # Missing converted path
        )

        review_item = review_service.add_to_queue(conversion_result)

        assert review_item is None


class TestBatchConversionQueueIntegration:
    """Tests for batch conversion and review queue integration."""

    def test_batch_result_tracks_added_to_queue_count(self):
        """BatchConversionResult tracks the number of items added to queue."""
        result = BatchConversionResult(
            total=5,
            successful=3,
            failed=2,
            added_to_queue=3,
        )

        assert result.added_to_queue == 3

        # Verify serialization includes the field
        result_dict = result.to_dict()
        assert "added_to_queue" in result_dict
        assert result_dict["added_to_queue"] == 3
