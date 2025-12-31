"""Unit tests for ReviewService.

Tests for services/review.py.
Target coverage: 70%+ (ビジネスロジック)
"""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from vco.metadata.manager import VideoMetadata
from vco.quality.checker import QualityResult
from vco.services.convert import ConversionResult
from vco.services.review import (
    ReviewItem,
    ReviewQueue,
    ReviewService,
)


class TestReviewItem:
    """Tests for ReviewItem dataclass."""

    def test_to_dict(self, tmp_path):
        """Test ReviewItem serialization to dict."""
        item = ReviewItem(
            id="rev_12345678_20241215120000",
            original_uuid="uuid-123",
            original_path=tmp_path / "original.mp4",
            converted_path=tmp_path / "converted.mp4",
            conversion_date="2024-12-15T12:00:00",
            quality_result={"ssim_score": 0.95, "compression_ratio": 0.5},
            metadata={"filename": "test.mp4"},
            status="pending_review",
        )

        result = item.to_dict()

        assert result["id"] == "rev_12345678_20241215120000"
        assert result["original_uuid"] == "uuid-123"
        assert result["original_path"] == str(tmp_path / "original.mp4")
        assert result["converted_path"] == str(tmp_path / "converted.mp4")
        assert result["conversion_date"] == "2024-12-15T12:00:00"
        assert result["quality_result"]["ssim_score"] == 0.95
        assert result["status"] == "pending_review"

    def test_from_dict(self, tmp_path):
        """Test ReviewItem deserialization from dict."""
        data = {
            "id": "rev_test_123",
            "original_uuid": "uuid-456",
            "original_path": str(tmp_path / "orig.mp4"),
            "converted_path": str(tmp_path / "conv.mp4"),
            "conversion_date": "2024-12-15T10:00:00",
            "quality_result": {"ssim_score": 0.92},
            "metadata": {"albums": ["Test"]},
            "status": "approved",
        }

        item = ReviewItem.from_dict(data)

        assert item.id == "rev_test_123"
        assert item.original_uuid == "uuid-456"
        assert item.original_path == Path(tmp_path / "orig.mp4")
        assert item.converted_path == Path(tmp_path / "conv.mp4")
        assert item.status == "approved"

    def test_from_dict_default_status(self, tmp_path):
        """Test ReviewItem uses default status when not provided."""
        data = {
            "id": "rev_test",
            "original_uuid": "uuid",
            "original_path": str(tmp_path / "orig.mp4"),
            "converted_path": str(tmp_path / "conv.mp4"),
            "conversion_date": "2024-12-15",
            "quality_result": {},
            "metadata": {},
            # status not provided
        }

        item = ReviewItem.from_dict(data)

        assert item.status == "pending_review"


class TestReviewQueue:
    """Tests for ReviewQueue dataclass."""

    def test_to_dict(self, tmp_path):
        """Test ReviewQueue serialization."""
        item = ReviewItem(
            id="rev_1",
            original_uuid="uuid-1",
            original_path=tmp_path / "orig.mp4",
            converted_path=tmp_path / "conv.mp4",
            conversion_date="2024-12-15",
            quality_result={},
            metadata={},
        )
        queue = ReviewQueue(schema_version="1.0", last_updated="2024-12-15T12:00:00", items=[item])

        result = queue.to_dict()

        assert result["schema_version"] == "1.0"
        assert result["last_updated"] == "2024-12-15T12:00:00"
        assert len(result["items"]) == 1
        assert result["items"][0]["id"] == "rev_1"

    def test_from_dict(self, tmp_path):
        """Test ReviewQueue deserialization."""
        data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15T10:00:00",
            "items": [
                {
                    "id": "rev_1",
                    "original_uuid": "uuid-1",
                    "original_path": str(tmp_path / "orig.mp4"),
                    "converted_path": str(tmp_path / "conv.mp4"),
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                }
            ],
        }

        queue = ReviewQueue.from_dict(data)

        assert queue.schema_version == "1.0"
        assert len(queue.items) == 1
        assert queue.items[0].id == "rev_1"

    def test_from_dict_empty_items(self):
        """Test ReviewQueue with empty items."""
        data = {"schema_version": "1.0", "last_updated": "2024-12-15"}

        queue = ReviewQueue.from_dict(data)

        assert queue.items == []

    def test_default_values(self):
        """Test ReviewQueue default values."""
        queue = ReviewQueue()

        assert queue.schema_version == "1.0"
        assert queue.items == []
        # last_updated should be set to current time
        assert queue.last_updated is not None


class TestReviewServiceInit:
    """Tests for ReviewService initialization."""

    def test_init_default(self):
        """Test default initialization."""
        with patch("vco.services.review.PhotosAccessManager"):
            with patch("vco.services.review.MetadataManager"):
                service = ReviewService()

        assert service.queue_path == Path.home() / ".config" / "vco" / "review_queue.json"

    def test_init_custom_queue_path(self, tmp_path):
        """Test initialization with custom queue path."""
        queue_path = tmp_path / "custom_queue.json"

        with patch("vco.services.review.PhotosAccessManager"):
            with patch("vco.services.review.MetadataManager"):
                service = ReviewService(queue_path=queue_path)

        assert service.queue_path == queue_path


class TestReviewServiceLoadSave:
    """Tests for load_queue and save_queue methods."""

    def test_load_queue_file_not_exists(self, tmp_path):
        """Test loading queue when file doesn't exist."""
        queue_path = tmp_path / "nonexistent.json"

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        queue = service.load_queue()

        assert isinstance(queue, ReviewQueue)
        assert queue.items == []

    def test_load_queue_success(self, tmp_path):
        """Test loading queue from existing file."""
        queue_path = tmp_path / "queue.json"
        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_1",
                    "original_uuid": "uuid-1",
                    "original_path": "/path/orig.mp4",
                    "converted_path": "/path/conv.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                }
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        queue = service.load_queue()

        assert len(queue.items) == 1
        assert queue.items[0].id == "rev_1"

    def test_load_queue_invalid_json(self, tmp_path):
        """Test loading queue with invalid JSON."""
        queue_path = tmp_path / "invalid.json"
        queue_path.write_text("not valid json")

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        queue = service.load_queue()

        # Should return empty queue on error
        assert isinstance(queue, ReviewQueue)
        assert queue.items == []

    def test_save_queue_success(self, tmp_path):
        """Test saving queue to file."""
        queue_path = tmp_path / "queue.json"

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        queue = ReviewQueue(schema_version="1.0", last_updated="2024-12-15", items=[])

        result = service.save_queue(queue)

        assert result is True
        assert queue_path.exists()

        # Verify content
        saved_data = json.loads(queue_path.read_text())
        assert saved_data["schema_version"] == "1.0"

    def test_save_queue_creates_directory(self, tmp_path):
        """Test save_queue creates parent directory if needed."""
        queue_path = tmp_path / "subdir" / "queue.json"

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        queue = ReviewQueue()
        result = service.save_queue(queue)

        assert result is True
        assert queue_path.parent.exists()


class TestReviewServiceAddToQueue:
    """Tests for add_to_queue method."""

    def test_add_to_queue_success(self, tmp_path):
        """Test adding successful conversion to queue."""
        queue_path = tmp_path / "queue.json"
        converted_path = tmp_path / "converted.mp4"
        converted_path.write_bytes(b"video data")

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        quality_result = QualityResult(
            job_id="job-123",
            original_s3_key="original/test.mp4",
            converted_s3_key="converted/test.mp4",
            status="passed",
            ssim_score=0.95,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=0.5,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
        )

        metadata = VideoMetadata(
            capture_date=datetime(2024, 6, 15),
            creation_date=datetime(2024, 6, 15),
            albums=["Test Album"],
        )

        conversion_result = ConversionResult(
            uuid="uuid-123",
            filename="test.mp4",
            original_path=tmp_path / "original.mp4",
            converted_path=converted_path,
            success=True,
            quality_result=quality_result,
            metadata=metadata,
        )

        item = service.add_to_queue(conversion_result)

        assert item is not None
        assert item.original_uuid == "uuid-123"
        assert item.status == "pending_review"
        assert "rev_uuid-123" in item.id

    def test_add_to_queue_failed_conversion(self, tmp_path):
        """Test adding failed conversion returns None."""
        queue_path = tmp_path / "queue.json"

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        conversion_result = ConversionResult(
            uuid="uuid-123",
            filename="test.mp4",
            original_path=tmp_path / "original.mp4",
            converted_path=None,
            success=False,
            error_message="Conversion failed",
        )

        item = service.add_to_queue(conversion_result)

        assert item is None

    def test_add_to_queue_no_converted_path(self, tmp_path):
        """Test adding conversion without converted_path returns None."""
        queue_path = tmp_path / "queue.json"

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        conversion_result = ConversionResult(
            uuid="uuid-123",
            filename="test.mp4",
            original_path=tmp_path / "original.mp4",
            converted_path=None,
            success=True,  # success but no path
        )

        item = service.add_to_queue(conversion_result)

        assert item is None


class TestReviewServiceGetPending:
    """Tests for get_pending_reviews method."""

    def test_get_pending_reviews(self, tmp_path):
        """Test getting pending reviews."""
        queue_path = tmp_path / "queue.json"
        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_1",
                    "original_uuid": "uuid-1",
                    "original_path": "/path/orig1.mp4",
                    "converted_path": "/path/conv1.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "pending_review",
                },
                {
                    "id": "rev_2",
                    "original_uuid": "uuid-2",
                    "original_path": "/path/orig2.mp4",
                    "converted_path": "/path/conv2.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "approved",
                },
                {
                    "id": "rev_3",
                    "original_uuid": "uuid-3",
                    "original_path": "/path/orig3.mp4",
                    "converted_path": "/path/conv3.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "pending_review",
                },
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        pending = service.get_pending_reviews()

        assert len(pending) == 2
        assert all(item.status == "pending_review" for item in pending)


class TestReviewServiceGetById:
    """Tests for get_review_by_id method."""

    def test_get_review_by_id_found(self, tmp_path):
        """Test getting review by ID when found."""
        queue_path = tmp_path / "queue.json"
        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_target",
                    "original_uuid": "uuid-target",
                    "original_path": "/path/orig.mp4",
                    "converted_path": "/path/conv.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                }
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        item = service.get_review_by_id("rev_target")

        assert item is not None
        assert item.id == "rev_target"

    def test_get_review_by_id_not_found(self, tmp_path):
        """Test getting review by ID when not found."""
        queue_path = tmp_path / "queue.json"
        queue_data = {"schema_version": "1.0", "last_updated": "2024-12-15", "items": []}
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        item = service.get_review_by_id("nonexistent")

        assert item is None


class TestReviewServiceReject:
    """Tests for reject method."""

    def test_reject_success(self, tmp_path):
        """Test rejecting a conversion."""
        queue_path = tmp_path / "queue.json"
        converted_path = tmp_path / "converted.mp4"
        converted_path.write_bytes(b"video data")
        metadata_path = tmp_path / "converted.json"
        metadata_path.write_text("{}")

        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_reject",
                    "original_uuid": "uuid-1",
                    "original_path": str(tmp_path / "orig.mp4"),
                    "converted_path": str(converted_path),
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "pending_review",
                }
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        result = service.reject("rev_reject")

        assert result is True
        assert not converted_path.exists()
        assert not metadata_path.exists()

        # Verify status updated
        queue = service.load_queue()
        assert queue.items[0].status == "rejected"

    def test_reject_not_found(self, tmp_path):
        """Test rejecting non-existent review."""
        queue_path = tmp_path / "queue.json"
        queue_data = {"schema_version": "1.0", "last_updated": "2024-12-15", "items": []}
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        result = service.reject("nonexistent")

        assert result is False

    def test_reject_not_pending(self, tmp_path):
        """Test rejecting already processed review."""
        queue_path = tmp_path / "queue.json"
        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_approved",
                    "original_uuid": "uuid-1",
                    "original_path": "/path/orig.mp4",
                    "converted_path": "/path/conv.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "approved",  # Already approved
                }
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        result = service.reject("rev_approved")

        assert result is False


class TestReviewServiceClearCompleted:
    """Tests for clear_completed method."""

    def test_clear_completed(self, tmp_path):
        """Test clearing completed items."""
        queue_path = tmp_path / "queue.json"
        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_1",
                    "original_uuid": "uuid-1",
                    "original_path": "/path/orig1.mp4",
                    "converted_path": "/path/conv1.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "pending_review",
                },
                {
                    "id": "rev_2",
                    "original_uuid": "uuid-2",
                    "original_path": "/path/orig2.mp4",
                    "converted_path": "/path/conv2.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "approved",
                },
                {
                    "id": "rev_3",
                    "original_uuid": "uuid-3",
                    "original_path": "/path/orig3.mp4",
                    "converted_path": "/path/conv3.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "rejected",
                },
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        removed = service.clear_completed()

        assert removed == 2

        queue = service.load_queue()
        assert len(queue.items) == 1
        assert queue.items[0].id == "rev_1"


class TestReviewServiceGetSummary:
    """Tests for get_queue_summary method."""

    def test_get_queue_summary(self, tmp_path):
        """Test getting queue summary."""
        queue_path = tmp_path / "queue.json"
        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_1",
                    "original_uuid": "uuid-1",
                    "original_path": "/path/orig1.mp4",
                    "converted_path": "/path/conv1.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {"space_saved_bytes": 100000},
                    "metadata": {},
                    "status": "pending_review",
                },
                {
                    "id": "rev_2",
                    "original_uuid": "uuid-2",
                    "original_path": "/path/orig2.mp4",
                    "converted_path": "/path/conv2.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {"space_saved_bytes": 200000},
                    "metadata": {},
                    "status": "pending_review",
                },
                {
                    "id": "rev_3",
                    "original_uuid": "uuid-3",
                    "original_path": "/path/orig3.mp4",
                    "converted_path": "/path/conv3.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "approved",
                },
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        summary = service.get_queue_summary()

        assert summary["total"] == 3
        assert summary["pending"] == 2
        assert summary["approved"] == 1
        assert summary["rejected"] == 0
        assert summary["pending_savings_bytes"] == 300000

    def test_get_queue_summary_empty(self, tmp_path):
        """Test getting summary for empty queue."""
        queue_path = tmp_path / "queue.json"

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        summary = service.get_queue_summary()

        assert summary["total"] == 0
        assert summary["pending"] == 0
        assert summary["approved"] == 0
        assert summary["rejected"] == 0
        assert summary["pending_savings_bytes"] == 0


class TestReviewServiceApprove:
    """Tests for approve method."""

    def test_approve_not_found(self, tmp_path):
        """Test approving non-existent review."""
        queue_path = tmp_path / "queue.json"
        queue_data = {"schema_version": "1.0", "last_updated": "2024-12-15", "items": []}
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        result = service.approve("nonexistent")

        assert result is False

    def test_approve_not_pending(self, tmp_path):
        """Test approving already processed review."""
        queue_path = tmp_path / "queue.json"
        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_rejected",
                    "original_uuid": "uuid-1",
                    "original_path": "/path/orig.mp4",
                    "converted_path": "/path/conv.mp4",
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "rejected",  # Already rejected
                }
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        result = service.approve("rev_rejected")

        assert result is False

    def test_approve_converted_file_not_found(self, tmp_path):
        """Test approving when converted file doesn't exist."""
        queue_path = tmp_path / "queue.json"
        nonexistent_path = tmp_path / "nonexistent.mp4"

        queue_data = {
            "schema_version": "1.0",
            "last_updated": "2024-12-15",
            "items": [
                {
                    "id": "rev_missing",
                    "original_uuid": "uuid-1",
                    "original_path": str(tmp_path / "orig.mp4"),
                    "converted_path": str(nonexistent_path),
                    "conversion_date": "2024-12-15",
                    "quality_result": {},
                    "metadata": {},
                    "status": "pending_review",
                }
            ],
        }
        queue_path.write_text(json.dumps(queue_data))

        service = ReviewService(
            photos_manager=MagicMock(), metadata_manager=MagicMock(), queue_path=queue_path
        )

        result = service.approve("rev_missing")

        assert result is False
