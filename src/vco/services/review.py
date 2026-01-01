"""Review service for managing converted video approval workflow.

This service manages the review queue:
1. Add converted videos to review queue
2. Display pending reviews
3. Approve conversions (import to Photos, delete original)
4. Reject conversions (delete converted file)
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from vco.metadata.manager import MetadataManager, VideoMetadata
from vco.models.types import ConversionResult
from vco.photos.manager import PhotosAccessManager

logger = logging.getLogger(__name__)


@dataclass
class ReviewItem:
    """Item in the review queue."""

    id: str  # Unique review ID
    original_uuid: str
    original_path: Path
    converted_path: Path
    conversion_date: str
    quality_result: dict
    metadata: dict
    status: str = "pending_review"  # pending_review, approved, rejected

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "original_uuid": self.original_uuid,
            "original_path": str(self.original_path),
            "converted_path": str(self.converted_path),
            "conversion_date": self.conversion_date,
            "quality_result": self.quality_result,
            "metadata": self.metadata,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ReviewItem":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            original_uuid=data["original_uuid"],
            original_path=Path(data["original_path"]),
            converted_path=Path(data["converted_path"]),
            conversion_date=data["conversion_date"],
            quality_result=data["quality_result"],
            metadata=data["metadata"],
            status=data.get("status", "pending_review"),
        )


@dataclass
class ReviewQueue:
    """Review queue data structure."""

    schema_version: str = "1.0"
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    items: list[ReviewItem] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "schema_version": self.schema_version,
            "last_updated": self.last_updated,
            "items": [item.to_dict() for item in self.items],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ReviewQueue":
        """Create from dictionary."""
        items = [ReviewItem.from_dict(item) for item in data.get("items", [])]
        return cls(
            schema_version=data.get("schema_version", "1.0"),
            last_updated=data.get("last_updated", ""),
            items=items,
        )


class ReviewService:
    """Service for managing the review queue and approval workflow."""

    def __init__(
        self,
        photos_manager: PhotosAccessManager | None = None,
        metadata_manager: MetadataManager | None = None,
        queue_path: Path | None = None,
    ):
        """Initialize ReviewService.

        Args:
            photos_manager: PhotosAccessManager instance
            metadata_manager: MetadataManager instance
            queue_path: Path to review_queue.json
        """
        self.photos_manager = photos_manager or PhotosAccessManager()
        self.metadata_manager = metadata_manager or MetadataManager()
        self.queue_path = queue_path or Path.home() / ".config" / "vco" / "review_queue.json"

    def add_to_queue(self, conversion_result: ConversionResult) -> ReviewItem | None:
        """Add a successful conversion to the review queue.

        Args:
            conversion_result: Result from ConvertService

        Returns:
            ReviewItem if added, None if conversion was not successful
        """
        if not conversion_result.success or not conversion_result.converted_path:
            return None

        # Generate review ID
        review_id = f"rev_{conversion_result.uuid[:8]}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Build quality result dict
        quality_dict = {}
        if conversion_result.quality_result:
            qr = conversion_result.quality_result
            quality_dict = {
                "ssim_score": qr.ssim_score,
                "original_size": qr.original_size,
                "converted_size": qr.converted_size,
                "compression_ratio": qr.compression_ratio,
                "space_saved_bytes": qr.space_saved_bytes,
                "space_saved_percent": qr.space_saved_percent,
            }

        # Build metadata dict
        metadata_dict = {}
        if conversion_result.metadata:
            metadata_dict = conversion_result.metadata.to_dict()

        item = ReviewItem(
            id=review_id,
            original_uuid=conversion_result.uuid,
            original_path=conversion_result.original_path,
            converted_path=conversion_result.converted_path,
            conversion_date=datetime.now().isoformat(),
            quality_result=quality_dict,
            metadata=metadata_dict,
        )

        # Load existing queue and add item
        queue = self.load_queue()
        queue.items.append(item)
        queue.last_updated = datetime.now().isoformat()

        self.save_queue(queue)

        return item

    def get_pending_reviews(self) -> list[ReviewItem]:
        """Get all pending review items.

        Returns:
            List of pending ReviewItems
        """
        queue = self.load_queue()
        return [item for item in queue.items if item.status == "pending_review"]

    def get_review_by_id(self, review_id: str) -> ReviewItem | None:
        """Get a review item by ID.

        Args:
            review_id: Review ID to find

        Returns:
            ReviewItem if found, None otherwise
        """
        queue = self.load_queue()
        for item in queue.items:
            if item.id == review_id:
                return item
        return None

    def get_pending_by_uuid(self, original_uuid: str) -> ReviewItem | None:
        """Get a pending review item by original UUID.

        Args:
            original_uuid: Original UUID (file_id) to find

        Returns:
            ReviewItem if found and pending, None otherwise
        """
        queue = self.load_queue()
        for item in queue.items:
            if item.original_uuid == original_uuid and item.status == "pending_review":
                return item
        return None

    def approve(self, review_id: str) -> bool:
        """Approve a conversion and import to Photos.

        This will:
        1. Import the converted video to Photos
        2. Add to the same albums as the original
        3. Move the original to Photos trash
        4. Update the review queue

        Args:
            review_id: Review ID to approve

        Returns:
            True if successful
        """
        queue = self.load_queue()
        item = None
        item_index = -1

        for i, review_item in enumerate(queue.items):
            if review_item.id == review_id:
                item = review_item
                item_index = i
                break

        if item is None:
            logger.error(f"Review item not found: {review_id}")
            return False

        if item.status != "pending_review":
            logger.error(f"Review item is not pending: {review_id}")
            return False

        if not item.converted_path.exists():
            logger.error(f"Converted file not found: {item.converted_path}")
            return False

        try:
            # Get metadata for import
            metadata = VideoMetadata.from_dict(item.metadata)

            # Import converted video to Photos
            new_uuid = self.photos_manager.import_video(video_path=item.converted_path)

            if not new_uuid:
                logger.error("Failed to import video to Photos")
                return False

            # Add to albums
            if metadata.albums:
                self.photos_manager.add_to_albums(new_uuid, metadata.albums)

            # Move original to trash
            self.photos_manager.delete_video(item.original_uuid)

            # Update review item status
            queue.items[item_index].status = "approved"
            queue.last_updated = datetime.now().isoformat()
            self.save_queue(queue)

            # Clean up converted file and metadata
            item.converted_path.unlink(missing_ok=True)
            metadata_path = item.converted_path.with_suffix(".json")
            metadata_path.unlink(missing_ok=True)

            logger.info(f"Approved conversion: {review_id}")
            return True

        except Exception:
            logger.exception(f"Failed to approve conversion: {review_id}")
            return False

    def reject(self, review_id: str) -> bool:
        """Reject a conversion and delete the converted file.

        This will:
        1. Delete the converted video file
        2. Keep the original unchanged
        3. Update the review queue

        Args:
            review_id: Review ID to reject

        Returns:
            True if successful
        """
        queue = self.load_queue()
        item = None
        item_index = -1

        for i, review_item in enumerate(queue.items):
            if review_item.id == review_id:
                item = review_item
                item_index = i
                break

        if item is None:
            logger.error(f"Review item not found: {review_id}")
            return False

        if item.status != "pending_review":
            logger.error(f"Review item is not pending: {review_id}")
            return False

        try:
            # Delete converted file
            if item.converted_path.exists():
                item.converted_path.unlink()

            # Delete metadata file
            metadata_path = item.converted_path.with_suffix(".json")
            if metadata_path.exists():
                metadata_path.unlink()

            # Update review item status
            queue.items[item_index].status = "rejected"
            queue.last_updated = datetime.now().isoformat()
            self.save_queue(queue)

            logger.info(f"Rejected conversion: {review_id}")
            return True

        except Exception:
            logger.exception(f"Failed to reject conversion: {review_id}")
            return False

    def load_queue(self) -> ReviewQueue:
        """Load the review queue from file.

        Returns:
            ReviewQueue (empty if file doesn't exist)
        """
        if not self.queue_path.exists():
            return ReviewQueue()

        try:
            with open(self.queue_path) as f:
                data = json.load(f)
            return ReviewQueue.from_dict(data)
        except Exception as e:
            logger.warning(f"Failed to load review queue: {e}")
            return ReviewQueue()

    def save_queue(self, queue: ReviewQueue) -> bool:
        """Save the review queue to file.

        Args:
            queue: ReviewQueue to save

        Returns:
            True if successful
        """
        try:
            self.queue_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.queue_path, "w") as f:
                json.dump(queue.to_dict(), f, indent=2)
            return True
        except Exception as e:
            logger.exception(f"Failed to save review queue: {e}")
            return False

    def clear_completed(self) -> int:
        """Remove approved and rejected items from the queue.

        Returns:
            Number of items removed
        """
        queue = self.load_queue()
        original_count = len(queue.items)

        queue.items = [item for item in queue.items if item.status == "pending_review"]
        queue.last_updated = datetime.now().isoformat()

        self.save_queue(queue)

        return original_count - len(queue.items)

    def get_queue_summary(self) -> dict:
        """Get summary of the review queue.

        Returns:
            Dictionary with queue statistics
        """
        queue = self.load_queue()

        pending = [item for item in queue.items if item.status == "pending_review"]
        approved = [item for item in queue.items if item.status == "approved"]
        rejected = [item for item in queue.items if item.status == "rejected"]

        total_savings = sum(item.quality_result.get("space_saved_bytes", 0) for item in pending)

        return {
            "total": len(queue.items),
            "pending": len(pending),
            "approved": len(approved),
            "rejected": len(rejected),
            "pending_savings_bytes": total_savings,
        }
