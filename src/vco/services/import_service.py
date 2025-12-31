"""Import service for importing converted videos to Photos library.

This service manages the import workflow:
1. List pending imports with album information
2. Import single video to Photos and add to albums
3. Batch import all pending items
"""

import logging
from dataclasses import dataclass, field

from vco.metadata.manager import VideoMetadata
from vco.photos.manager import PhotosAccessError, PhotosAccessManager
from vco.services.review import ReviewItem, ReviewService

logger = logging.getLogger(__name__)


@dataclass
class FileDeleteResult:
    """Result of deleting files associated with a review item."""

    video_deleted: bool
    metadata_deleted: bool
    video_error: str | None = None
    metadata_error: str | None = None


@dataclass
class RemoveResult:
    """Result of removing an item from the review queue."""

    success: bool
    review_id: str
    queue_removed: bool
    files_deleted: FileDeleteResult | None = None
    error_message: str | None = None


@dataclass
class ClearResult:
    """Result of clearing all items from the review queue."""

    success: bool
    items_removed: int
    files_deleted: int
    files_failed: int
    error_details: list[str] = field(default_factory=list)


@dataclass
class ImportResult:
    """Result of importing a single video to Photos."""

    success: bool
    review_id: str
    original_filename: str
    converted_filename: str
    albums: list[str] = field(default_factory=list)
    error_message: str | None = None


@dataclass
class BatchImportResult:
    """Result of batch importing multiple videos."""

    total: int
    successful: int
    failed: int
    results: list[ImportResult] = field(default_factory=list)


class ImportService:
    """Service for importing converted videos to Photos library."""

    def __init__(
        self,
        review_service: ReviewService | None = None,
        photos_manager: PhotosAccessManager | None = None,
    ):
        """Initialize ImportService.

        Args:
            review_service: ReviewService instance for queue management
            photos_manager: PhotosAccessManager instance for Photos operations
        """
        self.review_service = review_service or ReviewService()
        self.photos_manager = photos_manager or PhotosAccessManager()

    def list_pending(self) -> list[ReviewItem]:
        """Get all pending import items.

        Returns:
            List of pending ReviewItems
        """
        return self.review_service.get_pending_reviews()

    def import_single(self, review_id: str) -> ImportResult:
        """Import a single video to Photos library.

        This will:
        1. Import the converted video to Photos
        2. Add to the same albums as the original
        3. Update the review queue status to 'imported'

        Note: Original video deletion is NOT performed (user does it manually)

        Args:
            review_id: Review ID to import

        Returns:
            ImportResult with success status and details
        """
        # Get review item
        item = self.review_service.get_review_by_id(review_id)

        if item is None:
            return ImportResult(
                success=False,
                review_id=review_id,
                original_filename="",
                converted_filename="",
                error_message=f"Review item not found: {review_id}",
            )

        if item.status != "pending_review":
            return ImportResult(
                success=False,
                review_id=review_id,
                original_filename=item.original_path.name,
                converted_filename=item.converted_path.name,
                error_message=f"Review item is not pending: {item.status}",
            )

        if not item.converted_path.exists():
            return ImportResult(
                success=False,
                review_id=review_id,
                original_filename=item.original_path.name,
                converted_filename=item.converted_path.name,
                error_message=f"Converted file not found: {item.converted_path}",
            )

        # Get metadata for albums
        metadata = VideoMetadata.from_dict(item.metadata)
        albums = metadata.albums or []

        try:
            # Import converted video to Photos
            new_uuid = self.photos_manager.import_video(video_path=item.converted_path)

            if not new_uuid:
                return ImportResult(
                    success=False,
                    review_id=review_id,
                    original_filename=item.original_path.name,
                    converted_filename=item.converted_path.name,
                    albums=albums,
                    error_message="Failed to import video to Photos",
                )

            # Add to albums
            if albums:
                try:
                    self.photos_manager.add_to_albums(new_uuid, albums)
                except PhotosAccessError as e:
                    logger.warning(f"Failed to add to some albums: {e}")
                    # Continue - import was successful, album addition is secondary

            # Update review item status to 'imported'
            self._update_status(review_id, "imported")

            # Clean up converted file and metadata
            item.converted_path.unlink(missing_ok=True)
            metadata_path = item.converted_path.with_suffix(".json")
            metadata_path.unlink(missing_ok=True)

            logger.info(f"Imported video: {review_id}")

            return ImportResult(
                success=True,
                review_id=review_id,
                original_filename=item.original_path.name,
                converted_filename=item.converted_path.name,
                albums=albums,
            )

        except PhotosAccessError as e:
            logger.error(f"Photos access error during import: {e}")
            return ImportResult(
                success=False,
                review_id=review_id,
                original_filename=item.original_path.name,
                converted_filename=item.converted_path.name,
                albums=albums,
                error_message=str(e),
            )
        except Exception as e:
            logger.exception(f"Unexpected error during import: {review_id}")
            return ImportResult(
                success=False,
                review_id=review_id,
                original_filename=item.original_path.name,
                converted_filename=item.converted_path.name,
                albums=albums,
                error_message=str(e),
            )

    def import_all(self) -> BatchImportResult:
        """Import all pending videos to Photos library.

        Processes all items with status 'pending_review'.
        Continues processing even if some imports fail.

        Returns:
            BatchImportResult with overall statistics and individual results
        """
        pending = self.list_pending()

        results: list[ImportResult] = []
        successful = 0
        failed = 0

        for item in pending:
            result = self.import_single(item.id)
            results.append(result)

            if result.success:
                successful += 1
            else:
                failed += 1

        return BatchImportResult(
            total=len(pending), successful=successful, failed=failed, results=results
        )

    def _update_status(self, review_id: str, new_status: str) -> bool:
        """Update the status of a review item.

        Args:
            review_id: Review ID to update
            new_status: New status value

        Returns:
            True if successful
        """
        queue = self.review_service.load_queue()

        for i, item in enumerate(queue.items):
            if item.id == review_id:
                queue.items[i].status = new_status
                from datetime import datetime

                queue.last_updated = datetime.now().isoformat()
                return self.review_service.save_queue(queue)

        return False

    def _delete_item_files(self, item: ReviewItem) -> FileDeleteResult:
        """Delete files associated with a review item.

        Args:
            item: ReviewItem containing file paths

        Returns:
            FileDeleteResult with deletion status and any errors
        """
        video_deleted = False
        metadata_deleted = False
        video_error = None
        metadata_error = None

        # Delete converted video file
        try:
            item.converted_path.unlink(missing_ok=True)
            video_deleted = True
            logger.debug(f"Deleted video file: {item.converted_path}")
        except Exception as e:
            video_error = str(e)
            logger.warning(f"Failed to delete video file {item.converted_path}: {e}")

        # Delete metadata JSON file
        metadata_path = item.converted_path.with_suffix(".json")
        try:
            metadata_path.unlink(missing_ok=True)
            metadata_deleted = True
            logger.debug(f"Deleted metadata file: {metadata_path}")
        except Exception as e:
            metadata_error = str(e)
            logger.warning(f"Failed to delete metadata file {metadata_path}: {e}")

        return FileDeleteResult(
            video_deleted=video_deleted,
            metadata_deleted=metadata_deleted,
            video_error=video_error,
            metadata_error=metadata_error,
        )

    def remove_item(self, review_id: str) -> RemoveResult:
        """Remove a single item from the review queue and delete associated files.

        Args:
            review_id: Review ID to remove

        Returns:
            RemoveResult with removal status and file deletion details
        """
        queue = self.review_service.load_queue()

        # Find the item
        item = None
        for queue_item in queue.items:
            if queue_item.id == review_id:
                item = queue_item
                break

        if item is None:
            return RemoveResult(
                success=False,
                review_id=review_id,
                queue_removed=False,
                error_message="Item not found",
            )

        # Delete associated files
        file_result = self._delete_item_files(item)

        # Remove from queue
        queue.items = [i for i in queue.items if i.id != review_id]

        from datetime import datetime

        queue.last_updated = datetime.now().isoformat()
        queue_saved = self.review_service.save_queue(queue)

        return RemoveResult(
            success=queue_saved,
            review_id=review_id,
            queue_removed=queue_saved,
            files_deleted=file_result,
        )

    def clear_queue(self) -> ClearResult:
        """Clear all items from the review queue and delete associated files.

        Returns:
            ClearResult with removal status and file deletion details
        """
        queue = self.review_service.load_queue()
        items_count = len(queue.items)

        if items_count == 0:
            return ClearResult(success=True, items_removed=0, files_deleted=0, files_failed=0)

        files_deleted = 0
        files_failed = 0
        error_details = []

        # Delete files for each item
        for item in queue.items:
            file_result = self._delete_item_files(item)

            if file_result.video_deleted and file_result.metadata_deleted:
                files_deleted += 1
            else:
                files_failed += 1
                if file_result.video_error:
                    error_details.append(f"Video {item.converted_path}: {file_result.video_error}")
                if file_result.metadata_error:
                    error_details.append(
                        f"Metadata {item.converted_path.with_suffix('.json')}: {file_result.metadata_error}"
                    )

        # Clear queue
        queue.items = []

        from datetime import datetime

        queue.last_updated = datetime.now().isoformat()
        queue_saved = self.review_service.save_queue(queue)

        return ClearResult(
            success=queue_saved,
            items_removed=items_count,
            files_deleted=files_deleted,
            files_failed=files_failed,
            error_details=error_details,
        )
