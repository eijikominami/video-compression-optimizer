"""Unified Import Service for local and AWS imports.

This service integrates ImportService (local) and AwsImportService (AWS)
to provide a unified interface for importing videos from both sources.

Requirements: 1.1, 2.1, 3.1, 4.1, 7.1, 7.6
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from vco.metadata.manager import VideoMetadata
from vco.models.types import (
    ImportableItem,
    UnifiedBatchResult,
    UnifiedClearResult,
    UnifiedImportResult,
    UnifiedListResult,
    UnifiedRemoveResult,
)
from vco.photos.manager import PhotosAccessError, PhotosAccessManager
from vco.services.aws_import import AwsImportService
from vco.services.import_service import ImportService
from vco.services.review import ReviewItem

logger = logging.getLogger(__name__)


class UnifiedImportService:
    """Service for unified import from local and AWS sources.

    Integrates ImportService and AwsImportService to provide:
    - Unified listing of importable items
    - Single item import (local or AWS)
    - Batch import with parallel AWS downloads
    - Queue management (remove, clear)

    Requirements: 1.1, 2.1, 3.1, 4.1, 7.1, 7.6
    """

    def __init__(
        self,
        local_service: ImportService | None = None,
        aws_service: AwsImportService | None = None,
        photos_manager: PhotosAccessManager | None = None,
    ):
        """Initialize UnifiedImportService.

        Args:
            local_service: ImportService for local imports
            aws_service: AwsImportService for AWS imports (optional)
            photos_manager: PhotosAccessManager for Photos operations
        """
        self.local_service = local_service or ImportService()
        self.aws_service = aws_service
        self.photos_manager = photos_manager or PhotosAccessManager()

    def list_all_importable(self, user_id: str | None = None) -> UnifiedListResult:
        """List all importable items from local and AWS sources.

        Args:
            user_id: User identifier for AWS (defaults to machine ID)

        Returns:
            UnifiedListResult with items from both sources

        Requirements: 1.1, 1.5
        """
        # Get local items
        local_items = self._get_local_items()

        # Get AWS items
        aws_items: list[ImportableItem] = []
        aws_available = True
        aws_error: str | None = None

        if self.aws_service:
            try:
                aws_items = self.aws_service.list_completed_files(user_id)
            except Exception as e:
                logger.warning(f"Failed to list AWS items: {e}")
                aws_available = False
                aws_error = str(e)

        return UnifiedListResult(
            local_items=local_items,
            aws_items=aws_items,
            aws_available=aws_available,
            aws_error=aws_error,
        )

    def import_item(
        self,
        item_id: str,
        user_id: str | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ) -> UnifiedImportResult:
        """Import a single item from local or AWS source.

        Item ID format:
        - Local: review_id (no colon)
        - AWS: task_id:file_id (contains colon)

        Args:
            item_id: Item ID to import
            user_id: User identifier for AWS
            progress_callback: Callback for download progress (AWS only)

        Returns:
            UnifiedImportResult

        Requirements: 2.1, 3.1
        """
        if self._is_aws_item(item_id):
            return self._import_aws_item(item_id, user_id, progress_callback)
        else:
            return self._import_local_item(item_id)

    def import_all(
        self,
        user_id: str | None = None,
        max_concurrent_downloads: int = 3,
        progress_callback: Callable[..., Any] | None = None,
    ) -> UnifiedBatchResult:
        """Import all items from local and AWS sources.

        Local items are processed sequentially.
        AWS items are downloaded in parallel (up to max_concurrent_downloads).

        Args:
            user_id: User identifier for AWS
            max_concurrent_downloads: Maximum concurrent AWS downloads
            progress_callback: Callback for download progress

        Returns:
            UnifiedBatchResult

        Requirements: 4.1, 4.4, 4.5
        """
        result = UnifiedBatchResult()

        # Get all items
        list_result = self.list_all_importable(user_id)

        # Process local items sequentially
        result.local_total = len(list_result.local_items)
        for item in list_result.local_items:
            import_result = self._import_local_item(item.item_id)
            result.results.append(import_result)
            if import_result.success:
                result.local_successful += 1
            else:
                result.local_failed += 1

        # Process AWS items in parallel
        result.aws_total = len(list_result.aws_items)
        if list_result.aws_items and self.aws_service:
            aws_results = self._import_aws_items_parallel(
                list_result.aws_items,
                user_id,
                max_concurrent_downloads,
                progress_callback,
            )
            for import_result in aws_results:
                result.results.append(import_result)
                if import_result.success:
                    result.aws_successful += 1
                else:
                    result.aws_failed += 1

        return result

    def remove_item(self, item_id: str, user_id: str | None = None) -> UnifiedRemoveResult:
        """Remove a single item from local or AWS source.

        Args:
            item_id: Item ID to remove
            user_id: User identifier for AWS

        Returns:
            UnifiedRemoveResult

        Requirements: 7.1, 7.2, 7.3
        """
        if self._is_aws_item(item_id):
            return self._remove_aws_item(item_id, user_id)
        else:
            return self._remove_local_item(item_id)

    def clear_all_queues(self, user_id: str | None = None) -> UnifiedClearResult:
        """Clear all items from both local and AWS sources.

        Deletes local files and S3 files, updates AWS file statuses to REMOVED.

        Args:
            user_id: User identifier for AWS

        Returns:
            UnifiedClearResult with deletion statistics

        Requirements: 7.6, 7.7, 7.8, 10.7
        """
        # Get all importable items before clearing
        list_result = self.list_all_importable(user_id)

        aws_items = [item for item in list_result.all_items if item.source == "aws"]

        # Clear local queue (includes file deletion)
        local_result = self.local_service.clear_queue()

        # Clear AWS items using cleanup API
        aws_files_deleted = 0
        aws_files_failed = 0
        aws_error_details = []

        if self.aws_service and aws_items:
            for item in aws_items:
                try:
                    task_id, file_id = self._parse_aws_item_id(item.item_id)

                    # Use cleanup API with action="removed"
                    cleanup_result = self.aws_service.cleanup_file(
                        task_id=task_id,
                        file_id=file_id,
                        action="removed",
                        user_id=user_id,
                    )

                    if cleanup_result.success:
                        aws_files_deleted += 1
                    else:
                        aws_files_failed += 1
                        aws_error_details.append(
                            f"Failed to cleanup {item.item_id}: {cleanup_result.error_message}"
                        )

                except Exception as e:
                    aws_files_failed += 1
                    aws_error_details.append(f"Error processing {item.item_id}: {str(e)}")
                    logger.warning(f"Failed to remove AWS item {item.item_id}: {e}")

        return UnifiedClearResult(
            success=local_result.success,
            local_items_removed=local_result.items_removed,
            local_files_deleted=local_result.files_deleted,
            local_files_failed=local_result.files_failed,
            aws_items_removed=len(aws_items),
            aws_files_deleted=aws_files_deleted,
            aws_files_failed=aws_files_failed,
            error_details=local_result.error_details + aws_error_details,
        )

    # =========================================================================
    # Private methods
    # =========================================================================

    def _is_aws_item(self, item_id: str) -> bool:
        """Check if item_id is an AWS item (contains colon)."""
        return ":" in item_id

    def _parse_aws_item_id(self, item_id: str) -> tuple[str, str]:
        """Parse AWS item_id into task_id and file_id."""
        parts = item_id.split(":", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid AWS item ID format: {item_id}")
        return parts[0], parts[1]

    def _get_local_items(self) -> list[ImportableItem]:
        """Convert local ReviewItems to ImportableItems."""
        items: list[ImportableItem] = []
        pending = self.local_service.list_pending()

        for review_item in pending:
            item = self._review_item_to_importable(review_item)
            items.append(item)

        return items

    def _review_item_to_importable(self, review_item: ReviewItem) -> ImportableItem:
        """Convert ReviewItem to ImportableItem."""
        metadata = VideoMetadata.from_dict(review_item.metadata)

        return ImportableItem(
            item_id=review_item.id,
            source="local",
            original_filename=review_item.original_path.name,
            converted_filename=review_item.converted_path.name,
            original_size=review_item.quality_result.get("original_size", 0),
            converted_size=review_item.quality_result.get("converted_size", 0),
            compression_ratio=review_item.quality_result.get("compression_ratio", 0.0),
            ssim_score=review_item.quality_result.get("ssim_score", 0.0),
            albums=metadata.albums or [],
            capture_date=metadata.capture_date,
            converted_path=review_item.converted_path,
        )

    def _import_local_item(self, review_id: str) -> UnifiedImportResult:
        """Import a local item using ImportService."""
        result = self.local_service.import_single(review_id)

        return UnifiedImportResult(
            success=result.success,
            item_id=review_id,
            source="local",
            original_filename=result.original_filename,
            converted_filename=result.converted_filename,
            albums=result.albums,
            error_message=result.error_message,
        )

    def _import_aws_item(
        self,
        item_id: str,
        user_id: str | None,
        progress_callback: Callable[..., Any] | None,
    ) -> UnifiedImportResult:
        """Import an AWS item: download, import to Photos, delete S3."""
        if not self.aws_service:
            return UnifiedImportResult(
                success=False,
                item_id=item_id,
                source="aws",
                original_filename="",
                converted_filename="",
                error_message="AWS service not configured",
            )

        try:
            task_id, file_id = self._parse_aws_item_id(item_id)
        except ValueError as e:
            return UnifiedImportResult(
                success=False,
                item_id=item_id,
                source="aws",
                original_filename="",
                converted_filename="",
                error_message=str(e),
            )

        # Download file
        download_result = self.aws_service.download_and_prepare(
            task_id=task_id,
            file_id=file_id,
            user_id=user_id,
            progress_callback=progress_callback,
        )

        if not download_result.success:
            return UnifiedImportResult(
                success=False,
                item_id=item_id,
                source="aws",
                original_filename="",
                converted_filename="",
                error_message=download_result.error_message,
                downloaded=False,
            )

        local_path = download_result.local_path
        if local_path is None:
            return UnifiedImportResult(
                success=False,
                item_id=item_id,
                source="aws",
                original_filename="",
                converted_filename="",
                error_message="Download succeeded but no local path returned",
                downloaded=True,
                download_resumed=download_result.download_resumed,
                checksum_verified=download_result.checksum_verified,
            )

        # Import to Photos
        try:
            new_uuid = self.photos_manager.import_video(video_path=local_path)
            if not new_uuid:
                return UnifiedImportResult(
                    success=False,
                    item_id=item_id,
                    source="aws",
                    original_filename="",
                    converted_filename=local_path.name,
                    error_message="Failed to import video to Photos",
                    downloaded=True,
                    download_resumed=download_result.download_resumed,
                    checksum_verified=download_result.checksum_verified,
                )
        except PhotosAccessError as e:
            return UnifiedImportResult(
                success=False,
                item_id=item_id,
                source="aws",
                original_filename="",
                converted_filename=local_path.name,
                error_message=str(e),
                downloaded=True,
                download_resumed=download_result.download_resumed,
                checksum_verified=download_result.checksum_verified,
            )

        # Update file status to DOWNLOADED and delete S3 file via cleanup API
        cleanup_result = self.aws_service.cleanup_file(
            task_id=task_id,
            file_id=file_id,
            action="downloaded",
            user_id=user_id,
        )

        s3_deleted = cleanup_result.s3_deleted if cleanup_result.success else False
        if not cleanup_result.success:
            logger.warning(f"Cleanup API failed for {item_id}: {cleanup_result.error_message}")

        # Clean up local downloaded file
        try:
            local_path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete local file {local_path}: {e}")

        return UnifiedImportResult(
            success=True,
            item_id=item_id,
            source="aws",
            original_filename="",  # Not available from AWS
            converted_filename=local_path.name,
            albums=[],  # Albums not available from AWS
            downloaded=True,
            download_resumed=download_result.download_resumed,
            checksum_verified=download_result.checksum_verified,
            s3_deleted=s3_deleted,
        )

    def _import_aws_items_parallel(
        self,
        items: list[ImportableItem],
        user_id: str | None,
        max_concurrent: int,
        progress_callback: Callable[..., Any] | None,
    ) -> list[UnifiedImportResult]:
        """Import AWS items in parallel with concurrency limit."""
        results: list[UnifiedImportResult] = []

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {
                executor.submit(
                    self._import_aws_item,
                    item.item_id,
                    user_id,
                    progress_callback,
                ): item
                for item in items
            }

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    item = futures[future]
                    # Extract user-friendly message
                    error_msg = str(e)
                    if "404" in error_msg or "Not Found" in error_msg:
                        error_msg = "File not found in S3. The conversion may still be in progress."
                    elif "403" in error_msg or "Forbidden" in error_msg:
                        error_msg = "Access denied to S3 file. Check your AWS credentials."
                    elif "ExpiredToken" in error_msg:
                        error_msg = "AWS credentials have expired. Please refresh your credentials."

                    logger.warning(f"Failed to import AWS item {item.item_id}: {error_msg}")
                    results.append(
                        UnifiedImportResult(
                            success=False,
                            item_id=item.item_id,
                            source="aws",
                            original_filename=item.original_filename,
                            converted_filename=item.converted_filename,
                            error_message=error_msg,
                        )
                    )

        return results

    def _remove_local_item(self, review_id: str) -> UnifiedRemoveResult:
        """Remove a local item using ImportService."""
        result = self.local_service.remove_item(review_id)

        file_deleted = False
        metadata_deleted = False
        if result.files_deleted:
            file_deleted = result.files_deleted.video_deleted
            metadata_deleted = result.files_deleted.metadata_deleted

        return UnifiedRemoveResult(
            success=result.success,
            item_id=review_id,
            source="local",
            file_deleted=file_deleted,
            metadata_deleted=metadata_deleted,
            error_message=result.error_message,
        )

    def _remove_aws_item(self, item_id: str, user_id: str | None) -> UnifiedRemoveResult:
        """Remove an AWS item by updating status and deleting S3 file via cleanup API."""
        if not self.aws_service:
            return UnifiedRemoveResult(
                success=False,
                item_id=item_id,
                source="aws",
                error_message="AWS service not configured",
            )

        try:
            task_id, file_id = self._parse_aws_item_id(item_id)
        except ValueError as e:
            return UnifiedRemoveResult(
                success=False,
                item_id=item_id,
                source="aws",
                error_message=str(e),
            )

        # Use cleanup API with action="removed"
        cleanup_result = self.aws_service.cleanup_file(
            task_id=task_id,
            file_id=file_id,
            action="removed",
            user_id=user_id,
        )

        return UnifiedRemoveResult(
            success=cleanup_result.success,
            item_id=item_id,
            source="aws",
            s3_deleted=cleanup_result.s3_deleted,
            error_message=cleanup_result.error_message if not cleanup_result.success else None,
        )
