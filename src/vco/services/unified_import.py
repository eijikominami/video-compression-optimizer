"""Unified Import Service for AWS imports.

This service provides a unified interface for importing videos from AWS.

Requirements: 1.1, 2.1, 3.1, 4.1, 7.1, 7.6
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

from vco.metadata.embedder import EmbedResult, MetadataEmbedder
from vco.metadata.manager import VideoMetadata
from vco.metadata.verifier import MetadataVerifier
from vco.models.metadata import OriginalMetadata, VerificationResult
from vco.models.types import (
    DeleteResult,
    ImportableItem,
    UnifiedBatchResult,
    UnifiedClearResult,
    UnifiedImportResult,
    UnifiedListResult,
    UnifiedRemoveResult,
)
from vco.photos.manager import PhotosAccessError
from vco.photos.swift_bridge import SwiftBridge
from vco.services.aws_import import AwsImportService

logger = logging.getLogger(__name__)


class UnifiedImportService:
    """Service for unified import from AWS sources.

    Provides:
    - Listing of importable items from AWS
    - Single item import from AWS
    - Batch import with parallel AWS downloads
    - Queue management (remove, clear)
    - Metadata verification before import

    Requirements: 1.1, 2.1, 3.1, 4.1, 7.1, 7.6
    """

    def __init__(
        self,
        aws_service: AwsImportService | None = None,
        swift_bridge: SwiftBridge | None = None,
        metadata_embedder: MetadataEmbedder | None = None,
        metadata_verifier: MetadataVerifier | None = None,
    ):
        """Initialize UnifiedImportService.

        Args:
            aws_service: AwsImportService for AWS imports (optional)
            swift_bridge: SwiftBridge for Photos operations
            metadata_embedder: MetadataEmbedder for embedding metadata (optional)
            metadata_verifier: MetadataVerifier for metadata verification (optional)
        """
        self.aws_service = aws_service
        self.swift_bridge = swift_bridge or SwiftBridge()
        self.metadata_embedder = metadata_embedder or MetadataEmbedder()
        self.metadata_verifier = metadata_verifier or MetadataVerifier()

    def list_all_importable(self, user_id: str | None = None) -> UnifiedListResult:
        """List all importable items from AWS sources.

        Args:
            user_id: User identifier for AWS (defaults to machine ID)

        Returns:
            UnifiedListResult with items from AWS

        Requirements: 1.1, 1.5
        """
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
            local_items=[],
            aws_items=aws_items,
            aws_available=aws_available,
            aws_error=aws_error,
        )

    def import_item(
        self,
        item_id: str,
        user_id: str | None = None,
        progress_callback: Callable[..., Any] | None = None,
        status_callback: Callable[..., None] | None = None,
        delete_original: bool = False,
        original_uuid: str | None = None,
        force_import: bool = False,
    ) -> UnifiedImportResult:
        """Import a single item from AWS source.

        Item ID format:
        - AWS: task_id:file_id (contains colon)

        Args:
            item_id: Item ID to import
            user_id: User identifier for AWS
            progress_callback: Callback for download progress (AWS only)
            status_callback: Callback for status updates (filename, status)
            delete_original: Whether to delete original video after import
            original_uuid: UUID of original video in Photos library (for deletion)
            force_import: Whether to import even if metadata verification fails

        Returns:
            UnifiedImportResult

        Requirements: 2.1, 3.1, 5.1, 5.4
        """
        if not self._is_aws_item(item_id):
            return UnifiedImportResult(
                success=False,
                item_id=item_id,
                source="local",
                original_filename="",
                converted_filename="",
                error_message="Local imports are not supported. Use AWS item ID format (task_id:file_id).",
            )
        return self._import_aws_item(
            item_id,
            user_id,
            progress_callback,
            status_callback=status_callback,
            delete_original=delete_original,
            original_uuid=original_uuid,
            force_import=force_import,
        )

    def import_all(
        self,
        user_id: str | None = None,
        max_concurrent_downloads: int = 3,
        progress_callback: Callable[..., Any] | None = None,
        status_callback: Callable[..., None] | None = None,
        delete_original: bool = False,
        original_uuids: dict[str, str] | None = None,
        force_import: bool = False,
    ) -> UnifiedBatchResult:
        """Import all items from AWS sources.

        AWS items are downloaded in parallel (up to max_concurrent_downloads).

        Args:
            user_id: User identifier for AWS
            max_concurrent_downloads: Maximum concurrent AWS downloads
            progress_callback: Callback for download progress
            status_callback: Callback for status updates (filename, status)
            delete_original: Whether to delete original videos after import
            original_uuids: Mapping of item_id to original video UUID for deletion
            force_import: Whether to import even if metadata verification fails

        Returns:
            UnifiedBatchResult

        Requirements: 4.1, 4.4, 4.5, 5.5, 5.9
        """
        result = UnifiedBatchResult()

        # Get all items
        list_result = self.list_all_importable(user_id)

        # Process AWS items in parallel
        result.aws_total = len(list_result.aws_items)
        if list_result.aws_items and self.aws_service:
            aws_results = self._import_aws_items_parallel(
                list_result.aws_items,
                user_id,
                max_concurrent_downloads,
                progress_callback,
                status_callback,
                delete_original=delete_original,
                original_uuids=original_uuids,
                force_import=force_import,
            )
            for import_result in aws_results:
                result.results.append(import_result)
                if import_result.success:
                    result.aws_successful += 1
                    if import_result.metadata_verified:
                        result.metadata_verified_count += 1
                else:
                    result.aws_failed += 1
                    if import_result.metadata_mismatch:
                        result.metadata_mismatch_count += 1
                        result.skipped_files.append(import_result.converted_filename)

        return result

    def remove_item(self, item_id: str, user_id: str | None = None) -> UnifiedRemoveResult:
        """Remove a single item from AWS source.

        Args:
            item_id: Item ID to remove
            user_id: User identifier for AWS

        Returns:
            UnifiedRemoveResult

        Requirements: 7.1, 7.2, 7.3
        """
        if not self._is_aws_item(item_id):
            return UnifiedRemoveResult(
                success=False,
                item_id=item_id,
                source="local",
                error_message="Local items are not supported. Use AWS item ID format (task_id:file_id).",
            )
        return self._remove_aws_item(item_id, user_id)

    def clear_all_queues(self, user_id: str | None = None) -> UnifiedClearResult:
        """Clear all items from AWS sources.

        Deletes S3 files and updates AWS file statuses to REMOVED.

        Args:
            user_id: User identifier for AWS

        Returns:
            UnifiedClearResult with deletion statistics

        Requirements: 7.6, 7.7, 7.8, 10.7
        """
        # Get all importable items before clearing
        list_result = self.list_all_importable(user_id)

        aws_items = [item for item in list_result.all_items if item.source == "aws"]

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
            success=True,
            local_items_removed=0,
            local_files_deleted=0,
            local_files_failed=0,
            aws_items_removed=len(aws_items),
            aws_files_deleted=aws_files_deleted,
            aws_files_failed=aws_files_failed,
            error_details=aws_error_details,
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

    def _import_aws_item(
        self,
        item_id: str,
        user_id: str | None,
        progress_callback: Callable[..., Any] | None,
        status_callback: Callable[..., None] | None = None,
        delete_original: bool = False,
        original_uuid: str | None = None,
        force_import: bool = False,
    ) -> UnifiedImportResult:
        """Import an AWS item: download, verify metadata, import to Photos, delete S3, optionally delete original."""
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

        # Load metadata for verification and album assignment
        metadata: VideoMetadata | None = None
        original_metadata: OriginalMetadata | None = None
        albums: list[str] = []
        original_filename = ""

        if download_result.metadata_path and download_result.metadata_path.exists():
            try:
                import json

                with open(download_result.metadata_path) as f:
                    metadata_dict = json.load(f)
                metadata = VideoMetadata.from_dict(metadata_dict)
                if metadata and metadata.albums:
                    albums = metadata.albums

                # Create OriginalMetadata for verification
                original_metadata = OriginalMetadata.from_dict(metadata_dict)
                original_filename = metadata_dict.get("original_filename", "")
            except Exception as e:
                logger.warning(f"Failed to load metadata: {e}")

        # Embed metadata using exiftool (Keys:CreationDate for correct timezone in Photos)
        embed_result: EmbedResult | None = None
        metadata_embedded = False

        if original_metadata and self.metadata_embedder:
            try:
                # Notify status: embedding metadata
                if status_callback:
                    status_callback(local_path.name, "embedding", None, None)

                embed_result = self.metadata_embedder.embed(
                    video_path=local_path,
                    metadata=original_metadata,
                )

                if embed_result.success and not embed_result.skipped:
                    metadata_embedded = True
                    logger.info(
                        f"Metadata embedded: {embed_result.embedded_fields} for {local_path.name}"
                    )
                elif embed_result.skipped:
                    logger.info(f"Metadata embedding skipped for {local_path.name}")
                else:
                    logger.warning(
                        f"Metadata embedding failed for {local_path.name}: {embed_result.error_message}"
                    )
            except Exception as e:
                logger.warning(f"Metadata embedding failed: {e}")

        # Verify metadata before import
        verification_result: VerificationResult | None = None
        metadata_verified = False
        metadata_mismatch = False
        verification_skipped = False

        if original_metadata and self.metadata_verifier:
            try:
                # Notify status: verifying metadata
                if status_callback:
                    status_callback(local_path.name, "verifying", None, None)

                verification_result = self.metadata_verifier.verify(
                    converted_path=local_path,
                    original_metadata=original_metadata,
                    processing_time=datetime.now(),
                )

                if verification_result.has_mismatch:
                    metadata_mismatch = True
                    if not force_import:
                        # Skip import due to metadata mismatch
                        logger.warning(
                            f"Metadata mismatch for {local_path.name}: "
                            f"{verification_result.mismatched_fields}"
                        )
                        return UnifiedImportResult(
                            success=False,
                            item_id=item_id,
                            source="aws",
                            original_filename=original_filename,
                            converted_filename=local_path.name,
                            albums=albums,
                            error_message=f"Metadata mismatch: {', '.join(verification_result.mismatched_fields)}",
                            downloaded=True,
                            download_resumed=download_result.download_resumed,
                            checksum_verified=download_result.checksum_verified,
                            metadata_embedded=metadata_embedded,
                            embed_result=embed_result,
                            metadata_verified=False,
                            metadata_mismatch=True,
                            verification_result=verification_result,
                        )
                else:
                    metadata_verified = True

                # Log processing time proximity warning if present
                if verification_result.has_warning and verification_result.processing_time_warning:
                    logger.warning(verification_result.processing_time_warning.message)

            except Exception as e:
                logger.warning(f"Metadata verification failed, skipping: {e}")
                verification_skipped = True
        else:
            verification_skipped = True

        # Notify status: importing to Photos
        if status_callback:
            status_callback(local_path.name, "importing", None, None)

        # Import to Photos (with albums if available)
        try:
            new_uuid = self.swift_bridge.import_video(
                video_path=local_path,
                album_names=albums if albums else None,
            )
            if not new_uuid:
                return UnifiedImportResult(
                    success=False,
                    item_id=item_id,
                    source="aws",
                    original_filename=original_filename,
                    converted_filename=local_path.name,
                    error_message="Failed to import video to Photos",
                    downloaded=True,
                    download_resumed=download_result.download_resumed,
                    checksum_verified=download_result.checksum_verified,
                    metadata_embedded=metadata_embedded,
                    embed_result=embed_result,
                    metadata_verified=metadata_verified,
                    metadata_mismatch=metadata_mismatch,
                    verification_result=verification_result,
                    verification_skipped=verification_skipped,
                )

        except PhotosAccessError as e:
            return UnifiedImportResult(
                success=False,
                item_id=item_id,
                source="aws",
                original_filename=original_filename,
                converted_filename=local_path.name,
                error_message=str(e),
                downloaded=True,
                download_resumed=download_result.download_resumed,
                checksum_verified=download_result.checksum_verified,
                metadata_embedded=metadata_embedded,
                embed_result=embed_result,
                metadata_verified=metadata_verified,
                metadata_mismatch=metadata_mismatch,
                verification_result=verification_result,
                verification_skipped=verification_skipped,
            )
        except Exception as e:
            # Catch any other exceptions during Photos import
            logger.warning(f"Unexpected error during Photos import: {e}")
            return UnifiedImportResult(
                success=False,
                item_id=item_id,
                source="aws",
                original_filename=original_filename,
                converted_filename=local_path.name,
                error_message=str(e),
                downloaded=True,
                download_resumed=download_result.download_resumed,
                checksum_verified=download_result.checksum_verified,
                metadata_embedded=metadata_embedded,
                embed_result=embed_result,
                metadata_verified=metadata_verified,
                metadata_mismatch=metadata_mismatch,
                verification_result=verification_result,
                verification_skipped=verification_skipped,
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

        # Clean up metadata file if downloaded
        if download_result.metadata_path and download_result.metadata_path.exists():
            try:
                download_result.metadata_path.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(
                    f"Failed to delete metadata file {download_result.metadata_path}: {e}"
                )

        # Handle original video deletion if requested
        original_deleted = False
        original_delete_error: str | None = None

        # Use original_uuid from download result if not provided as argument
        # This allows AWS items to use the UUID extracted from metadata JSON
        effective_original_uuid = original_uuid or download_result.original_uuid

        if delete_original and effective_original_uuid:
            delete_result = self.delete_original_video(effective_original_uuid, original_filename)
            original_deleted = delete_result.success
            if not delete_result.success:
                original_delete_error = delete_result.error_message
                logger.warning(
                    f"Failed to delete original video {effective_original_uuid}: {original_delete_error}"
                )
        elif delete_original and not effective_original_uuid:
            # delete_original requested but no UUID available
            original_delete_error = "Original video UUID not available in metadata"
            logger.warning(f"Cannot delete original video: {original_delete_error}")

        # Notify status: import completed
        if status_callback:
            status_callback(
                local_path.name,
                "imported",
                verification_result,
                albums,
            )

        return UnifiedImportResult(
            success=True,
            item_id=item_id,
            source="aws",
            original_filename=original_filename,
            converted_filename=local_path.name,
            albums=albums,
            downloaded=True,
            download_resumed=download_result.download_resumed,
            checksum_verified=download_result.checksum_verified,
            s3_deleted=s3_deleted,
            original_deleted=original_deleted,
            original_delete_error=original_delete_error,
            original_uuid=effective_original_uuid,
            metadata_embedded=metadata_embedded,
            embed_result=embed_result,
            metadata_verified=metadata_verified,
            metadata_mismatch=metadata_mismatch,
            verification_result=verification_result,
            verification_skipped=verification_skipped,
        )

    def _import_aws_items_parallel(
        self,
        items: list[ImportableItem],
        user_id: str | None,
        max_concurrent: int,
        progress_callback: Callable[..., Any] | None,
        status_callback: Callable[..., None] | None = None,
        delete_original: bool = False,
        original_uuids: dict[str, str] | None = None,
        force_import: bool = False,
    ) -> list[UnifiedImportResult]:
        """Import AWS items in parallel with concurrency limit."""
        results: list[UnifiedImportResult] = []
        original_uuids = original_uuids or {}

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {
                executor.submit(
                    self._import_aws_item,
                    item.item_id,
                    user_id,
                    progress_callback,
                    status_callback,
                    delete_original,
                    original_uuids.get(item.item_id),
                    force_import,
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

    def delete_original_video(self, uuid: str, filename: str) -> DeleteResult:
        """Delete original video from Photos library.

        Moves the video to Photos trash using SwiftBridge.

        Args:
            uuid: UUID of the video in Photos library
            filename: Filename for display in result

        Returns:
            DeleteResult with success status

        Requirements: 5.2, 5.6
        """
        try:
            success = self.swift_bridge.delete_video(uuid)
            return DeleteResult(
                success=success,
                uuid=uuid,
                filename=filename,
                error_message=None if success else "Failed to delete video",
            )
        except PhotosAccessError as e:
            logger.warning(f"Failed to delete original video {uuid}: {e}")
            return DeleteResult(
                success=False,
                uuid=uuid,
                filename=filename,
                error_message=str(e),
            )
        except Exception as e:
            logger.warning(f"Unexpected error deleting original video {uuid}: {e}")
            return DeleteResult(
                success=False,
                uuid=uuid,
                filename=filename,
                error_message=str(e),
            )
