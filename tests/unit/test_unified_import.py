"""Unit tests for UnifiedImportService.

Tests the unified import service for AWS imports.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vco.models.types import (
    ImportableItem,
    UnifiedBatchResult,
    UnifiedClearResult,
    UnifiedListResult,
)
from vco.services.aws_import import AwsDownloadResult, AwsImportService
from vco.services.unified_import import UnifiedImportService


class TestUnifiedImportServiceInit:
    """Tests for UnifiedImportService initialization."""

    def test_init_with_defaults(self):
        """Test initialization with default services."""
        service = UnifiedImportService()
        assert service.aws_service is None
        assert service.swift_bridge is not None

    def test_init_with_custom_services(self):
        """Test initialization with custom services."""
        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        assert service.aws_service is aws_service
        assert service.swift_bridge is swift_bridge


class TestListAllImportable:
    """Tests for list_all_importable method."""

    def test_list_aws_items(self):
        """Test listing AWS items."""
        aws_service = MagicMock(spec=AwsImportService)
        aws_item = ImportableItem(
            item_id="task123:file456",
            source="aws",
            original_filename="video.mov",
            converted_filename="video_h265.mp4",
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            ssim_score=0.95,
            task_id="task123",
            file_id="file456",
        )
        aws_service.list_completed_files.return_value = [aws_item]

        service = UnifiedImportService(aws_service=aws_service)
        result = service.list_all_importable()

        assert isinstance(result, UnifiedListResult)
        assert len(result.local_items) == 0
        assert len(result.aws_items) == 1
        assert result.aws_available is True
        assert result.total_count == 1

    def test_list_aws_unavailable_fallback(self):
        """Test fallback when AWS is unavailable (Property 13)."""
        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.side_effect = RuntimeError("AWS unavailable")

        service = UnifiedImportService(aws_service=aws_service)
        result = service.list_all_importable()

        assert len(result.local_items) == 0
        assert len(result.aws_items) == 0
        assert result.aws_available is False
        assert result.aws_error == "AWS unavailable"

    def test_list_no_aws_service(self):
        """Test listing when AWS service is not configured."""
        service = UnifiedImportService()
        result = service.list_all_importable()

        assert len(result.local_items) == 0
        assert len(result.aws_items) == 0
        assert result.aws_available is True
        assert result.total_count == 0


class TestImportItem:
    """Tests for import_item method."""

    def test_import_aws_item(self):
        """Test importing an AWS item."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"

        # Mock cleanup_file to return success
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item("task123:file456")

        assert result.success is True
        assert result.source == "aws"
        assert result.downloaded is True
        assert result.checksum_verified is True
        assert result.s3_deleted is True
        # Verify cleanup_file was called with action="downloaded"
        aws_service.cleanup_file.assert_called_once_with(
            task_id="task123",
            file_id="file456",
            action="downloaded",
            user_id=None,
        )

    def test_import_aws_item_no_service(self):
        """Test importing AWS item when service not configured."""
        service = UnifiedImportService()
        result = service.import_item("task123:file456")

        assert result.success is False
        assert result.source == "aws"
        assert "not configured" in result.error_message

    def test_import_aws_item_download_fails(self):
        """Test importing AWS item when download fails."""
        aws_service = MagicMock(spec=AwsImportService)
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=False,
            task_id="task123",
            file_id="file456",
            error_message="Download failed",
        )

        service = UnifiedImportService(aws_service=aws_service)
        result = service.import_item("task123:file456")

        assert result.success is False
        assert result.downloaded is False
        assert result.error_message == "Download failed"

    def test_import_aws_item_photos_import_fails_no_cleanup(self):
        """Test that cleanup API is NOT called when Photos import fails.

        This is a critical test: when Photos import fails, the S3 file
        should NOT be deleted so the user can retry the import.
        """
        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        # Download succeeds
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )

        # Photos import fails with an exception
        # Use Exception directly to avoid module reload issues in test suite
        swift_bridge.import_video.side_effect = Exception(
            "PhotosAccessError: Failed to import video: Invalid photo id: 46356852-4789-4948-8325-571A950227CA"
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        result = service.import_item("task123:file456")

        # Verify import failed
        assert result.success is False
        assert result.downloaded is True
        assert "Invalid photo id" in result.error_message

        # CRITICAL: cleanup_file should NOT be called when Photos import fails
        aws_service.cleanup_file.assert_not_called()

    def test_import_aws_item_photos_import_returns_empty_no_cleanup(self):
        """Test that cleanup API is NOT called when Photos import returns empty UUID."""
        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        # Download succeeds
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )

        # Photos import returns empty UUID (failure)
        swift_bridge.import_video.return_value = ""

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        result = service.import_item("task123:file456")

        # Verify import failed
        assert result.success is False
        assert result.downloaded is True
        assert "Failed to import video to Photos" in result.error_message

        # CRITICAL: cleanup_file should NOT be called when Photos import fails
        aws_service.cleanup_file.assert_not_called()

    def test_import_non_aws_item_returns_error(self):
        """Test that non-AWS item ID returns error."""
        service = UnifiedImportService()
        result = service.import_item("local_review_id")

        assert result.success is False
        assert result.source == "local"
        assert "not supported" in result.error_message


class TestImportAll:
    """Tests for import_all method."""

    def test_import_all_aws_items(self):
        """Test batch import with AWS items."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        aws_item = ImportableItem(
            item_id="task123:file456",
            source="aws",
            original_filename="video.mov",
            converted_filename="video_h265.mp4",
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            ssim_score=0.95,
            task_id="task123",
            file_id="file456",
        )
        aws_service.list_completed_files.return_value = [aws_item]
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="DOWNLOADED",
            s3_deleted=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_all()

        assert isinstance(result, UnifiedBatchResult)
        assert result.aws_total == 1
        assert result.aws_successful == 1
        assert result.aws_failed == 0
        assert result.total == 1
        assert result.successful == 1

    def test_import_all_empty(self):
        """Test batch import with no items."""
        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.return_value = []

        service = UnifiedImportService(aws_service=aws_service)
        result = service.import_all()

        assert result.aws_total == 0
        assert result.total == 0


class TestRemoveItem:
    """Tests for remove_item method."""

    def test_remove_aws_item(self):
        """Test removing an AWS item."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="REMOVED",
            s3_deleted=True,
        )

        service = UnifiedImportService(aws_service=aws_service)
        result = service.remove_item("task123:file456")

        assert result.success is True
        assert result.source == "aws"
        assert result.s3_deleted is True
        # Verify cleanup_file was called with action="removed"
        aws_service.cleanup_file.assert_called_once_with(
            task_id="task123",
            file_id="file456",
            action="removed",
            user_id=None,
        )

    def test_remove_non_aws_item_returns_error(self):
        """Test that non-AWS item ID returns error."""
        service = UnifiedImportService()
        result = service.remove_item("local_review_id")

        assert result.success is False
        assert result.source == "local"
        assert "not supported" in result.error_message


class TestClearAllQueues:
    """Tests for clear_all_queues method."""

    def test_clear_all_queues_aws_items(self):
        """Test clearing all queues with AWS items."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        aws_item = ImportableItem(
            item_id="task123:file456",
            source="aws",
            original_filename="video.mov",
            converted_filename="video_h265.mp4",
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            ssim_score=0.95,
            task_id="task123",
            file_id="file456",
        )
        aws_service.list_completed_files.return_value = [aws_item]
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="REMOVED",
            s3_deleted=True,
        )

        service = UnifiedImportService(aws_service=aws_service)
        result = service.clear_all_queues()

        assert isinstance(result, UnifiedClearResult)
        assert result.success is True
        assert result.aws_items_removed == 1
        assert result.aws_files_deleted == 1

    def test_clear_all_queues_empty(self):
        """Test clearing empty queues."""
        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.return_value = []

        service = UnifiedImportService(aws_service=aws_service)
        result = service.clear_all_queues()

        assert result.success is True
        assert result.aws_items_removed == 0


class TestItemIdParsing:
    """Tests for item ID parsing."""

    def test_is_aws_item_with_colon(self):
        """Test AWS item detection with colon."""
        service = UnifiedImportService()
        assert service._is_aws_item("task123:file456") is True

    def test_is_aws_item_without_colon(self):
        """Test local item detection without colon."""
        service = UnifiedImportService()
        assert service._is_aws_item("review123") is False

    def test_parse_aws_item_id_valid(self):
        """Test parsing valid AWS item ID."""
        service = UnifiedImportService()
        task_id, file_id = service._parse_aws_item_id("task123:file456")
        assert task_id == "task123"
        assert file_id == "file456"

    def test_parse_aws_item_id_invalid(self):
        """Test parsing invalid AWS item ID."""
        service = UnifiedImportService()
        with pytest.raises(ValueError):
            service._parse_aws_item_id("invalid")


class TestImportableItemModel:
    """Tests for ImportableItem data model."""

    def test_display_id_aws(self):
        """Test display_id for AWS item."""
        item = ImportableItem(
            item_id="task123:file456",
            source="aws",
            original_filename="video.mov",
            converted_filename="video_h265.mp4",
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            ssim_score=0.95,
            task_id="task123",
            file_id="file456",
        )
        assert item.display_id == "task123:file456"


class TestUnifiedListResult:
    """Tests for UnifiedListResult data model."""

    def test_total_count(self):
        """Test total_count property."""
        result = UnifiedListResult(
            local_items=[],
            aws_items=[MagicMock()],
        )
        assert result.total_count == 1

    def test_all_items(self):
        """Test all_items property."""
        aws = [MagicMock()]
        result = UnifiedListResult(local_items=[], aws_items=aws)
        assert result.all_items == aws


class TestUnifiedBatchResult:
    """Tests for UnifiedBatchResult data model."""

    def test_totals(self):
        """Test total properties."""
        result = UnifiedBatchResult(
            local_total=0,
            local_successful=0,
            local_failed=0,
            aws_total=2,
            aws_successful=1,
            aws_failed=1,
        )
        assert result.total == 2
        assert result.successful == 1
        assert result.failed == 1


class TestDeleteOriginalVideo:
    """Tests for delete_original_video method.

    Requirements: 5.2, 5.6, 5.7
    """

    def test_delete_original_video_success(self):
        """Test successful deletion of original video."""
        swift_bridge = MagicMock()
        swift_bridge.delete_video.return_value = True

        service = UnifiedImportService(swift_bridge=swift_bridge)
        result = service.delete_original_video(
            uuid="test-uuid-123",
            filename="video.mov",
        )

        assert result.success is True
        assert result.uuid == "test-uuid-123"
        assert result.filename == "video.mov"
        assert result.error_message is None
        swift_bridge.delete_video.assert_called_once_with("test-uuid-123")

    def test_delete_original_video_not_found(self):
        """Test deletion when video UUID does not exist."""
        from vco.photos.manager import PhotosAccessError

        swift_bridge = MagicMock()
        swift_bridge.delete_video.side_effect = PhotosAccessError("Video not found: test-uuid")

        service = UnifiedImportService(swift_bridge=swift_bridge)
        result = service.delete_original_video(
            uuid="test-uuid",
            filename="video.mov",
        )

        assert result.success is False
        assert result.uuid == "test-uuid"
        assert result.filename == "video.mov"
        assert "Video not found" in result.error_message

    def test_delete_original_video_permission_error(self):
        """Test deletion when permission is denied."""
        from vco.photos.manager import PhotosAccessError

        swift_bridge = MagicMock()
        swift_bridge.delete_video.side_effect = PhotosAccessError(
            "Permission denied: Cannot delete video"
        )

        service = UnifiedImportService(swift_bridge=swift_bridge)
        result = service.delete_original_video(
            uuid="test-uuid",
            filename="video.mov",
        )

        assert result.success is False
        assert result.uuid == "test-uuid"
        assert "Permission denied" in result.error_message

    def test_delete_original_video_returns_false(self):
        """Test deletion when SwiftBridge returns False."""
        swift_bridge = MagicMock()
        swift_bridge.delete_video.return_value = False

        service = UnifiedImportService(swift_bridge=swift_bridge)
        result = service.delete_original_video(
            uuid="test-uuid",
            filename="video.mov",
        )

        assert result.success is False
        assert result.uuid == "test-uuid"
        assert result.error_message == "Failed to delete video"

    def test_delete_original_video_unexpected_error(self):
        """Test deletion with unexpected error."""
        swift_bridge = MagicMock()
        swift_bridge.delete_video.side_effect = RuntimeError("Unexpected error")

        service = UnifiedImportService(swift_bridge=swift_bridge)
        result = service.delete_original_video(
            uuid="test-uuid",
            filename="video.mov",
        )

        assert result.success is False
        assert result.uuid == "test-uuid"
        assert "Unexpected error" in result.error_message


class TestImportItemWithDeleteOriginal:
    """Tests for import_item with delete_original parameter.

    Requirements: 5.1, 5.2, 5.4, 5.7, 5.10
    """

    def test_import_with_delete_original_success(self):
        """Test import with successful original deletion (--delete-original flag)."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        swift_bridge.delete_video.return_value = True

        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(
                "task123:file456",
                delete_original=True,
                original_uuid="original-uuid-789",
            )

        assert result.success is True
        assert result.original_deleted is True
        assert result.original_delete_error is None
        assert result.original_uuid == "original-uuid-789"
        swift_bridge.delete_video.assert_called_once_with("original-uuid-789")

    def test_import_with_delete_original_failure_still_succeeds(self):
        """Test that import succeeds even if original deletion fails (Requirement 5.7)."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        # Original deletion fails
        swift_bridge.delete_video.return_value = False

        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(
                "task123:file456",
                delete_original=True,
                original_uuid="original-uuid-789",
            )

        # Import should still succeed
        assert result.success is True
        # But original deletion failed
        assert result.original_deleted is False
        assert result.original_delete_error == "Failed to delete video"

    def test_import_without_delete_original_flag(self):
        """Test import without --delete-original flag (no deletion)."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"

        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(
                "task123:file456",
                delete_original=False,
                original_uuid="original-uuid-789",
            )

        assert result.success is True
        assert result.original_deleted is False
        assert result.original_delete_error is None
        # delete_video should NOT be called
        swift_bridge.delete_video.assert_not_called()

    def test_import_with_delete_original_no_uuid(self):
        """Test import with delete_original=True but no original_uuid provided."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"

        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(
                "task123:file456",
                delete_original=True,
                original_uuid=None,  # No UUID provided
            )

        assert result.success is True
        # No deletion attempted without UUID
        assert result.original_deleted is False
        swift_bridge.delete_video.assert_not_called()


class TestImportAllWithDeleteOriginal:
    """Tests for import_all with delete_original parameter.

    Requirements: 5.5, 5.9
    """

    def test_import_all_with_delete_original(self):
        """Test batch import with delete_original flag."""
        from vco.services.aws_import import CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        aws_item = ImportableItem(
            item_id="task123:file456",
            source="aws",
            original_filename="video.mov",
            converted_filename="video_h265.mp4",
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            ssim_score=0.95,
            task_id="task123",
            file_id="file456",
        )
        aws_service.list_completed_files.return_value = [aws_item]
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="DOWNLOADED",
            s3_deleted=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        swift_bridge.delete_video.return_value = True

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        original_uuids = {"task123:file456": "original-uuid-789"}

        with patch.object(Path, "unlink"):
            result = service.import_all(
                delete_original=True,
                original_uuids=original_uuids,
            )

        assert result.aws_successful == 1
        assert len(result.results) == 1
        assert result.results[0].original_deleted is True
        swift_bridge.delete_video.assert_called_once_with("original-uuid-789")
