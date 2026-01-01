"""Unit tests for UnifiedImportService.

Tests the unified import service that integrates local and AWS imports.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from vco.models.types import (
    ImportableItem,
    UnifiedBatchResult,
    UnifiedClearResult,
    UnifiedImportResult,
    UnifiedListResult,
    UnifiedRemoveResult,
)
from vco.services.aws_import import AwsDownloadResult, AwsImportService
from vco.services.import_service import (
    ClearResult as LocalClearResult,
)
from vco.services.import_service import (
    FileDeleteResult,
    ImportResult,
    ImportService,
    RemoveResult,
)
from vco.services.review import ReviewItem
from vco.services.unified_import import UnifiedImportService


class TestUnifiedImportServiceInit:
    """Tests for UnifiedImportService initialization."""

    def test_init_with_defaults(self):
        """Test initialization with default services."""
        service = UnifiedImportService()
        assert service.local_service is not None
        assert service.aws_service is None
        assert service.photos_manager is not None

    def test_init_with_custom_services(self):
        """Test initialization with custom services."""
        local_service = MagicMock(spec=ImportService)
        aws_service = MagicMock(spec=AwsImportService)
        photos_manager = MagicMock()

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
            photos_manager=photos_manager,
        )

        assert service.local_service is local_service
        assert service.aws_service is aws_service
        assert service.photos_manager is photos_manager


class TestListAllImportable:
    """Tests for list_all_importable method."""

    def test_list_local_items_only(self):
        """Test listing when only local items exist."""
        local_service = MagicMock(spec=ImportService)
        review_item = MagicMock(spec=ReviewItem)
        review_item.id = "review123"
        review_item.original_path = Path("/original/video.mov")
        review_item.converted_path = Path("/converted/video_h265.mp4")
        review_item.quality_result = {
            "original_size": 1000000,
            "converted_size": 500000,
            "compression_ratio": 2.0,
            "ssim_score": 0.95,
        }
        review_item.metadata = {
            "albums": ["Album1"],
            "capture_date": "2024-01-01T12:00:00",
        }
        local_service.list_pending.return_value = [review_item]

        service = UnifiedImportService(local_service=local_service)
        result = service.list_all_importable()

        assert isinstance(result, UnifiedListResult)
        assert len(result.local_items) == 1
        assert len(result.aws_items) == 0
        assert result.aws_available is True
        assert result.total_count == 1

    def test_list_aws_items_only(self):
        """Test listing when only AWS items exist."""
        local_service = MagicMock(spec=ImportService)
        local_service.list_pending.return_value = []

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

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
        )
        result = service.list_all_importable()

        assert len(result.local_items) == 0
        assert len(result.aws_items) == 1
        assert result.aws_available is True
        assert result.total_count == 1

    def test_list_both_sources(self):
        """Test listing items from both local and AWS."""
        local_service = MagicMock(spec=ImportService)
        review_item = MagicMock(spec=ReviewItem)
        review_item.id = "review123"
        review_item.original_path = Path("/original/video.mov")
        review_item.converted_path = Path("/converted/video_h265.mp4")
        review_item.quality_result = {
            "original_size": 1000000,
            "converted_size": 500000,
            "compression_ratio": 2.0,
            "ssim_score": 0.95,
        }
        review_item.metadata = {"albums": [], "capture_date": None}
        local_service.list_pending.return_value = [review_item]

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

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
        )
        result = service.list_all_importable()

        assert len(result.local_items) == 1
        assert len(result.aws_items) == 1
        assert result.total_count == 2
        assert result.all_items == result.local_items + result.aws_items

    def test_list_aws_unavailable_fallback(self):
        """Test fallback when AWS is unavailable (Property 13)."""
        local_service = MagicMock(spec=ImportService)
        review_item = MagicMock(spec=ReviewItem)
        review_item.id = "review123"
        review_item.original_path = Path("/original/video.mov")
        review_item.converted_path = Path("/converted/video_h265.mp4")
        review_item.quality_result = {}
        review_item.metadata = {"albums": [], "capture_date": None}
        local_service.list_pending.return_value = [review_item]

        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.side_effect = RuntimeError("AWS unavailable")

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
        )
        result = service.list_all_importable()

        assert len(result.local_items) == 1
        assert len(result.aws_items) == 0
        assert result.aws_available is False
        assert result.aws_error == "AWS unavailable"


class TestImportItem:
    """Tests for import_item method."""

    def test_import_local_item(self):
        """Test importing a local item."""
        local_service = MagicMock(spec=ImportService)
        local_service.import_single.return_value = ImportResult(
            success=True,
            review_id="review123",
            original_filename="video.mov",
            converted_filename="video_h265.mp4",
            albums=["Album1"],
        )

        service = UnifiedImportService(local_service=local_service)
        result = service.import_item("review123")

        assert isinstance(result, UnifiedImportResult)
        assert result.success is True
        assert result.source == "local"
        assert result.item_id == "review123"
        local_service.import_single.assert_called_once_with("review123")

    def test_import_aws_item(self):
        """Test importing an AWS item."""
        from vco.services.aws_import import CleanupResult

        local_service = MagicMock(spec=ImportService)
        aws_service = MagicMock(spec=AwsImportService)
        photos_manager = MagicMock()

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )
        photos_manager.import_video.return_value = "new-uuid-123"

        # Mock cleanup_file to return success
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id="file456",
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
            photos_manager=photos_manager,
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
        from vco.photos.manager import PhotosAccessError

        aws_service = MagicMock(spec=AwsImportService)
        photos_manager = MagicMock()

        # Download succeeds
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )

        # Photos import fails with PhotosAccessError
        photos_manager.import_video.side_effect = PhotosAccessError(
            "Failed to import video: Invalid photo id: 46356852-4789-4948-8325-571A950227CA"
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            photos_manager=photos_manager,
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
        photos_manager = MagicMock()

        # Download succeeds
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id="task123",
            file_id="file456",
            local_path=Path("/tmp/video_h265.mp4"),
            checksum_verified=True,
        )

        # Photos import returns empty UUID (failure)
        photos_manager.import_video.return_value = ""

        service = UnifiedImportService(
            aws_service=aws_service,
            photos_manager=photos_manager,
        )

        result = service.import_item("task123:file456")

        # Verify import failed
        assert result.success is False
        assert result.downloaded is True
        assert "Failed to import video to Photos" in result.error_message

        # CRITICAL: cleanup_file should NOT be called when Photos import fails
        aws_service.cleanup_file.assert_not_called()


class TestImportAll:
    """Tests for import_all method."""

    def test_import_all_local_only(self):
        """Test batch import with local items only."""
        local_service = MagicMock(spec=ImportService)
        review_item = MagicMock(spec=ReviewItem)
        review_item.id = "review123"
        review_item.original_path = Path("/original/video.mov")
        review_item.converted_path = Path("/converted/video_h265.mp4")
        review_item.quality_result = {}
        review_item.metadata = {"albums": [], "capture_date": None}
        local_service.list_pending.return_value = [review_item]
        local_service.import_single.return_value = ImportResult(
            success=True,
            review_id="review123",
            original_filename="video.mov",
            converted_filename="video_h265.mp4",
        )

        service = UnifiedImportService(local_service=local_service)
        result = service.import_all()

        assert isinstance(result, UnifiedBatchResult)
        assert result.local_total == 1
        assert result.local_successful == 1
        assert result.local_failed == 0
        assert result.aws_total == 0
        assert result.total == 1
        assert result.successful == 1

    def test_import_all_continues_on_failure(self):
        """Test batch import continues when some items fail (Property 7)."""
        local_service = MagicMock(spec=ImportService)
        review_items = []
        for i in range(3):
            item = MagicMock(spec=ReviewItem)
            item.id = f"review{i}"
            item.original_path = Path(f"/original/video{i}.mov")
            item.converted_path = Path(f"/converted/video{i}_h265.mp4")
            item.quality_result = {}
            item.metadata = {"albums": [], "capture_date": None}
            review_items.append(item)

        local_service.list_pending.return_value = review_items
        local_service.import_single.side_effect = [
            ImportResult(
                success=True, review_id="review0", original_filename="", converted_filename=""
            ),
            ImportResult(
                success=False,
                review_id="review1",
                original_filename="",
                converted_filename="",
                error_message="Failed",
            ),
            ImportResult(
                success=True, review_id="review2", original_filename="", converted_filename=""
            ),
        ]

        service = UnifiedImportService(local_service=local_service)
        result = service.import_all()

        assert result.local_total == 3
        assert result.local_successful == 2
        assert result.local_failed == 1
        assert len(result.results) == 3


class TestRemoveItem:
    """Tests for remove_item method."""

    def test_remove_local_item(self):
        """Test removing a local item."""
        local_service = MagicMock(spec=ImportService)
        local_service.remove_item.return_value = RemoveResult(
            success=True,
            review_id="review123",
            queue_removed=True,
            files_deleted=FileDeleteResult(
                video_deleted=True,
                metadata_deleted=True,
            ),
        )

        service = UnifiedImportService(local_service=local_service)
        result = service.remove_item("review123")

        assert isinstance(result, UnifiedRemoveResult)
        assert result.success is True
        assert result.source == "local"
        assert result.file_deleted is True
        assert result.metadata_deleted is True

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


class TestClearAllQueues:
    """Tests for clear_all_queues method."""

    def test_clear_all_queues_local_only(self):
        """Test clearing all queues with local items only."""
        local_service = MagicMock()
        local_service.clear_queue.return_value = LocalClearResult(
            success=True,
            items_removed=5,
            files_deleted=5,
            files_failed=0,
        )
        local_service.list_importable.return_value = []  # No local items to list

        service = UnifiedImportService(local_service=local_service)
        result = service.clear_all_queues()

        assert isinstance(result, UnifiedClearResult)
        assert result.success is True
        assert result.local_items_removed == 5
        assert result.local_files_deleted == 5
        local_service.clear_queue.assert_called_once()


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

    def test_display_id_local(self):
        """Test display_id for local item."""
        item = ImportableItem(
            item_id="review123",
            source="local",
            original_filename="video.mov",
            converted_filename="video_h265.mp4",
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            ssim_score=0.95,
        )
        assert item.display_id == "review123"

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
            local_items=[MagicMock(), MagicMock()],
            aws_items=[MagicMock()],
        )
        assert result.total_count == 3

    def test_all_items(self):
        """Test all_items property."""
        local = [MagicMock()]
        aws = [MagicMock()]
        result = UnifiedListResult(local_items=local, aws_items=aws)
        assert result.all_items == local + aws


class TestUnifiedBatchResult:
    """Tests for UnifiedBatchResult data model."""

    def test_totals(self):
        """Test total properties."""
        result = UnifiedBatchResult(
            local_total=3,
            local_successful=2,
            local_failed=1,
            aws_total=2,
            aws_successful=1,
            aws_failed=1,
        )
        assert result.total == 5
        assert result.successful == 3
        assert result.failed == 2
