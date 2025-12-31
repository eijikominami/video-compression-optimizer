"""Unit tests for ImportService."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from vco.services.import_service import BatchImportResult, ImportResult, ImportService
from vco.services.review import ReviewItem


class TestImportResult:
    """Tests for ImportResult dataclass."""

    def test_import_result_success(self):
        """Test successful import result."""
        result = ImportResult(
            success=True,
            review_id="rev_123",
            original_filename="original.mov",
            converted_filename="converted.mov",
            albums=["Album1", "Album2"],
        )
        assert result.success is True
        assert result.review_id == "rev_123"
        assert result.albums == ["Album1", "Album2"]
        assert result.error_message is None

    def test_import_result_failure(self):
        """Test failed import result."""
        result = ImportResult(
            success=False,
            review_id="rev_456",
            original_filename="original.mov",
            converted_filename="converted.mov",
            error_message="Import failed",
        )
        assert result.success is False
        assert result.error_message == "Import failed"


class TestBatchImportResult:
    """Tests for BatchImportResult dataclass."""

    def test_batch_import_result(self):
        """Test batch import result."""
        results = [
            ImportResult(
                success=True,
                review_id="rev_1",
                original_filename="a.mov",
                converted_filename="a_converted.mov",
            ),
            ImportResult(
                success=False,
                review_id="rev_2",
                original_filename="b.mov",
                converted_filename="b_converted.mov",
                error_message="Failed",
            ),
        ]
        batch = BatchImportResult(total=2, successful=1, failed=1, results=results)
        assert batch.total == 2
        assert batch.successful == 1
        assert batch.failed == 1
        assert len(batch.results) == 2


class TestImportServiceListPending:
    """Tests for ImportService.list_pending()."""

    def test_list_pending_returns_pending_items(self):
        """Test that list_pending returns pending review items."""
        mock_review_service = MagicMock()
        mock_item = MagicMock(spec=ReviewItem)
        mock_review_service.get_pending_reviews.return_value = [mock_item]

        service = ImportService(review_service=mock_review_service)
        result = service.list_pending()

        assert len(result) == 1
        mock_review_service.get_pending_reviews.assert_called_once()

    def test_list_pending_empty(self):
        """Test list_pending with no pending items."""
        mock_review_service = MagicMock()
        mock_review_service.get_pending_reviews.return_value = []

        service = ImportService(review_service=mock_review_service)
        result = service.list_pending()

        assert result == []


class TestImportServiceImportSingle:
    """Tests for ImportService.import_single()."""

    def test_import_single_not_found(self):
        """Test import_single with non-existent review ID."""
        mock_review_service = MagicMock()
        mock_review_service.get_review_by_id.return_value = None

        service = ImportService(review_service=mock_review_service)
        result = service.import_single("nonexistent_id")

        assert result.success is False
        assert "not found" in result.error_message

    def test_import_single_not_pending(self):
        """Test import_single with already processed item."""
        mock_review_service = MagicMock()
        mock_item = MagicMock()
        mock_item.status = "imported"
        mock_item.original_path = Path("/original.mov")
        mock_item.converted_path = Path("/converted.mov")
        mock_review_service.get_review_by_id.return_value = mock_item

        service = ImportService(review_service=mock_review_service)
        result = service.import_single("rev_123")

        assert result.success is False
        assert "not pending" in result.error_message

    def test_import_single_converted_file_missing(self, tmp_path):
        """Test import_single when converted file doesn't exist."""
        mock_review_service = MagicMock()
        mock_item = MagicMock()
        mock_item.status = "pending_review"
        mock_item.original_path = Path("/original.mov")
        mock_item.converted_path = tmp_path / "nonexistent.mov"
        mock_review_service.get_review_by_id.return_value = mock_item

        service = ImportService(review_service=mock_review_service)
        result = service.import_single("rev_123")

        assert result.success is False
        assert "not found" in result.error_message

    def test_import_single_success(self, tmp_path):
        """Test successful import_single."""
        # Create a temporary converted file
        converted_file = tmp_path / "converted.mov"
        converted_file.write_text("dummy video content")

        mock_review_service = MagicMock()
        mock_item = MagicMock()
        mock_item.id = "rev_123"
        mock_item.status = "pending_review"
        mock_item.original_path = Path("/original.mov")
        mock_item.converted_path = converted_file
        mock_item.metadata = {"albums": ["Album1", "Album2"]}
        mock_review_service.get_review_by_id.return_value = mock_item

        mock_photos_manager = MagicMock()
        mock_photos_manager.import_video.return_value = "new_uuid_123"

        service = ImportService(
            review_service=mock_review_service, photos_manager=mock_photos_manager
        )
        result = service.import_single("rev_123")

        assert result.success is True
        assert result.albums == ["Album1", "Album2"]
        mock_photos_manager.import_video.assert_called_once_with(video_path=converted_file)
        mock_photos_manager.add_to_albums.assert_called_once_with(
            "new_uuid_123", ["Album1", "Album2"]
        )

    def test_import_single_updates_status_to_imported(self, tmp_path):
        """Test that import_single updates status to 'imported' on success.

        Property 2: Status update on successful import
        Validates: Requirements 2.4
        """
        converted_file = tmp_path / "converted.mov"
        converted_file.write_text("dummy")

        mock_review_service = MagicMock()
        mock_item = MagicMock()
        mock_item.id = "rev_123"
        mock_item.status = "pending_review"
        mock_item.original_path = Path("/original.mov")
        mock_item.converted_path = converted_file
        mock_item.metadata = {"albums": []}
        mock_review_service.get_review_by_id.return_value = mock_item

        # Mock queue for status update
        mock_queue = MagicMock()
        mock_queue.items = [mock_item]
        mock_review_service.load_queue.return_value = mock_queue
        mock_review_service.save_queue.return_value = True

        mock_photos_manager = MagicMock()
        mock_photos_manager.import_video.return_value = "new_uuid"

        service = ImportService(
            review_service=mock_review_service, photos_manager=mock_photos_manager
        )
        result = service.import_single("rev_123")

        assert result.success is True
        # Verify status was updated
        mock_review_service.load_queue.assert_called()
        mock_review_service.save_queue.assert_called()


class TestImportServiceRemoveItem:
    """Tests for ImportService.remove_item()."""

    def test_remove_item_success(self):
        """Test removing an existing item from the queue."""
        from pathlib import Path

        mock_item = MagicMock()
        mock_item.id = "rev_123"
        mock_item.converted_path = Path("/tmp/test_video.mp4")

        mock_queue = MagicMock()
        mock_queue.items = [mock_item]

        mock_review_service = MagicMock()
        mock_review_service.load_queue.return_value = mock_queue
        mock_review_service.save_queue.return_value = True

        service = ImportService(review_service=mock_review_service)

        # Mock file deletion to avoid actual file operations
        with patch.object(service, "_delete_item_files") as mock_delete:
            from vco.services.import_service import FileDeleteResult

            mock_delete.return_value = FileDeleteResult(video_deleted=True, metadata_deleted=True)

            result = service.remove_item("rev_123")

        assert result.success is True
        assert result.review_id == "rev_123"
        assert result.queue_removed is True
        assert result.files_deleted.video_deleted is True
        assert result.files_deleted.metadata_deleted is True
        assert len(mock_queue.items) == 0
        mock_review_service.save_queue.assert_called_once()

    def test_remove_item_not_found(self):
        """Test removing a non-existent item returns failure result."""
        mock_item = MagicMock()
        mock_item.id = "rev_123"

        mock_queue = MagicMock()
        mock_queue.items = [mock_item]

        mock_review_service = MagicMock()
        mock_review_service.load_queue.return_value = mock_queue

        service = ImportService(review_service=mock_review_service)
        result = service.remove_item("nonexistent_id")

        assert result.success is False
        assert result.review_id == "nonexistent_id"
        assert result.queue_removed is False
        assert result.error_message == "Item not found"
        # save_queue should not be called if item not found
        mock_review_service.save_queue.assert_not_called()

    def test_remove_item_preserves_other_items(self):
        """Test that removing one item preserves other items."""
        from pathlib import Path

        mock_item1 = MagicMock()
        mock_item1.id = "rev_1"
        mock_item1.converted_path = Path("/tmp/video1.mp4")
        mock_item2 = MagicMock()
        mock_item2.id = "rev_2"
        mock_item2.converted_path = Path("/tmp/video2.mp4")
        mock_item3 = MagicMock()
        mock_item3.id = "rev_3"
        mock_item3.converted_path = Path("/tmp/video3.mp4")

        mock_queue = MagicMock()
        mock_queue.items = [mock_item1, mock_item2, mock_item3]

        mock_review_service = MagicMock()
        mock_review_service.load_queue.return_value = mock_queue
        mock_review_service.save_queue.return_value = True

        service = ImportService(review_service=mock_review_service)

        # Mock file deletion to avoid actual file operations
        with patch.object(service, "_delete_item_files") as mock_delete:
            from vco.services.import_service import FileDeleteResult

            mock_delete.return_value = FileDeleteResult(video_deleted=True, metadata_deleted=True)

            result = service.remove_item("rev_2")

        assert result.success is True
        assert result.review_id == "rev_2"
        assert len(mock_queue.items) == 2
        remaining_ids = [item.id for item in mock_queue.items]
        assert "rev_1" in remaining_ids
        assert "rev_3" in remaining_ids
        assert "rev_2" not in remaining_ids


class TestImportServiceClearQueue:
    """Tests for ImportService.clear_queue()."""

    def test_clear_queue_returns_count(self):
        """Test that clear_queue returns the number of removed items."""
        from pathlib import Path

        mock_item1 = MagicMock()
        mock_item1.converted_path = Path("/tmp/video1.mp4")
        mock_item2 = MagicMock()
        mock_item2.converted_path = Path("/tmp/video2.mp4")
        mock_item3 = MagicMock()
        mock_item3.converted_path = Path("/tmp/video3.mp4")

        mock_queue = MagicMock()
        mock_queue.items = [mock_item1, mock_item2, mock_item3]

        mock_review_service = MagicMock()
        mock_review_service.load_queue.return_value = mock_queue
        mock_review_service.save_queue.return_value = True

        service = ImportService(review_service=mock_review_service)

        # Mock file deletion to avoid actual file operations
        with patch.object(service, "_delete_item_files") as mock_delete:
            from vco.services.import_service import FileDeleteResult

            mock_delete.return_value = FileDeleteResult(video_deleted=True, metadata_deleted=True)

            result = service.clear_queue()

        assert result.success is True
        assert result.items_removed == 3
        assert result.files_deleted == 3
        assert result.files_failed == 0
        assert len(mock_queue.items) == 0
        mock_review_service.save_queue.assert_called_once()

    def test_clear_queue_empty(self):
        """Test clearing an already empty queue."""
        mock_queue = MagicMock()
        mock_queue.items = []

        mock_review_service = MagicMock()
        mock_review_service.load_queue.return_value = mock_queue

        service = ImportService(review_service=mock_review_service)
        result = service.clear_queue()

        assert result.success is True
        assert result.items_removed == 0
        assert result.files_deleted == 0
        assert result.files_failed == 0


class TestImportServiceDeleteItemFiles:
    """Tests for ImportService._delete_item_files()."""

    def test_delete_item_files_success(self):
        """Test successful deletion of both video and metadata files."""
        import tempfile
        from pathlib import Path

        service = ImportService()

        # Create temporary files
        with tempfile.TemporaryDirectory() as temp_dir:
            video_path = Path(temp_dir) / "test_video.mp4"
            metadata_path = Path(temp_dir) / "test_video.json"

            # Create the files
            video_path.write_text("fake video content")
            metadata_path.write_text("fake metadata content")

            # Create mock item
            mock_item = MagicMock()
            mock_item.converted_path = video_path

            # Test deletion
            result = service._delete_item_files(mock_item)

            assert result.video_deleted is True
            assert result.metadata_deleted is True
            assert result.video_error is None
            assert result.metadata_error is None
            assert not video_path.exists()
            assert not metadata_path.exists()

    def test_delete_item_files_missing_files(self):
        """Test deletion when files don't exist (should not be an error)."""
        from pathlib import Path

        service = ImportService()

        # Create mock item with non-existent paths
        mock_item = MagicMock()
        mock_item.converted_path = Path("/nonexistent/video.mp4")

        # Test deletion
        result = service._delete_item_files(mock_item)

        # Should succeed because missing_ok=True
        assert result.video_deleted is True
        assert result.metadata_deleted is True
        assert result.video_error is None
        assert result.metadata_error is None

    def test_delete_item_files_with_errors(self):
        """Test deletion with permission errors."""
        from pathlib import Path
        from unittest.mock import patch

        service = ImportService()

        mock_item = MagicMock()
        mock_item.converted_path = Path("/tmp/test_video.mp4")

        # Mock unlink to raise permission error
        with patch.object(Path, "unlink", side_effect=PermissionError("Access denied")):
            result = service._delete_item_files(mock_item)

            assert result.video_deleted is False
            assert result.metadata_deleted is False
            assert "Access denied" in result.video_error
            assert "Access denied" in result.metadata_error


class TestImportServiceImportAll:
    """Tests for ImportService.import_all()."""

    def test_import_all_empty(self):
        """Test import_all with no pending items."""
        mock_review_service = MagicMock()
        mock_review_service.get_pending_reviews.return_value = []

        service = ImportService(review_service=mock_review_service)
        result = service.import_all()

        assert result.total == 0
        assert result.successful == 0
        assert result.failed == 0

    def test_import_all_processes_all_items(self, tmp_path):
        """Test that import_all processes all pending items.

        Property 3: Batch import processes all items
        Validates: Requirements 3.1
        """
        # Create temporary files
        file1 = tmp_path / "converted1.mov"
        file1.write_text("dummy1")
        file2 = tmp_path / "converted2.mov"
        file2.write_text("dummy2")

        mock_item1 = MagicMock()
        mock_item1.id = "rev_1"
        mock_item1.status = "pending_review"
        mock_item1.original_path = Path("/original1.mov")
        mock_item1.converted_path = file1
        mock_item1.metadata = {"albums": []}

        mock_item2 = MagicMock()
        mock_item2.id = "rev_2"
        mock_item2.status = "pending_review"
        mock_item2.original_path = Path("/original2.mov")
        mock_item2.converted_path = file2
        mock_item2.metadata = {"albums": []}

        mock_review_service = MagicMock()
        mock_review_service.get_pending_reviews.return_value = [mock_item1, mock_item2]
        mock_review_service.get_review_by_id.side_effect = lambda id: (
            mock_item1 if id == "rev_1" else mock_item2
        )

        mock_queue = MagicMock()
        mock_queue.items = [mock_item1, mock_item2]
        mock_review_service.load_queue.return_value = mock_queue
        mock_review_service.save_queue.return_value = True

        mock_photos_manager = MagicMock()
        mock_photos_manager.import_video.return_value = "new_uuid"

        service = ImportService(
            review_service=mock_review_service, photos_manager=mock_photos_manager
        )
        result = service.import_all()

        assert result.total == 2
        assert result.successful == 2
        assert result.failed == 0
        assert len(result.results) == 2

    def test_import_all_continues_on_error(self, tmp_path):
        """Test that import_all continues processing when some imports fail.

        Property 4: Error resilience in batch import
        Validates: Requirements 3.4
        """
        file1 = tmp_path / "converted1.mov"
        file1.write_text("dummy1")
        file2 = tmp_path / "converted2.mov"
        file2.write_text("dummy2")

        mock_item1 = MagicMock()
        mock_item1.id = "rev_1"
        mock_item1.status = "pending_review"
        mock_item1.original_path = Path("/original1.mov")
        mock_item1.converted_path = file1
        mock_item1.metadata = {"albums": []}

        mock_item2 = MagicMock()
        mock_item2.id = "rev_2"
        mock_item2.status = "pending_review"
        mock_item2.original_path = Path("/original2.mov")
        mock_item2.converted_path = file2
        mock_item2.metadata = {"albums": []}

        mock_review_service = MagicMock()
        mock_review_service.get_pending_reviews.return_value = [mock_item1, mock_item2]
        mock_review_service.get_review_by_id.side_effect = lambda id: (
            mock_item1 if id == "rev_1" else mock_item2
        )

        mock_queue = MagicMock()
        mock_queue.items = [mock_item1, mock_item2]
        mock_review_service.load_queue.return_value = mock_queue
        mock_review_service.save_queue.return_value = True

        mock_photos_manager = MagicMock()
        # First import fails, second succeeds
        mock_photos_manager.import_video.side_effect = [None, "new_uuid"]

        service = ImportService(
            review_service=mock_review_service, photos_manager=mock_photos_manager
        )
        result = service.import_all()

        # Both items should be processed
        assert result.total == 2
        assert result.successful == 1
        assert result.failed == 1
        assert len(result.results) == 2
