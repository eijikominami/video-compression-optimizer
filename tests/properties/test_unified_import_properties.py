"""Property-based tests for UnifiedImportService.

Tests the correctness properties defined in design.md.
"""

from pathlib import Path
from unittest.mock import MagicMock

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from vco.models.types import ImportableItem
from vco.services.aws_import import AwsImportService
from vco.services.import_service import (
    FileDeleteResult,
    ImportResult,
    ImportService,
    RemoveResult,
)
from vco.services.review import ReviewItem
from vco.services.unified_import import UnifiedImportService


def create_mock_review_item(item_id: str) -> MagicMock:
    """Create a mock ReviewItem with given ID."""
    item = MagicMock(spec=ReviewItem)
    item.id = item_id
    item.original_path = Path(f"/original/{item_id}.mov")
    item.converted_path = Path(f"/converted/{item_id}_h265.mp4")
    item.quality_result = {
        "original_size": 1000000,
        "converted_size": 500000,
        "compression_ratio": 2.0,
        "ssim_score": 0.95,
    }
    item.metadata = {"albums": [], "capture_date": None}
    return item


def create_aws_importable_item(task_id: str, file_id: str) -> ImportableItem:
    """Create an AWS ImportableItem."""
    return ImportableItem(
        item_id=f"{task_id}:{file_id}",
        source="aws",
        original_filename=f"{file_id}.mov",
        converted_filename=f"{file_id}_h265.mp4",
        original_size=1000000,
        converted_size=500000,
        compression_ratio=2.0,
        ssim_score=0.95,
        task_id=task_id,
        file_id=file_id,
    )


class TestProperty1UnifiedListContainsAllSources:
    """Property 1: Unified list contains all sources.

    For any set of local ReviewQueue items and AWS completed files,
    the unified list SHALL contain all items from both sources with
    correct source labels.

    Validates: Requirements 1.1
    """

    @given(
        local_count=st.integers(min_value=0, max_value=5),
        aws_count=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=30, suppress_health_check=[HealthCheck.too_slow])
    def test_unified_list_contains_all_items(self, local_count, aws_count):
        """Unified list contains all items from both sources."""
        # Create local items
        local_items = [create_mock_review_item(f"local{i}") for i in range(local_count)]

        # Create AWS items
        aws_items = [create_aws_importable_item(f"task{i}", f"file{i}") for i in range(aws_count)]

        local_service = MagicMock(spec=ImportService)
        local_service.list_pending.return_value = local_items

        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.return_value = aws_items

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
        )
        result = service.list_all_importable()

        # Property: total count equals sum of both sources
        assert result.total_count == local_count + aws_count

        # Property: local items count matches
        assert len(result.local_items) == local_count

        # Property: AWS items count matches
        assert len(result.aws_items) == aws_count

        # Property: all local items have source="local"
        for item in result.local_items:
            assert item.source == "local"

        # Property: all AWS items have source="aws"
        for item in result.aws_items:
            assert item.source == "aws"

    @given(local_count=st.integers(min_value=1, max_value=5))
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_local_item_ids_preserved(self, local_count):
        """Local item IDs are preserved in unified list."""
        local_items = [create_mock_review_item(f"local{i}") for i in range(local_count)]

        local_service = MagicMock(spec=ImportService)
        local_service.list_pending.return_value = local_items

        service = UnifiedImportService(local_service=local_service)
        result = service.list_all_importable()

        local_ids = {item.id for item in local_items}
        result_ids = {item.item_id for item in result.local_items}

        assert local_ids == result_ids


class TestProperty13AwsUnavailabilityFallback:
    """Property 13: AWS unavailability fallback.

    For any list operation when AWS API is unavailable,
    local items SHALL still be displayed with a warning message.

    Validates: Requirements 1.5
    """

    @given(local_count=st.integers(min_value=1, max_value=5))
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_local_items_returned_when_aws_fails(self, local_count):
        """Local items are returned when AWS is unavailable."""
        local_items = [create_mock_review_item(f"local{i}") for i in range(local_count)]

        local_service = MagicMock(spec=ImportService)
        local_service.list_pending.return_value = local_items

        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.side_effect = RuntimeError("AWS unavailable")

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
        )
        result = service.list_all_importable()

        # Property: local items are still returned
        assert len(result.local_items) == local_count

        # Property: AWS unavailable flag is set
        assert result.aws_available is False

        # Property: AWS error message is set
        assert result.aws_error is not None
        assert "unavailable" in result.aws_error.lower()

        # Property: AWS items list is empty
        assert len(result.aws_items) == 0

    @given(
        error_message=st.sampled_from(
            ["Connection timeout", "Access denied", "Service unavailable"]
        )
    )
    @settings(max_examples=10)
    def test_aws_error_message_preserved(self, error_message):
        """AWS error message is preserved in result."""
        local_service = MagicMock(spec=ImportService)
        local_service.list_pending.return_value = []

        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.side_effect = RuntimeError(error_message)

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
        )
        result = service.list_all_importable()

        assert result.aws_error == error_message


class TestProperty7BatchImportProcessesAllItems:
    """Property 7: Batch import processes all items.

    For any batch import operation, all items from both local and AWS
    sources SHALL be processed, and failures SHALL not prevent remaining
    items from being processed.

    Validates: Requirements 4.1, 4.4
    """

    @given(
        success_pattern=st.lists(st.booleans(), min_size=1, max_size=5),
    )
    @settings(max_examples=20)
    def test_all_items_processed_despite_failures(self, success_pattern):
        """All items are processed even when some fail."""
        # Create local items matching the pattern
        local_items = [create_mock_review_item(f"review{i}") for i in range(len(success_pattern))]

        local_service = MagicMock(spec=ImportService)
        local_service.list_pending.return_value = local_items

        # Create import results based on pattern
        import_results = [
            ImportResult(
                success=success,
                review_id=f"review{i}",
                original_filename=f"video{i}.mov",
                converted_filename=f"video{i}_h265.mp4",
                error_message=None if success else "Failed",
            )
            for i, success in enumerate(success_pattern)
        ]
        local_service.import_single.side_effect = import_results

        service = UnifiedImportService(local_service=local_service)
        result = service.import_all()

        # Property: all items were processed
        assert result.local_total == len(success_pattern)
        assert len(result.results) == len(success_pattern)

        # Property: success/failure counts match pattern
        expected_success = sum(success_pattern)
        expected_failed = len(success_pattern) - expected_success
        assert result.local_successful == expected_success
        assert result.local_failed == expected_failed


class TestProperty8BatchSummaryAccuracy:
    """Property 8: Batch summary accuracy.

    For any batch import operation, the summary SHALL accurately report
    local_items_processed, aws_items_processed, successful_imports,
    and failed_imports counts.

    Validates: Requirements 4.3
    """

    @given(
        local_success=st.integers(min_value=0, max_value=5),
        local_failed=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=20)
    def test_batch_summary_counts_accurate(self, local_success, local_failed):
        """Batch summary counts are accurate."""
        total = local_success + local_failed

        # Create local items
        local_items = [create_mock_review_item(f"review{i}") for i in range(total)]

        local_service = MagicMock(spec=ImportService)
        local_service.list_pending.return_value = local_items

        # Create results: first local_success succeed, rest fail
        import_results = []
        for i in range(total):
            success = i < local_success
            import_results.append(
                ImportResult(
                    success=success,
                    review_id=f"review{i}",
                    original_filename=f"video{i}.mov",
                    converted_filename=f"video{i}_h265.mp4",
                    error_message=None if success else "Failed",
                )
            )
        local_service.import_single.side_effect = import_results

        service = UnifiedImportService(local_service=local_service)
        result = service.import_all()

        # Property: counts are accurate
        assert result.local_total == total
        assert result.local_successful == local_success
        assert result.local_failed == local_failed
        assert result.total == total
        assert result.successful == local_success
        assert result.failed == local_failed


class TestProperty10RemoveItemIsolation:
    """Property 10: Remove item isolation.

    For any remove operation with a valid item_id, only the specified
    item SHALL be removed, and all other items SHALL remain unchanged.

    Validates: Requirements 7.1
    """

    @given(
        item_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=15)
    def test_remove_only_affects_specified_item(self, item_id):
        """Remove operation only affects the specified item."""
        local_service = MagicMock(spec=ImportService)
        local_service.remove_item.return_value = RemoveResult(
            success=True,
            review_id=item_id,
            queue_removed=True,
            files_deleted=FileDeleteResult(
                video_deleted=True,
                metadata_deleted=True,
            ),
        )

        service = UnifiedImportService(local_service=local_service)
        result = service.remove_item(item_id)

        # Property: only the specified item was targeted
        local_service.remove_item.assert_called_once_with(item_id)
        assert result.item_id == item_id


class TestProperty12ClearQueueAffectsLocalOnly:
    """Property 12: Clear queue affects local only.

    For any clear operation, all local queue items SHALL be removed,
    and AWS items SHALL remain unaffected.

    Validates: Requirements 7.6
    """

    def test_clear_only_calls_local_service(self):
        """Clear operation only affects local service."""
        local_service = MagicMock(spec=ImportService)
        aws_service = MagicMock(spec=AwsImportService)

        from vco.services.import_service import ClearResult as LocalClearResult

        local_service.clear_queue.return_value = LocalClearResult(
            success=True,
            items_removed=5,
            files_deleted=5,
            files_failed=0,
        )

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
        )
        service.clear_local_queue()

        # Property: local service clear was called
        local_service.clear_queue.assert_called_once()

        # Property: AWS service was not affected
        aws_service.delete_s3_file.assert_not_called()


class TestProperty6DownloadProgressPersistence:
    """Property 6: Download progress persistence.

    For any interrupted download, progress SHALL be saved; on retry,
    download SHALL resume from saved position; on success, progress
    SHALL be cleared.

    Validates: Requirements 8.1, 8.2, 8.4
    """

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        total_bytes=st.integers(min_value=1000, max_value=10000000),
        downloaded_bytes=st.integers(min_value=0, max_value=10000000),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_progress_roundtrip(self, task_id, file_id, total_bytes, downloaded_bytes):
        """Progress can be saved and retrieved."""
        import tempfile

        from vco.services.download_progress import DownloadProgress, DownloadProgressStore

        # Ensure downloaded_bytes <= total_bytes
        downloaded_bytes = min(downloaded_bytes, total_bytes)

        with tempfile.TemporaryDirectory() as tmpdir:
            store = DownloadProgressStore(cache_dir=Path(tmpdir))

            progress = DownloadProgress(
                task_id=task_id,
                file_id=file_id,
                total_bytes=total_bytes,
                downloaded_bytes=downloaded_bytes,
                local_temp_path=f"/tmp/{file_id}.tmp",
                s3_key=f"outputs/{task_id}/{file_id}.mp4",
                checksum="abc123",
            )

            # Save progress
            store.save_progress(progress)

            # Retrieve progress
            retrieved = store.get_progress(task_id, file_id)

            # Property: retrieved progress matches saved
            assert retrieved is not None
            assert retrieved.task_id == task_id
            assert retrieved.file_id == file_id
            assert retrieved.total_bytes == total_bytes
            assert retrieved.downloaded_bytes == downloaded_bytes
            assert retrieved.local_temp_path == f"/tmp/{file_id}.tmp"
            assert retrieved.s3_key == f"outputs/{task_id}/{file_id}.mp4"
            assert retrieved.checksum == "abc123"

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow])
    def test_progress_cleared_on_success(self, task_id, file_id):
        """Progress is cleared after successful download."""
        import tempfile

        from vco.services.download_progress import DownloadProgress, DownloadProgressStore

        with tempfile.TemporaryDirectory() as tmpdir:
            store = DownloadProgressStore(cache_dir=Path(tmpdir))

            # Save progress
            progress = DownloadProgress(
                task_id=task_id,
                file_id=file_id,
                total_bytes=1000,
                downloaded_bytes=1000,  # Complete
                local_temp_path=f"/tmp/{file_id}.tmp",
                s3_key=f"outputs/{task_id}/{file_id}.mp4",
            )
            store.save_progress(progress)

            # Verify saved
            assert store.get_progress(task_id, file_id) is not None

            # Clear progress (simulating successful completion)
            store.clear_progress(task_id, file_id)

            # Property: progress is cleared
            assert store.get_progress(task_id, file_id) is None

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        initial_bytes=st.integers(min_value=100, max_value=500),
        additional_bytes=st.integers(min_value=100, max_value=500),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow])
    def test_progress_resume_from_saved_position(
        self, task_id, file_id, initial_bytes, additional_bytes
    ):
        """Download resumes from saved position."""
        import tempfile

        from vco.services.download_progress import DownloadProgress, DownloadProgressStore

        total_bytes = initial_bytes + additional_bytes + 100

        with tempfile.TemporaryDirectory() as tmpdir:
            store = DownloadProgressStore(cache_dir=Path(tmpdir))

            # Save initial progress (simulating interrupted download)
            progress = DownloadProgress(
                task_id=task_id,
                file_id=file_id,
                total_bytes=total_bytes,
                downloaded_bytes=initial_bytes,
                local_temp_path=f"/tmp/{file_id}.tmp",
                s3_key=f"outputs/{task_id}/{file_id}.mp4",
            )
            store.save_progress(progress)

            # Simulate restart - create new store instance
            store2 = DownloadProgressStore(cache_dir=Path(tmpdir))

            # Retrieve saved progress
            retrieved = store2.get_progress(task_id, file_id)

            # Property: resume position matches saved position
            assert retrieved is not None
            assert retrieved.downloaded_bytes == initial_bytes

            # Simulate resuming download
            retrieved.downloaded_bytes += additional_bytes
            store2.save_progress(retrieved)

            # Verify updated progress
            updated = store2.get_progress(task_id, file_id)
            assert updated is not None
            assert updated.downloaded_bytes == initial_bytes + additional_bytes

    def test_incomplete_tasks_listed(self):
        """Incomplete tasks are listed for retry."""
        import tempfile

        from vco.services.download_progress import DownloadProgress, DownloadProgressStore

        with tempfile.TemporaryDirectory() as tmpdir:
            store = DownloadProgressStore(cache_dir=Path(tmpdir))

            # Add incomplete download
            incomplete = DownloadProgress(
                task_id="task1",
                file_id="file1",
                total_bytes=1000,
                downloaded_bytes=500,  # Incomplete
                local_temp_path="/tmp/file1.tmp",
                s3_key="outputs/task1/file1.mp4",
            )
            store.save_progress(incomplete)

            # Add complete download
            complete = DownloadProgress(
                task_id="task2",
                file_id="file2",
                total_bytes=1000,
                downloaded_bytes=1000,  # Complete
                local_temp_path="/tmp/file2.tmp",
                s3_key="outputs/task2/file2.mp4",
            )
            store.save_progress(complete)

            # Property: only incomplete tasks are listed
            incomplete_tasks = store.list_incomplete_tasks()
            assert "task1" in incomplete_tasks
            assert "task2" not in incomplete_tasks


class TestProperty9ConcurrentDownloadLimit:
    """Property 9: Concurrent download limit.

    For any batch import with AWS items, the number of concurrent
    downloads SHALL NOT exceed 3.

    Validates: Requirements 4.5
    """

    def test_concurrent_downloads_limited(self):
        """Concurrent downloads are limited to max_concurrent_downloads."""
        import threading
        from unittest.mock import patch

        max_concurrent = 3
        concurrent_count = 0
        max_observed = 0
        lock = threading.Lock()

        def mock_import_aws_item(item_id, user_id, progress_callback):
            nonlocal concurrent_count, max_observed
            with lock:
                concurrent_count += 1
                max_observed = max(max_observed, concurrent_count)

            # Simulate some work
            import time

            time.sleep(0.01)

            with lock:
                concurrent_count -= 1

            from vco.models.types import UnifiedImportResult

            return UnifiedImportResult(
                success=True,
                item_id=item_id,
                source="aws",
                original_filename="",
                converted_filename="",
            )

        # Create AWS items
        aws_items = [create_aws_importable_item(f"task{i}", f"file{i}") for i in range(10)]

        local_service = MagicMock(spec=ImportService)
        local_service.list_pending.return_value = []

        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.return_value = aws_items

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
        )

        with patch.object(service, "_import_aws_item", side_effect=mock_import_aws_item):
            service.import_all(max_concurrent_downloads=max_concurrent)

        # Property: max concurrent never exceeded limit
        assert max_observed <= max_concurrent


class TestProperty11RemoveItemCleanup:
    """Property 11: Remove item cleanup.

    For any local item removal, the converted file and metadata SHALL
    be deleted from disk; for AWS item removal, the S3 file SHALL be deleted.

    Validates: Requirements 7.2, 7.3
    """

    def test_local_remove_deletes_files(self):
        """Local item removal deletes files."""
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

        # Property: files were deleted
        assert result.file_deleted is True
        assert result.metadata_deleted is True

    def test_aws_remove_deletes_s3(self):
        """AWS item removal deletes S3 file."""
        aws_service = MagicMock(spec=AwsImportService)
        aws_service.delete_s3_file.return_value = True

        service = UnifiedImportService(aws_service=aws_service)
        result = service.remove_item("task123:file456")

        # Property: S3 file was deleted
        assert result.s3_deleted is True
        aws_service.delete_s3_file.assert_called_once_with("task123", "file456", None)


class TestProperty5AwsImportDownloadsAndVerifies:
    """Property 5: AWS import downloads and verifies.

    For any AWS item import, the system SHALL download from S3,
    verify checksum, and delete S3 file on success.

    Validates: Requirements 3.1, 3.3, 3.4
    """

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_aws_import_workflow_sequence(self, task_id, file_id):
        """AWS import follows download -> verify -> delete sequence."""
        from unittest.mock import patch

        from vco.services.aws_import import AwsDownloadResult

        local_service = MagicMock(spec=ImportService)
        aws_service = MagicMock(spec=AwsImportService)
        photos_manager = MagicMock()

        # Mock successful download with checksum verification
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        photos_manager.import_video.return_value = "new-uuid-123"
        aws_service.delete_s3_file.return_value = True

        service = UnifiedImportService(
            local_service=local_service,
            aws_service=aws_service,
            photos_manager=photos_manager,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(f"{task_id}:{file_id}")

        # Property: download was called
        assert result.downloaded is True

        # Property: checksum was verified
        assert result.checksum_verified is True

        # Property: S3 file was deleted on success
        assert result.s3_deleted is True
        aws_service.delete_s3_file.assert_called_once()

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow])
    def test_aws_import_no_delete_on_download_failure(self, task_id, file_id):
        """S3 file is NOT deleted when download fails."""
        from vco.services.aws_import import AwsDownloadResult

        aws_service = MagicMock(spec=AwsImportService)

        # Mock failed download
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=False,
            task_id=task_id,
            file_id=file_id,
            error_message="Download failed",
        )

        service = UnifiedImportService(aws_service=aws_service)
        result = service.import_item(f"{task_id}:{file_id}")

        # Property: download failed
        assert result.success is False
        assert result.downloaded is False

        # Property: S3 file was NOT deleted
        aws_service.delete_s3_file.assert_not_called()

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow])
    def test_aws_import_no_delete_on_photos_import_failure(self, task_id, file_id):
        """S3 file is NOT deleted when Photos import fails."""
        from unittest.mock import patch

        from vco.photos.manager import PhotosAccessError
        from vco.services.aws_import import AwsDownloadResult

        aws_service = MagicMock(spec=AwsImportService)
        photos_manager = MagicMock()

        # Mock successful download but failed Photos import
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        photos_manager.import_video.side_effect = PhotosAccessError("Photos import failed")

        service = UnifiedImportService(
            aws_service=aws_service,
            photos_manager=photos_manager,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(f"{task_id}:{file_id}")

        # Property: import failed
        assert result.success is False
        assert result.downloaded is True

        # Property: S3 file was NOT deleted
        aws_service.delete_s3_file.assert_not_called()


class TestProperty3AlbumMembershipPreservation:
    """Property 3: Album membership preservation.

    For any imported video that had album memberships in the original,
    the imported video SHALL be added to all those albums.

    Validates: Requirements 2.2
    """

    @given(
        album_names=st.lists(
            st.text(
                min_size=1,
                max_size=20,
                alphabet=st.characters(whitelist_categories=("L", "N", "P")),
            ),
            min_size=0,
            max_size=5,
            unique=True,
        ),
    )
    @settings(max_examples=25, suppress_health_check=[HealthCheck.too_slow])
    def test_all_albums_are_added(self, album_names):
        """All original albums are added to the imported video."""
        from unittest.mock import patch

        from vco.services.review import ReviewItem, ReviewService

        # Create mock review item with albums
        review_item = MagicMock(spec=ReviewItem)
        review_item.id = "review123"
        review_item.status = "pending_review"
        review_item.original_path = Path("/original/video.mov")
        review_item.converted_path = MagicMock()
        review_item.converted_path.exists.return_value = True
        review_item.converted_path.name = "video_h265.mp4"
        review_item.converted_path.with_suffix.return_value = Path("/converted/video.json")
        review_item.metadata = {"albums": album_names, "capture_date": None}

        review_service = MagicMock(spec=ReviewService)
        review_service.get_review_by_id.return_value = review_item

        photos_manager = MagicMock()
        photos_manager.import_video.return_value = "new-uuid-123"

        local_service = ImportService(
            review_service=review_service,
            photos_manager=photos_manager,
        )

        with patch.object(Path, "unlink"):
            result = local_service.import_single("review123")

        # Property: import succeeded
        assert result.success is True

        # Property: all albums from original are in result
        assert set(result.albums) == set(album_names)

        # Property: add_to_albums was called with all albums (if any)
        if album_names:
            photos_manager.add_to_albums.assert_called_once_with("new-uuid-123", album_names)
        else:
            photos_manager.add_to_albums.assert_not_called()

    @given(
        album_names=st.lists(
            st.text(min_size=1, max_size=10, alphabet="abcdefghijklmnopqrstuvwxyz"),
            min_size=1,
            max_size=3,
            unique=True,
        ),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow])
    def test_album_addition_failure_does_not_fail_import(self, album_names):
        """Album addition failure does not fail the import."""
        from unittest.mock import patch

        from vco.photos.manager import PhotosAccessError
        from vco.services.review import ReviewItem, ReviewService

        # Create mock review item with albums
        review_item = MagicMock(spec=ReviewItem)
        review_item.id = "review123"
        review_item.status = "pending_review"
        review_item.original_path = Path("/original/video.mov")
        review_item.converted_path = MagicMock()
        review_item.converted_path.exists.return_value = True
        review_item.converted_path.name = "video_h265.mp4"
        review_item.converted_path.with_suffix.return_value = Path("/converted/video.json")
        review_item.metadata = {"albums": album_names, "capture_date": None}

        review_service = MagicMock(spec=ReviewService)
        review_service.get_review_by_id.return_value = review_item

        photos_manager = MagicMock()
        photos_manager.import_video.return_value = "new-uuid-123"
        photos_manager.add_to_albums.side_effect = PhotosAccessError("Album not found")

        local_service = ImportService(
            review_service=review_service,
            photos_manager=photos_manager,
        )

        with patch.object(Path, "unlink"):
            result = local_service.import_single("review123")

        # Property: import still succeeded despite album failure
        assert result.success is True

        # Property: albums are still in result
        assert set(result.albums) == set(album_names)


class TestProperty2OutputContainsRequiredFields:
    """Property 2: Output contains required fields.

    For any ImportableItem displayed, the output SHALL contain: source,
    item_id, original_filename, converted_filename, file sizes,
    compression_ratio, ssim_score, albums, capture_date, and for AWS
    items: task_id and file_id.

    Validates: Requirements 1.2, 1.3
    """

    @given(
        source=st.sampled_from(["local", "aws"]),
        original_size=st.integers(min_value=1000, max_value=10000000),
        converted_size=st.integers(min_value=500, max_value=5000000),
        ssim_score=st.floats(min_value=0.8, max_value=1.0),
        album_count=st.integers(min_value=0, max_value=3),
    )
    @settings(max_examples=25, suppress_health_check=[HealthCheck.too_slow])
    def test_importable_item_has_all_required_fields(
        self, source, original_size, converted_size, ssim_score, album_count
    ):
        """ImportableItem has all required fields for display."""
        from datetime import datetime

        albums = [f"Album{i}" for i in range(album_count)]
        compression_ratio = original_size / converted_size if converted_size > 0 else 0

        if source == "aws":
            item = ImportableItem(
                item_id="task123:file456",
                source=source,
                original_filename="video.mov",
                converted_filename="video_h265.mp4",
                original_size=original_size,
                converted_size=converted_size,
                compression_ratio=compression_ratio,
                ssim_score=ssim_score,
                albums=albums,
                capture_date=datetime.now(),
                task_id="task123",
                file_id="file456",
            )
        else:
            item = ImportableItem(
                item_id="review123",
                source=source,
                original_filename="video.mov",
                converted_filename="video_h265.mp4",
                original_size=original_size,
                converted_size=converted_size,
                compression_ratio=compression_ratio,
                ssim_score=ssim_score,
                albums=albums,
                capture_date=datetime.now(),
            )

        # Property: all required fields are present and accessible
        assert item.source in ["local", "aws"]
        assert item.item_id is not None and len(item.item_id) > 0
        assert item.original_filename is not None
        assert item.converted_filename is not None
        assert item.original_size >= 0
        assert item.converted_size >= 0
        assert item.compression_ratio >= 0
        assert 0 <= item.ssim_score <= 1
        assert isinstance(item.albums, list)
        # capture_date can be None

        # Property: AWS items have task_id and file_id
        if source == "aws":
            assert item.task_id is not None
            assert item.file_id is not None

    @given(
        local_count=st.integers(min_value=0, max_value=3),
        aws_count=st.integers(min_value=0, max_value=3),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_json_output_contains_all_fields(self, local_count, aws_count):
        """JSON output contains all required fields for each item."""
        import json

        # Create items
        local_items = [
            ImportableItem(
                item_id=f"local{i}",
                source="local",
                original_filename=f"video{i}.mov",
                converted_filename=f"video{i}_h265.mp4",
                original_size=1000000,
                converted_size=500000,
                compression_ratio=2.0,
                ssim_score=0.95,
                albums=[f"Album{i}"],
                capture_date=None,
            )
            for i in range(local_count)
        ]

        aws_items = [
            ImportableItem(
                item_id=f"task{i}:file{i}",
                source="aws",
                original_filename=f"aws_video{i}.mov",
                converted_filename=f"aws_video{i}_h265.mp4",
                original_size=2000000,
                converted_size=800000,
                compression_ratio=2.5,
                ssim_score=0.92,
                albums=[],
                capture_date=None,
                task_id=f"task{i}",
                file_id=f"file{i}",
            )
            for i in range(aws_count)
        ]

        # Simulate JSON output format (as in CLI)
        all_items = local_items + aws_items
        json_items = [
            {
                "item_id": item.item_id,
                "source": item.source,
                "original_filename": item.original_filename,
                "converted_filename": item.converted_filename,
                "original_size": item.original_size,
                "converted_size": item.converted_size,
                "compression_ratio": item.compression_ratio,
                "ssim_score": item.ssim_score,
                "albums": item.albums,
                "capture_date": item.capture_date.isoformat() if item.capture_date else None,
                "task_id": item.task_id,
                "file_id": item.file_id,
            }
            for item in all_items
        ]

        # Verify JSON is serializable
        json_str = json.dumps({"items": json_items})
        parsed = json.loads(json_str)

        # Property: all items are in output
        assert len(parsed["items"]) == local_count + aws_count

        # Property: each item has all required fields
        required_fields = [
            "item_id",
            "source",
            "original_filename",
            "converted_filename",
            "original_size",
            "converted_size",
            "compression_ratio",
            "ssim_score",
            "albums",
            "capture_date",
            "task_id",
            "file_id",
        ]
        for item in parsed["items"]:
            for field in required_fields:
                assert field in item, f"Missing field: {field}"

            # Property: AWS items have non-null task_id and file_id
            if item["source"] == "aws":
                assert item["task_id"] is not None
                assert item["file_id"] is not None


class TestProperty4StatusUpdateOnSuccessfulImport:
    """Property 4: Status update on successful import.

    For any successful local import operation, the review queue item
    status SHALL be updated to "imported".

    Validates: Requirements 2.4
    """

    @given(
        review_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_status_updated_to_imported_on_success(self, review_id):
        """Status is updated to 'imported' on successful import."""
        from unittest.mock import patch

        from vco.services.review import ReviewItem, ReviewQueue, ReviewService

        # Create mock review item
        review_item = MagicMock(spec=ReviewItem)
        review_item.id = review_id
        review_item.status = "pending_review"
        review_item.original_path = Path("/original/video.mov")
        review_item.converted_path = MagicMock()
        review_item.converted_path.exists.return_value = True
        review_item.converted_path.name = "video_h265.mp4"
        review_item.converted_path.with_suffix.return_value = Path("/converted/video.json")
        review_item.metadata = {"albums": [], "capture_date": None}

        # Create mock queue with the item
        mock_queue = MagicMock(spec=ReviewQueue)
        mock_queue.items = [review_item]

        review_service = MagicMock(spec=ReviewService)
        review_service.get_review_by_id.return_value = review_item
        review_service.load_queue.return_value = mock_queue
        review_service.save_queue.return_value = True

        photos_manager = MagicMock()
        photos_manager.import_video.return_value = "new-uuid-123"

        local_service = ImportService(
            review_service=review_service,
            photos_manager=photos_manager,
        )

        with patch.object(Path, "unlink"):
            result = local_service.import_single(review_id)

        # Property: import succeeded
        assert result.success is True

        # Property: status was updated to "imported"
        assert review_item.status == "imported"

        # Property: queue was saved
        review_service.save_queue.assert_called()

    @given(
        review_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow])
    def test_status_not_updated_on_failure(self, review_id):
        """Status is NOT updated when import fails."""
        from vco.photos.manager import PhotosAccessError
        from vco.services.review import ReviewItem, ReviewService

        # Create mock review item
        review_item = MagicMock(spec=ReviewItem)
        review_item.id = review_id
        review_item.status = "pending_review"
        review_item.original_path = Path("/original/video.mov")
        review_item.converted_path = MagicMock()
        review_item.converted_path.exists.return_value = True
        review_item.converted_path.name = "video_h265.mp4"
        review_item.metadata = {"albums": [], "capture_date": None}

        review_service = MagicMock(spec=ReviewService)
        review_service.get_review_by_id.return_value = review_item

        photos_manager = MagicMock()
        photos_manager.import_video.side_effect = PhotosAccessError("Import failed")

        local_service = ImportService(
            review_service=review_service,
            photos_manager=photos_manager,
        )

        result = local_service.import_single(review_id)

        # Property: import failed
        assert result.success is False

        # Property: status was NOT updated (still pending_review)
        assert review_item.status == "pending_review"

        # Property: queue was NOT saved
        review_service.save_queue.assert_not_called()
