"""Property-based tests for UnifiedImportService.

Tests the correctness properties defined in design.md.
Focuses on AWS-related functionality (local import functionality has been removed).
"""

import json
import tempfile
import threading
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from vco.models.types import ImportableItem
from vco.services.aws_import import AwsImportService
from vco.services.unified_import import UnifiedImportService


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
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_progress_roundtrip(self, task_id, file_id, total_bytes, downloaded_bytes):
        """Progress can be saved and retrieved."""
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
        import time

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

        aws_service = MagicMock(spec=AwsImportService)
        aws_service.list_completed_files.return_value = aws_items

        service = UnifiedImportService(
            aws_service=aws_service,
        )

        with patch.object(service, "_import_aws_item", side_effect=mock_import_aws_item):
            service.import_all(max_concurrent_downloads=max_concurrent)

        # Property: max concurrent never exceeded limit
        assert max_observed <= max_concurrent


class TestProperty11AwsRemoveItemCleanup:
    """Property 11: AWS remove item cleanup.

    For AWS item removal, the S3 file SHALL be deleted via cleanup API.

    Validates: Requirements 7.3
    """

    def test_aws_remove_deletes_s3(self):
        """AWS item removal deletes S3 file via cleanup API."""
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

        # Property: S3 file was deleted via cleanup API
        assert result.s3_deleted is True
        aws_service.cleanup_file.assert_called_once_with(
            task_id="task123",
            file_id="file456",
            action="removed",
            user_id=None,
        )


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
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_aws_import_workflow_sequence(self, task_id, file_id):
        """AWS import follows download -> verify -> cleanup sequence."""
        from vco.photos.swift_bridge import SwiftBridge
        from vco.services.aws_import import AwsDownloadResult, CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock(spec=SwiftBridge)

        # Mock successful download with checksum verification
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id=file_id,
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(f"{task_id}:{file_id}")

        # Property: download was called
        assert result.downloaded is True

        # Property: checksum was verified
        assert result.checksum_verified is True

        # Property: S3 file was deleted on success via cleanup API
        assert result.s3_deleted is True
        aws_service.cleanup_file.assert_called_once_with(
            task_id=task_id,
            file_id=file_id,
            action="downloaded",
            user_id=None,
        )

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow])
    def test_aws_import_no_cleanup_on_download_failure(self, task_id, file_id):
        """Cleanup API is NOT called when download fails."""
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

        # Property: cleanup API was NOT called
        aws_service.cleanup_file.assert_not_called()

    def test_aws_import_no_cleanup_on_photos_import_failure(self):
        """Cleanup API is NOT called when Photos import fails."""
        from vco.services.aws_import import AwsDownloadResult

        task_id = "task123"
        file_id = "file456"

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock()

        # Mock successful download but failed Photos import
        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        # Use generic Exception to avoid module reload issues in test suite
        swift_bridge.import_video.side_effect = Exception("Photos import failed")

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(f"{task_id}:{file_id}")

        # Property: import failed
        assert result.success is False
        assert result.downloaded is True

        # Property: cleanup API was NOT called
        aws_service.cleanup_file.assert_not_called()


class TestProperty2OutputContainsRequiredFields:
    """Property 2: Output contains required fields.

    For any ImportableItem displayed, the output SHALL contain: source,
    item_id, original_filename, converted_filename, file sizes,
    compression_ratio, ssim_score, albums, capture_date, and for AWS
    items: task_id and file_id.

    Validates: Requirements 1.2, 1.3
    """

    @given(
        original_size=st.integers(min_value=1000, max_value=10000000),
        converted_size=st.integers(min_value=500, max_value=5000000),
        ssim_score=st.floats(min_value=0.8, max_value=1.0),
        album_count=st.integers(min_value=0, max_value=3),
    )
    @settings(max_examples=25, suppress_health_check=[HealthCheck.too_slow])
    def test_aws_importable_item_has_all_required_fields(
        self, original_size, converted_size, ssim_score, album_count
    ):
        """AWS ImportableItem has all required fields for display."""
        albums = [f"Album{i}" for i in range(album_count)]
        compression_ratio = original_size / converted_size if converted_size > 0 else 0

        item = ImportableItem(
            item_id="task123:file456",
            source="aws",
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

        # Property: all required fields are present and accessible
        assert item.source == "aws"
        assert item.item_id is not None and len(item.item_id) > 0
        assert item.original_filename is not None
        assert item.converted_filename is not None
        assert item.original_size >= 0
        assert item.converted_size >= 0
        assert item.compression_ratio >= 0
        assert 0 <= item.ssim_score <= 1
        assert isinstance(item.albums, list)

        # Property: AWS items have task_id and file_id
        assert item.task_id is not None
        assert item.file_id is not None

    @given(
        aws_count=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=20, suppress_health_check=[HealthCheck.too_slow])
    def test_json_output_contains_all_fields(self, aws_count):
        """JSON output contains all required fields for each AWS item."""
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
            for item in aws_items
        ]

        # Verify JSON is serializable
        json_str = json.dumps({"items": json_items})
        parsed = json.loads(json_str)

        # Property: all items are in output
        assert len(parsed["items"]) == aws_count

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
            assert item["task_id"] is not None
            assert item["file_id"] is not None


class TestProperty20OriginalDeletionPromptBehavior:
    """Property 20: Original deletion prompt behavior.

    For any successful import without --delete-original flag,
    the system SHALL prompt for original deletion (unless -y flag).

    Validates: Requirements 5.1
    """

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_no_deletion_without_flag_and_uuid(self, task_id, file_id):
        """Without --delete-original flag and no UUID, no deletion occurs."""
        from vco.photos.swift_bridge import SwiftBridge
        from vco.services.aws_import import AwsDownloadResult, CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock(spec=SwiftBridge)

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id=file_id,
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(
                f"{task_id}:{file_id}",
                delete_original=False,
                original_uuid=None,
            )

        # Property: import succeeds
        assert result.success is True

        # Property: no deletion attempted without UUID
        assert result.original_deleted is False
        swift_bridge.delete_video.assert_not_called()


class TestProperty21OriginalDeletionWithYFlag:
    """Property 21: Original deletion with -y flag.

    For any import with -y flag but without --delete-original,
    the system SHALL NOT delete original and SHALL NOT prompt.

    Validates: Requirements 5.10
    """

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        original_uuid=st.text(min_size=8, max_size=36, alphabet="abcdef0123456789-"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_y_flag_without_delete_original_no_deletion(self, task_id, file_id, original_uuid):
        """With -y flag but without --delete-original, no deletion occurs."""
        from vco.photos.swift_bridge import SwiftBridge
        from vco.services.aws_import import AwsDownloadResult, CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock(spec=SwiftBridge)

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id=file_id,
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            # Simulate -y flag: delete_original=False, original_uuid provided
            result = service.import_item(
                f"{task_id}:{file_id}",
                delete_original=False,  # -y flag without --delete-original
                original_uuid=original_uuid,
            )

        # Property: import succeeds
        assert result.success is True

        # Property: no deletion attempted (delete_original=False)
        assert result.original_deleted is False
        swift_bridge.delete_video.assert_not_called()


class TestProperty22OriginalDeletionWithDeleteOriginalFlag:
    """Property 22: Original deletion with --delete-original flag.

    For any import with --delete-original flag and valid UUID,
    the system SHALL delete original without prompting.

    Validates: Requirements 5.4
    """

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        original_uuid=st.text(min_size=8, max_size=36, alphabet="abcdef0123456789-"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_delete_original_flag_deletes_without_prompt(self, task_id, file_id, original_uuid):
        """With --delete-original flag and UUID, deletion occurs without prompt."""
        from vco.photos.swift_bridge import SwiftBridge
        from vco.services.aws_import import AwsDownloadResult, CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock(spec=SwiftBridge)

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        swift_bridge.delete_video.return_value = True
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id=file_id,
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(
                f"{task_id}:{file_id}",
                delete_original=True,
                original_uuid=original_uuid,
            )

        # Property: import succeeds
        assert result.success is True

        # Property: deletion was attempted with correct UUID
        swift_bridge.delete_video.assert_called_once_with(original_uuid)

        # Property: deletion succeeded
        assert result.original_deleted is True
        assert result.original_uuid == original_uuid

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        original_uuid=st.text(min_size=8, max_size=36, alphabet="abcdef0123456789-"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_delete_original_failure_does_not_fail_import(self, task_id, file_id, original_uuid):
        """Original deletion failure does not fail the import (Requirement 5.7)."""
        from vco.photos.swift_bridge import SwiftBridge
        from vco.services.aws_import import AwsDownloadResult, CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock(spec=SwiftBridge)

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        # Deletion fails
        swift_bridge.delete_video.return_value = False
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id=file_id,
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(
                f"{task_id}:{file_id}",
                delete_original=True,
                original_uuid=original_uuid,
            )

        # Property: import still succeeds despite deletion failure
        assert result.success is True

        # Property: deletion was attempted
        swift_bridge.delete_video.assert_called_once_with(original_uuid)

        # Property: deletion failure is recorded
        assert result.original_deleted is False
        assert result.original_delete_error is not None

    @given(
        task_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
        file_id=st.text(min_size=8, max_size=16, alphabet="abcdef0123456789"),
    )
    @settings(max_examples=15, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_delete_original_flag_without_uuid_no_deletion(self, task_id, file_id):
        """With --delete-original flag but no UUID, no deletion occurs."""
        from vco.photos.swift_bridge import SwiftBridge
        from vco.services.aws_import import AwsDownloadResult, CleanupResult

        aws_service = MagicMock(spec=AwsImportService)
        swift_bridge = MagicMock(spec=SwiftBridge)

        aws_service.download_and_prepare.return_value = AwsDownloadResult(
            success=True,
            task_id=task_id,
            file_id=file_id,
            local_path=Path(f"/tmp/{file_id}_h265.mp4"),
            checksum_verified=True,
        )
        swift_bridge.import_video.return_value = "new-uuid-123"
        aws_service.cleanup_file.return_value = CleanupResult(
            success=True,
            file_id=file_id,
            status="DOWNLOADED",
            s3_deleted=True,
        )

        service = UnifiedImportService(
            aws_service=aws_service,
            swift_bridge=swift_bridge,
        )

        with patch.object(Path, "unlink"):
            result = service.import_item(
                f"{task_id}:{file_id}",
                delete_original=True,
                original_uuid=None,  # No UUID provided
            )

        # Property: import succeeds
        assert result.success is True

        # Property: no deletion attempted without UUID
        assert result.original_deleted is False
        swift_bridge.delete_video.assert_not_called()
