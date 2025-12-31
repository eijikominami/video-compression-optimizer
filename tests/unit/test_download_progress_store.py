"""Unit tests for DownloadProgressStore.

Task 12.4: DownloadProgressStore 単体テスト
- get_progress(): 正常系、進捗未存在
- save_progress(): 正常系、上書き
- clear_progress(): 正常系、存在しない進捗
- JSON ファイル破損時のリカバリ

Requirements: 4.5
"""

from datetime import datetime
from pathlib import Path

from vco.services.download_progress import DownloadProgress, DownloadProgressStore


class TestDownloadProgressStoreGetProgress:
    """Tests for get_progress method."""

    def test_get_progress_returns_none_when_not_exists(self, tmp_path):
        """get_progress returns None when progress doesn't exist."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        result = store.get_progress("task-123", "file-456")

        assert result is None

    def test_get_progress_returns_none_for_unknown_task(self, tmp_path):
        """get_progress returns None for unknown task ID."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save progress for a different task
        progress = DownloadProgress(
            task_id="task-other",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        result = store.get_progress("task-123", "file-456")

        assert result is None

    def test_get_progress_returns_none_for_unknown_file(self, tmp_path):
        """get_progress returns None for unknown file ID."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save progress for a different file
        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-other",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        result = store.get_progress("task-123", "file-456")

        assert result is None

    def test_get_progress_returns_saved_progress(self, tmp_path):
        """get_progress returns previously saved progress."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
            checksum="abc123",
        )
        store.save_progress(progress)

        result = store.get_progress("task-123", "file-456")

        assert result is not None
        assert result.task_id == "task-123"
        assert result.file_id == "file-456"
        assert result.total_bytes == 1000
        assert result.downloaded_bytes == 500
        assert result.local_temp_path == "/tmp/test.mp4.part"
        assert result.s3_key == "output/test.mp4"
        assert result.checksum == "abc123"


class TestDownloadProgressStoreSaveProgress:
    """Tests for save_progress method."""

    def test_save_progress_creates_new_entry(self, tmp_path):
        """save_progress creates new entry for new progress."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        # Verify saved
        result = store.get_progress("task-123", "file-456")
        assert result is not None
        assert result.downloaded_bytes == 500

    def test_save_progress_overwrites_existing(self, tmp_path):
        """save_progress overwrites existing progress."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save initial progress
        progress1 = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress1)

        # Save updated progress
        progress2 = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=750,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress2)

        # Verify overwritten
        result = store.get_progress("task-123", "file-456")
        assert result is not None
        assert result.downloaded_bytes == 750

    def test_save_progress_updates_last_updated(self, tmp_path):
        """save_progress updates last_updated timestamp."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        old_time = datetime(2020, 1, 1, 0, 0, 0)
        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
            last_updated=old_time,
        )
        store.save_progress(progress)

        result = store.get_progress("task-123", "file-456")
        assert result is not None
        assert result.last_updated > old_time

    def test_save_progress_persists_to_file(self, tmp_path):
        """save_progress persists data to JSON file."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        # Create new store instance to verify persistence
        store2 = DownloadProgressStore(cache_dir=tmp_path)
        result = store2.get_progress("task-123", "file-456")

        assert result is not None
        assert result.downloaded_bytes == 500

    def test_save_progress_multiple_files_same_task(self, tmp_path):
        """save_progress handles multiple files for same task."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        progress1 = DownloadProgress(
            task_id="task-123",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test1.mp4.part",
            s3_key="output/test1.mp4",
        )
        progress2 = DownloadProgress(
            task_id="task-123",
            file_id="file-2",
            total_bytes=2000,
            downloaded_bytes=1000,
            local_temp_path="/tmp/test2.mp4.part",
            s3_key="output/test2.mp4",
        )
        store.save_progress(progress1)
        store.save_progress(progress2)

        result1 = store.get_progress("task-123", "file-1")
        result2 = store.get_progress("task-123", "file-2")

        assert result1 is not None
        assert result1.downloaded_bytes == 500
        assert result2 is not None
        assert result2.downloaded_bytes == 1000


class TestDownloadProgressStoreClearProgress:
    """Tests for clear_progress method."""

    def test_clear_progress_removes_entry(self, tmp_path):
        """clear_progress removes the specified entry."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        store.clear_progress("task-123", "file-456")

        result = store.get_progress("task-123", "file-456")
        assert result is None

    def test_clear_progress_nonexistent_no_error(self, tmp_path):
        """clear_progress doesn't raise error for nonexistent entry."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        # Should not raise
        store.clear_progress("task-123", "file-456")

    def test_clear_progress_nonexistent_task_no_error(self, tmp_path):
        """clear_progress doesn't raise error for nonexistent task."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save progress for different task
        progress = DownloadProgress(
            task_id="task-other",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        # Should not raise
        store.clear_progress("task-123", "file-456")

    def test_clear_progress_keeps_other_files(self, tmp_path):
        """clear_progress keeps other files in same task."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        progress1 = DownloadProgress(
            task_id="task-123",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test1.mp4.part",
            s3_key="output/test1.mp4",
        )
        progress2 = DownloadProgress(
            task_id="task-123",
            file_id="file-2",
            total_bytes=2000,
            downloaded_bytes=1000,
            local_temp_path="/tmp/test2.mp4.part",
            s3_key="output/test2.mp4",
        )
        store.save_progress(progress1)
        store.save_progress(progress2)

        store.clear_progress("task-123", "file-1")

        assert store.get_progress("task-123", "file-1") is None
        assert store.get_progress("task-123", "file-2") is not None

    def test_clear_progress_persists_to_file(self, tmp_path):
        """clear_progress persists deletion to file."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)
        store.clear_progress("task-123", "file-456")

        # Create new store instance to verify persistence
        store2 = DownloadProgressStore(cache_dir=tmp_path)
        result = store2.get_progress("task-123", "file-456")

        assert result is None


class TestDownloadProgressStoreClearTask:
    """Tests for clear_task method."""

    def test_clear_task_removes_all_files(self, tmp_path):
        """clear_task removes all files for the task."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        for i in range(3):
            progress = DownloadProgress(
                task_id="task-123",
                file_id=f"file-{i}",
                total_bytes=1000,
                downloaded_bytes=500,
                local_temp_path=f"/tmp/test{i}.mp4.part",
                s3_key=f"output/test{i}.mp4",
            )
            store.save_progress(progress)

        store.clear_task("task-123")

        for i in range(3):
            assert store.get_progress("task-123", f"file-{i}") is None

    def test_clear_task_keeps_other_tasks(self, tmp_path):
        """clear_task keeps files from other tasks."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        progress1 = DownloadProgress(
            task_id="task-123",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test1.mp4.part",
            s3_key="output/test1.mp4",
        )
        progress2 = DownloadProgress(
            task_id="task-456",
            file_id="file-2",
            total_bytes=2000,
            downloaded_bytes=1000,
            local_temp_path="/tmp/test2.mp4.part",
            s3_key="output/test2.mp4",
        )
        store.save_progress(progress1)
        store.save_progress(progress2)

        store.clear_task("task-123")

        assert store.get_progress("task-123", "file-1") is None
        assert store.get_progress("task-456", "file-2") is not None


class TestDownloadProgressStoreGetTaskProgress:
    """Tests for get_task_progress method."""

    def test_get_task_progress_returns_empty_for_unknown_task(self, tmp_path):
        """get_task_progress returns empty dict for unknown task."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        result = store.get_task_progress("task-123")

        assert result == {}

    def test_get_task_progress_returns_all_files(self, tmp_path):
        """get_task_progress returns all files for the task."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        for i in range(3):
            progress = DownloadProgress(
                task_id="task-123",
                file_id=f"file-{i}",
                total_bytes=1000 * (i + 1),
                downloaded_bytes=500 * (i + 1),
                local_temp_path=f"/tmp/test{i}.mp4.part",
                s3_key=f"output/test{i}.mp4",
            )
            store.save_progress(progress)

        result = store.get_task_progress("task-123")

        assert len(result) == 3
        assert "file-0" in result
        assert "file-1" in result
        assert "file-2" in result
        assert result["file-0"].total_bytes == 1000
        assert result["file-1"].total_bytes == 2000
        assert result["file-2"].total_bytes == 3000


class TestDownloadProgressStoreListIncompleteTasks:
    """Tests for list_incomplete_tasks method."""

    def test_list_incomplete_tasks_returns_empty_when_no_tasks(self, tmp_path):
        """list_incomplete_tasks returns empty list when no tasks."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        result = store.list_incomplete_tasks()

        assert result == []

    def test_list_incomplete_tasks_returns_incomplete_tasks(self, tmp_path):
        """list_incomplete_tasks returns tasks with incomplete downloads."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        # Incomplete task
        progress1 = DownloadProgress(
            task_id="task-incomplete",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test1.mp4.part",
            s3_key="output/test1.mp4",
        )
        # Complete task
        progress2 = DownloadProgress(
            task_id="task-complete",
            file_id="file-2",
            total_bytes=1000,
            downloaded_bytes=1000,
            local_temp_path="/tmp/test2.mp4.part",
            s3_key="output/test2.mp4",
        )
        store.save_progress(progress1)
        store.save_progress(progress2)

        result = store.list_incomplete_tasks()

        assert "task-incomplete" in result
        assert "task-complete" not in result

    def test_list_incomplete_tasks_task_with_mixed_files(self, tmp_path):
        """list_incomplete_tasks includes task if any file is incomplete."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        # Complete file
        progress1 = DownloadProgress(
            task_id="task-123",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=1000,
            local_temp_path="/tmp/test1.mp4.part",
            s3_key="output/test1.mp4",
        )
        # Incomplete file
        progress2 = DownloadProgress(
            task_id="task-123",
            file_id="file-2",
            total_bytes=2000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test2.mp4.part",
            s3_key="output/test2.mp4",
        )
        store.save_progress(progress1)
        store.save_progress(progress2)

        result = store.list_incomplete_tasks()

        assert "task-123" in result


class TestDownloadProgressStoreCorruptedFile:
    """Tests for handling corrupted JSON file."""

    def test_corrupted_json_file_recovers(self, tmp_path):
        """Store recovers from corrupted JSON file."""
        # Create corrupted JSON file
        db_path = tmp_path / "download_progress.json"
        db_path.write_text("{ invalid json }")

        # Should not raise, should recover with empty data
        store = DownloadProgressStore(cache_dir=tmp_path)

        result = store.get_progress("task-123", "file-456")
        assert result is None

    def test_invalid_json_structure_recovers(self, tmp_path):
        """Store recovers from invalid JSON structure."""
        # Create JSON with invalid structure
        db_path = tmp_path / "download_progress.json"
        db_path.write_text('{"task-123": "not a dict"}')

        # Should not raise, should recover with empty data
        store = DownloadProgressStore(cache_dir=tmp_path)

        result = store.get_progress("task-123", "file-456")
        assert result is None

    def test_missing_required_fields_recovers(self, tmp_path):
        """Store recovers from JSON with missing required fields."""
        # Create JSON with missing fields
        db_path = tmp_path / "download_progress.json"
        db_path.write_text('{"task-123": {"file-456": {"task_id": "task-123"}}}')

        # Should not raise, should recover with empty data
        store = DownloadProgressStore(cache_dir=tmp_path)

        result = store.get_progress("task-123", "file-456")
        assert result is None

    def test_empty_file_recovers(self, tmp_path):
        """Store recovers from empty file."""
        # Create empty file
        db_path = tmp_path / "download_progress.json"
        db_path.write_text("")

        # Should not raise, should recover with empty data
        store = DownloadProgressStore(cache_dir=tmp_path)

        result = store.get_progress("task-123", "file-456")
        assert result is None

    def test_can_save_after_recovery(self, tmp_path):
        """Store can save new data after recovering from corruption."""
        # Create corrupted JSON file
        db_path = tmp_path / "download_progress.json"
        db_path.write_text("{ invalid json }")

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Should be able to save new progress
        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        result = store.get_progress("task-123", "file-456")
        assert result is not None
        assert result.downloaded_bytes == 500


class TestDownloadProgressStoreCacheDirectory:
    """Tests for cache directory handling."""

    def test_creates_cache_directory_if_not_exists(self, tmp_path):
        """Store creates cache directory if it doesn't exist."""
        cache_dir = tmp_path / "new" / "nested" / "dir"
        assert not cache_dir.exists()

        DownloadProgressStore(cache_dir=cache_dir)

        assert cache_dir.exists()

    def test_uses_default_cache_directory(self):
        """Store uses default cache directory when not specified."""
        store = DownloadProgressStore()

        expected_dir = Path.home() / ".cache" / "vco"
        assert store.cache_dir == expected_dir


class TestDownloadProgressStoreSyncWithServer:
    """Tests for sync_with_server method (Requirement 8.5)."""

    def test_sync_clears_already_downloaded_files(self, tmp_path):
        """sync_with_server clears progress for files already downloaded on server."""
        from datetime import datetime
        from unittest.mock import MagicMock

        from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        now = datetime.now()
        # Mock status service - file already downloaded on server
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id="task-123",
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id="file-456",
                    original_uuid="photos-uuid-456",
                    filename="video.mov",
                    source_s3_key="tasks/task-123/source/file-456/video.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=datetime(2024, 1, 1, 12, 0, 0),  # Already downloaded
                    download_available=True,
                )
            ],
            created_at=now,
            updated_at=now,
        )

        result = store.sync_with_server(mock_service)

        assert "file-456" in result["cleared"]
        assert store.get_progress("task-123", "file-456") is None

    def test_sync_clears_unavailable_files(self, tmp_path):
        """sync_with_server clears progress for files no longer available."""
        from datetime import datetime
        from unittest.mock import MagicMock

        from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        now = datetime.now()
        # Mock status service - file no longer available
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id="task-123",
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id="file-456",
                    original_uuid="photos-uuid-456",
                    filename="video.mov",
                    source_s3_key="tasks/task-123/source/file-456/video.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=None,
                    download_available=False,  # No longer available
                )
            ],
            created_at=now,
            updated_at=now,
        )

        result = store.sync_with_server(mock_service)

        assert "file-456" in result["unavailable"]
        assert store.get_progress("task-123", "file-456") is None

    def test_sync_keeps_pending_downloads(self, tmp_path):
        """sync_with_server keeps progress for files still available."""
        from datetime import datetime
        from unittest.mock import MagicMock

        from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        now = datetime.now()
        # Mock status service - file still available
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id="task-123",
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id="file-456",
                    original_uuid="photos-uuid-456",
                    filename="video.mov",
                    source_s3_key="tasks/task-123/source/file-456/video.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=None,  # Not downloaded yet
                    download_available=True,  # Still available
                )
            ],
            created_at=now,
            updated_at=now,
        )

        result = store.sync_with_server(mock_service)

        assert result["cleared"] == []
        assert result["unavailable"] == []
        assert store.get_progress("task-123", "file-456") is not None

    def test_sync_handles_task_not_found(self, tmp_path):
        """sync_with_server clears progress when task not found on server."""
        from unittest.mock import MagicMock

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        # Mock status service - task not found
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = None

        result = store.sync_with_server(mock_service)

        assert "file-456" in result["unavailable"]
        assert store.get_progress("task-123", "file-456") is None

    def test_sync_handles_api_errors(self, tmp_path):
        """sync_with_server handles API errors gracefully."""
        from unittest.mock import MagicMock

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save local progress
        progress = DownloadProgress(
            task_id="task-123",
            file_id="file-456",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.mp4.part",
            s3_key="output/test.mp4",
        )
        store.save_progress(progress)

        # Mock status service - raises exception
        mock_service = MagicMock()
        mock_service.get_task_status.side_effect = Exception("API error")

        result = store.sync_with_server(mock_service)

        assert "task-123" in result["errors"]
        # Progress should be kept on error
        assert store.get_progress("task-123", "file-456") is not None

    def test_sync_with_specific_task_ids(self, tmp_path):
        """sync_with_server only syncs specified task IDs."""
        from datetime import datetime
        from unittest.mock import MagicMock

        from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save progress for two tasks
        progress1 = DownloadProgress(
            task_id="task-1",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test1.mp4.part",
            s3_key="output/test1.mp4",
        )
        progress2 = DownloadProgress(
            task_id="task-2",
            file_id="file-2",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test2.mp4.part",
            s3_key="output/test2.mp4",
        )
        store.save_progress(progress1)
        store.save_progress(progress2)

        now = datetime.now()
        # Mock status service - task-1 already downloaded
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id="task-1",
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id="file-1",
                    original_uuid="photos-uuid-1",
                    filename="video.mov",
                    source_s3_key="tasks/task-1/source/file-1/video.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=datetime(2024, 1, 1, 12, 0, 0),
                    download_available=True,
                )
            ],
            created_at=now,
            updated_at=now,
        )

        # Only sync task-1
        result = store.sync_with_server(mock_service, task_ids=["task-1"])

        assert "file-1" in result["cleared"]
        assert store.get_progress("task-1", "file-1") is None
        # task-2 should not be synced
        assert store.get_progress("task-2", "file-2") is not None
        mock_service.get_task_status.assert_called_once_with("task-1")

    def test_sync_returns_empty_when_no_tasks(self, tmp_path):
        """sync_with_server returns empty result when no tasks to sync."""
        from unittest.mock import MagicMock

        store = DownloadProgressStore(cache_dir=tmp_path)
        mock_service = MagicMock()

        result = store.sync_with_server(mock_service)

        assert result == {"cleared": [], "unavailable": [], "errors": []}
        mock_service.get_task_status.assert_not_called()

    def test_sync_multiple_files_same_task(self, tmp_path):
        """sync_with_server handles multiple files in same task."""
        from datetime import datetime
        from unittest.mock import MagicMock

        from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus

        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save progress for multiple files
        for i in range(3):
            progress = DownloadProgress(
                task_id="task-123",
                file_id=f"file-{i}",
                total_bytes=1000,
                downloaded_bytes=500,
                local_temp_path=f"/tmp/test{i}.mp4.part",
                s3_key=f"output/test{i}.mp4",
            )
            store.save_progress(progress)

        now = datetime.now()
        # Mock status service - file-0 downloaded, file-1 unavailable, file-2 pending
        mock_service = MagicMock()
        mock_service.get_task_status.return_value = AsyncTask(
            task_id="task-123",
            user_id="user-123",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[
                AsyncFile(
                    file_id="file-0",
                    original_uuid="photos-uuid-0",
                    filename="video0.mov",
                    source_s3_key="tasks/task-123/source/file-0/video0.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=datetime(2024, 1, 1, 12, 0, 0),  # Downloaded
                    download_available=True,
                ),
                AsyncFile(
                    file_id="file-1",
                    original_uuid="photos-uuid-1",
                    filename="video1.mov",
                    source_s3_key="tasks/task-123/source/file-1/video1.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=None,
                    download_available=False,  # Unavailable
                ),
                AsyncFile(
                    file_id="file-2",
                    original_uuid="photos-uuid-2",
                    filename="video2.mov",
                    source_s3_key="tasks/task-123/source/file-2/video2.mov",
                    status=FileStatus.COMPLETED,
                    downloaded_at=None,
                    download_available=True,  # Still pending
                ),
            ],
            created_at=now,
            updated_at=now,
        )

        result = store.sync_with_server(mock_service)

        assert "file-0" in result["cleared"]
        assert "file-1" in result["unavailable"]
        assert store.get_progress("task-123", "file-0") is None
        assert store.get_progress("task-123", "file-1") is None
        assert store.get_progress("task-123", "file-2") is not None


class TestDownloadProgressStoreGetFilesNeedingSync:
    """Tests for get_files_needing_sync method."""

    def test_returns_empty_when_no_progress(self, tmp_path):
        """get_files_needing_sync returns empty list when no progress."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        result = store.get_files_needing_sync()

        assert result == []

    def test_returns_all_files_with_progress(self, tmp_path):
        """get_files_needing_sync returns all files with local progress."""
        store = DownloadProgressStore(cache_dir=tmp_path)

        # Save progress for multiple tasks and files
        progress1 = DownloadProgress(
            task_id="task-1",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test1.mp4.part",
            s3_key="output/test1.mp4",
        )
        progress2 = DownloadProgress(
            task_id="task-1",
            file_id="file-2",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test2.mp4.part",
            s3_key="output/test2.mp4",
        )
        progress3 = DownloadProgress(
            task_id="task-2",
            file_id="file-3",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test3.mp4.part",
            s3_key="output/test3.mp4",
        )
        store.save_progress(progress1)
        store.save_progress(progress2)
        store.save_progress(progress3)

        result = store.get_files_needing_sync()

        assert len(result) == 3
        assert ("task-1", "file-1") in result
        assert ("task-1", "file-2") in result
        assert ("task-2", "file-3") in result
