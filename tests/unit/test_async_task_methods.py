"""Unit tests for AsyncTask and AsyncFile methods.

Task 1.3: 単体テスト: AsyncTask/AsyncFile の各メソッド
- calculate_progress(): 各状態での進捗率計算
- estimate_completion_time(): 推定完了時刻計算
- 境界値テスト: 空ファイルリスト、最大ファイル数

Requirements: 2.3, 2.4
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from vco.models.async_task import (
    AsyncFile,
    AsyncTask,
    DownloadProgress,
    FileStatus,
    TaskStatus,
)


class TestAsyncTaskCalculateProgress:
    """Test AsyncTask.calculate_progress() method.

    Progress breakdown per file (updated for Phase 9):
    - PENDING: 0%
    - CONVERTING: 0-65% (scaled from MediaConvert jobPercentComplete, default 32%)
    - VERIFYING: 65-99% (65 + verification_progress * 0.34)
    - COMPLETED/FAILED: 100%

    Task overall progress is the average of all file progress values.
    """

    def test_pending_status_returns_zero(self):
        """PENDING status should return 0% progress."""
        task = self._create_task(TaskStatus.PENDING, [])
        assert task.calculate_progress() == 0

    def test_uploading_status_returns_five(self):
        """UPLOADING status should return 5% progress."""
        task = self._create_task(TaskStatus.UPLOADING, [])
        assert task.calculate_progress() == 5

    def test_completed_status_returns_hundred(self):
        """COMPLETED status should return 100% progress."""
        task = self._create_task(TaskStatus.COMPLETED, [])
        assert task.calculate_progress() == 100

    def test_partially_completed_status_returns_hundred(self):
        """PARTIALLY_COMPLETED status should return 100% progress."""
        task = self._create_task(TaskStatus.PARTIALLY_COMPLETED, [])
        assert task.calculate_progress() == 100

    def test_failed_status_preserves_last_progress(self):
        """FAILED status should preserve last known progress."""
        task = self._create_task(TaskStatus.FAILED, [], progress_percentage=45)
        assert task.calculate_progress() == 45

    def test_cancelled_status_preserves_last_progress(self):
        """CANCELLED status should preserve last known progress."""
        task = self._create_task(TaskStatus.CANCELLED, [], progress_percentage=30)
        assert task.calculate_progress() == 30

    def test_converting_with_empty_files_returns_zero(self):
        """CONVERTING with empty files should return 0%."""
        task = self._create_task(TaskStatus.CONVERTING, [])
        assert task.calculate_progress() == 0

    def test_converting_file_returns_default_progress(self):
        """CONVERTING file should return default 32% (midpoint of 0-65%)."""
        files = [self._create_file("f1", FileStatus.CONVERTING)]
        task = self._create_task(TaskStatus.CONVERTING, files)
        assert task.calculate_progress() == 32

    def test_converting_with_mixed_files(self):
        """CONVERTING with mixed file statuses should average correctly."""
        files = [
            self._create_file("f1", FileStatus.COMPLETED),  # 100%
            self._create_file("f2", FileStatus.CONVERTING),  # 32%
        ]
        task = self._create_task(TaskStatus.CONVERTING, files)
        # (100 + 32) / 2 = 66
        assert task.calculate_progress() == 66

    def test_converting_with_mixed_progress(self):
        """CONVERTING with mixed file statuses should average correctly.

        Note: conversion_progress is not tracked per-file in the current implementation.
        CONVERTING files always return 32% (midpoint of 0-65% range).
        """
        files = [
            self._create_file("f1", FileStatus.CONVERTING),  # 32%
            self._create_file("f2", FileStatus.CONVERTING),  # 32%
        ]
        task = self._create_task(TaskStatus.CONVERTING, files)
        # (32 + 32) / 2 = 32
        assert task.calculate_progress() == 32

    def test_completed_file_returns_hundred(self):
        """COMPLETED file should return 100%."""
        files = [self._create_file("f1", FileStatus.COMPLETED)]
        task = self._create_task(TaskStatus.CONVERTING, files)
        assert task.calculate_progress() == 100

    def test_failed_file_returns_hundred(self):
        """FAILED file should return 100% (terminal state)."""
        files = [self._create_file("f1", FileStatus.FAILED)]
        task = self._create_task(TaskStatus.CONVERTING, files)
        assert task.calculate_progress() == 100

    def test_downloaded_file_returns_hundred(self):
        """DOWNLOADED file should return 100%."""
        files = [self._create_file("f1", FileStatus.DOWNLOADED)]
        task = self._create_task(TaskStatus.COMPLETED, files)
        assert task.calculate_progress() == 100

    def test_pending_file_returns_zero(self):
        """PENDING file should return 0%."""
        files = [self._create_file("f1", FileStatus.PENDING)]
        task = self._create_task(TaskStatus.CONVERTING, files)
        assert task.calculate_progress() == 0

    # Boundary tests
    def test_single_file_progress(self):
        """Single file progress should be calculated correctly.

        Note: CONVERTING files always return 32% (midpoint of 0-65% range).
        """
        files = [self._create_file("f1", FileStatus.CONVERTING)]
        task = self._create_task(TaskStatus.CONVERTING, files)
        # CONVERTING = 32%
        assert task.calculate_progress() == 32

    def test_many_files_progress(self):
        """Many files progress should average correctly."""
        # 10 files: 5 COMPLETED (100%), 5 CONVERTING (32%)
        files = [self._create_file(f"f{i}", FileStatus.COMPLETED) for i in range(5)]
        files += [self._create_file(f"f{i}", FileStatus.CONVERTING) for i in range(5, 10)]
        task = self._create_task(TaskStatus.CONVERTING, files)
        # (5 * 100 + 5 * 32) / 10 = (500 + 160) / 10 = 66
        assert task.calculate_progress() == 66

    def _create_task(
        self,
        status: TaskStatus,
        files: list[AsyncFile],
        progress_percentage: int = 0,
    ) -> AsyncTask:
        """Helper to create a task with given status and files."""
        return AsyncTask(
            task_id="test-task",
            user_id="test-user",
            status=status,
            quality_preset="balanced",
            files=files,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            progress_percentage=progress_percentage,
        )

    def _create_file(
        self,
        file_id: str,
        status: FileStatus,
        quality_result: dict | None = None,
        verification_progress: int = 0,
    ) -> AsyncFile:
        """Helper to create a file with given status."""
        return AsyncFile(
            file_id=file_id,
            uuid=f"uuid-{file_id}",
            filename=f"{file_id}.mp4",
            source_s3_key=f"source/{file_id}.mp4",
            status=status,
            quality_result=quality_result,
            verification_progress=verification_progress,
        )


class TestAsyncTaskEstimateCompletionTime:
    """Test AsyncTask.estimate_completion_time() method."""

    def test_completed_task_returns_none(self):
        """COMPLETED task should return None."""
        task = self._create_task(TaskStatus.COMPLETED, [])
        assert task.estimate_completion_time() is None

    def test_partially_completed_task_returns_none(self):
        """PARTIALLY_COMPLETED task should return None."""
        task = self._create_task(TaskStatus.PARTIALLY_COMPLETED, [])
        assert task.estimate_completion_time() is None

    def test_failed_task_returns_none(self):
        """FAILED task should return None."""
        task = self._create_task(TaskStatus.FAILED, [])
        assert task.estimate_completion_time() is None

    def test_cancelled_task_returns_none(self):
        """CANCELLED task should return None."""
        task = self._create_task(TaskStatus.CANCELLED, [])
        assert task.estimate_completion_time() is None

    def test_empty_files_returns_none(self):
        """Task with no files should return None."""
        task = self._create_task(TaskStatus.CONVERTING, [])
        assert task.estimate_completion_time() is None

    def test_no_remaining_files_returns_now(self):
        """Task with no remaining files should return approximately now."""
        files = [
            self._create_file("f1", FileStatus.COMPLETED),
            self._create_file("f2", FileStatus.COMPLETED),
        ]
        task = self._create_task(TaskStatus.CONVERTING, files)
        result = task.estimate_completion_time()
        assert result is not None
        # Should be very close to now
        assert abs((result - datetime.now()).total_seconds()) < 2

    def test_single_remaining_file(self):
        """Task with single remaining file should estimate correctly."""
        files = [
            self._create_file("f1", FileStatus.COMPLETED),
            self._create_file("f2", FileStatus.CONVERTING),
        ]
        task = self._create_task(TaskStatus.CONVERTING, files, max_concurrent=5)
        result = task.estimate_completion_time(avg_conversion_time_per_file=60.0)
        assert result is not None
        # 1 remaining file, 1 batch, 60 seconds
        expected_delta = timedelta(seconds=60)
        actual_delta = result - datetime.now()
        assert abs(actual_delta.total_seconds() - expected_delta.total_seconds()) < 5

    def test_multiple_remaining_files_within_concurrency(self):
        """Task with remaining files within concurrency should estimate one batch."""
        files = [
            self._create_file("f1", FileStatus.CONVERTING),
            self._create_file("f2", FileStatus.CONVERTING),
            self._create_file("f3", FileStatus.CONVERTING),
        ]
        task = self._create_task(TaskStatus.CONVERTING, files, max_concurrent=5)
        result = task.estimate_completion_time(avg_conversion_time_per_file=60.0)
        assert result is not None
        # 3 remaining files, max_concurrent=5, so 1 batch
        expected_delta = timedelta(seconds=60)
        actual_delta = result - datetime.now()
        assert abs(actual_delta.total_seconds() - expected_delta.total_seconds()) < 5

    def test_multiple_remaining_files_exceeding_concurrency(self):
        """Task with remaining files exceeding concurrency should estimate multiple batches."""
        files = [self._create_file(f"f{i}", FileStatus.PENDING) for i in range(12)]
        task = self._create_task(TaskStatus.CONVERTING, files, max_concurrent=5)
        result = task.estimate_completion_time(avg_conversion_time_per_file=60.0)
        assert result is not None
        # 12 remaining files, max_concurrent=5, so 3 batches (5+5+2)
        expected_delta = timedelta(seconds=180)  # 3 * 60
        actual_delta = result - datetime.now()
        assert abs(actual_delta.total_seconds() - expected_delta.total_seconds()) < 5

    def test_pending_files_counted_as_remaining(self):
        """PENDING files should be counted as remaining."""
        files = [
            self._create_file("f1", FileStatus.PENDING),
            self._create_file("f2", FileStatus.PENDING),
        ]
        task = self._create_task(TaskStatus.PENDING, files, max_concurrent=5)
        result = task.estimate_completion_time(avg_conversion_time_per_file=60.0)
        assert result is not None

    def test_custom_avg_conversion_time(self):
        """Custom average conversion time should be used."""
        files = [self._create_file("f1", FileStatus.CONVERTING)]
        task = self._create_task(TaskStatus.CONVERTING, files, max_concurrent=5)
        result = task.estimate_completion_time(avg_conversion_time_per_file=300.0)
        assert result is not None
        expected_delta = timedelta(seconds=300)
        actual_delta = result - datetime.now()
        assert abs(actual_delta.total_seconds() - expected_delta.total_seconds()) < 5

    def _create_task(
        self,
        status: TaskStatus,
        files: list[AsyncFile],
        max_concurrent: int = 5,
    ) -> AsyncTask:
        """Helper to create a task with given status and files."""
        return AsyncTask(
            task_id="test-task",
            user_id="test-user",
            status=status,
            quality_preset="balanced",
            files=files,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            max_concurrent=max_concurrent,
        )

    def _create_file(self, file_id: str, status: FileStatus) -> AsyncFile:
        """Helper to create a file with given status."""
        return AsyncFile(
            file_id=file_id,
            uuid=f"uuid-{file_id}",
            filename=f"{file_id}.mp4",
            source_s3_key=f"source/{file_id}.mp4",
            status=status,
        )


class TestAsyncTaskGetFiles:
    """Test AsyncTask.get_completed_files() and get_failed_files() methods."""

    def test_get_completed_files_empty(self):
        """get_completed_files() with no completed files returns empty list."""
        files = [
            self._create_file("f1", FileStatus.CONVERTING),
            self._create_file("f2", FileStatus.FAILED),
        ]
        task = self._create_task(files)
        assert task.get_completed_files() == []

    def test_get_completed_files_some(self):
        """get_completed_files() returns only completed files."""
        files = [
            self._create_file("f1", FileStatus.COMPLETED),
            self._create_file("f2", FileStatus.FAILED),
            self._create_file("f3", FileStatus.COMPLETED),
        ]
        task = self._create_task(files)
        completed = task.get_completed_files()
        assert len(completed) == 2
        assert all(f.status == FileStatus.COMPLETED for f in completed)

    def test_get_failed_files_empty(self):
        """get_failed_files() with no failed files returns empty list."""
        files = [
            self._create_file("f1", FileStatus.COMPLETED),
            self._create_file("f2", FileStatus.CONVERTING),
        ]
        task = self._create_task(files)
        assert task.get_failed_files() == []

    def test_get_failed_files_some(self):
        """get_failed_files() returns only failed files."""
        files = [
            self._create_file("f1", FileStatus.COMPLETED),
            self._create_file("f2", FileStatus.FAILED),
            self._create_file("f3", FileStatus.FAILED),
        ]
        task = self._create_task(files)
        failed = task.get_failed_files()
        assert len(failed) == 2
        assert all(f.status == FileStatus.FAILED for f in failed)

    def _create_task(self, files: list[AsyncFile]) -> AsyncTask:
        """Helper to create a task with given files."""
        return AsyncTask(
            task_id="test-task",
            user_id="test-user",
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=files,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

    def _create_file(self, file_id: str, status: FileStatus) -> AsyncFile:
        """Helper to create a file with given status."""
        return AsyncFile(
            file_id=file_id,
            uuid=f"uuid-{file_id}",
            filename=f"{file_id}.mp4",
            source_s3_key=f"source/{file_id}.mp4",
            status=status,
        )


class TestAsyncTaskDictRoundtripAlternateRoute:
    """Task 1.4: 別経路検証テスト: to_dict/from_dict

    Verify that data serialized with to_dict can be read back correctly
    through an alternate route (JSON file, subprocess).
    """

    def test_to_dict_json_file_roundtrip(self, tmp_path: Path):
        """to_dict output saved to JSON file can be read back with from_dict."""
        task = self._create_sample_task()

        # Save to JSON file
        json_path = tmp_path / "task.json"
        with open(json_path, "w") as f:
            json.dump(task.to_dict(), f)

        # Read back from JSON file
        with open(json_path) as f:
            loaded_data = json.load(f)

        restored = AsyncTask.from_dict(loaded_data)

        # Verify key fields
        assert restored.task_id == task.task_id
        assert restored.user_id == task.user_id
        assert restored.status == task.status
        assert restored.quality_preset == task.quality_preset
        assert len(restored.files) == len(task.files)

    def test_to_dict_subprocess_roundtrip(self, tmp_path: Path):
        """to_dict output can be read by a separate Python process."""
        task = self._create_sample_task()

        # Save to JSON file
        json_path = tmp_path / "task.json"
        with open(json_path, "w") as f:
            json.dump(task.to_dict(), f)

        # Read and verify in subprocess
        verify_script = f'''
import json
import sys
sys.path.insert(0, "video-compression-optimizer/src")
from vco.models.async_task import AsyncTask

with open("{json_path}", "r") as f:
    data = json.load(f)

task = AsyncTask.from_dict(data)
assert task.task_id == "{task.task_id}"
assert task.status.value == "{task.status.value}"
print("OK")
'''
        result = subprocess.run(
            [sys.executable, "-c", verify_script],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Subprocess failed: {result.stderr}"
        assert "OK" in result.stdout

    def test_dynamodb_format_compatibility(self):
        """to_dict output is compatible with DynamoDB item format."""
        task = self._create_sample_task()
        data = task.to_dict()

        # DynamoDB requirements:
        # - All values must be JSON-serializable
        # - No None values in nested structures (handled by from_dict defaults)
        # - Datetime must be strings

        # Verify JSON serializable
        json_str = json.dumps(data)
        assert json_str is not None

        # Verify datetime fields are strings
        assert isinstance(data["created_at"], str)
        assert isinstance(data["updated_at"], str)

        # Verify status is string
        assert isinstance(data["status"], str)

        # Verify files are list of dicts
        assert isinstance(data["files"], list)
        for f in data["files"]:
            assert isinstance(f, dict)
            assert isinstance(f["status"], str)

    def _create_sample_task(self) -> AsyncTask:
        """Create a sample task for testing."""
        files = [
            AsyncFile(
                file_id="file-1",
                uuid="uuid-1",
                filename="video1.mp4",
                source_s3_key="source/video1.mp4",
                status=FileStatus.COMPLETED,
                quality_result={"ssim": 0.98},
            ),
            AsyncFile(
                file_id="file-2",
                uuid="uuid-2",
                filename="video2.mp4",
                source_s3_key="source/video2.mp4",
                status=FileStatus.FAILED,
                error_code=1010,
                error_message="Invalid input",
            ),
        ]
        return AsyncTask(
            task_id="test-task-123",
            user_id="test-user",
            status=TaskStatus.PARTIALLY_COMPLETED,
            quality_preset="balanced",
            files=files,
            created_at=datetime(2024, 1, 15, 10, 30, 0),
            updated_at=datetime(2024, 1, 15, 11, 45, 0),
            started_at=datetime(2024, 1, 15, 10, 31, 0),
            completed_at=datetime(2024, 1, 15, 11, 45, 0),
            execution_arn="arn:aws:states:ap-northeast-1:123456789:execution:test",
            progress_percentage=100,
            current_step="completed",
        )


class TestDownloadProgressMethods:
    """Test DownloadProgress property methods."""

    def test_is_complete_true_when_fully_downloaded(self):
        """is_complete should be True when downloaded_bytes >= total_bytes."""
        progress = DownloadProgress(
            task_id="task-1",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=1000,
            local_temp_path="/tmp/test.part",
            s3_key="test/key.mp4",
        )
        assert progress.is_complete is True

    def test_is_complete_true_when_over_downloaded(self):
        """is_complete should be True when downloaded_bytes > total_bytes."""
        progress = DownloadProgress(
            task_id="task-1",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=1100,
            local_temp_path="/tmp/test.part",
            s3_key="test/key.mp4",
        )
        assert progress.is_complete is True

    def test_is_complete_false_when_partial(self):
        """is_complete should be False when downloaded_bytes < total_bytes."""
        progress = DownloadProgress(
            task_id="task-1",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.part",
            s3_key="test/key.mp4",
        )
        assert progress.is_complete is False

    def test_progress_percentage_zero_when_empty(self):
        """progress_percentage should be 0 when total_bytes is 0."""
        progress = DownloadProgress(
            task_id="task-1",
            file_id="file-1",
            total_bytes=0,
            downloaded_bytes=0,
            local_temp_path="/tmp/test.part",
            s3_key="test/key.mp4",
        )
        assert progress.progress_percentage == 0

    def test_progress_percentage_fifty(self):
        """progress_percentage should be 50 when half downloaded."""
        progress = DownloadProgress(
            task_id="task-1",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=500,
            local_temp_path="/tmp/test.part",
            s3_key="test/key.mp4",
        )
        assert progress.progress_percentage == 50

    def test_progress_percentage_hundred(self):
        """progress_percentage should be 100 when fully downloaded."""
        progress = DownloadProgress(
            task_id="task-1",
            file_id="file-1",
            total_bytes=1000,
            downloaded_bytes=1000,
            local_temp_path="/tmp/test.part",
            s3_key="test/key.mp4",
        )
        assert progress.progress_percentage == 100


class TestFileStatusDownloaded:
    """Test FileStatus.DOWNLOADED for tracking downloaded files.

    Requirements: 4.4 - File status transitions to DOWNLOADED after download
    """

    def test_file_status_has_downloaded_value(self):
        """FileStatus enum should include DOWNLOADED value."""
        from vco.models.async_task import FileStatus

        assert hasattr(FileStatus, "DOWNLOADED")
        assert FileStatus.DOWNLOADED.value == "DOWNLOADED"

    def test_file_can_have_downloaded_status(self):
        """AsyncFile can have DOWNLOADED status."""
        from vco.models.async_task import FileStatus

        file = AsyncFile(
            file_id="file-1",
            uuid="uuid-1",
            filename="test.mp4",
            source_s3_key="source/test.mp4",
            status=FileStatus.DOWNLOADED,
        )
        assert file.status == FileStatus.DOWNLOADED

    def test_to_dict_includes_downloaded_status(self):
        """to_dict should serialize DOWNLOADED status correctly."""
        from vco.models.async_task import FileStatus

        file = AsyncFile(
            file_id="file-1",
            uuid="uuid-1",
            filename="test.mp4",
            source_s3_key="source/test.mp4",
            status=FileStatus.DOWNLOADED,
        )
        data = file.to_dict()
        assert data["status"] == "DOWNLOADED"

    def test_from_dict_restores_downloaded_status(self):
        """from_dict should restore DOWNLOADED status."""
        data = {
            "file_id": "file-1",
            "original_uuid": "uuid-1",
            "filename": "test.mp4",
            "source_s3_key": "source/test.mp4",
            "status": "DOWNLOADED",
        }
        file = AsyncFile.from_dict(data)
        assert file.status == FileStatus.DOWNLOADED
        assert file.status.value == "DOWNLOADED"

    def test_roundtrip_with_downloaded_status(self):
        """to_dict -> from_dict roundtrip should preserve DOWNLOADED status."""
        original = AsyncFile(
            file_id="file-1",
            uuid="uuid-1",
            filename="test.mp4",
            source_s3_key="source/test.mp4",
            status=FileStatus.DOWNLOADED,
        )
        data = original.to_dict()
        restored = AsyncFile.from_dict(data)

        assert restored.status.value == original.status.value
        assert restored.status == FileStatus.DOWNLOADED


class TestVerificationProgress:
    """Test verification_progress field for VERIFYING phase progress tracking.

    Requirements: 2.5, 2.6 - verification_progress field for detailed VERIFYING progress
    """

    def test_verification_progress_default_value(self):
        """verification_progress should default to 0."""
        file = AsyncFile(
            file_id="file-1",
            uuid="uuid-1",
            filename="test.mp4",
            source_s3_key="source/test.mp4",
        )
        assert file.verification_progress == 0

    def test_verification_progress_can_be_set(self):
        """verification_progress can be set to any value 0-100."""
        file = AsyncFile(
            file_id="file-1",
            uuid="uuid-1",
            filename="test.mp4",
            source_s3_key="source/test.mp4",
            verification_progress=50,
        )
        assert file.verification_progress == 50

    def test_to_dict_includes_verification_progress(self):
        """to_dict should include verification_progress field."""
        file = AsyncFile(
            file_id="file-1",
            uuid="uuid-1",
            filename="test.mp4",
            source_s3_key="source/test.mp4",
            verification_progress=75,
        )
        data = file.to_dict()
        assert "verification_progress" in data
        assert data["verification_progress"] == 75

    def test_from_dict_restores_verification_progress(self):
        """from_dict should restore verification_progress."""
        data = {
            "file_id": "file-1",
            "original_uuid": "uuid-1",
            "filename": "test.mp4",
            "source_s3_key": "source/test.mp4",
            "verification_progress": 30,
        }
        file = AsyncFile.from_dict(data)
        assert file.verification_progress == 30

    def test_from_dict_defaults_verification_progress_to_zero(self):
        """from_dict should default verification_progress to 0 if not present."""
        data = {
            "file_id": "file-1",
            "original_uuid": "uuid-1",
            "filename": "test.mp4",
            "source_s3_key": "source/test.mp4",
        }
        file = AsyncFile.from_dict(data)
        assert file.verification_progress == 0

    def test_roundtrip_with_verification_progress(self):
        """to_dict -> from_dict roundtrip should preserve verification_progress."""
        original = AsyncFile(
            file_id="file-1",
            uuid="uuid-1",
            filename="test.mp4",
            source_s3_key="source/test.mp4",
            status=FileStatus.VERIFYING,
            verification_progress=100,
        )
        data = original.to_dict()
        restored = AsyncFile.from_dict(data)

        assert restored.verification_progress == original.verification_progress
        assert restored.verification_progress == 100

    def test_verification_progress_boundary_values(self):
        """verification_progress should handle boundary values 0 and 100."""
        # Test 0
        file_zero = AsyncFile(
            file_id="file-1",
            uuid="uuid-1",
            filename="test.mp4",
            source_s3_key="source/test.mp4",
            verification_progress=0,
        )
        assert file_zero.verification_progress == 0
        data_zero = file_zero.to_dict()
        assert data_zero["verification_progress"] == 0

        # Test 100
        file_hundred = AsyncFile(
            file_id="file-2",
            uuid="uuid-2",
            filename="test2.mp4",
            source_s3_key="source/test2.mp4",
            verification_progress=100,
        )
        assert file_hundred.verification_progress == 100
        data_hundred = file_hundred.to_dict()
        assert data_hundred["verification_progress"] == 100
