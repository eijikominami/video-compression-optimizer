"""Large data tests for task status aggregation.

Task 2.3: 大量データテスト: 状態集約
- 100+ ファイルでの状態集約パフォーマンス
- 境界ケース: 1 ファイル成功 + 99 ファイル失敗

Requirements: 9.2, 9.3, 9.4
"""

import time

from vco.models.async_task import (
    FileStatus,
    TaskStatus,
    aggregate_task_status,
)


class TestTaskStatusAggregationLargeData:
    """Test aggregate_task_status with large data sets."""

    def test_100_files_all_completed(self):
        """100 completed files should return COMPLETED."""
        file_statuses = [FileStatus.COMPLETED] * 100
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.COMPLETED

    def test_100_files_all_failed(self):
        """100 failed files should return FAILED."""
        file_statuses = [FileStatus.FAILED] * 100
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.FAILED

    def test_1_completed_99_failed(self):
        """1 completed + 99 failed should return PARTIALLY_COMPLETED."""
        file_statuses = [FileStatus.COMPLETED] + [FileStatus.FAILED] * 99
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_99_completed_1_failed(self):
        """99 completed + 1 failed should return PARTIALLY_COMPLETED."""
        file_statuses = [FileStatus.COMPLETED] * 99 + [FileStatus.FAILED]
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_1000_files_performance(self):
        """1000 files should complete in reasonable time (<100ms)."""
        file_statuses = [FileStatus.COMPLETED] * 500 + [FileStatus.FAILED] * 500

        start_time = time.perf_counter()
        result = aggregate_task_status(file_statuses)
        elapsed_time = time.perf_counter() - start_time

        assert result == TaskStatus.PARTIALLY_COMPLETED
        assert elapsed_time < 0.1, f"Aggregation took {elapsed_time:.3f}s, expected <0.1s"

    def test_10000_files_performance(self):
        """10000 files should complete in reasonable time (<500ms)."""
        file_statuses = [FileStatus.COMPLETED] * 5000 + [FileStatus.FAILED] * 5000

        start_time = time.perf_counter()
        result = aggregate_task_status(file_statuses)
        elapsed_time = time.perf_counter() - start_time

        assert result == TaskStatus.PARTIALLY_COMPLETED
        assert elapsed_time < 0.5, f"Aggregation took {elapsed_time:.3f}s, expected <0.5s"

    def test_mixed_statuses_large(self):
        """Large mixed status list should aggregate correctly."""
        # 25 of each status
        file_statuses = (
            [FileStatus.COMPLETED] * 25
            + [FileStatus.FAILED] * 25
            + [FileStatus.PENDING] * 25
            + [FileStatus.CONVERTING] * 25
        )
        result = aggregate_task_status(file_statuses)
        # Has both COMPLETED and FAILED, so PARTIALLY_COMPLETED
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_boundary_50_50_split(self):
        """Exactly 50-50 split should return PARTIALLY_COMPLETED."""
        file_statuses = [FileStatus.COMPLETED] * 50 + [FileStatus.FAILED] * 50
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_single_file_completed(self):
        """Single completed file should return COMPLETED."""
        result = aggregate_task_status([FileStatus.COMPLETED])
        assert result == TaskStatus.COMPLETED

    def test_single_file_failed(self):
        """Single failed file should return FAILED."""
        result = aggregate_task_status([FileStatus.FAILED])
        assert result == TaskStatus.FAILED

    def test_alternating_statuses(self):
        """Alternating COMPLETED/FAILED should return PARTIALLY_COMPLETED."""
        file_statuses = []
        for i in range(100):
            if i % 2 == 0:
                file_statuses.append(FileStatus.COMPLETED)
            else:
                file_statuses.append(FileStatus.FAILED)
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_mostly_pending_with_one_completed(self):
        """Mostly PENDING with one COMPLETED should return PARTIALLY_COMPLETED."""
        file_statuses = [FileStatus.PENDING] * 99 + [FileStatus.COMPLETED]
        result = aggregate_task_status(file_statuses)
        # Not all COMPLETED, not all FAILED
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_mostly_pending_with_one_failed(self):
        """Mostly PENDING with one FAILED should return PARTIALLY_COMPLETED."""
        file_statuses = [FileStatus.PENDING] * 99 + [FileStatus.FAILED]
        result = aggregate_task_status(file_statuses)
        # Not all COMPLETED, not all FAILED
        assert result == TaskStatus.PARTIALLY_COMPLETED
