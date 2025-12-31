"""Property-based tests for task status aggregation.

**Property 2: タスク状態集約の正確性**
**Validates: Requirements 9.2, 9.3, 9.4**

Tests that aggregate_task_status correctly determines overall task status
based on individual file statuses:
- All COMPLETED → COMPLETED
- All FAILED → FAILED
- Mixed → PARTIALLY_COMPLETED
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.models.async_task import (
    FileStatus,
    TaskStatus,
    aggregate_task_status,
)


class TestTaskStatusAggregation:
    """Test aggregate_task_status function."""

    # Property 2: タスク状態集約の正確性
    # **Validates: Requirements 9.2, 9.3, 9.4**

    @given(count=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_all_completed_returns_completed(self, count: int):
        """When all files are COMPLETED, task status should be COMPLETED.

        Validates: Requirement 9.2 - 全成功 → COMPLETED
        """
        file_statuses = [FileStatus.COMPLETED] * count
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.COMPLETED

    @given(count=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_all_failed_returns_failed(self, count: int):
        """When all files are FAILED, task status should be FAILED.

        Validates: Requirement 9.4 - 全失敗 → FAILED
        """
        file_statuses = [FileStatus.FAILED] * count
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.FAILED

    @given(
        completed_count=st.integers(min_value=1, max_value=50),
        failed_count=st.integers(min_value=1, max_value=50),
    )
    @settings(max_examples=50)
    def test_mixed_returns_partially_completed(self, completed_count: int, failed_count: int):
        """When some files succeed and some fail, status should be PARTIALLY_COMPLETED.

        Validates: Requirement 9.3 - 一部成功 → PARTIALLY_COMPLETED
        """
        file_statuses = [FileStatus.COMPLETED] * completed_count + [
            FileStatus.FAILED
        ] * failed_count
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_empty_list_returns_failed(self):
        """Empty file list should return FAILED.

        Edge case: no files to process is considered a failure.
        """
        result = aggregate_task_status([])
        assert result == TaskStatus.FAILED

    @given(
        completed_count=st.integers(min_value=0, max_value=50),
        failed_count=st.integers(min_value=0, max_value=50),
    )
    @settings(max_examples=100)
    def test_aggregation_invariants(self, completed_count: int, failed_count: int):
        """Test invariants of status aggregation.

        Invariants:
        1. Result is always one of: COMPLETED, FAILED, PARTIALLY_COMPLETED
        2. COMPLETED only when all files completed
        3. FAILED only when all files failed (or empty)
        4. PARTIALLY_COMPLETED when mixed results
        """
        # Skip empty case (handled separately)
        if completed_count == 0 and failed_count == 0:
            return

        file_statuses = [FileStatus.COMPLETED] * completed_count + [
            FileStatus.FAILED
        ] * failed_count
        result = aggregate_task_status(file_statuses)

        # Invariant 1: Result is one of the expected statuses
        assert result in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.PARTIALLY_COMPLETED)

        # Invariant 2: COMPLETED only when all completed
        if result == TaskStatus.COMPLETED:
            assert failed_count == 0
            assert completed_count > 0

        # Invariant 3: FAILED only when all failed
        if result == TaskStatus.FAILED:
            assert completed_count == 0

        # Invariant 4: PARTIALLY_COMPLETED when mixed
        if result == TaskStatus.PARTIALLY_COMPLETED:
            assert completed_count > 0
            assert failed_count > 0

    @given(
        completed_count=st.integers(min_value=0, max_value=20),
        failed_count=st.integers(min_value=0, max_value=20),
        pending_count=st.integers(min_value=0, max_value=20),
        processing_count=st.integers(min_value=0, max_value=20),
    )
    @settings(max_examples=100)
    def test_aggregation_with_all_statuses(
        self, completed_count: int, failed_count: int, pending_count: int, processing_count: int
    ):
        """Test aggregation with all possible file statuses.

        Note: PENDING and PROCESSING files are not counted as completed or failed.
        The aggregation only considers terminal states (COMPLETED, FAILED).
        """
        total = completed_count + failed_count + pending_count + processing_count
        if total == 0:
            return

        file_statuses = (
            [FileStatus.COMPLETED] * completed_count
            + [FileStatus.FAILED] * failed_count
            + [FileStatus.PENDING] * pending_count
            + [FileStatus.CONVERTING] * processing_count
        )
        result = aggregate_task_status(file_statuses)

        # Result should be valid
        assert result in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.PARTIALLY_COMPLETED)

        # If all are COMPLETED, result should be COMPLETED
        if completed_count == total:
            assert result == TaskStatus.COMPLETED

        # If all are FAILED, result should be FAILED
        if failed_count == total:
            assert result == TaskStatus.FAILED


class TestTaskStatusAggregationEdgeCases:
    """Test edge cases for task status aggregation."""

    def test_single_completed_file(self):
        """Single completed file should return COMPLETED."""
        result = aggregate_task_status([FileStatus.COMPLETED])
        assert result == TaskStatus.COMPLETED

    def test_single_failed_file(self):
        """Single failed file should return FAILED."""
        result = aggregate_task_status([FileStatus.FAILED])
        assert result == TaskStatus.FAILED

    def test_one_completed_one_failed(self):
        """One completed and one failed should return PARTIALLY_COMPLETED."""
        result = aggregate_task_status([FileStatus.COMPLETED, FileStatus.FAILED])
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_many_completed_one_failed(self):
        """Many completed with one failed should return PARTIALLY_COMPLETED."""
        file_statuses = [FileStatus.COMPLETED] * 99 + [FileStatus.FAILED]
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_one_completed_many_failed(self):
        """One completed with many failed should return PARTIALLY_COMPLETED."""
        file_statuses = [FileStatus.COMPLETED] + [FileStatus.FAILED] * 99
        result = aggregate_task_status(file_statuses)
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_pending_only_returns_partially_completed(self):
        """Only PENDING files should return PARTIALLY_COMPLETED (not terminal)."""
        # This is an edge case - if only PENDING files exist,
        # neither completed_count nor failed_count equals total
        result = aggregate_task_status([FileStatus.PENDING, FileStatus.PENDING])
        assert result == TaskStatus.PARTIALLY_COMPLETED

    def test_converting_only_returns_partially_completed(self):
        """Only CONVERTING files should return PARTIALLY_COMPLETED (not terminal)."""
        result = aggregate_task_status([FileStatus.CONVERTING, FileStatus.CONVERTING])
        assert result == TaskStatus.PARTIALLY_COMPLETED
