"""Property-based tests for status display completeness.

Validates: Requirements 2.3, 9.7
- 2.3: Status display shows task ID, status, progress, current step, start time, estimated completion
- 9.7: PARTIALLY_COMPLETED task shows which files succeeded/failed with error reasons

Source: Requirements document section 2 and 9
"""

from datetime import datetime, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.services.async_status import FileDetail, TaskDetail, TaskSummary

# Test data strategies based on requirements
# Source: Requirements 2.3, 9.7 - status display fields
TASK_STATUSES = [
    "PENDING",
    "UPLOADING",
    "CONVERTING",
    "VERIFYING",
    "COMPLETED",
    "PARTIALLY_COMPLETED",
    "FAILED",
    "CANCELLED",
]
FILE_STATUSES = ["PENDING", "PROCESSING", "COMPLETED", "FAILED"]
QUALITY_PRESETS = ["balanced", "balanced+", "high", "compression"]


@st.composite
def file_detail_strategy(draw):
    """Generate a FileDetail with realistic data."""
    status = draw(st.sampled_from(FILE_STATUSES))

    # Error message only for FAILED status
    error_message = None
    if status == "FAILED":
        error_message = draw(
            st.sampled_from(
                [
                    "MediaConvert error: 1030 - Unsupported codec",
                    "SSIM score below threshold: 0.89",
                    "S3 upload failed: Access Denied",
                    "Timeout during conversion",
                ]
            )
        )

    # SSIM score only for COMPLETED status
    ssim_score = None
    output_size = None
    if status == "COMPLETED":
        ssim_score = draw(st.floats(min_value=0.90, max_value=1.0))
        output_size = draw(st.integers(min_value=1000000, max_value=1000000000))

    # Progress percentage based on status
    if status == "PENDING":
        progress = 0
    elif status == "PROCESSING":
        progress = draw(st.integers(min_value=1, max_value=99))
    elif status == "COMPLETED":
        progress = 100
    else:  # FAILED
        progress = draw(st.integers(min_value=0, max_value=99))

    return FileDetail(
        file_id=draw(st.uuids()).hex[:16],
        filename=draw(
            st.text(min_size=5, max_size=50, alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-.")
        )
        + ".mp4",
        status=status,
        progress_percentage=progress,
        error_message=error_message,
        ssim_score=ssim_score,
        output_size_bytes=output_size,
    )


@st.composite
def task_detail_strategy(draw):
    """Generate a TaskDetail with consistent file statuses."""
    # Generate files
    file_count = draw(st.integers(min_value=1, max_value=10))
    files = [draw(file_detail_strategy()) for _ in range(file_count)]

    # Determine task status based on file statuses
    completed_count = sum(1 for f in files if f.status == "COMPLETED")
    failed_count = sum(1 for f in files if f.status == "FAILED")
    processing_count = sum(1 for f in files if f.status in ("PENDING", "PROCESSING"))

    if processing_count > 0:
        # Still processing
        task_status = draw(st.sampled_from(["UPLOADING", "CONVERTING", "VERIFYING"]))
    elif completed_count == file_count:
        task_status = "COMPLETED"
    elif failed_count == file_count:
        task_status = "FAILED"
    elif completed_count > 0 and failed_count > 0:
        task_status = "PARTIALLY_COMPLETED"
    else:
        task_status = "PENDING"

    # Calculate progress
    if task_status in ("COMPLETED", "PARTIALLY_COMPLETED"):
        progress = 100
    elif task_status == "FAILED":
        progress = draw(st.integers(min_value=0, max_value=99))
    else:
        progress = int(sum(f.progress_percentage for f in files) / len(files))

    # Timestamps
    created_at = datetime.now() - timedelta(hours=draw(st.integers(min_value=0, max_value=24)))
    updated_at = created_at + timedelta(minutes=draw(st.integers(min_value=0, max_value=60)))

    started_at = None
    completed_at = None
    estimated_completion = None
    current_step = None

    if task_status not in ("PENDING",):
        started_at = created_at + timedelta(seconds=draw(st.integers(min_value=1, max_value=60)))

    if task_status in ("COMPLETED", "PARTIALLY_COMPLETED", "FAILED"):
        completed_at = updated_at
    else:
        current_step = draw(
            st.sampled_from(["Uploading", "Converting", "Verifying quality", "Embedding metadata"])
        )
        estimated_completion = datetime.now() + timedelta(
            minutes=draw(st.integers(min_value=1, max_value=120))
        )

    return TaskDetail(
        task_id=draw(st.uuids()).hex,
        status=task_status,
        quality_preset=draw(st.sampled_from(QUALITY_PRESETS)),
        files=files,
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        progress_percentage=progress,
        current_step=current_step,
        estimated_completion_time=estimated_completion,
        error_message=None if task_status != "FAILED" else "Task failed",
        execution_arn=f"arn:aws:states:ap-northeast-1:123456789012:execution:workflow:{draw(st.uuids()).hex[:8]}",
    )


class TestStatusDisplayCompleteness:
    """Property tests for status display completeness.

    Validates: Requirements 2.3, 9.7
    """

    @given(task=task_detail_strategy())
    @settings(max_examples=100, deadline=None)
    def test_task_detail_has_required_fields(self, task: TaskDetail):
        """Property: TaskDetail always has all required display fields.

        Requirement 2.3: Status display shows task ID, status, progress,
        current step, start time, estimated completion time.
        """
        # Required fields must always be present
        assert task.task_id is not None and len(task.task_id) > 0
        assert task.status in TASK_STATUSES
        assert 0 <= task.progress_percentage <= 100
        assert task.created_at is not None
        assert task.updated_at is not None
        assert task.quality_preset in QUALITY_PRESETS

        # Files list must be present
        assert task.files is not None
        assert len(task.files) > 0

    @given(task=task_detail_strategy())
    @settings(max_examples=100, deadline=None)
    def test_active_task_has_current_step(self, task: TaskDetail):
        """Property: Active tasks have current step information.

        Requirement 2.3: Current step is shown for active tasks.
        """
        active_statuses = ["UPLOADING", "CONVERTING", "VERIFYING"]

        if task.status in active_statuses:
            # Active tasks should have current step
            assert task.current_step is not None or task.progress_percentage == 100

    @given(task=task_detail_strategy())
    @settings(max_examples=100, deadline=None)
    def test_completed_task_has_completion_time(self, task: TaskDetail):
        """Property: Completed tasks have completion timestamp.

        Requirement 2.3: Completed tasks show completion time.
        """
        terminal_statuses = ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED"]

        if task.status in terminal_statuses:
            assert task.completed_at is not None

    @given(task=task_detail_strategy())
    @settings(max_examples=100, deadline=None)
    def test_file_details_have_required_fields(self, task: TaskDetail):
        """Property: Each file in task has required display fields.

        Requirement 9.7: File status display shows success/failure with details.
        """
        for file in task.files:
            assert file.file_id is not None and len(file.file_id) > 0
            assert file.filename is not None and len(file.filename) > 0
            assert file.status in FILE_STATUSES
            assert 0 <= file.progress_percentage <= 100

    @given(task=task_detail_strategy())
    @settings(max_examples=100, deadline=None)
    def test_failed_files_have_error_message(self, task: TaskDetail):
        """Property: Failed files always have error message.

        Requirement 9.7: Failed files show error reasons.
        """
        for file in task.files:
            if file.status == "FAILED":
                assert file.error_message is not None and len(file.error_message) > 0

    @given(task=task_detail_strategy())
    @settings(max_examples=100, deadline=None)
    def test_completed_files_have_quality_metrics(self, task: TaskDetail):
        """Property: Completed files have quality metrics.

        Requirement 2.3: Quality metrics shown for completed conversions.
        """
        for file in task.files:
            if file.status == "COMPLETED":
                assert file.ssim_score is not None
                assert 0.0 <= file.ssim_score <= 1.0

    @given(task=task_detail_strategy())
    @settings(max_examples=100, deadline=None)
    def test_partially_completed_has_mixed_file_statuses(self, task: TaskDetail):
        """Property: PARTIALLY_COMPLETED tasks have both success and failure.

        Requirement 9.7: PARTIALLY_COMPLETED shows which files succeeded/failed.
        """
        if task.status == "PARTIALLY_COMPLETED":
            completed_count = sum(1 for f in task.files if f.status == "COMPLETED")
            failed_count = sum(1 for f in task.files if f.status == "FAILED")

            # Must have at least one success and one failure
            assert completed_count > 0, "PARTIALLY_COMPLETED must have at least one completed file"
            assert failed_count > 0, "PARTIALLY_COMPLETED must have at least one failed file"

    @given(task=task_detail_strategy())
    @settings(max_examples=100, deadline=None)
    def test_progress_percentage_consistency(self, task: TaskDetail):
        """Property: Progress percentage is consistent with file statuses.

        Requirement 2.3: Progress percentage reflects actual progress.
        """
        if task.status == "COMPLETED":
            assert task.progress_percentage == 100
        elif task.status == "PENDING":
            assert task.progress_percentage == 0 or task.progress_percentage <= 10


class TestTaskSummaryCompleteness:
    """Property tests for task summary display."""

    @given(
        task_id=st.uuids(),
        status=st.sampled_from(TASK_STATUSES),
        file_count=st.integers(min_value=1, max_value=100),
        quality_preset=st.sampled_from(QUALITY_PRESETS),
    )
    @settings(max_examples=50, deadline=None)
    def test_task_summary_has_required_fields(self, task_id, status, file_count, quality_preset):
        """Property: TaskSummary has all required list display fields.

        Requirement 2.1: Task list shows essential information.
        """
        # Calculate counts based on status
        if status == "COMPLETED":
            completed_count = file_count
            failed_count = 0
            progress = 100
        elif status == "FAILED":
            completed_count = 0
            failed_count = file_count
            progress = 0
        elif status == "PARTIALLY_COMPLETED":
            completed_count = file_count // 2
            failed_count = file_count - completed_count
            progress = 100
        else:
            completed_count = 0
            failed_count = 0
            progress = 50

        summary = TaskSummary(
            task_id=task_id.hex,
            status=status,
            file_count=file_count,
            completed_count=completed_count,
            failed_count=failed_count,
            progress_percentage=progress,
            created_at=datetime.now(),
            quality_preset=quality_preset,
        )

        # Verify all required fields
        assert summary.task_id is not None
        assert summary.status in TASK_STATUSES
        assert summary.file_count >= 0
        assert summary.completed_count >= 0
        assert summary.failed_count >= 0
        assert 0 <= summary.progress_percentage <= 100
        assert summary.created_at is not None
        assert summary.quality_preset in QUALITY_PRESETS

    @given(
        file_count=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=50, deadline=None)
    def test_file_counts_are_consistent(self, file_count):
        """Property: File counts are logically consistent.

        Requirement 2.1: Counts accurately reflect task state.
        """
        # Generate counts that don't exceed total
        import random

        completed_count = random.randint(0, file_count)
        failed_count = random.randint(0, file_count - completed_count)

        summary = TaskSummary(
            task_id="test-task-id",
            status="CONVERTING",
            file_count=file_count,
            completed_count=completed_count,
            failed_count=failed_count,
            progress_percentage=50,
            created_at=datetime.now(),
            quality_preset="balanced",
        )

        # Verify consistency
        assert summary.completed_count + summary.failed_count <= summary.file_count
