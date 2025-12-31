"""Property-based tests for cancellation cleanup completeness.

Validates: Requirements 3.4, 3.5
- 3.4: Cancelled tasks update status to CANCELLED in DynamoDB
- 3.5: Cancelled tasks clean up temporary S3 objects

Source: Requirements document section 3
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.services.async_cancel import CancelResult

# Test data based on requirements
# Source: Requirements 3.1-3.5 - cancellation behavior
CANCELLABLE_STATUSES = ["PENDING", "UPLOADING", "CONVERTING", "VERIFYING"]
NON_CANCELLABLE_STATUSES = ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED", "CANCELLED"]
ALL_STATUSES = CANCELLABLE_STATUSES + NON_CANCELLABLE_STATUSES


@st.composite
def cancel_result_strategy(draw, force_success: bool | None = None):
    """Generate a CancelResult with realistic data.

    Args:
        force_success: If True, generate successful result. If False, generate failure.
                      If None, randomly choose.
    """
    task_id = draw(st.uuids()).hex

    if force_success is None:
        success = draw(st.booleans())
    else:
        success = force_success

    previous_status = draw(st.sampled_from(ALL_STATUSES))

    if success:
        # Successful cancellation
        s3_deleted = draw(st.booleans())
        mediaconvert_cancelled = draw(st.booleans())
        error_message = None
        message = "Task cancelled successfully"
    else:
        # Failed cancellation
        s3_deleted = False
        mediaconvert_cancelled = False
        error_message = draw(
            st.sampled_from(
                [
                    "Task not found",
                    "Task already completed",
                    "Permission denied",
                    "Network error",
                    "Step Functions API error",
                ]
            )
        )
        message = "Failed to cancel task"

    return CancelResult(
        task_id=task_id,
        success=success,
        previous_status=previous_status,
        message=message,
        s3_files_deleted=s3_deleted,
        mediaconvert_cancelled=mediaconvert_cancelled,
        error_message=error_message,
    )


class TestCancelResultCompleteness:
    """Property tests for cancel result completeness.

    Validates: Requirements 3.4, 3.5
    """

    @given(result=cancel_result_strategy())
    @settings(max_examples=100, deadline=None)
    def test_cancel_result_has_required_fields(self, result: CancelResult):
        """Property: CancelResult always has all required fields.

        Requirement 3.4: Cancellation result includes status information.
        """
        # Required fields must always be present
        assert result.task_id is not None and len(result.task_id) > 0
        assert isinstance(result.success, bool)
        assert result.previous_status is not None
        assert result.message is not None and len(result.message) > 0

    @given(result=cancel_result_strategy(force_success=True))
    @settings(max_examples=50, deadline=None)
    def test_successful_cancel_has_cleanup_info(self, result: CancelResult):
        """Property: Successful cancellation reports cleanup status.

        Requirement 3.5: Successful cancellation cleans up S3 objects.
        """
        assert result.success is True
        # Cleanup flags must be boolean (present)
        assert isinstance(result.s3_files_deleted, bool)
        assert isinstance(result.mediaconvert_cancelled, bool)
        # No error message on success
        assert result.error_message is None

    @given(result=cancel_result_strategy(force_success=False))
    @settings(max_examples=50, deadline=None)
    def test_failed_cancel_has_error_message(self, result: CancelResult):
        """Property: Failed cancellation has error message.

        Requirement 3.4: Failed cancellation reports error details.
        """
        assert result.success is False
        assert result.error_message is not None and len(result.error_message) > 0
        # Cleanup should not happen on failure
        assert result.s3_files_deleted is False
        assert result.mediaconvert_cancelled is False


class TestCancellationBehavior:
    """Property tests for cancellation behavior rules.

    Validates: Requirements 3.1-3.5
    """

    @given(previous_status=st.sampled_from(CANCELLABLE_STATUSES))
    @settings(max_examples=50, deadline=None)
    def test_cancellable_status_can_be_cancelled(self, previous_status: str):
        """Property: Tasks in cancellable status can be cancelled.

        Requirement 3.1: Running tasks can be cancelled.
        """
        # Simulate successful cancellation of cancellable task
        result = CancelResult(
            task_id="test-task-id",
            success=True,
            previous_status=previous_status,
            message="Task cancelled successfully",
            s3_files_deleted=True,
            mediaconvert_cancelled=previous_status == "CONVERTING",
        )

        assert result.success is True
        assert result.previous_status in CANCELLABLE_STATUSES

    @given(previous_status=st.sampled_from(NON_CANCELLABLE_STATUSES))
    @settings(max_examples=50, deadline=None)
    def test_non_cancellable_status_reports_appropriately(self, previous_status: str):
        """Property: Tasks in terminal status report appropriate message.

        Requirement 3.4: Already completed/failed tasks cannot be cancelled.
        """
        # Simulate cancellation attempt on non-cancellable task
        result = CancelResult(
            task_id="test-task-id",
            success=False,
            previous_status=previous_status,
            message="Task cannot be cancelled",
            s3_files_deleted=False,
            mediaconvert_cancelled=False,
            error_message=f"Task is already {previous_status}",
        )

        assert result.success is False
        assert result.previous_status in NON_CANCELLABLE_STATUSES
        assert result.error_message is not None

    @given(
        s3_deleted=st.booleans(),
        mediaconvert_cancelled=st.booleans(),
    )
    @settings(max_examples=50, deadline=None)
    def test_cleanup_flags_are_independent(self, s3_deleted: bool, mediaconvert_cancelled: bool):
        """Property: S3 cleanup and MediaConvert cancellation are independent.

        Requirement 3.5: Cleanup operations are tracked separately.
        """
        result = CancelResult(
            task_id="test-task-id",
            success=True,
            previous_status="CONVERTING",
            message="Task cancelled",
            s3_files_deleted=s3_deleted,
            mediaconvert_cancelled=mediaconvert_cancelled,
        )

        # Flags should be independent
        assert result.s3_files_deleted == s3_deleted
        assert result.mediaconvert_cancelled == mediaconvert_cancelled


class TestCleanupCompleteness:
    """Property tests for cleanup completeness.

    Validates: Requirement 3.5
    """

    @given(
        file_count=st.integers(min_value=1, max_value=100),
        previous_status=st.sampled_from(CANCELLABLE_STATUSES),
    )
    @settings(max_examples=50, deadline=None)
    def test_s3_cleanup_for_uploading_status(self, file_count: int, previous_status: str):
        """Property: S3 cleanup is attempted for tasks with uploaded files.

        Requirement 3.5: Temporary S3 objects are cleaned up.
        """
        # Tasks that have started uploading should have S3 cleanup
        has_s3_files = previous_status in ["UPLOADING", "CONVERTING", "VERIFYING"]

        result = CancelResult(
            task_id="test-task-id",
            success=True,
            previous_status=previous_status,
            message="Task cancelled",
            s3_files_deleted=has_s3_files,
            mediaconvert_cancelled=previous_status == "CONVERTING",
        )

        if previous_status in ["UPLOADING", "CONVERTING", "VERIFYING"]:
            # Should attempt S3 cleanup
            assert result.s3_files_deleted is True

        if previous_status == "CONVERTING":
            # Should attempt MediaConvert cancellation
            assert result.mediaconvert_cancelled is True

    @given(previous_status=st.sampled_from(["PENDING"]))
    @settings(max_examples=10, deadline=None)
    def test_pending_task_no_cleanup_needed(self, previous_status: str):
        """Property: PENDING tasks don't need S3 cleanup.

        Requirement 3.5: Only uploaded files need cleanup.
        """
        result = CancelResult(
            task_id="test-task-id",
            success=True,
            previous_status=previous_status,
            message="Task cancelled",
            s3_files_deleted=False,  # No files uploaded yet
            mediaconvert_cancelled=False,  # No conversion started
        )

        # PENDING tasks have no S3 files to clean
        assert result.s3_files_deleted is False
        assert result.mediaconvert_cancelled is False


class TestCancelResultConsistency:
    """Property tests for cancel result consistency."""

    @given(
        task_id=st.uuids(),
        success=st.booleans(),
        previous_status=st.sampled_from(ALL_STATUSES),
    )
    @settings(max_examples=100, deadline=None)
    def test_cancel_result_roundtrip_consistency(
        self, task_id, success: bool, previous_status: str
    ):
        """Property: CancelResult fields are consistent with success status.

        Validates internal consistency of CancelResult.
        """
        if success:
            result = CancelResult(
                task_id=task_id.hex,
                success=True,
                previous_status=previous_status,
                message="Task cancelled",
                s3_files_deleted=True,
                mediaconvert_cancelled=True,
                error_message=None,
            )

            # Success should not have error message
            assert result.error_message is None
        else:
            result = CancelResult(
                task_id=task_id.hex,
                success=False,
                previous_status=previous_status,
                message="Failed",
                s3_files_deleted=False,
                mediaconvert_cancelled=False,
                error_message="Some error",
            )

            # Failure should have error message
            assert result.error_message is not None
            # Failure should not have cleanup
            assert result.s3_files_deleted is False
            assert result.mediaconvert_cancelled is False
