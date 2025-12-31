"""Unit tests for progress calculation.

Tests: Task 6.1 - Progress calculation function
Requirements: 6.1, 6.2, 6.5
"""

from vco.utils.progress import (
    PROGRESS_COMPLETED,
    PROGRESS_CONVERTING_MIDPOINT,
    PROGRESS_VERIFYING,
    calculate_progress,
    calculate_progress_simple,
)


class TestCalculateProgressBasic:
    """Basic tests for calculate_progress function."""

    def test_empty_files_returns_zero(self):
        """Test that empty file list returns 0% progress."""
        progress, step = calculate_progress([])
        assert progress == 0
        assert step == "pending"

    def test_single_pending_file(self):
        """Test progress for single PENDING file."""
        files = [{"status": "PENDING"}]
        progress, step = calculate_progress(files)
        assert progress == 0
        assert step == "pending"

    def test_single_completed_file(self):
        """Test progress for single COMPLETED file."""
        files = [{"status": "COMPLETED"}]
        progress, step = calculate_progress(files)
        assert progress == 100
        assert step == "completed"

    def test_single_failed_file(self):
        """Test progress for single FAILED file."""
        files = [{"status": "FAILED"}]
        progress, step = calculate_progress(files)
        assert progress == 100
        assert step == "completed"

    def test_single_verifying_file(self):
        """Test progress for single VERIFYING file."""
        files = [{"status": "VERIFYING"}]
        progress, step = calculate_progress(files)
        assert progress == PROGRESS_VERIFYING
        assert step == "verifying"


class TestCalculateProgressConverting:
    """Tests for CONVERTING status progress calculation."""

    def test_converting_without_callback_uses_midpoint(self):
        """Test CONVERTING status uses midpoint when no callback provided."""
        files = [{"status": "CONVERTING"}]
        progress, step = calculate_progress(files)
        assert progress == PROGRESS_CONVERTING_MIDPOINT
        assert step == "converting"

    def test_converting_with_callback(self):
        """Test CONVERTING status uses callback when provided."""
        files = [{"status": "CONVERTING", "mediaconvert_job_id": "job-123"}]

        def mock_get_progress(job_id: str) -> int:
            return 50  # 50% MediaConvert progress

        progress, step = calculate_progress(files, get_mediaconvert_progress=mock_get_progress)
        # 50% * 0.3 = 15%
        assert progress == 15
        assert step == "converting"

    def test_converting_with_callback_full_progress(self):
        """Test CONVERTING at 100% MediaConvert progress."""
        files = [{"status": "CONVERTING", "mediaconvert_job_id": "job-123"}]

        def mock_get_progress(job_id: str) -> int:
            return 100

        progress, step = calculate_progress(files, get_mediaconvert_progress=mock_get_progress)
        # 100% * 0.3 = 30%
        assert progress == 30
        assert step == "converting"

    def test_converting_without_job_id(self):
        """Test CONVERTING without job_id returns 0% even with callback."""
        files = [{"status": "CONVERTING"}]  # No mediaconvert_job_id

        def mock_get_progress(job_id: str) -> int:
            return 50

        progress, step = calculate_progress(files, get_mediaconvert_progress=mock_get_progress)
        # No job_id, so 0%
        assert progress == 0
        assert step == "converting"


class TestCalculateProgressMultipleFiles:
    """Tests for progress calculation with multiple files."""

    def test_two_files_one_completed_one_pending(self):
        """Test average progress with mixed statuses."""
        files = [
            {"status": "COMPLETED"},
            {"status": "PENDING"},
        ]
        progress, step = calculate_progress(files)
        # (100 + 0) / 2 = 50%
        assert progress == 50
        assert step == "pending"  # Not all completed

    def test_two_files_both_completed(self):
        """Test all files completed."""
        files = [
            {"status": "COMPLETED"},
            {"status": "COMPLETED"},
        ]
        progress, step = calculate_progress(files)
        assert progress == 100
        assert step == "completed"

    def test_three_files_mixed_statuses(self):
        """Test progress with various statuses."""
        files = [
            {"status": "COMPLETED"},  # 100%
            {"status": "VERIFYING"},  # 65%
            {"status": "PENDING"},  # 0%
        ]
        progress, step = calculate_progress(files)
        # (100 + 65 + 0) / 3 = 55%
        assert progress == 55
        assert step == "verifying"

    def test_completed_and_failed_counts_as_completed(self):
        """Test that FAILED files count toward completion."""
        files = [
            {"status": "COMPLETED"},
            {"status": "FAILED"},
        ]
        progress, step = calculate_progress(files)
        assert progress == 100
        assert step == "completed"


class TestCalculateProgressSimple:
    """Tests for calculate_progress_simple function."""

    def test_simple_uses_midpoint_for_converting(self):
        """Test that simple version uses midpoint for CONVERTING."""
        files = [{"status": "CONVERTING", "mediaconvert_job_id": "job-123"}]
        progress, step = calculate_progress_simple(files)
        # Should use midpoint, not query MediaConvert
        assert progress == PROGRESS_CONVERTING_MIDPOINT
        assert step == "converting"

    def test_simple_same_as_calculate_for_non_converting(self):
        """Test that simple version matches calculate for non-CONVERTING statuses."""
        files = [
            {"status": "COMPLETED"},
            {"status": "VERIFYING"},
        ]
        simple_progress, simple_step = calculate_progress_simple(files)
        full_progress, full_step = calculate_progress(files)

        assert simple_progress == full_progress
        assert simple_step == full_step


class TestProgressConstants:
    """Tests for progress constants."""

    def test_progress_constants_values(self):
        """Test that progress constants have expected values."""
        assert PROGRESS_CONVERTING_MIDPOINT == 15
        assert PROGRESS_VERIFYING == 65
        assert PROGRESS_COMPLETED == 100

    def test_converting_range(self):
        """Test that CONVERTING progress is in 0-30% range."""
        # Test with various MediaConvert progress values
        for mc_progress in [0, 25, 50, 75, 100]:
            files = [{"status": "CONVERTING", "mediaconvert_job_id": "job-123"}]

            def mock_get_progress(job_id: str) -> int:
                return mc_progress

            progress, _ = calculate_progress(files, get_mediaconvert_progress=mock_get_progress)
            assert 0 <= progress <= 30, (
                f"Progress {progress} out of range for MC progress {mc_progress}"
            )


class TestCurrentStepDetermination:
    """Tests for current_step determination logic."""

    def test_step_pending_when_all_pending(self):
        """Test step is 'pending' when all files are PENDING."""
        files = [{"status": "PENDING"}, {"status": "PENDING"}]
        _, step = calculate_progress(files)
        assert step == "pending"

    def test_step_converting_when_any_converting(self):
        """Test step is 'converting' when any file is CONVERTING."""
        files = [{"status": "PENDING"}, {"status": "CONVERTING"}]
        _, step = calculate_progress(files)
        assert step == "converting"

    def test_step_verifying_when_any_verifying(self):
        """Test step is 'verifying' when any file is VERIFYING."""
        files = [{"status": "CONVERTING"}, {"status": "VERIFYING"}]
        _, step = calculate_progress(files)
        assert step == "verifying"

    def test_step_completed_when_all_done(self):
        """Test step is 'completed' when all files are COMPLETED or FAILED."""
        files = [{"status": "COMPLETED"}, {"status": "FAILED"}]
        _, step = calculate_progress(files)
        assert step == "completed"

    def test_step_not_completed_when_some_pending(self):
        """Test step is not 'completed' when some files are still pending."""
        files = [{"status": "COMPLETED"}, {"status": "PENDING"}]
        _, step = calculate_progress(files)
        assert step != "completed"
