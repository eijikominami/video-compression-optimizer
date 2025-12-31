"""Property tests for progress calculation consistency.

Property 10: Progress Calculation Consistency
For any list of file statuses, the progress calculated by Lambda and CLI
SHALL be identical when using the same input data.

Requirements: 6.4
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.utils.progress import (
    PROGRESS_VERIFYING,
    calculate_progress,
    calculate_progress_simple,
)

# Valid file statuses
VALID_STATUSES = ["PENDING", "CONVERTING", "VERIFYING", "COMPLETED", "FAILED"]


@st.composite
def file_dict_strategy(draw):
    """Generate a file dictionary with valid status."""
    status = draw(st.sampled_from(VALID_STATUSES))
    file_dict = {"status": status}

    # Add job_id for CONVERTING status
    if status == "CONVERTING":
        if draw(st.booleans()):
            file_dict["mediaconvert_job_id"] = draw(
                st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz0123456789-")
            )

    return file_dict


@st.composite
def files_list_strategy(draw):
    """Generate a list of file dictionaries."""
    return draw(st.lists(file_dict_strategy(), min_size=0, max_size=10))


class TestProgressConsistencyProperty:
    """Property tests for progress calculation consistency."""

    @given(files=files_list_strategy())
    @settings(max_examples=100)
    def test_progress_in_valid_range(self, files):
        """Property: Progress percentage is always 0-100."""
        progress, _ = calculate_progress(files)
        assert 0 <= progress <= 100

    @given(files=files_list_strategy())
    @settings(max_examples=100)
    def test_current_step_is_valid(self, files):
        """Property: Current step is always a valid value."""
        _, step = calculate_progress(files)
        assert step in ["pending", "converting", "verifying", "completed"]

    @given(files=files_list_strategy())
    @settings(max_examples=100)
    def test_simple_matches_calculate_without_callback(self, files):
        """Property: calculate_progress_simple matches calculate_progress without callback."""
        simple_progress, simple_step = calculate_progress_simple(files)
        full_progress, full_step = calculate_progress(files, get_mediaconvert_progress=None)

        assert simple_progress == full_progress
        assert simple_step == full_step

    @given(files=files_list_strategy())
    @settings(max_examples=100)
    def test_completed_step_only_when_all_done(self, files):
        """Property: 'completed' step only when all files are COMPLETED or FAILED."""
        _, step = calculate_progress(files)

        if step == "completed":
            # All files must be COMPLETED or FAILED
            for f in files:
                assert f.get("status") in ("COMPLETED", "FAILED")

    @given(files=files_list_strategy())
    @settings(max_examples=100)
    def test_empty_files_returns_zero_pending(self, files):
        """Property: Empty file list returns 0% and 'pending'."""
        if not files:
            progress, step = calculate_progress(files)
            assert progress == 0
            assert step == "pending"


class TestProgressBoundaryConditions:
    """Tests for progress calculation boundary conditions."""

    @given(count=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_all_pending_is_zero(self, count):
        """Property: All PENDING files result in 0% progress."""
        files = [{"status": "PENDING"} for _ in range(count)]
        progress, step = calculate_progress(files)
        assert progress == 0
        assert step == "pending"

    @given(count=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_all_completed_is_hundred(self, count):
        """Property: All COMPLETED files result in 100% progress."""
        files = [{"status": "COMPLETED"} for _ in range(count)]
        progress, step = calculate_progress(files)
        assert progress == 100
        assert step == "completed"

    @given(count=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_all_failed_is_hundred(self, count):
        """Property: All FAILED files result in 100% progress."""
        files = [{"status": "FAILED"} for _ in range(count)]
        progress, step = calculate_progress(files)
        assert progress == 100
        assert step == "completed"

    @given(count=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_all_verifying_is_65(self, count):
        """Property: All VERIFYING files result in 65% progress."""
        files = [{"status": "VERIFYING"} for _ in range(count)]
        progress, step = calculate_progress(files)
        assert progress == PROGRESS_VERIFYING
        assert step == "verifying"


class TestProgressDeterminism:
    """Tests for progress calculation determinism."""

    @given(files=files_list_strategy())
    @settings(max_examples=100)
    def test_same_input_same_output(self, files):
        """Property: Same input always produces same output."""
        result1 = calculate_progress(files)
        result2 = calculate_progress(files)
        assert result1 == result2

    @given(files=files_list_strategy())
    @settings(max_examples=100)
    def test_simple_is_deterministic(self, files):
        """Property: calculate_progress_simple is deterministic."""
        result1 = calculate_progress_simple(files)
        result2 = calculate_progress_simple(files)
        assert result1 == result2
