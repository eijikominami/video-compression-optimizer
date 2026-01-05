"""Property-based tests for verification progress calculation.

Tests Task 28: Property tests for progress calculation.
"""

from datetime import datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus


class TestVerifyingProgressCalculation:
    """Property 8: VERIFYING progress calculation accuracy.

    Validates: Requirements 2.5, 2.10
    """

    @given(verification_progress=st.integers(min_value=0, max_value=100))
    @settings(max_examples=100)
    def test_verifying_progress_scales_to_65_99_range(self, verification_progress: int):
        """Test that verification_progress (0-100) scales to 65-99% range.

        Formula: 65 + (verification_progress * 0.34)
        - verification_progress=0 -> 65%
        - verification_progress=100 -> 99%
        """
        file = AsyncFile(
            file_id="file-001",
            uuid="uuid-001",
            filename="video.mp4",
            source_s3_key="input/video.mp4",
            status=FileStatus.VERIFYING,
            verification_progress=verification_progress,
        )

        task = AsyncTask(
            task_id="task-001",
            user_id="user-001",
            status=TaskStatus.VERIFYING,
            quality_preset="balanced",
            files=[file],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        progress = task.calculate_progress()

        # Expected: 65 + (verification_progress * 0.34)
        expected = 65 + int(verification_progress * 0.34)

        assert progress == expected
        assert 65 <= progress <= 99

    @given(verification_progress=st.integers(min_value=0, max_value=100))
    @settings(max_examples=50)
    def test_verifying_progress_monotonically_increasing(self, verification_progress: int):
        """Test that higher verification_progress results in higher overall progress."""
        file_low = AsyncFile(
            file_id="file-001",
            uuid="uuid-001",
            filename="video.mp4",
            source_s3_key="input/video.mp4",
            status=FileStatus.VERIFYING,
            verification_progress=0,
        )

        file_high = AsyncFile(
            file_id="file-002",
            uuid="uuid-002",
            filename="video2.mp4",
            source_s3_key="input/video2.mp4",
            status=FileStatus.VERIFYING,
            verification_progress=verification_progress,
        )

        task_low = AsyncTask(
            task_id="task-001",
            user_id="user-001",
            status=TaskStatus.VERIFYING,
            quality_preset="balanced",
            files=[file_low],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        task_high = AsyncTask(
            task_id="task-002",
            user_id="user-001",
            status=TaskStatus.VERIFYING,
            quality_preset="balanced",
            files=[file_high],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        progress_low = task_low.calculate_progress()
        progress_high = task_high.calculate_progress()

        # Higher verification_progress should result in >= progress
        assert progress_high >= progress_low

    @given(
        progress1=st.integers(min_value=0, max_value=100),
        progress2=st.integers(min_value=0, max_value=100),
    )
    @settings(max_examples=50)
    def test_verifying_progress_ordering(self, progress1: int, progress2: int):
        """Test that verification_progress ordering is preserved in overall progress."""
        file1 = AsyncFile(
            file_id="file-001",
            uuid="uuid-001",
            filename="video.mp4",
            source_s3_key="input/video.mp4",
            status=FileStatus.VERIFYING,
            verification_progress=progress1,
        )

        file2 = AsyncFile(
            file_id="file-002",
            uuid="uuid-002",
            filename="video2.mp4",
            source_s3_key="input/video2.mp4",
            status=FileStatus.VERIFYING,
            verification_progress=progress2,
        )

        task1 = AsyncTask(
            task_id="task-001",
            user_id="user-001",
            status=TaskStatus.VERIFYING,
            quality_preset="balanced",
            files=[file1],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        task2 = AsyncTask(
            task_id="task-002",
            user_id="user-001",
            status=TaskStatus.VERIFYING,
            quality_preset="balanced",
            files=[file2],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        overall1 = task1.calculate_progress()
        overall2 = task2.calculate_progress()

        # If progress1 <= progress2, then overall1 <= overall2
        if progress1 <= progress2:
            assert overall1 <= overall2
        else:
            assert overall1 >= overall2


class TestConvertingProgressScaling:
    """Property 9: CONVERTING progress scaling accuracy.

    Validates: Requirements 2.4
    """

    @given(file_count=st.integers(min_value=1, max_value=20))
    @settings(max_examples=50)
    def test_converting_progress_uses_default_midpoint(self, file_count: int):
        """Test that CONVERTING files use default 32% (midpoint of 0-65%)."""
        files = [
            AsyncFile(
                file_id=f"file-{i:03d}",
                uuid=f"uuid-{i:03d}",
                filename=f"video{i}.mp4",
                source_s3_key=f"input/video{i}.mp4",
                status=FileStatus.CONVERTING,
            )
            for i in range(file_count)
        ]

        task = AsyncTask(
            task_id="task-001",
            user_id="user-001",
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=files,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        progress = task.calculate_progress()

        # All files are CONVERTING, each contributes 32%
        # Average should be 32%
        assert progress == 32

    @given(
        converting_count=st.integers(min_value=0, max_value=10),
        completed_count=st.integers(min_value=0, max_value=10),
    )
    @settings(max_examples=50)
    def test_mixed_converting_completed_progress(self, converting_count: int, completed_count: int):
        """Test progress calculation with mixed CONVERTING and COMPLETED files."""
        if converting_count + completed_count == 0:
            return  # Skip empty case

        files = []

        for i in range(converting_count):
            files.append(
                AsyncFile(
                    file_id=f"converting-{i:03d}",
                    uuid=f"uuid-c-{i:03d}",
                    filename=f"converting{i}.mp4",
                    source_s3_key=f"input/converting{i}.mp4",
                    status=FileStatus.CONVERTING,
                )
            )

        for i in range(completed_count):
            files.append(
                AsyncFile(
                    file_id=f"completed-{i:03d}",
                    uuid=f"uuid-d-{i:03d}",
                    filename=f"completed{i}.mp4",
                    source_s3_key=f"input/completed{i}.mp4",
                    status=FileStatus.COMPLETED,
                )
            )

        task = AsyncTask(
            task_id="task-001",
            user_id="user-001",
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=files,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        progress = task.calculate_progress()

        # Expected: (converting_count * 32 + completed_count * 100) / total
        total = converting_count + completed_count
        expected = (converting_count * 32 + completed_count * 100) // total

        assert progress == expected


class TestMixedStatusProgress:
    """Property tests for mixed file status progress calculation."""

    @given(
        pending_count=st.integers(min_value=0, max_value=5),
        converting_count=st.integers(min_value=0, max_value=5),
        verifying_count=st.integers(min_value=0, max_value=5),
        completed_count=st.integers(min_value=0, max_value=5),
        failed_count=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=100)
    def test_progress_bounds(
        self,
        pending_count: int,
        converting_count: int,
        verifying_count: int,
        completed_count: int,
        failed_count: int,
    ):
        """Test that progress is always within valid bounds (0-100)."""
        total = pending_count + converting_count + verifying_count + completed_count + failed_count
        if total == 0:
            return  # Skip empty case

        files = []

        for i in range(pending_count):
            files.append(
                AsyncFile(
                    file_id=f"pending-{i}",
                    uuid=f"uuid-p-{i}",
                    filename=f"pending{i}.mp4",
                    source_s3_key=f"input/pending{i}.mp4",
                    status=FileStatus.PENDING,
                )
            )

        for i in range(converting_count):
            files.append(
                AsyncFile(
                    file_id=f"converting-{i}",
                    uuid=f"uuid-c-{i}",
                    filename=f"converting{i}.mp4",
                    source_s3_key=f"input/converting{i}.mp4",
                    status=FileStatus.CONVERTING,
                )
            )

        for i in range(verifying_count):
            files.append(
                AsyncFile(
                    file_id=f"verifying-{i}",
                    uuid=f"uuid-v-{i}",
                    filename=f"verifying{i}.mp4",
                    source_s3_key=f"input/verifying{i}.mp4",
                    status=FileStatus.VERIFYING,
                    verification_progress=50,  # Mid-point
                )
            )

        for i in range(completed_count):
            files.append(
                AsyncFile(
                    file_id=f"completed-{i}",
                    uuid=f"uuid-d-{i}",
                    filename=f"completed{i}.mp4",
                    source_s3_key=f"input/completed{i}.mp4",
                    status=FileStatus.COMPLETED,
                )
            )

        for i in range(failed_count):
            files.append(
                AsyncFile(
                    file_id=f"failed-{i}",
                    uuid=f"uuid-f-{i}",
                    filename=f"failed{i}.mp4",
                    source_s3_key=f"input/failed{i}.mp4",
                    status=FileStatus.FAILED,
                )
            )

        task = AsyncTask(
            task_id="task-001",
            user_id="user-001",
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=files,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        progress = task.calculate_progress()

        # Progress should always be within bounds
        assert 0 <= progress <= 100
