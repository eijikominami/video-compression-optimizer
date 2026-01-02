"""Property-based tests for AsyncTask roundtrip preservation.

**Property 1: タスクメタデータの往復整合性**
**Validates: Requirements 6.1, 6.2**

For any AsyncTask, converting to dict and back should preserve all fields.
This ensures data integrity when storing/retrieving from DynamoDB.
"""

import json
import uuid
from datetime import datetime, timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from vco.models.async_task import (
    AsyncFile,
    AsyncTask,
    DownloadProgress,
    FileStatus,
    TaskStatus,
)

# Strategy for generating valid datetime objects
datetime_strategy = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2030, 12, 31),
)

# Strategy for UUIDs - generate actual UUIDs
uuid_strategy = st.builds(lambda: str(uuid.uuid4()))

# Strategy for S3 keys - generate valid S3 keys directly
s3_key_strategy = st.from_regex(r"[a-z0-9]{5,20}/[a-z0-9]{5,20}\.[a-z]{3}", fullmatch=True)

# Strategy for filenames - generate valid filenames directly
filename_strategy = st.from_regex(r"[a-z0-9_-]{3,20}\.(mp4|mov|avi|mkv)", fullmatch=True)

# Strategy for quality presets
preset_strategy = st.sampled_from(["balanced", "high", "balanced+", "high+", "compression"])

# Strategy for error codes
error_code_strategy = st.sampled_from([None, 1010, 1030, 1040, 1401, 1517, 1522, 1550, 1999])


@st.composite
def async_file_strategy(draw):
    """Generate a valid AsyncFile."""
    return AsyncFile(
        file_id=draw(uuid_strategy),
        uuid=draw(uuid_strategy),
        filename=draw(filename_strategy),
        source_s3_key=draw(s3_key_strategy),
        output_s3_key=draw(st.one_of(st.none(), s3_key_strategy)),
        metadata_s3_key=draw(st.one_of(st.none(), s3_key_strategy)),
        status=draw(st.sampled_from(list(FileStatus))),
        mediaconvert_job_id=draw(st.one_of(st.none(), uuid_strategy)),
        quality_result=draw(
            st.one_of(
                st.none(),
                st.fixed_dictionaries(
                    {
                        "ssim_score": st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
                        "compression_ratio": st.floats(
                            min_value=0.1, max_value=10.0, allow_nan=False
                        ),
                    }
                ),
            )
        ),
        error_code=draw(error_code_strategy),
        error_message=draw(st.one_of(st.none(), st.text(min_size=1, max_size=200))),
        retry_count=draw(st.integers(min_value=0, max_value=5)),
        preset_attempts=draw(st.lists(preset_strategy, max_size=3)),
        source_size_bytes=draw(
            st.one_of(st.none(), st.integers(min_value=1000, max_value=10_000_000_000))
        ),
        output_size_bytes=draw(
            st.one_of(st.none(), st.integers(min_value=1000, max_value=10_000_000_000))
        ),
        output_checksum=draw(
            st.one_of(st.none(), st.text(min_size=32, max_size=64, alphabet="0123456789abcdef"))
        ),
        checksum_algorithm=draw(st.sampled_from(["ETag", "SHA256"])),
    )


@st.composite
def async_task_strategy(draw):
    """Generate a valid AsyncTask."""
    created_at = draw(datetime_strategy)
    updated_at = created_at + timedelta(seconds=draw(st.integers(min_value=0, max_value=86400)))

    status = draw(st.sampled_from(list(TaskStatus)))

    # Generate started_at and completed_at based on status
    started_at = None
    completed_at = None
    if status not in (TaskStatus.PENDING,):
        started_at = created_at + timedelta(seconds=draw(st.integers(min_value=1, max_value=60)))
    if status in (
        TaskStatus.COMPLETED,
        TaskStatus.PARTIALLY_COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
    ):
        completed_at = updated_at

    return AsyncTask(
        task_id=draw(uuid_strategy),
        user_id=draw(
            st.text(min_size=5, max_size=50, alphabet="abcdefghijklmnopqrstuvwxyz0123456789_-")
        ),
        status=status,
        quality_preset=draw(preset_strategy),
        files=draw(st.lists(async_file_strategy(), min_size=1, max_size=5)),
        created_at=created_at,
        updated_at=updated_at,
        started_at=started_at,
        completed_at=completed_at,
        execution_arn=draw(
            st.one_of(
                st.none(),
                st.text(
                    min_size=50, max_size=200, alphabet="abcdefghijklmnopqrstuvwxyz0123456789:/-"
                ),
            )
        ),
        error_message=draw(st.one_of(st.none(), st.text(min_size=1, max_size=500))),
        ttl=draw(st.one_of(st.none(), st.integers(min_value=1700000000, max_value=2000000000))),
        progress_percentage=draw(st.integers(min_value=0, max_value=100)),
        current_step=draw(
            st.one_of(st.none(), st.sampled_from(["uploading", "converting", "verifying"]))
        ),
        estimated_completion_time=draw(st.one_of(st.none(), datetime_strategy)),
        max_concurrent=draw(st.integers(min_value=1, max_value=10)),
    )


class TestAsyncFileRoundtrip:
    """Test AsyncFile roundtrip through dict serialization."""

    # Property 1: タスクメタデータの往復整合性
    # **Validates: Requirements 6.1, 6.2**

    @given(file=async_file_strategy())
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_async_file_dict_roundtrip(self, file: AsyncFile):
        """AsyncFile should survive dict serialization roundtrip."""
        # Convert to dict and back
        data = file.to_dict()
        restored = AsyncFile.from_dict(data)

        # Verify all fields match
        assert restored.file_id == file.file_id
        assert restored.original_uuid == file.original_uuid
        assert restored.filename == file.filename
        assert restored.source_s3_key == file.source_s3_key
        assert restored.output_s3_key == file.output_s3_key
        assert restored.metadata_s3_key == file.metadata_s3_key
        assert restored.status == file.status
        assert restored.mediaconvert_job_id == file.mediaconvert_job_id
        assert restored.quality_result == file.quality_result
        assert restored.error_code == file.error_code
        assert restored.error_message == file.error_message
        assert restored.retry_count == file.retry_count
        assert restored.preset_attempts == file.preset_attempts
        assert restored.source_size_bytes == file.source_size_bytes
        assert restored.output_size_bytes == file.output_size_bytes
        assert restored.output_checksum == file.output_checksum
        assert restored.checksum_algorithm == file.checksum_algorithm

    @given(file=async_file_strategy())
    @settings(max_examples=50)
    def test_async_file_json_roundtrip(self, file: AsyncFile):
        """AsyncFile should survive JSON serialization roundtrip."""
        # Convert to dict, then JSON, then back
        data = file.to_dict()
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        restored = AsyncFile.from_dict(restored_data)

        # Verify key fields match
        assert restored.file_id == file.file_id
        assert restored.status == file.status
        assert restored.filename == file.filename


class TestAsyncTaskRoundtrip:
    """Test AsyncTask roundtrip through dict serialization."""

    # Property 1: タスクメタデータの往復整合性
    # **Validates: Requirements 6.1, 6.2**

    @given(task=async_task_strategy())
    @settings(max_examples=100)
    def test_async_task_dict_roundtrip(self, task: AsyncTask):
        """AsyncTask should survive dict serialization roundtrip."""
        # Convert to dict and back
        data = task.to_dict()
        restored = AsyncTask.from_dict(data)

        # Verify all scalar fields match
        assert restored.task_id == task.task_id
        assert restored.user_id == task.user_id
        assert restored.status == task.status
        assert restored.quality_preset == task.quality_preset
        assert restored.created_at == task.created_at
        assert restored.updated_at == task.updated_at
        assert restored.started_at == task.started_at
        assert restored.completed_at == task.completed_at
        assert restored.execution_arn == task.execution_arn
        assert restored.error_message == task.error_message
        assert restored.ttl == task.ttl
        assert restored.progress_percentage == task.progress_percentage
        assert restored.current_step == task.current_step
        assert restored.estimated_completion_time == task.estimated_completion_time
        assert restored.max_concurrent == task.max_concurrent

        # Verify files list
        assert len(restored.files) == len(task.files)
        for orig, rest in zip(task.files, restored.files):
            assert rest.file_id == orig.file_id
            assert rest.status == orig.status
            assert rest.filename == orig.filename

    @given(task=async_task_strategy())
    @settings(max_examples=50)
    def test_async_task_json_roundtrip(self, task: AsyncTask):
        """AsyncTask should survive JSON serialization roundtrip."""
        # Convert to dict, then JSON, then back
        data = task.to_dict()
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        restored = AsyncTask.from_dict(restored_data)

        # Verify key fields match
        assert restored.task_id == task.task_id
        assert restored.status == task.status
        assert restored.quality_preset == task.quality_preset
        assert len(restored.files) == len(task.files)


class TestDownloadProgressRoundtrip:
    """Test DownloadProgress roundtrip through dict serialization."""

    @given(
        task_id=uuid_strategy,
        file_id=uuid_strategy,
        total_bytes=st.integers(min_value=1000, max_value=10_000_000_000),
        downloaded_bytes=st.integers(min_value=0, max_value=10_000_000_000),
        s3_key=s3_key_strategy,
        checksum=st.one_of(
            st.none(), st.text(min_size=32, max_size=64, alphabet="0123456789abcdef")
        ),
    )
    @settings(max_examples=100)
    def test_download_progress_dict_roundtrip(
        self, task_id, file_id, total_bytes, downloaded_bytes, s3_key, checksum
    ):
        """DownloadProgress should survive dict serialization roundtrip."""
        # Ensure downloaded_bytes <= total_bytes
        downloaded_bytes = min(downloaded_bytes, total_bytes)

        original = DownloadProgress(
            task_id=task_id,
            file_id=file_id,
            total_bytes=total_bytes,
            downloaded_bytes=downloaded_bytes,
            local_temp_path="/tmp/test_file.mp4.part",
            s3_key=s3_key,
            checksum=checksum,
        )

        # Convert to dict and back
        data = original.to_dict()
        restored = DownloadProgress.from_dict(data)

        # Verify all fields match
        assert restored.task_id == original.task_id
        assert restored.file_id == original.file_id
        assert restored.total_bytes == original.total_bytes
        assert restored.downloaded_bytes == original.downloaded_bytes
        assert restored.local_temp_path == original.local_temp_path
        assert restored.s3_key == original.s3_key
        assert restored.checksum == original.checksum

    @given(
        total_bytes=st.integers(min_value=1000, max_value=10_000_000_000),
        downloaded_bytes=st.integers(min_value=0, max_value=10_000_000_000),
    )
    @settings(max_examples=50)
    def test_download_progress_percentage(self, total_bytes, downloaded_bytes):
        """DownloadProgress percentage should be calculated correctly."""
        # Ensure downloaded_bytes <= total_bytes
        downloaded_bytes = min(downloaded_bytes, total_bytes)

        progress = DownloadProgress(
            task_id="test-task",
            file_id="test-file",
            total_bytes=total_bytes,
            downloaded_bytes=downloaded_bytes,
            local_temp_path="/tmp/test.part",
            s3_key="test/key.mp4",
        )

        expected_percentage = int(downloaded_bytes / total_bytes * 100)
        assert progress.progress_percentage == expected_percentage

    @given(total_bytes=st.integers(min_value=1000, max_value=10_000_000_000))
    @settings(max_examples=50)
    def test_download_progress_is_complete(self, total_bytes):
        """DownloadProgress.is_complete should be True when fully downloaded."""
        progress = DownloadProgress(
            task_id="test-task",
            file_id="test-file",
            total_bytes=total_bytes,
            downloaded_bytes=total_bytes,  # Fully downloaded
            local_temp_path="/tmp/test.part",
            s3_key="test/key.mp4",
        )

        assert progress.is_complete is True
        assert progress.progress_percentage == 100


class TestAsyncTaskProgressCalculation:
    """Test AsyncTask progress calculation."""

    def test_pending_task_progress_is_zero(self):
        """PENDING task should have 0% progress."""
        task = AsyncTask(
            task_id="test",
            user_id="user",
            status=TaskStatus.PENDING,
            quality_preset="balanced",
            files=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        assert task.calculate_progress() == 0

    def test_uploading_task_progress_is_ten(self):
        """UPLOADING task should have 10% progress."""
        task = AsyncTask(
            task_id="test",
            user_id="user",
            status=TaskStatus.UPLOADING,
            quality_preset="balanced",
            files=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        assert task.calculate_progress() == 10

    def test_completed_task_progress_is_hundred(self):
        """COMPLETED task should have 100% progress."""
        task = AsyncTask(
            task_id="test",
            user_id="user",
            status=TaskStatus.COMPLETED,
            quality_preset="balanced",
            files=[],
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        assert task.calculate_progress() == 100

    @given(
        completed_count=st.integers(min_value=0, max_value=10),
        total_count=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=50)
    def test_converting_task_progress(self, completed_count, total_count):
        """CONVERTING task progress should reflect file completion."""
        # Ensure completed_count <= total_count
        completed_count = min(completed_count, total_count)

        files = []
        for i in range(total_count):
            status = FileStatus.COMPLETED if i < completed_count else FileStatus.CONVERTING
            files.append(
                AsyncFile(
                    file_id=f"file-{i}",
                    uuid=f"uuid-{i}",
                    filename=f"video-{i}.mp4",
                    source_s3_key=f"source/{i}.mp4",
                    status=status,
                )
            )

        task = AsyncTask(
            task_id="test",
            user_id="user",
            status=TaskStatus.CONVERTING,
            quality_preset="balanced",
            files=files,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        expected_progress = 10 + int((completed_count / total_count) * 70)
        assert task.calculate_progress() == expected_progress
