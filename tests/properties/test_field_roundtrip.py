"""Property tests for field roundtrip conversion.

Property 2: Field Naming Consistency
For any AsyncFile object, when converted to API response format using
async_file_to_api() and back using api_to_async_file(), the resulting
AsyncFile SHALL be equivalent to the original.

Requirements: 2.1, 2.3, 2.4
"""

from datetime import datetime

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.models.async_task import AsyncFile, AsyncTask, FileStatus, TaskStatus
from vco.models.converters import (
    api_to_async_file,
    api_to_async_task,
    async_file_to_api,
    async_task_to_api,
)

# Custom strategies for generating valid test data
file_status_strategy = st.sampled_from(list(FileStatus))
task_status_strategy = st.sampled_from(list(TaskStatus))

# Strategy for valid filenames (no path separators, reasonable length)
filename_strategy = st.text(
    alphabet=st.characters(
        whitelist_categories=("L", "N"),
        whitelist_characters="_-.",
    ),
    min_size=1,
    max_size=50,
).filter(lambda x: x.strip() and not x.startswith("."))

# Strategy for valid UUIDs
uuid_strategy = st.uuids().map(str)

# Strategy for S3 keys
s3_key_strategy = st.text(
    alphabet=st.characters(
        whitelist_categories=("L", "N"),
        whitelist_characters="_-./",
    ),
    min_size=1,
    max_size=100,
).filter(lambda x: x.strip() and not x.startswith("/"))

# Strategy for quality results
quality_result_strategy = st.one_of(
    st.none(),
    st.fixed_dictionaries(
        {
            "ssim_score": st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
            "compression_ratio": st.floats(min_value=0.1, max_value=10.0, allow_nan=False),
        }
    ),
)

# Strategy for preset attempts
preset_attempts_strategy = st.lists(
    st.sampled_from(["balanced", "high", "compression", "balanced+", "high+"]),
    max_size=5,
)


@st.composite
def async_file_strategy(draw):
    """Generate valid AsyncFile objects."""
    # Generate optional downloaded_at datetime
    downloaded_at = draw(st.one_of(st.none(), st.just(datetime.now())))

    return AsyncFile(
        file_id=draw(uuid_strategy),
        uuid=draw(uuid_strategy),
        filename=draw(filename_strategy) + ".mov",
        source_s3_key=draw(s3_key_strategy),
        output_s3_key=draw(st.one_of(st.none(), s3_key_strategy)),
        metadata_s3_key=draw(st.one_of(st.none(), s3_key_strategy)),
        status=draw(file_status_strategy),
        mediaconvert_job_id=draw(st.one_of(st.none(), uuid_strategy)),
        quality_result=draw(quality_result_strategy),
        error_code=draw(st.one_of(st.none(), st.integers(min_value=1, max_value=999))),
        error_message=draw(st.one_of(st.none(), st.text(max_size=100))),
        retry_count=draw(st.integers(min_value=0, max_value=10)),
        preset_attempts=draw(preset_attempts_strategy),
        source_size_bytes=draw(st.one_of(st.none(), st.integers(min_value=0, max_value=10**12))),
        output_size_bytes=draw(st.one_of(st.none(), st.integers(min_value=0, max_value=10**12))),
        output_checksum=draw(st.one_of(st.none(), st.text(min_size=32, max_size=64))),
        checksum_algorithm=draw(st.sampled_from(["ETag", "SHA256"])),
        downloaded_at=downloaded_at,
        download_available=draw(st.booleans()),
    )


@st.composite
def async_task_strategy(draw):
    """Generate valid AsyncTask objects."""
    now = datetime.now()
    files = draw(st.lists(async_file_strategy(), min_size=0, max_size=5))

    return AsyncTask(
        task_id=draw(uuid_strategy),
        user_id=draw(st.text(min_size=1, max_size=64)),
        status=draw(task_status_strategy),
        quality_preset=draw(
            st.sampled_from(["balanced", "high", "compression", "balanced+", "high+"])
        ),
        files=files,
        created_at=now,
        updated_at=now,
        started_at=draw(st.one_of(st.none(), st.just(now))),
        completed_at=draw(st.one_of(st.none(), st.just(now))),
        execution_arn=draw(st.one_of(st.none(), st.text(min_size=10, max_size=200))),
        error_message=draw(st.one_of(st.none(), st.text(max_size=100))),
        ttl=draw(st.one_of(st.none(), st.integers(min_value=0))),
        progress_percentage=draw(st.integers(min_value=0, max_value=100)),
        current_step=draw(
            st.one_of(
                st.none(), st.sampled_from(["pending", "converting", "verifying", "completed"])
            )
        ),
        estimated_completion_time=draw(st.one_of(st.none(), st.just(now))),
        max_concurrent=draw(st.integers(min_value=1, max_value=20)),
    )


class TestAsyncFileRoundtrip:
    """Property tests for AsyncFile roundtrip conversion."""

    @given(file=async_file_strategy())
    @settings(max_examples=100)
    def test_async_file_roundtrip_preserves_all_fields(self, file: AsyncFile):
        """Property 2: AsyncFile -> API -> AsyncFile roundtrip preserves all fields.

        For any AsyncFile object, when converted to API response format using
        async_file_to_api() and back using api_to_async_file(), the resulting
        AsyncFile SHALL be equivalent to the original.
        """
        # Convert to API format
        api_data = async_file_to_api(file)

        # Convert back to AsyncFile
        restored = api_to_async_file(api_data)

        # Verify all fields are preserved
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
        assert restored.downloaded_at == file.downloaded_at
        assert restored.download_available == file.download_available

    @given(file=async_file_strategy())
    @settings(max_examples=50)
    def test_api_format_has_correct_status_type(self, file: AsyncFile):
        """API format should have status as string, not enum."""
        api_data = async_file_to_api(file)

        # Status should be a string in API format
        assert isinstance(api_data["status"], str)
        assert api_data["status"] == file.status.value


class TestAsyncTaskRoundtrip:
    """Property tests for AsyncTask roundtrip conversion."""

    @given(task=async_task_strategy())
    @settings(max_examples=50)
    def test_async_task_roundtrip_preserves_all_fields(self, task: AsyncTask):
        """AsyncTask -> API -> AsyncTask roundtrip preserves all fields."""
        # Convert to API format
        api_data = async_task_to_api(task)

        # Convert back to AsyncTask
        restored = api_to_async_task(api_data)

        # Verify task-level fields are preserved
        assert restored.task_id == task.task_id
        assert restored.user_id == task.user_id
        assert restored.status == task.status
        assert restored.quality_preset == task.quality_preset
        assert len(restored.files) == len(task.files)
        assert restored.execution_arn == task.execution_arn
        assert restored.error_message == task.error_message
        assert restored.progress_percentage == task.progress_percentage
        assert restored.current_step == task.current_step
        assert restored.max_concurrent == task.max_concurrent

        # Verify file-level fields are preserved
        for original_file, restored_file in zip(task.files, restored.files):
            assert restored_file.file_id == original_file.file_id
            assert restored_file.filename == original_file.filename
            assert restored_file.status == original_file.status

    @given(task=async_task_strategy())
    @settings(max_examples=50)
    def test_api_format_has_correct_types(self, task: AsyncTask):
        """API format should have correct types for all fields."""
        api_data = async_task_to_api(task)

        # Status should be a string
        assert isinstance(api_data["status"], str)
        assert api_data["status"] == task.status.value

        # Dates should be ISO format strings
        assert isinstance(api_data["created_at"], str)
        assert isinstance(api_data["updated_at"], str)

        # Files should be a list of dicts
        assert isinstance(api_data["files"], list)
        for file_data in api_data["files"]:
            assert isinstance(file_data, dict)
            assert isinstance(file_data["status"], str)
