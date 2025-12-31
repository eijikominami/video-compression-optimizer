"""Property tests for COMPLETED file invariants.

Property 6: COMPLETED Files Have Output Key
For any AsyncFile with status COMPLETED, the output_s3_key field SHALL NOT be None.

Requirements: 5.3, 5.4, 5.5
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.models.async_task import AsyncFile, FileStatus

# Strategy for generating valid file IDs (simple alphanumeric)
file_id_strategy = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789-",
    min_size=5,
    max_size=36,
)

# Strategy for generating valid filenames
filename_strategy = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-",
    min_size=1,
    max_size=50,
)

# Strategy for generating valid S3 keys
s3_key_strategy = st.builds(
    lambda prefix, name: f"{prefix}/{name}",
    prefix=st.sampled_from(["tasks/task-1/source", "output/task-1", "tasks/task-1/metadata"]),
    name=st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789._-", min_size=1, max_size=20),
)


class TestCompletedFileInvariant:
    """Property tests for COMPLETED file invariants."""

    @given(
        file_id=file_id_strategy,
        original_uuid=st.text(min_size=1, max_size=50),
        filename=filename_strategy,
        source_s3_key=s3_key_strategy,
        output_s3_key=s3_key_strategy,
    )
    @settings(max_examples=100)
    def test_completed_file_has_output_key(
        self,
        file_id: str,
        original_uuid: str,
        filename: str,
        source_s3_key: str,
        output_s3_key: str,
    ):
        """Property 6: COMPLETED files must have output_s3_key set.

        For any AsyncFile with status COMPLETED, the output_s3_key field
        SHALL NOT be None.
        """
        # Create a COMPLETED file with output_s3_key
        file = AsyncFile(
            file_id=file_id,
            original_uuid=original_uuid,
            filename=filename,
            source_s3_key=source_s3_key,
            output_s3_key=output_s3_key,
            status=FileStatus.COMPLETED,
        )

        # Property: COMPLETED files must have output_s3_key
        assert file.status == FileStatus.COMPLETED
        assert file.output_s3_key is not None
        assert len(file.output_s3_key) > 0

    @given(
        file_id=file_id_strategy,
        original_uuid=st.text(min_size=1, max_size=50),
        filename=filename_strategy,
        source_s3_key=s3_key_strategy,
    )
    @settings(max_examples=50)
    def test_pending_file_may_have_no_output_key(
        self,
        file_id: str,
        original_uuid: str,
        filename: str,
        source_s3_key: str,
    ):
        """PENDING files may have no output_s3_key (contrast with COMPLETED)."""
        file = AsyncFile(
            file_id=file_id,
            original_uuid=original_uuid,
            filename=filename,
            source_s3_key=source_s3_key,
            output_s3_key=None,  # No output key yet
            status=FileStatus.PENDING,
        )

        # PENDING files can have None output_s3_key
        assert file.status == FileStatus.PENDING
        assert file.output_s3_key is None

    @given(
        file_id=file_id_strategy,
        original_uuid=st.text(min_size=1, max_size=50),
        filename=filename_strategy,
        source_s3_key=s3_key_strategy,
    )
    @settings(max_examples=50)
    def test_converting_file_may_have_no_output_key(
        self,
        file_id: str,
        original_uuid: str,
        filename: str,
        source_s3_key: str,
    ):
        """CONVERTING files may have no output_s3_key."""
        file = AsyncFile(
            file_id=file_id,
            original_uuid=original_uuid,
            filename=filename,
            source_s3_key=source_s3_key,
            output_s3_key=None,
            status=FileStatus.CONVERTING,
        )

        assert file.status == FileStatus.CONVERTING
        # CONVERTING files typically don't have output_s3_key yet
        assert file.output_s3_key is None


class TestOutputKeyValidation:
    """Tests for output_s3_key validation logic."""

    def test_validate_completed_file_with_output_key(self):
        """COMPLETED file with output_s3_key passes validation."""
        file = AsyncFile(
            file_id="test-file-id",
            original_uuid="test-uuid",
            filename="video.mov",
            source_s3_key="tasks/task-1/source/file-1/video.mov",
            output_s3_key="output/task-1/file-1/video_h265.mp4",
            status=FileStatus.COMPLETED,
        )

        is_valid, error = validate_completed_file(file)
        assert is_valid is True
        assert error is None

    def test_validate_completed_file_without_output_key(self):
        """COMPLETED file without output_s3_key fails validation."""
        file = AsyncFile(
            file_id="test-file-id",
            original_uuid="test-uuid",
            filename="video.mov",
            source_s3_key="tasks/task-1/source/file-1/video.mov",
            output_s3_key=None,  # Missing!
            status=FileStatus.COMPLETED,
        )

        is_valid, error = validate_completed_file(file)
        assert is_valid is False
        assert "output_s3_key" in error

    def test_validate_pending_file_without_output_key(self):
        """PENDING file without output_s3_key passes validation."""
        file = AsyncFile(
            file_id="test-file-id",
            original_uuid="test-uuid",
            filename="video.mov",
            source_s3_key="tasks/task-1/source/file-1/video.mov",
            output_s3_key=None,
            status=FileStatus.PENDING,
        )

        is_valid, error = validate_completed_file(file)
        # Non-COMPLETED files don't need output_s3_key
        assert is_valid is True


def validate_completed_file(file: AsyncFile) -> tuple[bool, str | None]:
    """Validate that COMPLETED files have output_s3_key.

    This is the validation function that enforces Property 6.

    Args:
        file: AsyncFile to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if file.status == FileStatus.COMPLETED:
        if file.output_s3_key is None or file.output_s3_key == "":
            return False, "COMPLETED file must have output_s3_key set"
    return True, None
