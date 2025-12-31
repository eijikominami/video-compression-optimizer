"""Property-based tests for S3 key format consistency.

Tests: Task 1.2
Properties: 3, 4, 5
Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.utils.s3_keys import S3KeyBuilder

# Strategies for generating valid identifiers
uuid_strategy = st.uuids().map(str)
filename_strategy = st.from_regex(r"[A-Za-z0-9_-]{1,50}\.(mov|mp4|avi|mkv|MOV|MP4)", fullmatch=True)


class TestProperty3SourceKeyFormat:
    """Property 3: Source key format is consistent."""

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=100)
    def test_source_key_format_invariant(self, task_id: str, file_id: str, filename: str):
        """Source key always follows format: async/{task_id}/input/{file_id}/{filename}."""
        key = S3KeyBuilder.source_key(task_id, file_id, filename)

        # Verify format
        assert key.startswith("async/")
        assert "/input/" in key
        assert key.endswith(f"/{filename}")

        # Verify roundtrip
        parsed_task_id, parsed_file_id, parsed_filename = S3KeyBuilder.parse_source_key(key)
        assert parsed_task_id == task_id
        assert parsed_file_id == file_id
        assert parsed_filename == filename

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=50)
    def test_source_key_no_double_slashes(self, task_id: str, file_id: str, filename: str):
        """Source key should never contain double slashes."""
        key = S3KeyBuilder.source_key(task_id, file_id, filename)
        assert "//" not in key


class TestProperty4OutputKeyFormat:
    """Property 4: Output key format is consistent."""

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=100)
    def test_output_key_format_invariant(self, task_id: str, file_id: str, filename: str):
        """Output key always follows format: output/{task_id}/{file_id}/{stem}_h265.mp4."""
        key = S3KeyBuilder.output_key(task_id, file_id, filename)

        # Verify format
        assert key.startswith("output/")
        assert key.endswith("_h265.mp4")

        # Verify roundtrip (note: original extension is lost)
        parsed_task_id, parsed_file_id, parsed_filename = S3KeyBuilder.parse_output_key(key)
        assert parsed_task_id == task_id
        assert parsed_file_id == file_id
        assert parsed_filename.endswith("_h265.mp4")

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=50)
    def test_output_key_always_mp4(self, task_id: str, file_id: str, filename: str):
        """Output key always ends with .mp4 regardless of input format."""
        key = S3KeyBuilder.output_key(task_id, file_id, filename)
        assert key.endswith(".mp4")

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=50)
    def test_output_key_no_double_slashes(self, task_id: str, file_id: str, filename: str):
        """Output key should never contain double slashes."""
        key = S3KeyBuilder.output_key(task_id, file_id, filename)
        assert "//" not in key


class TestProperty5MetadataKeyFormat:
    """Property 5: Metadata key format is consistent."""

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=100)
    def test_metadata_key_format_invariant(self, task_id: str, file_id: str, filename: str):
        """Metadata key always follows format: async/{task_id}/input/{file_id}/metadata.json."""
        key = S3KeyBuilder.metadata_key(task_id, file_id, filename)

        # Verify format
        assert key.startswith("async/")
        assert "/input/" in key
        assert key.endswith("/metadata.json")

        # Verify roundtrip
        parsed_task_id, parsed_file_id, parsed_filename = S3KeyBuilder.parse_metadata_key(key)
        assert parsed_task_id == task_id
        assert parsed_file_id == file_id
        assert parsed_filename == "metadata.json"

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=50)
    def test_metadata_key_no_double_slashes(self, task_id: str, file_id: str, filename: str):
        """Metadata key should never contain double slashes."""
        key = S3KeyBuilder.metadata_key(task_id, file_id, filename)
        assert "//" not in key


class TestCrossKeyConsistency:
    """Tests for consistency across different key types."""

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=50)
    def test_same_task_id_in_all_keys(self, task_id: str, file_id: str, filename: str):
        """All key types should contain the same task_id."""
        source = S3KeyBuilder.source_key(task_id, file_id, filename)
        output = S3KeyBuilder.output_key(task_id, file_id, filename)
        metadata = S3KeyBuilder.metadata_key(task_id, file_id, filename)

        # Parse and verify task_id
        assert S3KeyBuilder.parse_source_key(source)[0] == task_id
        assert S3KeyBuilder.parse_output_key(output)[0] == task_id
        assert S3KeyBuilder.parse_metadata_key(metadata)[0] == task_id

    @given(task_id=uuid_strategy, file_id=uuid_strategy, filename=filename_strategy)
    @settings(max_examples=50)
    def test_same_file_id_in_all_keys(self, task_id: str, file_id: str, filename: str):
        """All key types should contain the same file_id."""
        source = S3KeyBuilder.source_key(task_id, file_id, filename)
        output = S3KeyBuilder.output_key(task_id, file_id, filename)
        metadata = S3KeyBuilder.metadata_key(task_id, file_id, filename)

        # Parse and verify file_id
        assert S3KeyBuilder.parse_source_key(source)[1] == file_id
        assert S3KeyBuilder.parse_output_key(output)[1] == file_id
        assert S3KeyBuilder.parse_metadata_key(metadata)[1] == file_id
