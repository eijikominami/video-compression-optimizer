"""Unit tests for S3KeyBuilder utility.

Tests: Task 1.2
Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6
"""

import pytest

from vco.utils.s3_keys import S3KeyBuilder


class TestSourceKey:
    """Tests for source_key method."""

    def test_basic_format(self):
        """Source key should follow format: async/{task_id}/input/{file_id}/{filename}."""
        key = S3KeyBuilder.source_key("task-123", "file-456", "video.mov")
        assert key == "async/task-123/input/file-456/video.mov"

    def test_with_uuid(self):
        """Should work with UUID-style identifiers."""
        key = S3KeyBuilder.source_key(
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "f1e2d3c4-b5a6-0987-dcba-0987654321fe",
            "MVI_1234.MOV",
        )
        assert key == (
            "async/a1b2c3d4-e5f6-7890-abcd-ef1234567890/"
            "input/f1e2d3c4-b5a6-0987-dcba-0987654321fe/MVI_1234.MOV"
        )

    def test_preserves_filename_extension(self):
        """Should preserve original filename with extension."""
        key = S3KeyBuilder.source_key("t1", "f1", "video.MP4")
        assert key.endswith("/video.MP4")


class TestOutputKey:
    """Tests for output_key method."""

    def test_basic_format(self):
        """Output key should follow format: output/{task_id}/{file_id}/{stem}_h265.mp4."""
        key = S3KeyBuilder.output_key("task-123", "file-456", "video.mov")
        assert key == "output/task-123/file-456/video_h265.mp4"

    def test_removes_original_extension(self):
        """Should remove original extension and add _h265.mp4."""
        key = S3KeyBuilder.output_key("t1", "f1", "MVI_1234.MOV")
        assert key == "output/t1/f1/MVI_1234_h265.mp4"

    def test_handles_multiple_dots_in_filename(self):
        """Should handle filenames with multiple dots."""
        key = S3KeyBuilder.output_key("t1", "f1", "video.2024.01.01.mov")
        assert key == "output/t1/f1/video.2024.01.01_h265.mp4"

    def test_always_outputs_mp4(self):
        """Output should always be .mp4 regardless of input format."""
        for ext in [".mov", ".MOV", ".avi", ".mkv", ".MP4"]:
            key = S3KeyBuilder.output_key("t1", "f1", f"video{ext}")
            assert key.endswith("_h265.mp4")


class TestMetadataKey:
    """Tests for metadata_key method."""

    def test_basic_format(self):
        """Metadata key should follow format: async/{task_id}/input/{file_id}/metadata.json."""
        key = S3KeyBuilder.metadata_key("task-123", "file-456", "video.mov")
        assert key == "async/task-123/input/file-456/metadata.json"

    def test_filename_not_used(self):
        """Filename parameter should not affect the key (kept for API consistency)."""
        key1 = S3KeyBuilder.metadata_key("t1", "f1", "video1.mov")
        key2 = S3KeyBuilder.metadata_key("t1", "f1", "video2.mov")
        assert key1 == key2 == "async/t1/input/f1/metadata.json"


class TestParseSourceKey:
    """Tests for parse_source_key method."""

    def test_parses_valid_key(self):
        """Should parse valid source key."""
        task_id, file_id, filename = S3KeyBuilder.parse_source_key(
            "async/task-123/input/file-456/video.mov"
        )
        assert task_id == "task-123"
        assert file_id == "file-456"
        assert filename == "video.mov"

    def test_roundtrip(self):
        """parse_source_key should reverse source_key."""
        original_key = S3KeyBuilder.source_key("t1", "f1", "video.mov")
        task_id, file_id, filename = S3KeyBuilder.parse_source_key(original_key)
        assert task_id == "t1"
        assert file_id == "f1"
        assert filename == "video.mov"

    def test_invalid_format_raises(self):
        """Should raise ValueError for invalid format."""
        with pytest.raises(ValueError, match="Invalid source key format"):
            S3KeyBuilder.parse_source_key("invalid/key/format")

    def test_wrong_prefix_raises(self):
        """Should raise ValueError if prefix is not 'async'."""
        with pytest.raises(ValueError, match="Invalid source key format"):
            S3KeyBuilder.parse_source_key("output/t1/input/f1/video.mov")


class TestParseOutputKey:
    """Tests for parse_output_key method."""

    def test_parses_valid_key(self):
        """Should parse valid output key."""
        task_id, file_id, filename = S3KeyBuilder.parse_output_key(
            "output/task-123/file-456/video_h265.mp4"
        )
        assert task_id == "task-123"
        assert file_id == "file-456"
        assert filename == "video_h265.mp4"

    def test_roundtrip(self):
        """parse_output_key should reverse output_key."""
        original_key = S3KeyBuilder.output_key("t1", "f1", "video.mov")
        task_id, file_id, filename = S3KeyBuilder.parse_output_key(original_key)
        assert task_id == "t1"
        assert file_id == "f1"
        assert filename == "video_h265.mp4"

    def test_invalid_format_raises(self):
        """Should raise ValueError for invalid format."""
        with pytest.raises(ValueError, match="Invalid output key format"):
            S3KeyBuilder.parse_output_key("invalid/key")

    def test_wrong_prefix_raises(self):
        """Should raise ValueError if prefix is not 'output'."""
        with pytest.raises(ValueError, match="Invalid output key format"):
            S3KeyBuilder.parse_output_key("async/t1/f1/video.mp4")


class TestParseMetadataKey:
    """Tests for parse_metadata_key method."""

    def test_parses_valid_key(self):
        """Should parse valid metadata key."""
        task_id, file_id, filename = S3KeyBuilder.parse_metadata_key(
            "async/task-123/input/file-456/metadata.json"
        )
        assert task_id == "task-123"
        assert file_id == "file-456"
        assert filename == "metadata.json"

    def test_roundtrip(self):
        """parse_metadata_key should reverse metadata_key."""
        original_key = S3KeyBuilder.metadata_key("t1", "f1", "video.mov")
        task_id, file_id, filename = S3KeyBuilder.parse_metadata_key(original_key)
        assert task_id == "t1"
        assert file_id == "f1"
        assert filename == "metadata.json"

    def test_invalid_format_raises(self):
        """Should raise ValueError for invalid format."""
        with pytest.raises(ValueError, match="Invalid metadata key format"):
            S3KeyBuilder.parse_metadata_key("invalid/key")


class TestNoAwsDependencies:
    """Tests that S3KeyBuilder has no AWS dependencies."""

    def test_import_without_boto3(self):
        """S3KeyBuilder should be importable without boto3."""
        # This test passes if the import at the top of this file succeeded
        # The module should not import boto3 or any AWS SDK
        import vco.utils.s3_keys as module

        # Check module doesn't have boto3 in its namespace
        assert not hasattr(module, "boto3")
        assert not hasattr(module, "botocore")
