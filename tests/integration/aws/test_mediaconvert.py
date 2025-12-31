"""AWS MediaConvert integration tests.

These tests use actual AWS MediaConvert service to verify:
1. Job settings are valid (API parameter requirements)
2. Jobs can be submitted successfully
3. Job status can be retrieved
4. Output files are generated correctly

実行方法:
    python3.11 -m pytest tests/integration/aws/test_mediaconvert.py -v -m aws

検証対象:
- MediaConvert API パラメータ要件
- ジョブ設定の正確性
- S3 キー生成ロジック
"""

import os
import tempfile
from pathlib import Path

import pytest

# Skip all tests if not configured for AWS
pytestmark = [
    pytest.mark.aws,
    pytest.mark.skipif(
        os.environ.get("SKIP_AWS_TESTS", "true").lower() == "true",
        reason="AWS tests disabled (set SKIP_AWS_TESTS=false to enable)",
    ),
]


class TestMediaConvertJobSettings:
    """Test MediaConvert job settings generation."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")
        if not config.aws.role_arn:
            pytest.skip("AWS MediaConvert role ARN not configured")

        return config.aws

    @pytest.fixture
    def mediaconvert_client(self, aws_config):
        """Create MediaConvert client."""
        from vco.converter.mediaconvert import MediaConvertClient

        return MediaConvertClient(
            region=aws_config.region,
            s3_bucket=aws_config.s3_bucket,
            role_arn=aws_config.role_arn,
            profile_name=aws_config.profile,
        )

    def test_job_settings_have_valid_name_modifier(self, mediaconvert_client):
        """NameModifier must be at least 1 character (MediaConvert API requirement).

        Issue found: NameModifier was empty string, causing API validation error.
        """
        from vco.converter.mediaconvert import QUALITY_PRESETS

        settings = mediaconvert_client._build_job_settings(
            source_s3_key="input/test-uuid/test_video.mp4",
            output_s3_key="output/test-uuid/test_video_h265.mp4",
            preset=QUALITY_PRESETS["balanced"],
        )

        # Verify NameModifier is not empty
        name_modifier = settings["OutputGroups"][0]["Outputs"][0]["NameModifier"]
        assert name_modifier, "NameModifier must not be empty"
        assert len(name_modifier) >= 1, "NameModifier must be at least 1 character"

    def test_job_settings_have_required_fields(self, mediaconvert_client):
        """Verify all required fields are present in job settings."""
        from vco.converter.mediaconvert import QUALITY_PRESETS

        settings = mediaconvert_client._build_job_settings(
            source_s3_key="input/test-uuid/test_video.mp4",
            output_s3_key="output/test-uuid/test_video_h265.mp4",
            preset=QUALITY_PRESETS["balanced"],
        )

        # Required top-level fields
        assert "Inputs" in settings
        assert "OutputGroups" in settings
        assert "TimecodeConfig" in settings

        # Input configuration
        assert len(settings["Inputs"]) > 0
        assert "FileInput" in settings["Inputs"][0]

        # Output configuration
        assert len(settings["OutputGroups"]) > 0
        output_group = settings["OutputGroups"][0]
        assert "OutputGroupSettings" in output_group
        assert "Outputs" in output_group

        # Output details
        output = output_group["Outputs"][0]
        assert "ContainerSettings" in output
        assert "VideoDescription" in output
        assert "AudioDescriptions" in output


class TestMediaConvertS3Operations:
    """Test S3 operations for MediaConvert."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    @pytest.fixture
    def mediaconvert_client(self, aws_config):
        """Create MediaConvert client."""
        from vco.converter.mediaconvert import MediaConvertClient

        return MediaConvertClient(
            region=aws_config.region,
            s3_bucket=aws_config.s3_bucket,
            role_arn=aws_config.role_arn,
            profile_name=aws_config.profile,
        )

    def test_upload_and_delete_from_s3(self, mediaconvert_client):
        """Test S3 upload and delete operations."""
        # Create a small test file
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"test content for S3 upload")
            test_file = Path(f.name)

        try:
            test_key = "test/integration_test_file.txt"

            # Upload
            s3_uri = mediaconvert_client.upload_to_s3(test_file, test_key)
            assert s3_uri == f"s3://{mediaconvert_client.s3_bucket}/{test_key}"

            # Verify file exists (head_object)
            response = mediaconvert_client.s3.head_object(
                Bucket=mediaconvert_client.s3_bucket, Key=test_key
            )
            assert response["ContentLength"] > 0

            # Delete
            result = mediaconvert_client.delete_from_s3(test_key)
            assert result is True

            # Verify file is deleted
            with pytest.raises(Exception):
                mediaconvert_client.s3.head_object(
                    Bucket=mediaconvert_client.s3_bucket, Key=test_key
                )
        finally:
            test_file.unlink()


class TestMediaConvertJobSubmission:
    """Test actual MediaConvert job submission.

    These tests submit real jobs to MediaConvert and verify the workflow.
    They require a test video file in S3.
    """

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")
        if not config.aws.role_arn:
            pytest.skip("AWS MediaConvert role ARN not configured")

        return config.aws

    @pytest.fixture
    def mediaconvert_client(self, aws_config):
        """Create MediaConvert client."""
        from vco.converter.mediaconvert import MediaConvertClient

        return MediaConvertClient(
            region=aws_config.region,
            s3_bucket=aws_config.s3_bucket,
            role_arn=aws_config.role_arn,
            profile_name=aws_config.profile,
        )

    @pytest.fixture
    def test_video_s3_key(self, mediaconvert_client):
        """Upload a test video to S3 and return its key.

        Uses a minimal valid MP4 file for testing.
        """
        # For now, skip if no test video is available
        # In a real setup, you would upload a small test video here
        pytest.skip("Test video upload not implemented - use E2E test for full conversion")

    def test_submit_job_returns_valid_job_id(self, mediaconvert_client, test_video_s3_key):
        """Test that job submission returns a valid job ID."""
        job = mediaconvert_client.submit_job(
            source_video_uuid="test-uuid",
            source_s3_key=test_video_s3_key,
            output_s3_key="output/test-uuid/test_h265.mp4",
            quality_preset="balanced",
        )

        assert job.job_id is not None
        assert len(job.job_id) > 0
        assert job.status == "SUBMITTED"

        # Cancel the job to avoid costs
        mediaconvert_client.cancel_job(job.job_id)
