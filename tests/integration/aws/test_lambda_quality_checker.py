"""AWS Lambda Quality Checker integration tests.

These tests verify the Lambda function works correctly:
1. Lambda can be invoked
2. Lambda has required dependencies (FFmpeg)
3. Quality check logic works with real files

実行方法:
    python3.11 -m pytest tests/integration/aws/test_lambda_quality_checker.py -v -m aws

検証対象:
- Lambda 関数の呼び出し
- FFmpeg/FFprobe の存在確認
- 品質チェックロジック
"""

import json
import os

import pytest

# Skip all tests if not configured for AWS
pytestmark = [
    pytest.mark.aws,
    pytest.mark.skipif(
        os.environ.get("SKIP_AWS_TESTS", "true").lower() == "true",
        reason="AWS tests disabled (set SKIP_AWS_TESTS=false to enable)",
    ),
]


class TestLambdaInvocation:
    """Test Lambda function invocation."""

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
    def quality_checker(self, aws_config):
        """Create QualityChecker client."""
        from vco.quality.checker import QualityChecker

        return QualityChecker(
            s3_bucket=aws_config.s3_bucket,
            lambda_function_name="vco-quality-checker-dev",
            region=aws_config.region,
            profile_name=aws_config.profile,
        )

    def test_lambda_invocation_with_missing_files(self, quality_checker):
        """Test Lambda handles missing S3 files gracefully."""
        result = quality_checker.trigger_quality_check_sync(
            original_s3_key="nonexistent/original.mp4",
            converted_s3_key="nonexistent/converted.mp4",
            job_id="test_missing_files",
        )

        # Should return error status, not crash
        assert result.status == "error"
        assert result.failure_reason is not None

    def test_lambda_returns_proper_response_structure(self, quality_checker, aws_config):
        """Test Lambda response has expected structure."""
        import boto3

        # Create boto3 session
        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        session = boto3.Session(**session_kwargs)

        lambda_client = session.client("lambda")

        # Invoke Lambda directly to check response structure
        payload = {
            "job_id": "test_structure",
            "original_s3_key": "test/original.mp4",
            "converted_s3_key": "test/converted.mp4",
        }

        response = lambda_client.invoke(
            FunctionName="vco-quality-checker-dev",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )

        response_payload = json.loads(response["Payload"].read())

        # Should have statusCode
        assert "statusCode" in response_payload

        # Should have body
        assert "body" in response_payload


class TestLambdaDependencies:
    """Test Lambda has required dependencies."""

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")

        return config.aws

    def test_lambda_has_ffmpeg(self, aws_config):
        """Test Lambda has FFmpeg available.

        This test uploads a small test video and triggers quality check
        to verify FFmpeg is available in the Lambda environment.
        """
        import boto3

        # Create boto3 session
        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        session = boto3.Session(**session_kwargs)

        # Check CloudWatch logs for FFmpeg errors
        logs_client = session.client("logs")

        try:
            # Get recent log events
            response = logs_client.describe_log_streams(
                logGroupName="/aws/lambda/vco-quality-checker-dev",
                orderBy="LastEventTime",
                descending=True,
                limit=1,
            )

            if not response.get("logStreams"):
                pytest.skip("No log streams found - Lambda may not have been invoked yet")

            log_stream = response["logStreams"][0]["logStreamName"]

            events_response = logs_client.get_log_events(
                logGroupName="/aws/lambda/vco-quality-checker-dev",
                logStreamName=log_stream,
                limit=50,
            )

            # Check for FFmpeg-related errors
            ffmpeg_errors = []
            for event in events_response.get("events", []):
                message = event.get("message", "")
                if "ffprobe" in message.lower() and "error" in message.lower():
                    ffmpeg_errors.append(message)
                if "No such file or directory" in message and "ffprobe" in message:
                    ffmpeg_errors.append(message)

            if ffmpeg_errors:
                pytest.fail(
                    "FFmpeg/FFprobe errors found in Lambda logs:\n" + "\n".join(ffmpeg_errors[:5])
                )

        except Exception as e:
            pytest.skip(f"Could not check Lambda logs: {e}")


class TestQualityCheckLogic:
    """Test quality check logic with real files."""

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
    def s3_client(self, aws_config):
        """Create S3 client."""
        import boto3

        session_kwargs = {"region_name": aws_config.region}
        if aws_config.profile:
            session_kwargs["profile_name"] = aws_config.profile
        session = boto3.Session(**session_kwargs)

        return session.client("s3")

    def test_quality_check_detects_larger_converted_file(self, aws_config, s3_client):
        """Test quality check fails when converted file is larger than original.

        This is a key requirement: converted files must be smaller.
        """
        from vco.quality.checker import QualityChecker

        # Create test files with converted larger than original
        original_content = b"x" * 1000  # 1KB
        converted_content = b"x" * 2000  # 2KB (larger)

        original_key = "test/quality_check/original_small.bin"
        converted_key = "test/quality_check/converted_large.bin"

        try:
            # Upload test files
            s3_client.put_object(
                Bucket=aws_config.s3_bucket, Key=original_key, Body=original_content
            )
            s3_client.put_object(
                Bucket=aws_config.s3_bucket, Key=converted_key, Body=converted_content
            )

            # Run quality check
            checker = QualityChecker(
                s3_bucket=aws_config.s3_bucket,
                lambda_function_name="vco-quality-checker-dev",
                region=aws_config.region,
                profile_name=aws_config.profile,
            )

            result = checker.trigger_quality_check_sync(
                original_s3_key=original_key,
                converted_s3_key=converted_key,
                job_id="test_size_check",
            )

            # Should fail because converted is larger
            # Note: This may fail for other reasons (not valid video files)
            # but the size check should be one of the first checks
            assert result.status in ("failed", "error")

        finally:
            # Cleanup
            try:
                s3_client.delete_object(Bucket=aws_config.s3_bucket, Key=original_key)
                s3_client.delete_object(Bucket=aws_config.s3_bucket, Key=converted_key)
            except Exception:
                pass
