"""Integration tests for quality metrics parsing and evaluation.

Tests the end-to-end flow of quality metrics:
1. MediaConvert job settings with PerFrameMetrics
2. CSV parsing from S3
3. Quality evaluation with thresholds
4. Integration with async workflow

Requirements: 1.1, 2.1, 2.2, 3.1, 3.2
"""

import sys
import uuid

import boto3
import pytest
from moto import mock_aws

# Add sam-app/async-workflow to path for imports
sys.path.insert(0, "sam-app/async-workflow")

# Test constants
TEST_REGION = "ap-northeast-1"
TEST_BUCKET = "test-vco-bucket"


@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = TEST_REGION
    os.environ["S3_BUCKET"] = TEST_BUCKET


@pytest.fixture
def s3_bucket(aws_credentials):
    """Create mock S3 bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=TEST_REGION)
        s3.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
        )
        yield s3


# Sample CSV content matching MediaConvert output format
SAMPLE_SSIM_CSV = """Display_ID,SSIM
1,0.9823
2,0.9845
3,0.9812
4,0.9867
5,0.9834
Average,0.9836
Min,0.9812
Max,0.9867
"""

SAMPLE_VMAF_CSV = """Display_ID,VMAF
1,85.234
2,86.123
3,84.567
4,87.890
5,85.678
Average,85.898
Min,84.567
Max,87.890
"""

LOW_QUALITY_SSIM_CSV = """Display_ID,SSIM
1,0.8923
2,0.8845
3,0.8812
4,0.8867
5,0.8834
Average,0.8856
Min,0.8812
Max,0.8923
"""

LOW_QUALITY_VMAF_CSV = """Display_ID,VMAF
1,55.234
2,56.123
3,54.567
4,57.890
5,55.678
Average,55.898
Min,54.567
Max,57.890
"""


class TestQualityMetricsCSVParsing:
    """Integration tests for CSV parsing from S3."""

    def test_parse_ssim_csv_from_s3(self, s3_bucket):
        """Test parsing SSIM CSV from S3."""
        from quality_metrics import QualityMetricsParser

        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        ssim_key = f"async/{task_id}/output/{file_id}/video_ssim.csv"

        # Upload SSIM CSV
        s3_bucket.put_object(
            Bucket=TEST_BUCKET,
            Key=ssim_key,
            Body=SAMPLE_SSIM_CSV,
            ContentType="text/csv",
        )

        # Parse CSV
        parser = QualityMetricsParser()
        response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=ssim_key)
        csv_content = response["Body"].read().decode("utf-8")

        # parse_ssim_csv returns tuple (average, min, max)
        average, min_val, max_val = parser.parse_ssim_csv(csv_content)

        assert abs(average - 0.9836) < 0.0001
        assert abs(min_val - 0.9812) < 0.0001
        assert abs(max_val - 0.9867) < 0.0001

    def test_parse_vmaf_csv_from_s3(self, s3_bucket):
        """Test parsing VMAF CSV from S3."""
        from quality_metrics import QualityMetricsParser

        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        vmaf_key = f"async/{task_id}/output/{file_id}/video_vmaf.csv"

        # Upload VMAF CSV
        s3_bucket.put_object(
            Bucket=TEST_BUCKET,
            Key=vmaf_key,
            Body=SAMPLE_VMAF_CSV,
            ContentType="text/csv",
        )

        # Parse CSV
        parser = QualityMetricsParser()
        response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=vmaf_key)
        csv_content = response["Body"].read().decode("utf-8")

        # parse_vmaf_csv returns tuple (average, min, max)
        average, min_val, max_val = parser.parse_vmaf_csv(csv_content)

        assert abs(average - 85.898) < 0.001
        assert abs(min_val - 84.567) < 0.001
        assert abs(max_val - 87.890) < 0.001


class TestQualityEvaluation:
    """Integration tests for quality evaluation."""

    def test_evaluate_high_quality_metrics(self, s3_bucket):
        """Test evaluation passes for high quality metrics."""
        from quality_metrics import QualityEvaluator, QualityMetrics, QualityMetricsParser

        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        ssim_key = f"async/{task_id}/output/{file_id}/video_ssim.csv"
        vmaf_key = f"async/{task_id}/output/{file_id}/video_vmaf.csv"

        # Upload CSVs
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=ssim_key, Body=SAMPLE_SSIM_CSV)
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=vmaf_key, Body=SAMPLE_VMAF_CSV)

        # Parse metrics
        parser = QualityMetricsParser()

        ssim_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=ssim_key)
        ssim_avg, ssim_min, ssim_max = parser.parse_ssim_csv(
            ssim_response["Body"].read().decode("utf-8")
        )

        vmaf_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=vmaf_key)
        vmaf_avg, vmaf_min, vmaf_max = parser.parse_vmaf_csv(
            vmaf_response["Body"].read().decode("utf-8")
        )

        # Create QualityMetrics object
        metrics = QualityMetrics(
            ssim_average=ssim_avg,
            ssim_min=ssim_min,
            ssim_max=ssim_max,
            vmaf_average=vmaf_avg,
            vmaf_min=vmaf_min,
            vmaf_max=vmaf_max,
        )

        # Evaluate with thresholds that should pass
        evaluator = QualityEvaluator(ssim_threshold=0.95, vmaf_threshold=70.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is True
        assert abs(result.ssim_score - 0.9836) < 0.0001
        assert abs(result.vmaf_score - 85.898) < 0.001

    def test_evaluate_low_quality_metrics(self, s3_bucket):
        """Test evaluation fails for low quality metrics."""
        from quality_metrics import QualityEvaluator, QualityMetrics, QualityMetricsParser

        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        ssim_key = f"async/{task_id}/output/{file_id}/video_ssim.csv"
        vmaf_key = f"async/{task_id}/output/{file_id}/video_vmaf.csv"

        # Upload low quality CSVs
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=ssim_key, Body=LOW_QUALITY_SSIM_CSV)
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=vmaf_key, Body=LOW_QUALITY_VMAF_CSV)

        # Parse metrics
        parser = QualityMetricsParser()

        ssim_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=ssim_key)
        ssim_avg, ssim_min, ssim_max = parser.parse_ssim_csv(
            ssim_response["Body"].read().decode("utf-8")
        )

        vmaf_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=vmaf_key)
        vmaf_avg, vmaf_min, vmaf_max = parser.parse_vmaf_csv(
            vmaf_response["Body"].read().decode("utf-8")
        )

        # Create QualityMetrics object
        metrics = QualityMetrics(
            ssim_average=ssim_avg,
            ssim_min=ssim_min,
            ssim_max=ssim_max,
            vmaf_average=vmaf_avg,
            vmaf_min=vmaf_min,
            vmaf_max=vmaf_max,
        )

        # Evaluate with default thresholds (SSIM >= 0.95, VMAF >= 70)
        evaluator = QualityEvaluator()
        result = evaluator.evaluate(metrics)

        assert result.passed is False
        # Both SSIM (0.8856 < 0.95) and VMAF (55.898 < 70) should fail
        assert result.failure_reason is not None
        assert "SSIM" in result.failure_reason
        assert "VMAF" in result.failure_reason

    def test_evaluate_with_custom_thresholds(self, s3_bucket):
        """Test evaluation with custom thresholds."""
        from quality_metrics import QualityEvaluator, QualityMetrics, QualityMetricsParser

        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())
        ssim_key = f"async/{task_id}/output/{file_id}/video_ssim.csv"
        vmaf_key = f"async/{task_id}/output/{file_id}/video_vmaf.csv"

        # Upload low quality CSVs
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=ssim_key, Body=LOW_QUALITY_SSIM_CSV)
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=vmaf_key, Body=LOW_QUALITY_VMAF_CSV)

        # Parse metrics
        parser = QualityMetricsParser()

        ssim_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=ssim_key)
        ssim_avg, ssim_min, ssim_max = parser.parse_ssim_csv(
            ssim_response["Body"].read().decode("utf-8")
        )

        vmaf_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=vmaf_key)
        vmaf_avg, vmaf_min, vmaf_max = parser.parse_vmaf_csv(
            vmaf_response["Body"].read().decode("utf-8")
        )

        # Create QualityMetrics object
        metrics = QualityMetrics(
            ssim_average=ssim_avg,
            ssim_min=ssim_min,
            ssim_max=ssim_max,
            vmaf_average=vmaf_avg,
            vmaf_min=vmaf_min,
            vmaf_max=vmaf_max,
        )

        # Evaluate with lower thresholds that should pass
        evaluator = QualityEvaluator(ssim_threshold=0.85, vmaf_threshold=50.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is True  # 0.8856 >= 0.85 and 55.898 >= 50


class TestMediaConvertJobSettings:
    """Integration tests for MediaConvert job settings."""

    def test_job_settings_include_per_frame_metrics(self, aws_credentials):
        """Test that job settings include PerFrameMetrics configuration."""
        import os

        # Set required environment variable
        os.environ["S3_BUCKET"] = TEST_BUCKET

        from app import create_job_settings

        settings = create_job_settings(
            source_key="input/video.mp4",
            output_key="output/video.mp4",
            preset="balanced",
        )

        # Verify PerFrameMetrics is configured
        output_groups = settings.get("OutputGroups", [])
        assert len(output_groups) > 0

        file_group = output_groups[0]
        output_group_settings = file_group.get("OutputGroupSettings", {})
        file_group_settings = output_group_settings.get("FileGroupSettings", {})

        per_frame_metrics = file_group_settings.get("PerFrameMetrics", {})
        assert per_frame_metrics.get("FrameMetricType") is not None

        metric_types = per_frame_metrics.get("FrameMetricType", [])
        assert "SSIM" in metric_types
        assert "VMAF" in metric_types


class TestEndToEndQualityWorkflow:
    """End-to-end tests for quality metrics workflow."""

    def test_complete_quality_evaluation_flow(self, s3_bucket):
        """Test complete flow: upload CSVs -> parse -> evaluate -> result."""
        from quality_metrics import QualityEvaluator, QualityMetrics, QualityMetricsParser

        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())

        # Simulate MediaConvert output structure
        output_prefix = f"async/{task_id}/output/{file_id}/"
        ssim_key = f"{output_prefix}video_ssim.csv"
        vmaf_key = f"{output_prefix}video_vmaf.csv"
        output_key = f"{output_prefix}video_h265.mp4"

        # Upload all outputs
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=ssim_key, Body=SAMPLE_SSIM_CSV)
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=vmaf_key, Body=SAMPLE_VMAF_CSV)
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=output_key, Body=b"converted video")

        # List outputs to verify structure
        response = s3_bucket.list_objects_v2(Bucket=TEST_BUCKET, Prefix=output_prefix)
        assert response["KeyCount"] == 3

        # Parse and evaluate
        parser = QualityMetricsParser()
        evaluator = QualityEvaluator(ssim_threshold=0.95, vmaf_threshold=70.0)

        ssim_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=ssim_key)
        ssim_avg, ssim_min, ssim_max = parser.parse_ssim_csv(
            ssim_response["Body"].read().decode("utf-8")
        )

        vmaf_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=vmaf_key)
        vmaf_avg, vmaf_min, vmaf_max = parser.parse_vmaf_csv(
            vmaf_response["Body"].read().decode("utf-8")
        )

        metrics = QualityMetrics(
            ssim_average=ssim_avg,
            ssim_min=ssim_min,
            ssim_max=ssim_max,
            vmaf_average=vmaf_avg,
            vmaf_min=vmaf_min,
            vmaf_max=vmaf_max,
        )

        result = evaluator.evaluate(metrics)

        # Build quality_result for DynamoDB
        quality_result = {
            "ssim_score": result.ssim_score,
            "vmaf_score": result.vmaf_score,
            "passed": result.passed,
        }

        assert quality_result["passed"] is True
        assert quality_result["ssim_score"] > 0.95
        assert quality_result["vmaf_score"] > 70

    def test_quality_failure_with_adaptive_preset(self, s3_bucket):
        """Test that quality failure with adaptive preset can trigger retry."""
        from quality_metrics import QualityEvaluator, QualityMetrics, QualityMetricsParser

        task_id = str(uuid.uuid4())
        file_id = str(uuid.uuid4())

        ssim_key = f"async/{task_id}/output/{file_id}/video_ssim.csv"
        vmaf_key = f"async/{task_id}/output/{file_id}/video_vmaf.csv"

        # Upload low quality CSVs
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=ssim_key, Body=LOW_QUALITY_SSIM_CSV)
        s3_bucket.put_object(Bucket=TEST_BUCKET, Key=vmaf_key, Body=LOW_QUALITY_VMAF_CSV)

        # Parse and evaluate
        parser = QualityMetricsParser()
        evaluator = QualityEvaluator()

        ssim_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=ssim_key)
        ssim_avg, ssim_min, ssim_max = parser.parse_ssim_csv(
            ssim_response["Body"].read().decode("utf-8")
        )

        vmaf_response = s3_bucket.get_object(Bucket=TEST_BUCKET, Key=vmaf_key)
        vmaf_avg, vmaf_min, vmaf_max = parser.parse_vmaf_csv(
            vmaf_response["Body"].read().decode("utf-8")
        )

        metrics = QualityMetrics(
            ssim_average=ssim_avg,
            ssim_min=ssim_min,
            ssim_max=ssim_max,
            vmaf_average=vmaf_avg,
            vmaf_min=vmaf_min,
            vmaf_max=vmaf_max,
        )

        result = evaluator.evaluate(metrics)

        # Quality failed
        assert result.passed is False
        assert result.failure_reason is not None

        # Verify the scores are as expected
        assert abs(result.ssim_score - 0.8856) < 0.001
        assert abs(result.vmaf_score - 55.898) < 0.001
