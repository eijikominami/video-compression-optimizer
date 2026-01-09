"""Quality Metrics Parser and Evaluator.

Parses MediaConvert per-frame metrics CSV files and evaluates quality
against SSIM and VMAF thresholds.

Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 3.4, 9.1, 9.2, 9.3
"""

import logging
import os
from dataclasses import dataclass

import boto3

logger = logging.getLogger(__name__)

# Environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "")
SSIM_THRESHOLD = float(os.environ.get("SSIM_THRESHOLD", "0.95"))
VMAF_THRESHOLD = float(os.environ.get("VMAF_THRESHOLD", "70.0"))


class QualityMetricsError(Exception):
    """Error during quality metrics parsing or evaluation."""

    pass


@dataclass
class QualityMetrics:
    """Parsed quality metrics from MediaConvert CSV files.

    Attributes:
        ssim_average: Average SSIM score (0-1)
        ssim_min: Minimum SSIM score (0-1)
        ssim_max: Maximum SSIM score (0-1)
        vmaf_average: Average VMAF score (0-100)
        vmaf_min: Minimum VMAF score (0-100)
        vmaf_max: Maximum VMAF score (0-100)
    """

    ssim_average: float
    ssim_min: float
    ssim_max: float
    vmaf_average: float
    vmaf_min: float
    vmaf_max: float


@dataclass
class QualityEvaluationResult:
    """Result of quality evaluation.

    Attributes:
        passed: Whether quality thresholds were met
        ssim_score: Average SSIM score
        vmaf_score: Average VMAF score
        ssim_threshold: SSIM threshold used
        vmaf_threshold: VMAF threshold used
        failure_reason: Reason for failure (if any)
    """

    passed: bool
    ssim_score: float
    vmaf_score: float
    ssim_threshold: float
    vmaf_threshold: float
    failure_reason: str | None = None


class QualityMetricsParser:
    """Parser for MediaConvert per-frame metrics CSV files.

    MediaConvert outputs CSV files with the following format:
    - Display_ID,SSIM (or VMAF)
    - Frame numbers with values
    - Average, Min, Max summary rows at the end
    """

    def __init__(self, s3_bucket: str | None = None):
        """Initialize parser.

        Args:
            s3_bucket: S3 bucket name (defaults to S3_BUCKET env var)
        """
        self.s3_bucket = s3_bucket or S3_BUCKET
        self._s3_client = None

    @property
    def s3_client(self):
        """Lazy-load S3 client."""
        if self._s3_client is None:
            self._s3_client = boto3.client("s3")
        return self._s3_client

    def parse_ssim_csv(self, csv_content: str) -> tuple[float, float, float]:
        """Parse SSIM CSV and extract average, min, max values.

        CSV format:
        Display_ID,SSIM
        1,0.987654
        2,0.987123
        ...
        Average,0.987400
        Min,0.985000
        Max,0.989000

        Args:
            csv_content: CSV file content as string

        Returns:
            Tuple of (average, min, max) SSIM values

        Raises:
            QualityMetricsError: If CSV is malformed or missing required values
        """
        return self._parse_metrics_csv(csv_content, "SSIM")

    def parse_vmaf_csv(self, csv_content: str) -> tuple[float, float, float]:
        """Parse VMAF CSV and extract average, min, max values.

        CSV format:
        Display_ID,VMAF
        1,85.123456
        2,84.987654
        ...
        Average,85.000000
        Min,80.000000
        Max,90.000000

        Args:
            csv_content: CSV file content as string

        Returns:
            Tuple of (average, min, max) VMAF values

        Raises:
            QualityMetricsError: If CSV is malformed or missing required values
        """
        return self._parse_metrics_csv(csv_content, "VMAF")

    def _parse_metrics_csv(self, csv_content: str, metric_name: str) -> tuple[float, float, float]:
        """Parse metrics CSV and extract average, min, max values.

        MediaConvert outputs CSV in the following format:
        Display_ID,Value
        0,0.99
        1,0.98
        ...
        Average: 0.97
        Min: 0.97
        Max: 0.99

        Note: Summary rows use "Key: Value" format (colon-space separator),
        not CSV format.

        Args:
            csv_content: CSV file content as string
            metric_name: Name of the metric (SSIM or VMAF)

        Returns:
            Tuple of (average, min, max) values

        Raises:
            QualityMetricsError: If CSV is malformed or missing required values
        """
        if not csv_content or not csv_content.strip():
            raise QualityMetricsError(f"{metric_name} CSV is empty")

        average = None
        min_val = None
        max_val = None

        try:
            for line in csv_content.strip().split("\n"):
                line = line.strip()
                if not line:
                    continue

                # Handle summary rows with "Key: Value" format
                if line.startswith("Average:"):
                    value_str = line.split(":", 1)[1].strip()
                    average = float(value_str)
                elif line.startswith("Min:"):
                    value_str = line.split(":", 1)[1].strip()
                    min_val = float(value_str)
                elif line.startswith("Max:"):
                    value_str = line.split(":", 1)[1].strip()
                    max_val = float(value_str)

        except ValueError as e:
            raise QualityMetricsError(f"Failed to parse {metric_name} CSV: {e}")

        if average is None:
            raise QualityMetricsError(f"{metric_name} CSV missing Average value")
        if min_val is None:
            raise QualityMetricsError(f"{metric_name} CSV missing Min value")
        if max_val is None:
            raise QualityMetricsError(f"{metric_name} CSV missing Max value")

        # Validate ranges
        if metric_name == "SSIM":
            # SSIM should be 0-1, but clamp if slightly out of range
            average = max(0.0, min(1.0, average))
            min_val = max(0.0, min(1.0, min_val))
            max_val = max(0.0, min(1.0, max_val))
            if average > 1.0 or min_val > 1.0 or max_val > 1.0:
                logger.warning("SSIM values out of range, clamped to 0-1")
        elif metric_name == "VMAF":
            # VMAF should be 0-100, but clamp if slightly out of range
            average = max(0.0, min(100.0, average))
            min_val = max(0.0, min(100.0, min_val))
            max_val = max(0.0, min(100.0, max_val))
            if average > 100.0 or min_val > 100.0 or max_val > 100.0:
                logger.warning("VMAF values out of range, clamped to 0-100")

        return average, min_val, max_val

    def parse_metrics_from_s3(
        self,
        task_id: str,
        file_id: str,
        output_s3_key: str,
    ) -> QualityMetrics:
        """Parse quality metrics from S3 CSV files.

        MediaConvert outputs CSV files with naming convention:
        - {output_basename}_SSIM.csv
        - {output_basename}_VMAF.csv

        Args:
            task_id: Task ID
            file_id: File ID
            output_s3_key: S3 key of the output video

        Returns:
            QualityMetrics with parsed values

        Raises:
            QualityMetricsError: If CSV files are missing or malformed
        """
        # Construct CSV file paths
        # Output: output/task_id/file_id/filename_h265.mp4
        # CSV: output/task_id/file_id/filename_h265_SSIM.csv
        output_base = output_s3_key.rsplit(".", 1)[0]  # Remove extension
        ssim_key = f"{output_base}_SSIM.csv"
        vmaf_key = f"{output_base}_VMAF.csv"

        logger.info(f"Parsing quality metrics from S3: {ssim_key}, {vmaf_key}")

        # Download and parse SSIM CSV
        try:
            ssim_response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=ssim_key)
            ssim_content = ssim_response["Body"].read().decode("utf-8")
            ssim_avg, ssim_min, ssim_max = self.parse_ssim_csv(ssim_content)
        except self.s3_client.exceptions.NoSuchKey:
            raise QualityMetricsError(f"SSIM CSV not found: {ssim_key}")
        except Exception as e:
            raise QualityMetricsError(f"Failed to read SSIM CSV: {e}")

        # Download and parse VMAF CSV
        try:
            vmaf_response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=vmaf_key)
            vmaf_content = vmaf_response["Body"].read().decode("utf-8")
            vmaf_avg, vmaf_min, vmaf_max = self.parse_vmaf_csv(vmaf_content)
        except self.s3_client.exceptions.NoSuchKey:
            raise QualityMetricsError(f"VMAF CSV not found: {vmaf_key}")
        except Exception as e:
            raise QualityMetricsError(f"Failed to read VMAF CSV: {e}")

        return QualityMetrics(
            ssim_average=ssim_avg,
            ssim_min=ssim_min,
            ssim_max=ssim_max,
            vmaf_average=vmaf_avg,
            vmaf_min=vmaf_min,
            vmaf_max=vmaf_max,
        )


class QualityEvaluator:
    """Evaluates video quality based on SSIM and VMAF metrics.

    Both SSIM and VMAF must meet their respective thresholds
    for the evaluation to pass.
    """

    def __init__(
        self,
        ssim_threshold: float | None = None,
        vmaf_threshold: float | None = None,
    ):
        """Initialize evaluator with thresholds.

        Args:
            ssim_threshold: SSIM threshold (defaults to SSIM_THRESHOLD env var or 0.95)
            vmaf_threshold: VMAF threshold (defaults to VMAF_THRESHOLD env var or 70.0)
        """
        self.ssim_threshold = ssim_threshold if ssim_threshold is not None else SSIM_THRESHOLD
        self.vmaf_threshold = vmaf_threshold if vmaf_threshold is not None else VMAF_THRESHOLD

    def evaluate(self, metrics: QualityMetrics) -> QualityEvaluationResult:
        """Evaluate quality metrics against thresholds.

        Both SSIM and VMAF must meet their respective thresholds
        for the evaluation to pass.

        Args:
            metrics: Parsed quality metrics

        Returns:
            QualityEvaluationResult with pass/fail status and details
        """
        ssim_ok = metrics.ssim_average >= self.ssim_threshold
        vmaf_ok = metrics.vmaf_average >= self.vmaf_threshold

        if ssim_ok and vmaf_ok:
            return QualityEvaluationResult(
                passed=True,
                ssim_score=metrics.ssim_average,
                vmaf_score=metrics.vmaf_average,
                ssim_threshold=self.ssim_threshold,
                vmaf_threshold=self.vmaf_threshold,
            )

        reasons = []
        if not ssim_ok:
            reasons.append(f"SSIM {metrics.ssim_average:.4f} < {self.ssim_threshold}")
        if not vmaf_ok:
            reasons.append(f"VMAF {metrics.vmaf_average:.2f} < {self.vmaf_threshold}")

        return QualityEvaluationResult(
            passed=False,
            ssim_score=metrics.ssim_average,
            vmaf_score=metrics.vmaf_average,
            ssim_threshold=self.ssim_threshold,
            vmaf_threshold=self.vmaf_threshold,
            failure_reason="; ".join(reasons),
        )


def get_ssim_threshold() -> float:
    """Get SSIM threshold from environment or default."""
    return SSIM_THRESHOLD


def get_vmaf_threshold() -> float:
    """Get VMAF threshold from environment or default."""
    return VMAF_THRESHOLD
