"""Unit tests for QualityMetricsParser and QualityEvaluator.

Tests CSV parsing and quality evaluation logic.

Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.2, 3.3, 3.4
"""

import sys
from pathlib import Path

import pytest

# Add sam-app/async-workflow to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))

from quality_metrics import (
    QualityEvaluationResult,
    QualityEvaluator,
    QualityMetrics,
    QualityMetricsError,
    QualityMetricsParser,
)


class TestQualityMetricsParser:
    """Tests for QualityMetricsParser."""

    def test_parse_ssim_csv_valid(self):
        """Test parsing valid SSIM CSV."""
        csv_content = """Display_ID,SSIM
1,0.987654
2,0.987123
3,0.988000
Average,0.987592
Min,0.987123
Max,0.988000
"""
        parser = QualityMetricsParser()
        avg, min_val, max_val = parser.parse_ssim_csv(csv_content)

        assert avg == pytest.approx(0.987592)
        assert min_val == pytest.approx(0.987123)
        assert max_val == pytest.approx(0.988000)

    def test_parse_vmaf_csv_valid(self):
        """Test parsing valid VMAF CSV."""
        csv_content = """Display_ID,VMAF
1,85.123456
2,84.987654
3,86.000000
Average,85.370370
Min,84.987654
Max,86.000000
"""
        parser = QualityMetricsParser()
        avg, min_val, max_val = parser.parse_vmaf_csv(csv_content)

        assert avg == pytest.approx(85.370370)
        assert min_val == pytest.approx(84.987654)
        assert max_val == pytest.approx(86.000000)

    def test_parse_ssim_csv_empty(self):
        """Test parsing empty SSIM CSV raises error."""
        parser = QualityMetricsParser()

        with pytest.raises(QualityMetricsError, match="SSIM CSV is empty"):
            parser.parse_ssim_csv("")

        with pytest.raises(QualityMetricsError, match="SSIM CSV is empty"):
            parser.parse_ssim_csv("   \n  ")

    def test_parse_vmaf_csv_empty(self):
        """Test parsing empty VMAF CSV raises error."""
        parser = QualityMetricsParser()

        with pytest.raises(QualityMetricsError, match="VMAF CSV is empty"):
            parser.parse_vmaf_csv("")

    def test_parse_ssim_csv_missing_average(self):
        """Test parsing SSIM CSV without Average raises error."""
        csv_content = """Display_ID,SSIM
1,0.987654
Min,0.987123
Max,0.988000
"""
        parser = QualityMetricsParser()

        with pytest.raises(QualityMetricsError, match="missing Average"):
            parser.parse_ssim_csv(csv_content)

    def test_parse_ssim_csv_missing_min(self):
        """Test parsing SSIM CSV without Min raises error."""
        csv_content = """Display_ID,SSIM
1,0.987654
Average,0.987592
Max,0.988000
"""
        parser = QualityMetricsParser()

        with pytest.raises(QualityMetricsError, match="missing Min"):
            parser.parse_ssim_csv(csv_content)

    def test_parse_ssim_csv_missing_max(self):
        """Test parsing SSIM CSV without Max raises error."""
        csv_content = """Display_ID,SSIM
1,0.987654
Average,0.987592
Min,0.987123
"""
        parser = QualityMetricsParser()

        with pytest.raises(QualityMetricsError, match="missing Max"):
            parser.parse_ssim_csv(csv_content)

    def test_parse_ssim_csv_invalid_value(self):
        """Test parsing SSIM CSV with invalid value raises error."""
        csv_content = """Display_ID,SSIM
1,0.987654
Average,not_a_number
Min,0.987123
Max,0.988000
"""
        parser = QualityMetricsParser()

        with pytest.raises(QualityMetricsError, match="Failed to parse"):
            parser.parse_ssim_csv(csv_content)

    def test_parse_ssim_csv_clamps_values(self):
        """Test SSIM values are clamped to 0-1 range."""
        csv_content = """Display_ID,SSIM
1,0.987654
Average,1.001
Min,-0.001
Max,1.5
"""
        parser = QualityMetricsParser()
        avg, min_val, max_val = parser.parse_ssim_csv(csv_content)

        # Values should be clamped
        assert avg == 1.0
        assert min_val == 0.0
        assert max_val == 1.0

    def test_parse_vmaf_csv_clamps_values(self):
        """Test VMAF values are clamped to 0-100 range."""
        csv_content = """Display_ID,VMAF
1,85.0
Average,101.0
Min,-5.0
Max,150.0
"""
        parser = QualityMetricsParser()
        avg, min_val, max_val = parser.parse_vmaf_csv(csv_content)

        # Values should be clamped
        assert avg == 100.0
        assert min_val == 0.0
        assert max_val == 100.0

    def test_parse_csv_with_extra_whitespace(self):
        """Test parsing CSV with extra whitespace."""
        # Build CSV with intentional whitespace using explicit strings
        lines = [
            "Display_ID,SSIM",
            "  1  ,  0.987654  ",  # noqa: W291
            "  Average  ,  0.987592  ",  # noqa: W291
            "  Min  ,  0.987123  ",  # noqa: W291
            "  Max  ,  0.988000  ",  # noqa: W291
        ]
        csv_content = "\n".join(lines) + "\n"
        parser = QualityMetricsParser()
        avg, min_val, max_val = parser.parse_ssim_csv(csv_content)

        assert avg == pytest.approx(0.987592)
        assert min_val == pytest.approx(0.987123)
        assert max_val == pytest.approx(0.988000)

    def test_parse_csv_with_many_frames(self):
        """Test parsing CSV with many frame entries."""
        # Build CSV with 1000 frames
        lines = ["Display_ID,SSIM"]
        for i in range(1, 1001):
            lines.append(f"{i},0.98{i % 10}")
        lines.extend(["Average,0.985000", "Min,0.980000", "Max,0.989000"])
        csv_content = "\n".join(lines)

        parser = QualityMetricsParser()
        avg, min_val, max_val = parser.parse_ssim_csv(csv_content)

        assert avg == pytest.approx(0.985000)
        assert min_val == pytest.approx(0.980000)
        assert max_val == pytest.approx(0.989000)


class TestQualityEvaluator:
    """Tests for QualityEvaluator."""

    def test_evaluate_both_pass(self):
        """Test evaluation when both SSIM and VMAF pass."""
        metrics = QualityMetrics(
            ssim_average=0.98,
            ssim_min=0.95,
            ssim_max=0.99,
            vmaf_average=85.0,
            vmaf_min=75.0,
            vmaf_max=90.0,
        )
        evaluator = QualityEvaluator(ssim_threshold=0.95, vmaf_threshold=70.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is True
        assert result.ssim_score == 0.98
        assert result.vmaf_score == 85.0
        assert result.failure_reason is None

    def test_evaluate_ssim_fails(self):
        """Test evaluation when SSIM fails threshold."""
        metrics = QualityMetrics(
            ssim_average=0.90,
            ssim_min=0.85,
            ssim_max=0.95,
            vmaf_average=85.0,
            vmaf_min=75.0,
            vmaf_max=90.0,
        )
        evaluator = QualityEvaluator(ssim_threshold=0.95, vmaf_threshold=70.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is False
        assert result.ssim_score == 0.90
        assert result.vmaf_score == 85.0
        assert "SSIM 0.9000 < 0.95" in result.failure_reason

    def test_evaluate_vmaf_fails(self):
        """Test evaluation when VMAF fails threshold."""
        metrics = QualityMetrics(
            ssim_average=0.98,
            ssim_min=0.95,
            ssim_max=0.99,
            vmaf_average=60.0,
            vmaf_min=50.0,
            vmaf_max=70.0,
        )
        evaluator = QualityEvaluator(ssim_threshold=0.95, vmaf_threshold=70.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is False
        assert result.ssim_score == 0.98
        assert result.vmaf_score == 60.0
        assert "VMAF 60.00 < 70.0" in result.failure_reason

    def test_evaluate_both_fail(self):
        """Test evaluation when both SSIM and VMAF fail."""
        metrics = QualityMetrics(
            ssim_average=0.90,
            ssim_min=0.85,
            ssim_max=0.95,
            vmaf_average=60.0,
            vmaf_min=50.0,
            vmaf_max=70.0,
        )
        evaluator = QualityEvaluator(ssim_threshold=0.95, vmaf_threshold=70.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is False
        assert "SSIM" in result.failure_reason
        assert "VMAF" in result.failure_reason

    def test_evaluate_at_threshold(self):
        """Test evaluation when values are exactly at threshold."""
        metrics = QualityMetrics(
            ssim_average=0.95,
            ssim_min=0.95,
            ssim_max=0.95,
            vmaf_average=70.0,
            vmaf_min=70.0,
            vmaf_max=70.0,
        )
        evaluator = QualityEvaluator(ssim_threshold=0.95, vmaf_threshold=70.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is True
        assert result.failure_reason is None

    def test_evaluate_just_below_threshold(self):
        """Test evaluation when values are just below threshold."""
        metrics = QualityMetrics(
            ssim_average=0.9499,
            ssim_min=0.94,
            ssim_max=0.96,
            vmaf_average=69.99,
            vmaf_min=65.0,
            vmaf_max=75.0,
        )
        evaluator = QualityEvaluator(ssim_threshold=0.95, vmaf_threshold=70.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is False

    def test_evaluate_custom_thresholds(self):
        """Test evaluation with custom thresholds."""
        metrics = QualityMetrics(
            ssim_average=0.90,
            ssim_min=0.85,
            ssim_max=0.95,
            vmaf_average=60.0,
            vmaf_min=50.0,
            vmaf_max=70.0,
        )
        # Lower thresholds
        evaluator = QualityEvaluator(ssim_threshold=0.85, vmaf_threshold=55.0)
        result = evaluator.evaluate(metrics)

        assert result.passed is True
        assert result.ssim_threshold == 0.85
        assert result.vmaf_threshold == 55.0

    def test_evaluate_default_thresholds(self):
        """Test evaluation uses default thresholds."""
        metrics = QualityMetrics(
            ssim_average=0.96,
            ssim_min=0.95,
            ssim_max=0.97,
            vmaf_average=75.0,
            vmaf_min=70.0,
            vmaf_max=80.0,
        )
        evaluator = QualityEvaluator()
        result = evaluator.evaluate(metrics)

        # Default thresholds: SSIM >= 0.95, VMAF >= 70
        assert result.passed is True
        assert result.ssim_threshold == 0.95
        assert result.vmaf_threshold == 70.0


class TestQualityMetrics:
    """Tests for QualityMetrics dataclass."""

    def test_quality_metrics_creation(self):
        """Test QualityMetrics dataclass creation."""
        metrics = QualityMetrics(
            ssim_average=0.98,
            ssim_min=0.95,
            ssim_max=0.99,
            vmaf_average=85.0,
            vmaf_min=75.0,
            vmaf_max=90.0,
        )

        assert metrics.ssim_average == 0.98
        assert metrics.ssim_min == 0.95
        assert metrics.ssim_max == 0.99
        assert metrics.vmaf_average == 85.0
        assert metrics.vmaf_min == 75.0
        assert metrics.vmaf_max == 90.0


class TestQualityEvaluationResult:
    """Tests for QualityEvaluationResult dataclass."""

    def test_result_passed(self):
        """Test QualityEvaluationResult for passed evaluation."""
        result = QualityEvaluationResult(
            passed=True,
            ssim_score=0.98,
            vmaf_score=85.0,
            ssim_threshold=0.95,
            vmaf_threshold=70.0,
        )

        assert result.passed is True
        assert result.failure_reason is None

    def test_result_failed(self):
        """Test QualityEvaluationResult for failed evaluation."""
        result = QualityEvaluationResult(
            passed=False,
            ssim_score=0.90,
            vmaf_score=60.0,
            ssim_threshold=0.95,
            vmaf_threshold=70.0,
            failure_reason="SSIM 0.9000 < 0.95; VMAF 60.00 < 70.0",
        )

        assert result.passed is False
        assert "SSIM" in result.failure_reason
        assert "VMAF" in result.failure_reason
