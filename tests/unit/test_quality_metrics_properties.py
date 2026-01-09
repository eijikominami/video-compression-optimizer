"""Property-based tests for QualityMetricsParser and QualityEvaluator.

Tests correctness properties using Hypothesis.

Feature: mediaconvert-quality-metrics
Requirements: 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 3.4, 9.1, 9.2, 9.3
"""

import sys
from pathlib import Path

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

# Add sam-app/async-workflow to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))

from quality_metrics import (
    QualityEvaluator,
    QualityMetrics,
    QualityMetricsParser,
)

# =============================================================================
# Strategies for generating test data
# =============================================================================

# SSIM values are in range 0-1
ssim_value = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)

# VMAF values are in range 0-100
vmaf_value = st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)

# Frame count for CSV generation
frame_count = st.integers(min_value=1, max_value=100)


def generate_ssim_csv(avg: float, min_val: float, max_val: float, num_frames: int) -> str:
    """Generate a valid SSIM CSV string with given summary values.

    MediaConvert CSV format:
    Display_ID,Value
    0,<value>
    ...
    Average: <avg>
    Min: <min>
    Max: <max>
    """
    lines = ["Display_ID,Value"]
    for i in range(num_frames):
        # Generate frame values between min and max
        frame_val = min_val + (max_val - min_val) * (i / max(1, num_frames - 1))
        lines.append(f"{i},{frame_val:.6f}")
    lines.append(f"Average: {avg:.6f}")
    lines.append(f"Min: {min_val:.6f}")
    lines.append(f"Max: {max_val:.6f}")
    return "\n".join(lines)


def generate_vmaf_csv(avg: float, min_val: float, max_val: float, num_frames: int) -> str:
    """Generate a valid VMAF CSV string with given summary values.

    MediaConvert CSV format:
    Display_ID,Value
    0,<value>
    ...
    Average: <avg>
    Min: <min>
    Max: <max>
    """
    lines = ["Display_ID,Value"]
    for i in range(num_frames):
        # Generate frame values between min and max
        frame_val = min_val + (max_val - min_val) * (i / max(1, num_frames - 1))
        lines.append(f"{i},{frame_val:.6f}")
    lines.append(f"Average: {avg:.6f}")
    lines.append(f"Min: {min_val:.6f}")
    lines.append(f"Max: {max_val:.6f}")
    return "\n".join(lines)


# =============================================================================
# Property 2: CSV Parsing Extracts Correct Values
# Feature: mediaconvert-quality-metrics, Property 2: CSV Parsing Extracts Correct Values
# Validates: Requirements 2.1, 2.2, 2.3
# =============================================================================


class TestCSVParsingProperty:
    """Property 2: CSV Parsing Extracts Correct Values.

    *For any* valid MediaConvert quality metrics CSV file, parsing SHALL extract
    the correct average, min, and max values for both SSIM (0-1 range) and
    VMAF (0-100 range).
    """

    @given(
        avg=ssim_value,
        min_val=ssim_value,
        max_val=ssim_value,
        num_frames=frame_count,
    )
    @settings(max_examples=100)
    def test_ssim_csv_parsing_extracts_correct_values(
        self, avg: float, min_val: float, max_val: float, num_frames: int
    ):
        """Property 2: For any valid SSIM CSV, parsing extracts correct values.

        Feature: mediaconvert-quality-metrics, Property 2: CSV Parsing Extracts Correct Values
        Validates: Requirements 2.1, 2.2, 2.3
        """
        # Ensure min <= avg <= max for realistic data
        sorted_vals = sorted([avg, min_val, max_val])
        min_val, avg, max_val = sorted_vals[0], sorted_vals[1], sorted_vals[2]

        csv_content = generate_ssim_csv(avg, min_val, max_val, num_frames)
        parser = QualityMetricsParser()

        parsed_avg, parsed_min, parsed_max = parser.parse_ssim_csv(csv_content)

        # Verify extracted values match input (within floating point tolerance)
        assert abs(parsed_avg - avg) < 1e-5, f"Average mismatch: {parsed_avg} != {avg}"
        assert abs(parsed_min - min_val) < 1e-5, f"Min mismatch: {parsed_min} != {min_val}"
        assert abs(parsed_max - max_val) < 1e-5, f"Max mismatch: {parsed_max} != {max_val}"

        # Verify values are in valid SSIM range (0-1)
        assert 0.0 <= parsed_avg <= 1.0, f"Average out of range: {parsed_avg}"
        assert 0.0 <= parsed_min <= 1.0, f"Min out of range: {parsed_min}"
        assert 0.0 <= parsed_max <= 1.0, f"Max out of range: {parsed_max}"

    @given(
        avg=vmaf_value,
        min_val=vmaf_value,
        max_val=vmaf_value,
        num_frames=frame_count,
    )
    @settings(max_examples=100)
    def test_vmaf_csv_parsing_extracts_correct_values(
        self, avg: float, min_val: float, max_val: float, num_frames: int
    ):
        """Property 2: For any valid VMAF CSV, parsing extracts correct values.

        Feature: mediaconvert-quality-metrics, Property 2: CSV Parsing Extracts Correct Values
        Validates: Requirements 2.1, 2.2, 2.3
        """
        # Ensure min <= avg <= max for realistic data
        sorted_vals = sorted([avg, min_val, max_val])
        min_val, avg, max_val = sorted_vals[0], sorted_vals[1], sorted_vals[2]

        csv_content = generate_vmaf_csv(avg, min_val, max_val, num_frames)
        parser = QualityMetricsParser()

        parsed_avg, parsed_min, parsed_max = parser.parse_vmaf_csv(csv_content)

        # Verify extracted values match input (within floating point tolerance)
        assert abs(parsed_avg - avg) < 1e-5, f"Average mismatch: {parsed_avg} != {avg}"
        assert abs(parsed_min - min_val) < 1e-5, f"Min mismatch: {parsed_min} != {min_val}"
        assert abs(parsed_max - max_val) < 1e-5, f"Max mismatch: {parsed_max} != {max_val}"

        # Verify values are in valid VMAF range (0-100)
        assert 0.0 <= parsed_avg <= 100.0, f"Average out of range: {parsed_avg}"
        assert 0.0 <= parsed_min <= 100.0, f"Min out of range: {parsed_min}"
        assert 0.0 <= parsed_max <= 100.0, f"Max out of range: {parsed_max}"

    @given(
        ssim_avg=ssim_value,
        ssim_min=ssim_value,
        ssim_max=ssim_value,
        vmaf_avg=vmaf_value,
        vmaf_min=vmaf_value,
        vmaf_max=vmaf_value,
        num_frames=frame_count,
    )
    @settings(max_examples=100)
    def test_both_metrics_parsed_consistently(
        self,
        ssim_avg: float,
        ssim_min: float,
        ssim_max: float,
        vmaf_avg: float,
        vmaf_min: float,
        vmaf_max: float,
        num_frames: int,
    ):
        """Property 2: Both SSIM and VMAF CSVs are parsed consistently.

        Feature: mediaconvert-quality-metrics, Property 2: CSV Parsing Extracts Correct Values
        Validates: Requirements 2.1, 2.2, 2.3
        """
        # Sort values for realistic data
        ssim_sorted = sorted([ssim_avg, ssim_min, ssim_max])
        vmaf_sorted = sorted([vmaf_avg, vmaf_min, vmaf_max])

        ssim_csv = generate_ssim_csv(ssim_sorted[1], ssim_sorted[0], ssim_sorted[2], num_frames)
        vmaf_csv = generate_vmaf_csv(vmaf_sorted[1], vmaf_sorted[0], vmaf_sorted[2], num_frames)

        parser = QualityMetricsParser()

        ssim_result = parser.parse_ssim_csv(ssim_csv)
        vmaf_result = parser.parse_vmaf_csv(vmaf_csv)

        # Both should return 3-tuples
        assert len(ssim_result) == 3
        assert len(vmaf_result) == 3

        # All values should be numeric
        for val in ssim_result + vmaf_result:
            assert isinstance(val, float)


# =============================================================================
# Property 3: Quality Evaluation Applies Thresholds Correctly
# Feature: mediaconvert-quality-metrics, Property 3: Quality Evaluation Applies Thresholds Correctly
# Validates: Requirements 3.1, 3.2, 3.3, 3.4
# =============================================================================


class TestQualityEvaluationProperty:
    """Property 3: Quality Evaluation Applies Thresholds Correctly.

    *For any* quality metrics with SSIM and VMAF scores:
    - If SSIM >= threshold AND VMAF >= threshold, evaluation SHALL pass
    - If SSIM < threshold OR VMAF < threshold, evaluation SHALL fail with specific reason
    """

    @given(
        ssim_score=ssim_value,
        vmaf_score=vmaf_value,
        ssim_threshold=ssim_value,
        vmaf_threshold=vmaf_value,
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.filter_too_much])
    def test_evaluation_passes_when_both_meet_thresholds(
        self,
        ssim_score: float,
        vmaf_score: float,
        ssim_threshold: float,
        vmaf_threshold: float,
    ):
        """Property 3: Evaluation passes when both metrics meet thresholds.

        Feature: mediaconvert-quality-metrics, Property 3: Quality Evaluation Applies Thresholds Correctly
        Validates: Requirements 3.1, 3.2, 3.3, 3.4
        """
        # Only test cases where both meet thresholds
        assume(ssim_score >= ssim_threshold)
        assume(vmaf_score >= vmaf_threshold)

        metrics = QualityMetrics(
            ssim_average=ssim_score,
            ssim_min=ssim_score * 0.95,
            ssim_max=min(1.0, ssim_score * 1.05),
            vmaf_average=vmaf_score,
            vmaf_min=vmaf_score * 0.95,
            vmaf_max=min(100.0, vmaf_score * 1.05),
        )

        evaluator = QualityEvaluator(
            ssim_threshold=ssim_threshold,
            vmaf_threshold=vmaf_threshold,
        )
        result = evaluator.evaluate(metrics)

        assert result.passed is True, (
            f"Expected pass but got fail: "
            f"SSIM {ssim_score} >= {ssim_threshold}, "
            f"VMAF {vmaf_score} >= {vmaf_threshold}"
        )
        assert result.failure_reason is None

    @given(
        ssim_score=ssim_value,
        vmaf_score=vmaf_value,
        ssim_threshold=ssim_value,
        vmaf_threshold=vmaf_value,
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.filter_too_much])
    def test_evaluation_fails_when_ssim_below_threshold(
        self,
        ssim_score: float,
        vmaf_score: float,
        ssim_threshold: float,
        vmaf_threshold: float,
    ):
        """Property 3: Evaluation fails when SSIM is below threshold.

        Feature: mediaconvert-quality-metrics, Property 3: Quality Evaluation Applies Thresholds Correctly
        Validates: Requirements 3.1, 3.2, 3.3, 3.4
        """
        # Only test cases where SSIM fails but VMAF passes
        assume(ssim_score < ssim_threshold)
        assume(vmaf_score >= vmaf_threshold)

        metrics = QualityMetrics(
            ssim_average=ssim_score,
            ssim_min=ssim_score * 0.95,
            ssim_max=min(1.0, ssim_score * 1.05),
            vmaf_average=vmaf_score,
            vmaf_min=vmaf_score * 0.95,
            vmaf_max=min(100.0, vmaf_score * 1.05),
        )

        evaluator = QualityEvaluator(
            ssim_threshold=ssim_threshold,
            vmaf_threshold=vmaf_threshold,
        )
        result = evaluator.evaluate(metrics)

        assert result.passed is False, (
            f"Expected fail but got pass: SSIM {ssim_score} < {ssim_threshold}"
        )
        assert result.failure_reason is not None
        assert "SSIM" in result.failure_reason

    @given(
        ssim_score=ssim_value,
        vmaf_score=vmaf_value,
        ssim_threshold=ssim_value,
        vmaf_threshold=vmaf_value,
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.filter_too_much])
    def test_evaluation_fails_when_vmaf_below_threshold(
        self,
        ssim_score: float,
        vmaf_score: float,
        ssim_threshold: float,
        vmaf_threshold: float,
    ):
        """Property 3: Evaluation fails when VMAF is below threshold.

        Feature: mediaconvert-quality-metrics, Property 3: Quality Evaluation Applies Thresholds Correctly
        Validates: Requirements 3.1, 3.2, 3.3, 3.4
        """
        # Only test cases where VMAF fails but SSIM passes
        assume(ssim_score >= ssim_threshold)
        assume(vmaf_score < vmaf_threshold)

        metrics = QualityMetrics(
            ssim_average=ssim_score,
            ssim_min=ssim_score * 0.95,
            ssim_max=min(1.0, ssim_score * 1.05),
            vmaf_average=vmaf_score,
            vmaf_min=vmaf_score * 0.95,
            vmaf_max=min(100.0, vmaf_score * 1.05),
        )

        evaluator = QualityEvaluator(
            ssim_threshold=ssim_threshold,
            vmaf_threshold=vmaf_threshold,
        )
        result = evaluator.evaluate(metrics)

        assert result.passed is False, (
            f"Expected fail but got pass: VMAF {vmaf_score} < {vmaf_threshold}"
        )
        assert result.failure_reason is not None
        assert "VMAF" in result.failure_reason

    @given(
        ssim_score=ssim_value,
        vmaf_score=vmaf_value,
        ssim_threshold=ssim_value,
        vmaf_threshold=vmaf_value,
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.filter_too_much])
    def test_evaluation_fails_when_both_below_threshold(
        self,
        ssim_score: float,
        vmaf_score: float,
        ssim_threshold: float,
        vmaf_threshold: float,
    ):
        """Property 3: Evaluation fails when both metrics are below threshold.

        Feature: mediaconvert-quality-metrics, Property 3: Quality Evaluation Applies Thresholds Correctly
        Validates: Requirements 3.1, 3.2, 3.3, 3.4
        """
        # Only test cases where both fail
        assume(ssim_score < ssim_threshold)
        assume(vmaf_score < vmaf_threshold)

        metrics = QualityMetrics(
            ssim_average=ssim_score,
            ssim_min=ssim_score * 0.95,
            ssim_max=min(1.0, ssim_score * 1.05),
            vmaf_average=vmaf_score,
            vmaf_min=vmaf_score * 0.95,
            vmaf_max=min(100.0, vmaf_score * 1.05),
        )

        evaluator = QualityEvaluator(
            ssim_threshold=ssim_threshold,
            vmaf_threshold=vmaf_threshold,
        )
        result = evaluator.evaluate(metrics)

        assert result.passed is False
        assert result.failure_reason is not None
        assert "SSIM" in result.failure_reason
        assert "VMAF" in result.failure_reason

    @given(
        ssim_score=ssim_value,
        vmaf_score=vmaf_value,
        ssim_threshold=ssim_value,
        vmaf_threshold=vmaf_value,
    )
    @settings(max_examples=100)
    def test_evaluation_result_contains_correct_scores(
        self,
        ssim_score: float,
        vmaf_score: float,
        ssim_threshold: float,
        vmaf_threshold: float,
    ):
        """Property 3: Evaluation result contains correct scores and thresholds.

        Feature: mediaconvert-quality-metrics, Property 3: Quality Evaluation Applies Thresholds Correctly
        Validates: Requirements 3.1, 3.2, 3.3, 3.4
        """
        metrics = QualityMetrics(
            ssim_average=ssim_score,
            ssim_min=ssim_score * 0.95,
            ssim_max=min(1.0, ssim_score * 1.05),
            vmaf_average=vmaf_score,
            vmaf_min=vmaf_score * 0.95,
            vmaf_max=min(100.0, vmaf_score * 1.05),
        )

        evaluator = QualityEvaluator(
            ssim_threshold=ssim_threshold,
            vmaf_threshold=vmaf_threshold,
        )
        result = evaluator.evaluate(metrics)

        # Result should contain the input scores
        assert result.ssim_score == ssim_score
        assert result.vmaf_score == vmaf_score

        # Result should contain the thresholds used
        assert result.ssim_threshold == ssim_threshold
        assert result.vmaf_threshold == vmaf_threshold


# =============================================================================
# Property 1: MediaConvert Job Settings Include Per-Frame Metrics
# Feature: mediaconvert-quality-metrics, Property 1: MediaConvert Job Settings Include Per-Frame Metrics
# Validates: Requirements 1.1, 1.3
# =============================================================================


class TestMediaConvertJobSettingsProperty:
    """Property 1: MediaConvert Job Settings Include Per-Frame Metrics.

    *For any* MediaConvert job created by the system, the job settings SHALL
    include per-frame metrics configuration with SSIM and VMAF enabled at
    the output group level.
    """

    # Strategy for valid preset names
    preset_names = st.sampled_from(["balanced", "high", "compression", "balanced+", "high+"])

    # Strategy for S3 keys - simpler approach with fixed prefix
    s3_key_suffix = st.text(
        alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="-_"),
        min_size=1,
        max_size=50,
    )

    @given(
        suffix=s3_key_suffix,
        preset=preset_names,
    )
    @settings(max_examples=50)
    def test_job_settings_include_per_frame_metrics(self, suffix: str, preset: str):
        """Property 1: Job settings include per-frame metrics for any valid preset.

        Feature: mediaconvert-quality-metrics, Property 1: MediaConvert Job Settings Include Per-Frame Metrics
        Validates: Requirements 1.1, 1.3
        """
        # Import the function from app.py
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))
        from app import create_job_settings

        source_key = f"input/{suffix}.mp4"
        output_key = f"output/{suffix}.mp4"

        settings = create_job_settings(source_key, output_key, preset)

        # Verify OutputGroups exists
        assert "OutputGroups" in settings
        assert len(settings["OutputGroups"]) > 0

        # Verify per-frame metrics configuration
        output_group = settings["OutputGroups"][0]
        output_group_settings = output_group["OutputGroupSettings"]

        # PerFrameMetrics must be present
        assert "PerFrameMetrics" in output_group_settings, (
            "PerFrameMetrics not found in OutputGroupSettings"
        )

        per_frame_metrics = output_group_settings["PerFrameMetrics"]

        # Must include both SSIM and VMAF
        assert "SSIM" in per_frame_metrics, "SSIM not enabled in PerFrameMetrics"
        assert "VMAF" in per_frame_metrics, "VMAF not enabled in PerFrameMetrics"

    @given(preset=preset_names)
    @settings(max_examples=20)
    def test_job_settings_have_valid_structure(self, preset: str):
        """Property 1: Job settings have valid MediaConvert structure.

        Feature: mediaconvert-quality-metrics, Property 1: MediaConvert Job Settings Include Per-Frame Metrics
        Validates: Requirements 1.1, 1.3
        """
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))
        from app import create_job_settings

        settings = create_job_settings("input/test.mp4", "output/test.mp4", preset)

        # Required top-level keys
        assert "Inputs" in settings
        assert "OutputGroups" in settings

        # Inputs structure
        assert len(settings["Inputs"]) > 0
        assert "FileInput" in settings["Inputs"][0]

        # OutputGroups structure
        output_group = settings["OutputGroups"][0]
        assert "OutputGroupSettings" in output_group
        assert "Outputs" in output_group

        # Output settings
        assert len(output_group["Outputs"]) > 0
        output = output_group["Outputs"][0]
        assert "VideoDescription" in output
        assert "CodecSettings" in output["VideoDescription"]


# =============================================================================
# Property 8: Adaptive Preset Retry Logic
# Feature: mediaconvert-quality-metrics, Property 8: Adaptive Preset Retry Logic
# Validates: Requirements 3.5
# =============================================================================


class TestAdaptivePresetRetryProperty:
    """Property 8: Adaptive Preset Retry Logic.

    *For any* adaptive preset (ending with +) that fails quality evaluation:
    - If next preset in chain available: retry with higher quality preset
    - If all presets exhausted: use best-effort mode (accept with warning)

    *For any* non-adaptive preset that fails quality evaluation:
    - Fail immediately without retry

    Note: Tests use moto to mock DynamoDB since handle_quality_failure
    calls update_file_status internally.
    """

    # Strategies
    adaptive_presets = st.sampled_from(["balanced+", "high+"])
    non_adaptive_presets = st.sampled_from(["balanced", "high", "compression"])

    quality_result = st.fixed_dictionaries(
        {
            "passed": st.just(False),
            "ssim_score": ssim_value,
            "vmaf_score": vmaf_value,
            "failure_reason": st.text(min_size=1, max_size=100),
        }
    )

    @pytest.fixture(autouse=True)
    def setup_moto(self, monkeypatch):
        """Setup moto mock for DynamoDB."""
        from unittest.mock import MagicMock

        # Set environment variables for Lambda
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

        # Mock update_file_status to avoid DynamoDB calls
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))
        import app

        monkeypatch.setattr(app, "update_file_status", MagicMock())

    @given(
        preset=non_adaptive_presets,
        quality_result=quality_result,
    )
    @settings(max_examples=50)
    def test_non_adaptive_preset_fails_immediately(self, preset: str, quality_result: dict):
        """Property 8: Non-adaptive presets fail immediately without retry.

        Feature: mediaconvert-quality-metrics, Property 8: Adaptive Preset Retry Logic
        Validates: Requirements 3.5
        """
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))
        from app import handle_quality_failure

        event = {
            "task_id": "test-task",
            "file": {
                "file_id": "test-file",
                "preset_attempts": [],  # No previous attempts
            },
            "quality_preset": preset,
            "quality_result": quality_result,
        }

        result = handle_quality_failure(event)

        # Non-adaptive should not retry
        assert result["should_retry"] is False
        assert result["reason"] == "non_adaptive_preset"

    @given(quality_result=quality_result)
    @settings(max_examples=50)
    def test_adaptive_balanced_plus_retries_with_high(self, quality_result: dict):
        """Property 8: balanced+ retries with high preset.

        Feature: mediaconvert-quality-metrics, Property 8: Adaptive Preset Retry Logic
        Validates: Requirements 3.5
        """
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))
        from app import handle_quality_failure

        event = {
            "task_id": "test-task",
            "file": {
                "file_id": "test-file",
                "preset_attempts": [],
            },
            "quality_preset": "balanced+",
            "quality_result": quality_result,
        }

        result = handle_quality_failure(event)

        # balanced+ should retry with high
        assert result["should_retry"] is True
        assert result["next_preset"] == "high"
        assert "updated_file" in result
        assert "balanced+" in result["updated_file"]["preset_attempts"]

    @given(quality_result=quality_result)
    @settings(max_examples=50)
    def test_adaptive_high_plus_uses_best_effort(self, quality_result: dict):
        """Property 8: high+ uses best-effort mode (no more presets).

        Feature: mediaconvert-quality-metrics, Property 8: Adaptive Preset Retry Logic
        Validates: Requirements 3.5
        """
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))
        from app import handle_quality_failure

        event = {
            "task_id": "test-task",
            "file": {
                "file_id": "test-file",
                "preset_attempts": [],
            },
            "quality_preset": "high+",
            "quality_result": quality_result,
        }

        result = handle_quality_failure(event)

        # high+ has no next preset, should use best-effort
        assert result["should_retry"] is False
        assert result["reason"] == "best_effort"
        assert result["accept_anyway"] is True

    @given(quality_result=quality_result)
    @settings(max_examples=50)
    def test_retry_from_adaptive_uses_best_effort(self, quality_result: dict):
        """Property 8: Retry from adaptive preset uses best-effort on failure.

        Feature: mediaconvert-quality-metrics, Property 8: Adaptive Preset Retry Logic
        Validates: Requirements 3.5
        """
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "sam-app" / "async-workflow"))
        from app import handle_quality_failure

        # Simulating: balanced+ failed, retried with high, high also failed
        event = {
            "task_id": "test-task",
            "file": {
                "file_id": "test-file",
                "preset_attempts": ["balanced+"],  # Previous attempt
            },
            "quality_preset": "high",  # Non-adaptive, but has previous attempts
            "quality_result": quality_result,
        }

        result = handle_quality_failure(event)

        # Should use best-effort since it's a retry from adaptive
        assert result["should_retry"] is False
        assert result["reason"] == "best_effort"
        assert result["accept_anyway"] is True


# =============================================================================
# Property 5: Progress Calculation Uses MediaConvert Directly
# Feature: mediaconvert-quality-metrics, Property 5: Progress Calculation Uses MediaConvert Directly
# Validates: Requirements 5.1, 5.2, 5.3
# =============================================================================


class TestProgressCalculationProperty:
    """Property 5: Progress Calculation Uses MediaConvert Directly.

    *For any* file in CONVERTING status, progress SHALL be the MediaConvert
    jobPercentComplete value scaled to 0-99%. For COMPLETED/FAILED files,
    progress SHALL be 100%. For PENDING files, progress SHALL be 0%.
    """

    # Strategies
    file_status = st.sampled_from(["PENDING", "CONVERTING", "COMPLETED", "FAILED"])
    mediaconvert_progress = st.integers(min_value=0, max_value=100)
    file_count = st.integers(min_value=1, max_value=10)

    @given(mc_progress=mediaconvert_progress)
    @settings(max_examples=100)
    def test_converting_uses_mediaconvert_progress(self, mc_progress: int):
        """Property 5: CONVERTING status uses MediaConvert progress.

        Feature: mediaconvert-quality-metrics, Property 5: Progress Calculation Uses MediaConvert Directly
        Validates: Requirements 5.1, 5.2, 5.3
        """
        from vco.utils.progress import calculate_progress

        files = [{"status": "CONVERTING", "mediaconvert_job_id": "job-123"}]

        def get_mc_progress(job_id: str) -> int:
            return mc_progress

        progress, step = calculate_progress(files, get_mediaconvert_progress=get_mc_progress)

        # Progress should be capped at 99% for CONVERTING
        expected = min(mc_progress, 99)
        assert progress == expected
        assert step == "converting"

    @given(status=st.sampled_from(["COMPLETED", "FAILED"]))
    @settings(max_examples=20)
    def test_terminal_status_is_100_percent(self, status: str):
        """Property 5: COMPLETED/FAILED files are 100%.

        Feature: mediaconvert-quality-metrics, Property 5: Progress Calculation Uses MediaConvert Directly
        Validates: Requirements 5.1, 5.2, 5.3
        """
        from vco.utils.progress import calculate_progress

        files = [{"status": status}]
        progress, step = calculate_progress(files)

        assert progress == 100
        assert step == "completed"

    @settings(max_examples=20)
    @given(st.data())
    def test_pending_is_0_percent(self, data):
        """Property 5: PENDING files are 0%.

        Feature: mediaconvert-quality-metrics, Property 5: Progress Calculation Uses MediaConvert Directly
        Validates: Requirements 5.1, 5.2, 5.3
        """
        from vco.utils.progress import calculate_progress

        files = [{"status": "PENDING"}]
        progress, step = calculate_progress(files)

        assert progress == 0
        assert step == "pending"

    @given(
        completed_count=st.integers(min_value=0, max_value=5),
        converting_count=st.integers(min_value=0, max_value=5),
        pending_count=st.integers(min_value=0, max_value=5),
    )
    @settings(max_examples=100)
    def test_average_progress_calculation(
        self, completed_count: int, converting_count: int, pending_count: int
    ):
        """Property 5: Task progress is average of file progress.

        Feature: mediaconvert-quality-metrics, Property 5: Progress Calculation Uses MediaConvert Directly
        Validates: Requirements 5.1, 5.2, 5.3
        """
        from vco.utils.progress import calculate_progress

        total = completed_count + converting_count + pending_count
        assume(total > 0)

        files = []
        files.extend([{"status": "COMPLETED"}] * completed_count)
        files.extend(
            [
                {"status": "CONVERTING", "mediaconvert_job_id": f"job-{i}"}
                for i in range(converting_count)
            ]
        )
        files.extend([{"status": "PENDING"}] * pending_count)

        # Use midpoint (50%) for CONVERTING when no callback
        progress, _ = calculate_progress(files)

        # Calculate expected: COMPLETED=100, CONVERTING=50, PENDING=0
        expected_total = completed_count * 100 + converting_count * 50 + pending_count * 0
        expected = expected_total // total if total > 0 else 0

        assert progress == expected


# =============================================================================
# Property 4: Status Transitions Follow Simplified Workflow
# Feature: mediaconvert-quality-metrics, Property 4: Status Transitions Follow Simplified Workflow
# Validates: Requirements 4.1, 4.2, 4.3, 4.4
# =============================================================================


class TestStatusTransitionsProperty:
    """Property 4: Status Transitions Follow Simplified Workflow.

    *For any* file processing:
    - Valid statuses are: PENDING, CONVERTING, COMPLETED, DOWNLOADED, FAILED
    - VERIFYING status SHALL NOT be used
    - On MediaConvert completion with quality pass: CONVERTING → COMPLETED
    - On MediaConvert completion with quality fail: CONVERTING → FAILED (or retry)
    """

    @settings(max_examples=20)
    @given(st.data())
    def test_verifying_status_not_in_enum(self, data):
        """Property 4: VERIFYING status is not in FileStatus enum.

        Feature: mediaconvert-quality-metrics, Property 4: Status Transitions Follow Simplified Workflow
        Validates: Requirements 4.1, 4.4
        """
        from vco.models.async_task import FileStatus

        valid_statuses = [s.value for s in FileStatus]

        assert "VERIFYING" not in valid_statuses
        assert "PENDING" in valid_statuses
        assert "CONVERTING" in valid_statuses
        assert "COMPLETED" in valid_statuses
        assert "DOWNLOADED" in valid_statuses
        assert "FAILED" in valid_statuses

    @given(status=st.sampled_from(["PENDING", "CONVERTING", "COMPLETED", "DOWNLOADED", "FAILED"]))
    @settings(max_examples=20)
    def test_all_valid_statuses_are_accepted(self, status: str):
        """Property 4: All valid statuses can be used in FileStatus.

        Feature: mediaconvert-quality-metrics, Property 4: Status Transitions Follow Simplified Workflow
        Validates: Requirements 4.1, 4.2, 4.3
        """
        from vco.models.async_task import FileStatus

        # Should not raise
        file_status = FileStatus(status)
        assert file_status.value == status

    @settings(max_examples=10)
    @given(st.data())
    def test_async_file_does_not_have_verification_progress(self, data):
        """Property 4: AsyncFile does not have verification_progress field.

        Feature: mediaconvert-quality-metrics, Property 4: Status Transitions Follow Simplified Workflow
        Validates: Requirements 4.4, 5.4
        """
        from vco.models.async_task import AsyncFile

        # Create an AsyncFile instance
        async_file = AsyncFile(
            file_id="test-file",
            source_s3_key="input/test.mp4",
        )

        # verification_progress should not exist
        assert (
            not hasattr(async_file, "verification_progress")
            or "verification_progress" not in async_file.__dataclass_fields__
        )

        # to_dict should not include verification_progress
        file_dict = async_file.to_dict()
        assert "verification_progress" not in file_dict


# =============================================================================
# Property 7: Threshold Configuration With Defaults
# Feature: mediaconvert-quality-metrics, Property 7: Threshold Configuration With Defaults
# Validates: Requirements 9.1, 9.2, 9.3
# =============================================================================


class TestThresholdConfigurationProperty:
    """Property 7: Threshold Configuration With Defaults.

    *For any* quality evaluation:
    - If SSIM threshold is configured, use configured value
    - If VMAF threshold is configured, use configured value
    - If not configured, use defaults: SSIM >= 0.95, VMAF >= 70
    """

    @given(
        ssim_threshold=st.floats(
            min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False
        ),
        vmaf_threshold=st.floats(
            min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False
        ),
    )
    @settings(max_examples=50)
    def test_evaluator_uses_configured_thresholds(
        self, ssim_threshold: float, vmaf_threshold: float
    ):
        """Property 7: QualityEvaluator uses configured thresholds.

        Feature: mediaconvert-quality-metrics, Property 7: Threshold Configuration With Defaults
        Validates: Requirements 9.1, 9.2, 9.3
        """
        evaluator = QualityEvaluator(
            ssim_threshold=ssim_threshold,
            vmaf_threshold=vmaf_threshold,
        )

        assert evaluator.ssim_threshold == ssim_threshold
        assert evaluator.vmaf_threshold == vmaf_threshold

    @settings(max_examples=10)
    @given(st.data())
    def test_evaluator_default_thresholds(self, data):
        """Property 7: QualityEvaluator uses default thresholds when not configured.

        Feature: mediaconvert-quality-metrics, Property 7: Threshold Configuration With Defaults
        Validates: Requirements 9.1, 9.2, 9.3
        """
        evaluator = QualityEvaluator()

        # Default values from design.md
        assert evaluator.ssim_threshold == 0.95
        assert evaluator.vmaf_threshold == 70.0

    @given(
        ssim_score=ssim_value,
        vmaf_score=vmaf_value,
        ssim_threshold=ssim_value,
        vmaf_threshold=vmaf_value,
    )
    @settings(max_examples=100)
    def test_evaluation_uses_configured_thresholds_correctly(
        self,
        ssim_score: float,
        vmaf_score: float,
        ssim_threshold: float,
        vmaf_threshold: float,
    ):
        """Property 7: Evaluation result reflects configured thresholds.

        Feature: mediaconvert-quality-metrics, Property 7: Threshold Configuration With Defaults
        Validates: Requirements 9.1, 9.2, 9.3
        """
        metrics = QualityMetrics(
            ssim_average=ssim_score,
            ssim_min=ssim_score * 0.95,
            ssim_max=min(1.0, ssim_score * 1.05),
            vmaf_average=vmaf_score,
            vmaf_min=vmaf_score * 0.95,
            vmaf_max=min(100.0, vmaf_score * 1.05),
        )

        evaluator = QualityEvaluator(
            ssim_threshold=ssim_threshold,
            vmaf_threshold=vmaf_threshold,
        )
        result = evaluator.evaluate(metrics)

        # Result should reflect the thresholds used
        assert result.ssim_threshold == ssim_threshold
        assert result.vmaf_threshold == vmaf_threshold

        # Pass/fail should be consistent with thresholds
        expected_pass = ssim_score >= ssim_threshold and vmaf_score >= vmaf_threshold
        assert result.passed == expected_pass
