"""Property-based tests for quality gate accuracy.

**Property 3: Quality Gate Accuracy**
**Validates: Requirements 4.2, 4.3**

For any completed conversion, if the SSIM score is below 0.95 or the converted
file size is greater than or equal to the original file size, the conversion
is rejected and the original file is preserved unchanged.
"""

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from vco.quality.checker import QualityChecker, QualityResult


class TestQualityGateAccuracy:
    """Test quality gate acceptance criteria."""

    # Property 3: Quality Gate Accuracy
    # **Validates: Requirements 4.2, 4.3**

    @given(
        ssim_score=st.floats(min_value=0.0, max_value=0.9499, allow_nan=False),
        original_size=st.integers(min_value=1000, max_value=10_000_000_000),
        converted_size=st.integers(min_value=100, max_value=9_999_999_999),
    )
    @settings(max_examples=100)
    def test_low_ssim_score_rejected(
        self, ssim_score: float, original_size: int, converted_size: int
    ):
        """Conversions with SSIM < 0.95 should be rejected."""
        # Ensure converted is smaller (so only SSIM fails)
        assume(converted_size < original_size)

        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=ssim_score, original_size=original_size, converted_size=converted_size
        )

        assert not is_acceptable, f"SSIM {ssim_score} should be rejected (threshold 0.95)"
        assert reason is not None
        assert "SSIM" in reason

    @given(
        ssim_score=st.floats(min_value=0.95, max_value=1.0, allow_nan=False),
        original_size=st.integers(min_value=1000, max_value=10_000_000_000),
        size_increase=st.integers(min_value=0, max_value=1_000_000_000),
    )
    @settings(max_examples=100)
    def test_larger_converted_file_rejected(
        self, ssim_score: float, original_size: int, size_increase: int
    ):
        """Conversions where converted >= original size should be rejected."""
        converted_size = original_size + size_increase

        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=ssim_score, original_size=original_size, converted_size=converted_size
        )

        assert not is_acceptable, (
            f"Converted size {converted_size} >= original {original_size} should be rejected"
        )
        assert reason is not None
        assert "smaller" in reason.lower()

    @given(
        ssim_score=st.floats(min_value=0.95, max_value=1.0, allow_nan=False),
        original_size=st.integers(min_value=1000, max_value=10_000_000_000),
        reduction_percent=st.floats(min_value=0.01, max_value=0.99, allow_nan=False),
    )
    @settings(max_examples=100)
    def test_good_quality_accepted(
        self, ssim_score: float, original_size: int, reduction_percent: float
    ):
        """Conversions with SSIM >= 0.95 and smaller size should be accepted."""
        converted_size = int(original_size * (1 - reduction_percent))
        assume(converted_size > 0)
        assume(converted_size < original_size)

        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=ssim_score, original_size=original_size, converted_size=converted_size
        )

        assert is_acceptable, (
            f"SSIM {ssim_score} with size reduction should be accepted, got: {reason}"
        )
        assert reason is None

    def test_ssim_exactly_at_threshold_accepted(self):
        """SSIM exactly at 0.95 threshold should be accepted."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.95, original_size=1000000, converted_size=500000
        )

        assert is_acceptable, f"SSIM 0.95 should be accepted, got: {reason}"
        assert reason is None

    def test_ssim_just_below_threshold_rejected(self):
        """SSIM just below 0.95 threshold should be rejected."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.9499, original_size=1000000, converted_size=500000
        )

        assert not is_acceptable
        assert "SSIM" in reason

    def test_none_ssim_score_rejected(self):
        """None SSIM score should be rejected."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=None, original_size=1000000, converted_size=500000
        )

        assert not is_acceptable
        assert "not available" in reason.lower()

    def test_equal_file_sizes_rejected(self):
        """Equal file sizes should be rejected."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.98, original_size=1000000, converted_size=1000000
        )

        assert not is_acceptable
        assert "smaller" in reason.lower()

    @given(
        original_size=st.integers(min_value=1000, max_value=10_000_000_000),
        converted_size=st.integers(min_value=100, max_value=9_999_999_999),
    )
    @settings(max_examples=100)
    def test_custom_ssim_threshold(self, original_size: int, converted_size: int):
        """Custom SSIM threshold should be respected."""
        assume(converted_size < original_size)

        # With custom threshold of 0.90, SSIM 0.92 should pass
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.92,
            original_size=original_size,
            converted_size=converted_size,
            ssim_threshold=0.90,
        )

        assert is_acceptable, f"SSIM 0.92 should pass with threshold 0.90, got: {reason}"

        # With custom threshold of 0.95, SSIM 0.92 should fail
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.92,
            original_size=original_size,
            converted_size=converted_size,
            ssim_threshold=0.95,
        )

        assert not is_acceptable


class TestQualityResultAcceptability:
    """Test QualityResult.is_acceptable property."""

    def test_passed_status_is_acceptable(self):
        """QualityResult with 'passed' status should be acceptable."""
        result = QualityResult(
            job_id="test_001",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="passed",
            ssim_score=0.97,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
        )

        assert result.is_acceptable

    def test_failed_status_not_acceptable(self):
        """QualityResult with 'failed' status should not be acceptable."""
        result = QualityResult(
            job_id="test_002",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="failed",
            ssim_score=0.90,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
            failure_reason="SSIM score below threshold",
        )

        assert not result.is_acceptable

    def test_error_status_not_acceptable(self):
        """QualityResult with 'error' status should not be acceptable."""
        result = QualityResult(
            job_id="test_003",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="error",
            ssim_score=None,
            original_size=0,
            converted_size=0,
            compression_ratio=0.0,
            space_saved_bytes=0,
            space_saved_percent=0.0,
            playback_verified=False,
            failure_reason="Lambda invocation failed",
        )

        assert not result.is_acceptable

    @given(status=st.sampled_from(["pending", "unknown", "processing"]))
    @settings(max_examples=50)
    def test_non_passed_status_not_acceptable(self, status: str):
        """Any status other than 'passed' should not be acceptable."""
        result = QualityResult(
            job_id="test_004",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status=status,
            ssim_score=0.97,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=2.0,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
        )

        assert not result.is_acceptable


class TestQualityGateBoundaryConditions:
    """Test boundary conditions for quality gate."""

    def test_perfect_ssim_score(self):
        """Perfect SSIM score of 1.0 should be accepted."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=1.0, original_size=1000000, converted_size=500000
        )

        assert is_acceptable
        assert reason is None

    def test_zero_ssim_score(self):
        """Zero SSIM score should be rejected."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.0, original_size=1000000, converted_size=500000
        )

        assert not is_acceptable
        assert "SSIM" in reason

    def test_minimal_size_reduction(self):
        """Minimal size reduction (1 byte) should be accepted if SSIM is good."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.98, original_size=1000000, converted_size=999999
        )

        assert is_acceptable
        assert reason is None

    def test_very_small_files(self):
        """Very small files should still follow quality rules."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.96, original_size=100, converted_size=50
        )

        assert is_acceptable
        assert reason is None

    def test_very_large_files(self):
        """Very large files should still follow quality rules."""
        is_acceptable, reason = QualityChecker.is_quality_acceptable(
            ssim_score=0.96,
            original_size=100_000_000_000,  # 100 GB
            converted_size=50_000_000_000,  # 50 GB
        )

        assert is_acceptable
        assert reason is None
