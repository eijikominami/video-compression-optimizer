"""Property-based tests for best-effort mode selection logic.

Tests Property 3: Best result selection by highest SSIM
Validates: Requirements 2.1, 2.2, 2.4
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.models.types import ConversionCandidate, VideoInfo
from vco.quality.checker import QualityResult
from vco.services.convert import AttemptResult, ConvertService


def create_mock_candidate() -> ConversionCandidate:
    """Create a mock ConversionCandidate for testing."""
    video = VideoInfo(
        uuid="test-uuid-123",
        filename="test_video.mp4",
        path=Path("/tmp/test_video.mp4"),
        file_size=1000000,
        duration=60.0,
        resolution="1920x1080",
        codec="h264",
        bitrate=5000000,
        frame_rate=30.0,
        is_local=True,
        is_in_icloud=False,
        capture_date=datetime(2024, 1, 1),
        creation_date=datetime(2024, 1, 1),
        albums=[],
        location=None,
    )
    return ConversionCandidate(
        video=video,
        estimated_savings_bytes=500000,
        estimated_savings_percent=50.0,
    )


def create_convert_service() -> ConvertService:
    """Create a ConvertService with mocked dependencies."""
    mock_mediaconvert = MagicMock()
    mock_quality_checker = MagicMock()

    return ConvertService(
        mediaconvert_client=mock_mediaconvert,
        quality_checker=mock_quality_checker,
    )


def create_attempt(preset: str, ssim_score: float | None, success: bool = True) -> AttemptResult:
    """Helper to create AttemptResult for testing."""
    quality_result = None
    if ssim_score is not None:
        quality_result = QualityResult(
            job_id=f"job-{preset}",
            original_s3_key="input/test.mp4",
            converted_s3_key=f"output/{preset}/test_h265.mp4",
            status="failed" if ssim_score < 0.95 else "passed",
            ssim_score=ssim_score,
            original_size=1000000,
            converted_size=500000,
            compression_ratio=0.5,
            space_saved_bytes=500000,
            space_saved_percent=50.0,
            playback_verified=True,
        )

    return AttemptResult(
        preset=preset,
        ssim_score=ssim_score,
        quality_result=quality_result,
        output_s3_key=f"output/{preset}/test_h265.mp4",
        source_s3_key="input/test.mp4",
        metadata_s3_key="input/metadata.json",
        success=success,
        error_message=None if success else "Conversion failed",
    )


class TestBestSSIMSelection:
    """Property tests for best SSIM selection.

    Property 3: Best result selection by highest SSIM
    For any adaptive preset conversion where all presets fail to meet SSIM threshold,
    the result with the highest SSIM score SHALL be selected.

    Validates: Requirements 2.1, 2.2, 2.4
    """

    @given(
        ssim_scores=st.lists(
            st.floats(min_value=0.5, max_value=0.949, allow_nan=False),
            min_size=2,
            max_size=5,
        )
    )
    @settings(max_examples=100)
    def test_highest_ssim_always_selected(self, ssim_scores):
        """Property: The attempt with highest SSIM is always selected."""
        convert_service = create_convert_service()
        mock_candidate = create_mock_candidate()

        # Create attempts with the given SSIM scores
        presets = ["preset_" + str(i) for i in range(len(ssim_scores))]
        attempts = [create_attempt(preset, ssim) for preset, ssim in zip(presets, ssim_scores)]

        best, is_best_effort = convert_service._select_best_attempt(mock_candidate, attempts)

        # The best attempt should have the highest SSIM
        assert best is not None
        assert best.ssim_score == max(ssim_scores)
        assert is_best_effort is True

    @given(ssim_score=st.floats(min_value=0.5, max_value=0.949, allow_nan=False))
    @settings(max_examples=50)
    def test_single_attempt_selected(self, ssim_score):
        """Property: Single valid attempt is always selected."""
        convert_service = create_convert_service()
        mock_candidate = create_mock_candidate()

        attempt = create_attempt("balanced", ssim_score)

        best, is_best_effort = convert_service._select_best_attempt(mock_candidate, [attempt])

        assert best is not None
        assert best.ssim_score == ssim_score
        assert is_best_effort is True

    @given(
        valid_ssim=st.floats(min_value=0.5, max_value=0.949, allow_nan=False),
        num_failed=st.integers(min_value=1, max_value=3),
    )
    @settings(max_examples=50)
    def test_valid_attempt_selected_over_failed(self, valid_ssim, num_failed):
        """Property: Valid attempt is selected even when others failed."""
        convert_service = create_convert_service()
        mock_candidate = create_mock_candidate()

        # Create failed attempts (no SSIM score)
        failed_attempts = [
            create_attempt(f"failed_{i}", None, success=False) for i in range(num_failed)
        ]

        # Create one valid attempt
        valid_attempt = create_attempt("valid", valid_ssim, success=True)

        # Mix them up
        attempts = failed_attempts + [valid_attempt]

        best, is_best_effort = convert_service._select_best_attempt(mock_candidate, attempts)

        assert best is not None
        assert best.preset == "valid"
        assert best.ssim_score == valid_ssim

    @given(num_failed=st.integers(min_value=1, max_value=5))
    @settings(max_examples=30)
    def test_all_failed_returns_none(self, num_failed):
        """Property: When all attempts fail, None is returned."""
        convert_service = create_convert_service()
        mock_candidate = create_mock_candidate()

        failed_attempts = [
            create_attempt(f"failed_{i}", None, success=False) for i in range(num_failed)
        ]

        best, is_best_effort = convert_service._select_best_attempt(mock_candidate, failed_attempts)

        assert best is None
        assert is_best_effort is False


class TestBestEffortFlagConsistency:
    """Property tests for best_effort flag consistency.

    Property 4: Best-effort flag and SSIM in result
    For any conversion that uses best-effort mode, the result SHALL have
    best_effort=True and SHALL include the actual SSIM score.

    Validates: Requirements 2.3, 4.2, 4.3
    """

    @given(
        ssim_scores=st.lists(
            st.floats(min_value=0.5, max_value=0.949, allow_nan=False),
            min_size=1,
            max_size=5,
        )
    )
    @settings(max_examples=50)
    def test_best_effort_flag_set_when_threshold_not_met(self, ssim_scores):
        """Property: is_best_effort is True when no attempt meets threshold."""
        convert_service = create_convert_service()
        mock_candidate = create_mock_candidate()

        # All SSIM scores are below 0.95 threshold
        presets = ["preset_" + str(i) for i in range(len(ssim_scores))]
        attempts = [create_attempt(preset, ssim) for preset, ssim in zip(presets, ssim_scores)]

        best, is_best_effort = convert_service._select_best_attempt(mock_candidate, attempts)

        # Since all scores are below threshold, best_effort should be True
        assert best is not None
        assert is_best_effort is True
        assert best.ssim_score is not None
