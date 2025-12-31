"""Unit tests for best-effort mode in ConvertService.

Tests for:
- AttemptResult dataclass
- _select_best_attempt() method
- ConversionResult with best_effort and selected_preset fields
- Non-adaptive preset backward compatibility
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from vco.models.types import ConversionCandidate, VideoInfo
from vco.quality.checker import QualityResult
from vco.services.convert import (
    AttemptResult,
    BatchConversionResult,
    ConversionResult,
    ConvertService,
)


# Test fixtures
@pytest.fixture
def mock_quality_result_passed():
    """Create a mock QualityResult that passed."""
    return QualityResult(
        job_id="test-job-1",
        original_s3_key="input/test.mp4",
        converted_s3_key="output/test_h265.mp4",
        status="passed",
        ssim_score=0.96,
        original_size=1000000,
        converted_size=500000,
        compression_ratio=0.5,
        space_saved_bytes=500000,
        space_saved_percent=50.0,
        playback_verified=True,
    )


@pytest.fixture
def mock_quality_result_failed():
    """Create a mock QualityResult that failed SSIM threshold."""
    return QualityResult(
        job_id="test-job-2",
        original_s3_key="input/test.mp4",
        converted_s3_key="output/test_h265.mp4",
        status="failed",
        ssim_score=0.92,
        original_size=1000000,
        converted_size=500000,
        compression_ratio=0.5,
        space_saved_bytes=500000,
        space_saved_percent=50.0,
        playback_verified=True,
        failure_reason="SSIM score 0.9200 is below threshold 0.95",
    )


@pytest.fixture
def mock_video_info():
    """Create a mock VideoInfo."""
    return VideoInfo(
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


@pytest.fixture
def mock_candidate(mock_video_info):
    """Create a mock ConversionCandidate."""
    return ConversionCandidate(
        video=mock_video_info,
        estimated_savings_bytes=500000,
        estimated_savings_percent=50.0,
    )


class TestAttemptResult:
    """Tests for AttemptResult dataclass.

    Validates: Requirements 2.1, 3.1
    """

    def test_attempt_result_initialization(self, mock_quality_result_passed):
        """Test AttemptResult can be initialized with all fields."""
        attempt = AttemptResult(
            preset="balanced",
            ssim_score=0.96,
            quality_result=mock_quality_result_passed,
            output_s3_key="output/test/balanced/test_h265.mp4",
            source_s3_key="input/test/test.mp4",
            metadata_s3_key="input/test/metadata.json",
            success=True,
        )

        assert attempt.preset == "balanced"
        assert attempt.ssim_score == 0.96
        assert attempt.quality_result == mock_quality_result_passed
        assert attempt.output_s3_key == "output/test/balanced/test_h265.mp4"
        assert attempt.source_s3_key == "input/test/test.mp4"
        assert attempt.metadata_s3_key == "input/test/metadata.json"
        assert attempt.success is True
        assert attempt.error_message is None

    def test_attempt_result_with_error(self):
        """Test AttemptResult with error message."""
        attempt = AttemptResult(
            preset="high",
            ssim_score=None,
            quality_result=None,
            output_s3_key="output/test/high/test_h265.mp4",
            source_s3_key="input/test/test.mp4",
            metadata_s3_key="input/test/metadata.json",
            success=False,
            error_message="MediaConvert job failed",
        )

        assert attempt.preset == "high"
        assert attempt.ssim_score is None
        assert attempt.quality_result is None
        assert attempt.success is False
        assert attempt.error_message == "MediaConvert job failed"

    def test_attempt_result_failed_quality_check(self, mock_quality_result_failed):
        """Test AttemptResult with failed quality check (SSIM below threshold)."""
        attempt = AttemptResult(
            preset="balanced",
            ssim_score=0.92,
            quality_result=mock_quality_result_failed,
            output_s3_key="output/test/balanced/test_h265.mp4",
            source_s3_key="input/test/test.mp4",
            metadata_s3_key="input/test/metadata.json",
            success=True,  # Conversion succeeded, but quality check failed
        )

        assert attempt.preset == "balanced"
        assert attempt.ssim_score == 0.92
        assert attempt.success is True
        assert attempt.quality_result.status == "failed"


class TestConversionResultExtension:
    """Tests for ConversionResult with best_effort and selected_preset fields.

    Validates: Requirements 2.3, 4.2, 4.3
    """

    def test_conversion_result_default_values(self):
        """Test ConversionResult has correct default values for new fields."""
        result = ConversionResult(
            uuid="test-uuid",
            filename="test.mp4",
            success=True,
            original_path=Path("/tmp/test.mp4"),
        )

        assert result.best_effort is False
        assert result.selected_preset is None

    def test_conversion_result_with_best_effort(self):
        """Test ConversionResult with best_effort mode enabled."""
        result = ConversionResult(
            uuid="test-uuid",
            filename="test.mp4",
            success=True,
            original_path=Path("/tmp/test.mp4"),
            best_effort=True,
            selected_preset="balanced",
        )

        assert result.best_effort is True
        assert result.selected_preset == "balanced"

    def test_conversion_result_to_dict_includes_new_fields(self):
        """Test BatchConversionResult._result_to_dict includes new fields."""
        result = ConversionResult(
            uuid="test-uuid",
            filename="test.mp4",
            success=True,
            original_path=Path("/tmp/test.mp4"),
            best_effort=True,
            selected_preset="high",
        )

        batch_result = BatchConversionResult()
        result_dict = batch_result._result_to_dict(result)

        assert "best_effort" in result_dict
        assert result_dict["best_effort"] is True
        assert "selected_preset" in result_dict
        assert result_dict["selected_preset"] == "high"


class TestSelectBestAttempt:
    """Tests for _select_best_attempt() method.

    Validates: Requirements 2.1, 2.2, 2.4
    """

    @pytest.fixture
    def convert_service(self):
        """Create a ConvertService with mocked dependencies."""
        mock_mediaconvert = MagicMock()
        mock_quality_checker = MagicMock()

        service = ConvertService(
            mediaconvert_client=mock_mediaconvert,
            quality_checker=mock_quality_checker,
        )
        return service

    def test_select_best_attempt_highest_ssim(
        self, convert_service, mock_candidate, mock_quality_result_failed
    ):
        """Test that highest SSIM score is selected."""
        # Create attempts with different SSIM scores
        attempt1 = AttemptResult(
            preset="balanced",
            ssim_score=0.92,
            quality_result=mock_quality_result_failed,
            output_s3_key="output/balanced/test_h265.mp4",
            source_s3_key="input/test.mp4",
            metadata_s3_key="input/metadata.json",
            success=True,
        )

        # Create a quality result with higher SSIM
        quality_result_higher = QualityResult(
            job_id="test-job-3",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="failed",
            ssim_score=0.94,
            original_size=1000000,
            converted_size=600000,
            compression_ratio=0.6,
            space_saved_bytes=400000,
            space_saved_percent=40.0,
            playback_verified=True,
            failure_reason="SSIM score 0.9400 is below threshold 0.95",
        )

        attempt2 = AttemptResult(
            preset="high",
            ssim_score=0.94,
            quality_result=quality_result_higher,
            output_s3_key="output/high/test_h265.mp4",
            source_s3_key="input/test.mp4",
            metadata_s3_key="input/metadata.json",
            success=True,
        )

        best, is_best_effort = convert_service._select_best_attempt(
            mock_candidate, [attempt1, attempt2]
        )

        assert best is not None
        assert best.preset == "high"
        assert best.ssim_score == 0.94
        assert is_best_effort is True

    def test_select_best_attempt_first_higher(
        self, convert_service, mock_candidate, mock_quality_result_failed
    ):
        """Test that first attempt is selected when it has higher SSIM."""
        attempt1 = AttemptResult(
            preset="balanced",
            ssim_score=0.93,
            quality_result=mock_quality_result_failed,
            output_s3_key="output/balanced/test_h265.mp4",
            source_s3_key="input/test.mp4",
            metadata_s3_key="input/metadata.json",
            success=True,
        )

        quality_result_lower = QualityResult(
            job_id="test-job-4",
            original_s3_key="input/test.mp4",
            converted_s3_key="output/test_h265.mp4",
            status="failed",
            ssim_score=0.91,
            original_size=1000000,
            converted_size=600000,
            compression_ratio=0.6,
            space_saved_bytes=400000,
            space_saved_percent=40.0,
            playback_verified=True,
        )

        attempt2 = AttemptResult(
            preset="high",
            ssim_score=0.91,
            quality_result=quality_result_lower,
            output_s3_key="output/high/test_h265.mp4",
            source_s3_key="input/test.mp4",
            metadata_s3_key="input/metadata.json",
            success=True,
        )

        best, is_best_effort = convert_service._select_best_attempt(
            mock_candidate, [attempt1, attempt2]
        )

        assert best is not None
        assert best.preset == "balanced"
        assert best.ssim_score == 0.93
        assert is_best_effort is True

    def test_select_best_attempt_all_failed(self, convert_service, mock_candidate):
        """Test handling when all attempts failed (no SSIM scores)."""
        attempt1 = AttemptResult(
            preset="balanced",
            ssim_score=None,
            quality_result=None,
            output_s3_key="output/balanced/test_h265.mp4",
            source_s3_key="input/test.mp4",
            metadata_s3_key="input/metadata.json",
            success=False,
            error_message="MediaConvert job failed",
        )

        attempt2 = AttemptResult(
            preset="high",
            ssim_score=None,
            quality_result=None,
            output_s3_key="output/high/test_h265.mp4",
            source_s3_key="input/test.mp4",
            metadata_s3_key="input/metadata.json",
            success=False,
            error_message="MediaConvert job failed",
        )

        best, is_best_effort = convert_service._select_best_attempt(
            mock_candidate, [attempt1, attempt2]
        )

        assert best is None
        assert is_best_effort is False

    def test_select_best_attempt_empty_list(self, convert_service, mock_candidate):
        """Test handling of empty attempts list."""
        best, is_best_effort = convert_service._select_best_attempt(mock_candidate, [])

        assert best is None
        assert is_best_effort is False

    def test_select_best_attempt_single_valid(
        self, convert_service, mock_candidate, mock_quality_result_failed
    ):
        """Test with single valid attempt."""
        attempt = AttemptResult(
            preset="balanced",
            ssim_score=0.92,
            quality_result=mock_quality_result_failed,
            output_s3_key="output/balanced/test_h265.mp4",
            source_s3_key="input/test.mp4",
            metadata_s3_key="input/metadata.json",
            success=True,
        )

        best, is_best_effort = convert_service._select_best_attempt(mock_candidate, [attempt])

        assert best is not None
        assert best.preset == "balanced"
        assert is_best_effort is True


class TestNonAdaptivePresetBackwardCompatibility:
    """Tests for non-adaptive preset backward compatibility.

    Validates: Requirements 5.1, 5.2, 5.3
    Property 8: Non-adaptive preset backward compatibility
    """

    @pytest.fixture
    def convert_service(self):
        """Create a ConvertService with mocked dependencies."""
        mock_mediaconvert = MagicMock()
        mock_quality_checker = MagicMock()

        service = ConvertService(
            mediaconvert_client=mock_mediaconvert,
            quality_checker=mock_quality_checker,
        )
        return service

    def test_non_adaptive_preset_uses_convert_single(self, convert_service, mock_candidate):
        """Test that non-adaptive presets use convert_single, not best-effort."""
        from vco.converter import is_adaptive_preset

        # Verify balanced and high are not adaptive
        assert is_adaptive_preset("balanced") is False
        assert is_adaptive_preset("high") is False

        # Verify balanced+ is adaptive
        assert is_adaptive_preset("balanced+") is True

    def test_non_adaptive_preset_result_no_best_effort_flag(self):
        """Test that non-adaptive preset results don't have best_effort=True."""
        # When using non-adaptive preset, best_effort should always be False
        result = ConversionResult(
            uuid="test-uuid",
            filename="test.mp4",
            success=True,
            original_path=Path("/tmp/test.mp4"),
            # Non-adaptive presets should not set these
            best_effort=False,
            selected_preset=None,
        )

        assert result.best_effort is False
        assert result.selected_preset is None
