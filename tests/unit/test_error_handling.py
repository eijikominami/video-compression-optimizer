"""Unit tests for error handling functions.

Task 7.5: 単体テスト: エラーハンドリング関数
- classify_mediaconvert_error: 全エラーコードの分類確認
- determine_ssim_action: 閾値境界値テスト（0.949, 0.950, 0.951）
- 未知のエラーコードの処理確認

Requirements: 5.2, 5.3, 5.4
"""

import pytest

from vco.services.error_handling import (
    CONFIG_ERRORS,
    PERMISSION_ERRORS,
    PRESET_CHAIN,
    TRANSIENT_ERRORS,
    SSIMAction,
    classify_mediaconvert_error,
    determine_ssim_action,
    get_next_preset,
    is_adaptive_preset,
)


class TestClassifyMediaConvertError:
    """Test classify_mediaconvert_error function."""

    # Transient errors - should be retryable
    @pytest.mark.parametrize("error_code", [1517, 1522, 1550, 1999])
    def test_transient_errors_are_retryable(self, error_code: int):
        """Transient errors should be classified as retryable."""
        result = classify_mediaconvert_error(error_code)
        assert result.is_retryable is True
        assert result.category == "transient"
        assert result.error_code == error_code

    # Config/input errors - should not be retryable
    @pytest.mark.parametrize("error_code", [1010, 1030, 1040])
    def test_config_errors_are_not_retryable(self, error_code: int):
        """Config/input errors should not be retryable."""
        result = classify_mediaconvert_error(error_code)
        assert result.is_retryable is False
        assert result.category == "config_or_input"
        assert result.error_code == error_code

    # Permission errors - should not be retryable
    @pytest.mark.parametrize("error_code", [1401, 1432, 1433])
    def test_permission_errors_are_not_retryable(self, error_code: int):
        """Permission errors should not be retryable."""
        result = classify_mediaconvert_error(error_code)
        assert result.is_retryable is False
        assert result.category == "permission"
        assert result.error_code == error_code

    # Unknown errors - should not be retryable (safe default)
    @pytest.mark.parametrize("error_code", [1000, 1100, 1200, 1300, 1500, 1600, 1700, 1800, 1900])
    def test_unknown_errors_are_not_retryable(self, error_code: int):
        """Unknown errors should not be retryable."""
        result = classify_mediaconvert_error(error_code)
        assert result.is_retryable is False
        assert result.category == "unknown"
        assert result.error_code == error_code

    def test_error_code_preserved(self):
        """Error code should be preserved in result."""
        for code in [1010, 1517, 1401, 1234]:
            result = classify_mediaconvert_error(code)
            assert result.error_code == code

    def test_all_known_transient_errors_covered(self):
        """All known transient errors should be in TRANSIENT_ERRORS."""
        expected = {1517, 1522, 1550, 1999}
        assert TRANSIENT_ERRORS == expected

    def test_all_known_config_errors_covered(self):
        """All known config errors should be in CONFIG_ERRORS."""
        expected = {1010, 1030, 1040}
        assert CONFIG_ERRORS == expected

    def test_all_known_permission_errors_covered(self):
        """All known permission errors should be in PERMISSION_ERRORS."""
        expected = {1401, 1432, 1433}
        assert PERMISSION_ERRORS == expected


class TestDetermineSSIMAction:
    """Test determine_ssim_action function."""

    # Threshold boundary tests (default threshold = 0.95)
    def test_ssim_at_threshold_accepts(self):
        """SSIM exactly at threshold should be accepted."""
        result = determine_ssim_action("balanced", 0.95, threshold=0.95)
        assert result.action == SSIMAction.ACCEPT.value

    def test_ssim_above_threshold_accepts(self):
        """SSIM above threshold should be accepted."""
        result = determine_ssim_action("balanced", 0.951, threshold=0.95)
        assert result.action == SSIMAction.ACCEPT.value

    def test_ssim_below_threshold_non_adaptive_fails(self):
        """SSIM below threshold with non-adaptive preset should fail."""
        result = determine_ssim_action("balanced", 0.949, threshold=0.95)
        assert result.action == SSIMAction.FAIL.value

    def test_ssim_just_below_threshold(self):
        """SSIM just below threshold (0.9499) should fail for non-adaptive."""
        result = determine_ssim_action("balanced", 0.9499, threshold=0.95)
        assert result.action == SSIMAction.FAIL.value

    # Non-adaptive presets
    @pytest.mark.parametrize("preset", ["balanced", "high", "compression"])
    def test_non_adaptive_presets_fail_on_low_ssim(self, preset: str):
        """Non-adaptive presets should fail immediately on low SSIM."""
        result = determine_ssim_action(preset, 0.90, threshold=0.95)
        assert result.action == SSIMAction.FAIL.value
        assert result.next_preset is None

    # Adaptive presets
    def test_adaptive_preset_retries_on_low_ssim(self):
        """Adaptive preset should retry with higher preset on low SSIM."""
        result = determine_ssim_action("balanced+", 0.90, threshold=0.95)
        assert result.action == SSIMAction.RETRY_WITH_HIGHER_PRESET.value
        assert result.next_preset == "high"

    def test_adaptive_preset_at_end_of_chain_fails(self):
        """Adaptive preset at end of chain should fail on low SSIM."""
        result = determine_ssim_action("high+", 0.90, threshold=0.95)
        assert result.action == SSIMAction.FAIL.value
        assert result.next_preset is None

    def test_adaptive_preset_accepts_on_good_ssim(self):
        """Adaptive preset should accept on good SSIM."""
        result = determine_ssim_action("balanced+", 0.98, threshold=0.95)
        assert result.action == SSIMAction.ACCEPT.value

    # Custom threshold tests
    def test_custom_threshold_lower(self):
        """Custom lower threshold should accept lower SSIM."""
        result = determine_ssim_action("balanced", 0.85, threshold=0.80)
        assert result.action == SSIMAction.ACCEPT.value

    def test_custom_threshold_higher(self):
        """Custom higher threshold should reject higher SSIM."""
        result = determine_ssim_action("balanced", 0.96, threshold=0.98)
        assert result.action == SSIMAction.FAIL.value

    # Edge cases
    def test_ssim_zero(self):
        """SSIM of 0 should fail for non-adaptive."""
        result = determine_ssim_action("balanced", 0.0, threshold=0.95)
        assert result.action == SSIMAction.FAIL.value

    def test_ssim_one(self):
        """SSIM of 1.0 should always accept."""
        result = determine_ssim_action("balanced", 1.0, threshold=0.95)
        assert result.action == SSIMAction.ACCEPT.value

    def test_unknown_adaptive_preset(self):
        """Unknown adaptive preset should try from beginning of chain."""
        result = determine_ssim_action("unknown+", 0.90, threshold=0.95)
        # Unknown preset treated as first in chain, so can retry
        assert result.action == SSIMAction.RETRY_WITH_HIGHER_PRESET.value


class TestIsAdaptivePreset:
    """Test is_adaptive_preset function."""

    @pytest.mark.parametrize("preset", ["balanced+", "high+", "compression+", "test+"])
    def test_adaptive_presets(self, preset: str):
        """Presets ending with + should be adaptive."""
        assert is_adaptive_preset(preset) is True

    @pytest.mark.parametrize("preset", ["balanced", "high", "compression", "test", ""])
    def test_non_adaptive_presets(self, preset: str):
        """Presets not ending with + should not be adaptive."""
        assert is_adaptive_preset(preset) is False

    def test_plus_only(self):
        """Single + should be adaptive."""
        assert is_adaptive_preset("+") is True

    def test_multiple_plus(self):
        """Multiple + should be adaptive."""
        assert is_adaptive_preset("balanced++") is True


class TestGetNextPreset:
    """Test get_next_preset function."""

    def test_balanced_returns_high(self):
        """balanced should return high."""
        result = get_next_preset("balanced")
        assert result == "high"

    def test_balanced_plus_returns_high(self):
        """balanced+ should return high."""
        result = get_next_preset("balanced+")
        assert result == "high"

    def test_high_returns_none(self):
        """high (end of chain) should return None."""
        result = get_next_preset("high")
        assert result is None

    def test_high_plus_returns_none(self):
        """high+ (end of chain) should return None."""
        result = get_next_preset("high+")
        assert result is None

    def test_unknown_preset_returns_none(self):
        """Unknown preset should return None."""
        result = get_next_preset("unknown")
        assert result is None

    def test_preset_chain_order(self):
        """Preset chain should be in correct order."""
        assert PRESET_CHAIN == ["balanced", "high"]


class TestSSIMBoundaryValues:
    """Detailed boundary value tests for SSIM threshold."""

    @pytest.mark.parametrize(
        "ssim,expected_action",
        [
            (0.9499999, SSIMAction.FAIL.value),
            (0.95, SSIMAction.ACCEPT.value),
            (0.9500001, SSIMAction.ACCEPT.value),
        ],
    )
    def test_threshold_boundary_non_adaptive(self, ssim: float, expected_action: str):
        """Test exact boundary values for non-adaptive preset."""
        result = determine_ssim_action("balanced", ssim, threshold=0.95)
        assert result.action == expected_action

    @pytest.mark.parametrize(
        "ssim,expected_action",
        [
            (0.9499999, SSIMAction.RETRY_WITH_HIGHER_PRESET.value),
            (0.95, SSIMAction.ACCEPT.value),
            (0.9500001, SSIMAction.ACCEPT.value),
        ],
    )
    def test_threshold_boundary_adaptive(self, ssim: float, expected_action: str):
        """Test exact boundary values for adaptive preset."""
        result = determine_ssim_action("balanced+", ssim, threshold=0.95)
        assert result.action == expected_action
