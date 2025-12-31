"""Unit tests for Quality_Config module.

Tests: Task 1.1
Requirements: 1.1, 1.2, 1.3
"""

import pytest

from vco.config.quality_config import (
    ADAPTIVE_PRESET_CHAIN,
    QUALITY_PRESETS,
    SSIM_THRESHOLD,
    get_base_preset_name,
    get_mediaconvert_settings,
    get_preset,
    is_adaptive_preset,
)


class TestSSIMThreshold:
    """Tests for SSIM threshold configuration."""

    def test_ssim_threshold_value(self):
        """SSIM threshold should be 0.95."""
        assert SSIM_THRESHOLD == 0.95

    def test_ssim_threshold_type(self):
        """SSIM threshold should be a float."""
        assert isinstance(SSIM_THRESHOLD, float)


class TestQualityPresets:
    """Tests for quality preset definitions."""

    def test_all_presets_defined(self):
        """All required presets should be defined."""
        required_presets = {"balanced", "high", "compression", "balanced+", "high+"}
        assert set(QUALITY_PRESETS.keys()) == required_presets

    def test_balanced_preset_values(self):
        """Balanced preset should have correct values."""
        preset = QUALITY_PRESETS["balanced"]
        assert preset.name == "balanced"
        assert preset.qvbr_max_bitrate == 20_000_000
        assert preset.qvbr_quality_level == 7
        assert preset.is_adaptive is False

    def test_high_preset_values(self):
        """High preset should have correct values."""
        preset = QUALITY_PRESETS["high"]
        assert preset.name == "high"
        assert preset.qvbr_max_bitrate == 50_000_000
        assert preset.qvbr_quality_level == 9
        assert preset.is_adaptive is False

    def test_compression_preset_values(self):
        """Compression preset should have correct values."""
        preset = QUALITY_PRESETS["compression"]
        assert preset.name == "compression"
        assert preset.qvbr_max_bitrate == 10_000_000
        assert preset.qvbr_quality_level == 5
        assert preset.is_adaptive is False

    def test_balanced_plus_preset_values(self):
        """Balanced+ preset should have correct values and be adaptive."""
        preset = QUALITY_PRESETS["balanced+"]
        assert preset.name == "balanced+"
        assert preset.qvbr_max_bitrate == 20_000_000
        assert preset.qvbr_quality_level == 7
        assert preset.is_adaptive is True

    def test_high_plus_preset_values(self):
        """High+ preset should have correct values and be adaptive."""
        preset = QUALITY_PRESETS["high+"]
        assert preset.name == "high+"
        assert preset.qvbr_max_bitrate == 50_000_000
        assert preset.qvbr_quality_level == 9
        assert preset.is_adaptive is True

    def test_preset_is_frozen_dataclass(self):
        """Presets should be immutable (frozen dataclass)."""
        preset = QUALITY_PRESETS["balanced"]
        with pytest.raises(AttributeError):
            preset.qvbr_max_bitrate = 999  # type: ignore


class TestAdaptivePresetChain:
    """Tests for adaptive preset chain."""

    def test_balanced_plus_chain(self):
        """Balanced+ adaptive chain should be balanced -> high."""
        assert "balanced+" in ADAPTIVE_PRESET_CHAIN
        assert ADAPTIVE_PRESET_CHAIN["balanced+"] == ["balanced", "high"]

    def test_chain_presets_exist(self):
        """All presets in chain should exist in QUALITY_PRESETS."""
        for chain in ADAPTIVE_PRESET_CHAIN.values():
            for preset_name in chain:
                assert preset_name in QUALITY_PRESETS


class TestGetPreset:
    """Tests for get_preset function."""

    def test_get_valid_preset(self):
        """Should return preset for valid name."""
        preset = get_preset("balanced")
        assert preset.name == "balanced"
        assert preset.qvbr_max_bitrate == 20_000_000

    def test_get_invalid_preset_raises(self):
        """Should raise ValueError for invalid preset name."""
        with pytest.raises(ValueError, match="Unknown preset"):
            get_preset("invalid")

    def test_get_all_presets(self):
        """Should be able to get all defined presets."""
        for name in QUALITY_PRESETS:
            preset = get_preset(name)
            assert preset.name == name


class TestGetMediaConvertSettings:
    """Tests for get_mediaconvert_settings function."""

    def test_returns_correct_keys(self):
        """Should return dict with max_bitrate and quality_level."""
        preset = QUALITY_PRESETS["balanced"]
        settings = get_mediaconvert_settings(preset)
        assert "max_bitrate" in settings
        assert "quality_level" in settings

    def test_returns_correct_values(self):
        """Should return correct values from preset."""
        preset = QUALITY_PRESETS["high"]
        settings = get_mediaconvert_settings(preset)
        assert settings["max_bitrate"] == 50_000_000
        assert settings["quality_level"] == 9


class TestIsAdaptivePreset:
    """Tests for is_adaptive_preset function."""

    def test_adaptive_presets(self):
        """Presets in ADAPTIVE_PRESET_CHAIN should be adaptive."""
        assert is_adaptive_preset("balanced+") is True

    def test_non_adaptive_presets(self):
        """Presets not in ADAPTIVE_PRESET_CHAIN should not be adaptive."""
        assert is_adaptive_preset("balanced") is False
        assert is_adaptive_preset("high") is False
        assert is_adaptive_preset("compression") is False
        assert is_adaptive_preset("high+") is False  # Not in chain


class TestGetBasePresetName:
    """Tests for get_base_preset_name function."""

    def test_removes_plus_suffix(self):
        """Should remove + suffix from preset name."""
        assert get_base_preset_name("balanced+") == "balanced"
        assert get_base_preset_name("high+") == "high"

    def test_keeps_non_adaptive_names(self):
        """Should keep names without + suffix unchanged."""
        assert get_base_preset_name("balanced") == "balanced"
        assert get_base_preset_name("high") == "high"
        assert get_base_preset_name("compression") == "compression"
