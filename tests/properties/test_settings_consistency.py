"""Property-based tests for settings consistency.

Tests: Task 1.1
Property 1: Settings Consistency
Requirements: 1.4, 1.5, 7.1, 7.2, 7.3

Verifies that sync and async modes use identical quality settings
by importing from the shared Quality_Config module.
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.config.quality_config import (
    QUALITY_PRESETS,
    SSIM_THRESHOLD,
    QualityPreset,
    get_mediaconvert_settings,
    get_preset,
)


class TestSettingsConsistencyProperties:
    """Property-based tests for settings consistency."""

    @given(preset_name=st.sampled_from(list(QUALITY_PRESETS.keys())))
    @settings(max_examples=100)
    def test_preset_retrieval_consistency(self, preset_name: str):
        """For any preset name, get_preset returns the same preset from QUALITY_PRESETS."""
        preset = get_preset(preset_name)
        expected = QUALITY_PRESETS[preset_name]
        assert preset is expected

    @given(preset_name=st.sampled_from(list(QUALITY_PRESETS.keys())))
    @settings(max_examples=100)
    def test_mediaconvert_settings_match_preset(self, preset_name: str):
        """For any preset, MediaConvert settings match preset values."""
        preset = get_preset(preset_name)
        mc_settings = get_mediaconvert_settings(preset)

        assert mc_settings["max_bitrate"] == preset.qvbr_max_bitrate
        assert mc_settings["quality_level"] == preset.qvbr_quality_level

    @given(preset_name=st.sampled_from(list(QUALITY_PRESETS.keys())))
    @settings(max_examples=100)
    def test_preset_values_are_valid(self, preset_name: str):
        """For any preset, values are within valid ranges."""
        preset = get_preset(preset_name)

        # Bitrate should be positive and reasonable (1Mbps to 100Mbps)
        assert 1_000_000 <= preset.qvbr_max_bitrate <= 100_000_000

        # Quality level should be 1-10 (QVBR range)
        assert 1 <= preset.qvbr_quality_level <= 10

    @given(
        preset_name1=st.sampled_from(list(QUALITY_PRESETS.keys())),
        preset_name2=st.sampled_from(list(QUALITY_PRESETS.keys())),
    )
    @settings(max_examples=100)
    def test_preset_identity(self, preset_name1: str, preset_name2: str):
        """Same preset name always returns same preset object."""
        if preset_name1 == preset_name2:
            preset1 = get_preset(preset_name1)
            preset2 = get_preset(preset_name2)
            assert preset1 is preset2


class TestSSIMThresholdConsistency:
    """Tests for SSIM threshold consistency."""

    def test_ssim_threshold_is_constant(self):
        """SSIM threshold should be a constant value."""
        # Import multiple times to verify it's the same
        from vco.config.quality_config import SSIM_THRESHOLD as THRESHOLD_1
        from vco.config.quality_config import SSIM_THRESHOLD as THRESHOLD_2

        assert THRESHOLD_1 == THRESHOLD_2
        assert THRESHOLD_1 == 0.95

    def test_ssim_threshold_in_valid_range(self):
        """SSIM threshold should be between 0 and 1."""
        assert 0.0 <= SSIM_THRESHOLD <= 1.0


class TestPresetImmutability:
    """Tests for preset immutability."""

    @given(preset_name=st.sampled_from(list(QUALITY_PRESETS.keys())))
    @settings(max_examples=100)
    def test_preset_is_immutable(self, preset_name: str):
        """Presets should be immutable (frozen dataclass)."""
        preset = get_preset(preset_name)

        # Verify it's a QualityPreset
        assert isinstance(preset, QualityPreset)

        # Frozen dataclass should raise on attribute assignment
        try:
            preset.qvbr_max_bitrate = 999  # type: ignore
            assert False, "Should have raised AttributeError"
        except AttributeError:
            pass  # Expected


class TestAdaptivePresetConsistency:
    """Tests for adaptive preset consistency."""

    def test_adaptive_presets_have_base_equivalents(self):
        """Adaptive presets (ending with +) should have base equivalents."""
        adaptive_presets = [name for name in QUALITY_PRESETS if name.endswith("+")]

        for adaptive_name in adaptive_presets:
            base_name = adaptive_name.rstrip("+")
            assert base_name in QUALITY_PRESETS, f"Base preset {base_name} not found"

            # Adaptive and base should have same bitrate/quality
            adaptive = QUALITY_PRESETS[adaptive_name]
            base = QUALITY_PRESETS[base_name]

            assert adaptive.qvbr_max_bitrate == base.qvbr_max_bitrate
            assert adaptive.qvbr_quality_level == base.qvbr_quality_level
            assert adaptive.is_adaptive is True
            assert base.is_adaptive is False
