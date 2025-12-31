"""Property-based tests for quality preset configuration.

**Property 4: QVBR Preset Configuration Accuracy**
**Validates: Requirements 3.3, 3.4, 3.5**

For any quality preset specified for a conversion job, the MediaConvert QVBR
quality level is set as follows:
- "high": 8-9
- "balanced": 6-7
- "compression": 4-5
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from vco.converter.mediaconvert import (
    QUALITY_PRESETS,
    QualityPreset,
    get_quality_preset,
)


class TestQualityPresetConfiguration:
    """Test quality preset QVBR configuration accuracy."""

    # Property 4: QVBR Preset Configuration Accuracy
    # **Validates: Requirements 3.3, 3.4, 3.5**

    def test_high_preset_qvbr_level_in_range(self):
        """High preset should have QVBR level 8-9."""
        preset = QUALITY_PRESETS["high"]
        assert 8 <= preset.qvbr_quality_level <= 9, (
            f"High preset QVBR level {preset.qvbr_quality_level} not in range 8-9"
        )

    def test_balanced_preset_qvbr_level_in_range(self):
        """Balanced preset should have QVBR level 6-7."""
        preset = QUALITY_PRESETS["balanced"]
        assert 6 <= preset.qvbr_quality_level <= 7, (
            f"Balanced preset QVBR level {preset.qvbr_quality_level} not in range 6-7"
        )

    def test_compression_preset_qvbr_level_in_range(self):
        """Compression preset should have QVBR level 4-5."""
        preset = QUALITY_PRESETS["compression"]
        assert 4 <= preset.qvbr_quality_level <= 5, (
            f"Compression preset QVBR level {preset.qvbr_quality_level} not in range 4-5"
        )

    @given(preset_name=st.sampled_from(["high", "balanced", "compression"]))
    @settings(max_examples=100)
    def test_all_presets_have_valid_qvbr_levels(self, preset_name: str):
        """For any valid preset name, QVBR level should be in valid range (1-10)."""
        preset = get_quality_preset(preset_name)
        assert 1 <= preset.qvbr_quality_level <= 10, (
            f"Preset {preset_name} has invalid QVBR level: {preset.qvbr_quality_level}"
        )

    @given(preset_name=st.sampled_from(["high", "balanced", "compression"]))
    @settings(max_examples=100)
    def test_presets_have_positive_max_bitrate(self, preset_name: str):
        """For any valid preset, max bitrate should be positive."""
        preset = get_quality_preset(preset_name)
        assert preset.qvbr_max_bitrate > 0, (
            f"Preset {preset_name} has non-positive max bitrate: {preset.qvbr_max_bitrate}"
        )

    @given(preset_name=st.sampled_from(["high", "balanced", "compression"]))
    @settings(max_examples=100)
    def test_presets_have_description(self, preset_name: str):
        """For any valid preset, description should not be empty."""
        preset = get_quality_preset(preset_name)
        assert preset.description, f"Preset {preset_name} has empty description"

    def test_preset_quality_ordering(self):
        """Higher quality presets should have higher QVBR levels."""
        high = QUALITY_PRESETS["high"]
        balanced = QUALITY_PRESETS["balanced"]
        compression = QUALITY_PRESETS["compression"]

        assert high.qvbr_quality_level > balanced.qvbr_quality_level, (
            "High preset should have higher QVBR level than balanced"
        )
        assert balanced.qvbr_quality_level > compression.qvbr_quality_level, (
            "Balanced preset should have higher QVBR level than compression"
        )

    def test_preset_bitrate_ordering(self):
        """Higher quality presets should have higher max bitrate."""
        high = QUALITY_PRESETS["high"]
        balanced = QUALITY_PRESETS["balanced"]
        compression = QUALITY_PRESETS["compression"]

        assert high.qvbr_max_bitrate > balanced.qvbr_max_bitrate, (
            "High preset should have higher max bitrate than balanced"
        )
        assert balanced.qvbr_max_bitrate > compression.qvbr_max_bitrate, (
            "Balanced preset should have higher max bitrate than compression"
        )

    def test_get_quality_preset_returns_correct_preset(self):
        """get_quality_preset should return the correct preset."""
        for name in ["high", "balanced", "compression"]:
            preset = get_quality_preset(name)
            assert preset.name == name
            # Compare by value, not identity
            expected = QUALITY_PRESETS[name]
            assert preset.name == expected.name
            assert preset.qvbr_quality_level == expected.qvbr_quality_level
            assert preset.qvbr_max_bitrate == expected.qvbr_max_bitrate

    def test_get_quality_preset_invalid_name_raises_error(self):
        """get_quality_preset should raise ValueError for invalid names."""
        with pytest.raises(ValueError) as exc_info:
            get_quality_preset("invalid")
        # Check for either error message format
        error_msg = str(exc_info.value)
        assert "invalid" in error_msg.lower() or "Unknown preset" in error_msg

    @given(
        invalid_name=st.text(min_size=1).filter(
            lambda x: x not in ["high", "balanced", "compression"]
        )
    )
    @settings(max_examples=50)
    def test_invalid_preset_names_raise_error(self, invalid_name: str):
        """For any invalid preset name, get_quality_preset should raise ValueError."""
        with pytest.raises(ValueError):
            get_quality_preset(invalid_name)


class TestQualityPresetDataclass:
    """Test QualityPreset dataclass properties."""

    def test_preset_is_dataclass(self):
        """QualityPreset should be a proper dataclass."""
        preset = QualityPreset(
            name="test",
            qvbr_quality_level=7,
            qvbr_max_bitrate=20_000_000,
            description="Test preset",
        )
        assert preset.name == "test"
        assert preset.qvbr_quality_level == 7
        assert preset.qvbr_max_bitrate == 20_000_000
        assert preset.description == "Test preset"

    @given(
        name=st.text(min_size=1, max_size=20),
        qvbr_level=st.integers(min_value=1, max_value=10),
        max_bitrate=st.integers(min_value=1_000_000, max_value=100_000_000),
        description=st.text(min_size=1, max_size=100),
    )
    @settings(max_examples=100)
    def test_preset_creation_with_valid_values(
        self, name: str, qvbr_level: int, max_bitrate: int, description: str
    ):
        """For any valid values, QualityPreset should be created successfully."""
        preset = QualityPreset(
            name=name,
            qvbr_quality_level=qvbr_level,
            qvbr_max_bitrate=max_bitrate,
            description=description,
        )
        assert preset.name == name
        assert preset.qvbr_quality_level == qvbr_level
        assert preset.qvbr_max_bitrate == max_bitrate
        assert preset.description == description
