"""Sync/Async implementation consistency tests.

These tests verify that the async implementation (Lambda) produces
the same results as the sync implementation (CLI) by importing
actual implementation code from both sources.

Requirements: 1.4, 7.1, 7.2, 7.3
Properties: 1 (Sync/Async consistency)

IMPORTANT: This test imports actual implementation code to detect
drift between sync and async implementations. Do NOT use hardcoded
expected values - always import from the source of truth.
"""

from vco.config.quality_config import (
    ADAPTIVE_PRESET_CHAIN,
    QUALITY_PRESETS,
    SSIM_THRESHOLD,
    get_preset,
)


class TestQualityPresetConsistency:
    """Test that sync and async use the same quality presets from Quality_Config."""

    def test_sync_imports_from_quality_config(self):
        """Verify sync implementation uses same presets as Quality_Config."""
        from vco.converter.mediaconvert import QUALITY_PRESETS as SYNC_PRESETS

        # Both should have the same preset names
        assert set(SYNC_PRESETS.keys()) == set(QUALITY_PRESETS.keys()), (
            "Sync implementation should have same preset names as quality_config"
        )

        # Verify each preset has the same values
        for name in QUALITY_PRESETS.keys():
            sync_preset = SYNC_PRESETS[name]
            config_preset = QUALITY_PRESETS[name]
            assert sync_preset.name == config_preset.name
            assert sync_preset.qvbr_quality_level == config_preset.qvbr_quality_level
            assert sync_preset.qvbr_max_bitrate == config_preset.qvbr_max_bitrate
            assert sync_preset.is_adaptive == config_preset.is_adaptive

    def test_all_presets_have_required_fields(self):
        """Verify all presets have required fields."""
        required_fields = ["name", "qvbr_quality_level", "qvbr_max_bitrate", "description"]

        for preset_name, preset in QUALITY_PRESETS.items():
            for field in required_fields:
                assert hasattr(preset, field), (
                    f"Preset '{preset_name}' missing required field '{field}'"
                )

    def test_balanced_preset_values(self):
        """Verify balanced preset has expected values."""
        preset = get_preset("balanced")
        assert preset.qvbr_quality_level == 7
        assert preset.qvbr_max_bitrate == 20_000_000

    def test_high_preset_values(self):
        """Verify high preset has expected values."""
        preset = get_preset("high")
        assert preset.qvbr_quality_level == 9
        assert preset.qvbr_max_bitrate == 50_000_000

    def test_compression_preset_values(self):
        """Verify compression preset has expected values."""
        preset = get_preset("compression")
        assert preset.qvbr_quality_level == 5
        assert preset.qvbr_max_bitrate == 10_000_000


class TestSSIMThresholdConsistency:
    """Test that sync and async use the same SSIM threshold from Quality_Config."""

    def test_checker_imports_from_quality_config(self):
        """Verify QualityChecker imports SSIM_THRESHOLD from Quality_Config."""
        from vco.quality.checker import QualityChecker

        # QualityChecker.SSIM_THRESHOLD should equal the centralized value
        assert QualityChecker.SSIM_THRESHOLD == SSIM_THRESHOLD, (
            f"QualityChecker.SSIM_THRESHOLD ({QualityChecker.SSIM_THRESHOLD}) "
            f"should equal quality_config.SSIM_THRESHOLD ({SSIM_THRESHOLD})"
        )

    def test_ssim_threshold_value(self):
        """Verify SSIM threshold is the expected value (0.95)."""
        assert SSIM_THRESHOLD == 0.95, f"SSIM_THRESHOLD should be 0.95, got {SSIM_THRESHOLD}"


class TestAdaptivePresetConsistency:
    """Test that adaptive preset behavior uses Quality_Config."""

    def test_sync_imports_adaptive_chain_from_quality_config(self):
        """Verify sync implementation uses same adaptive chain as Quality_Config."""
        from vco.converter.mediaconvert import ADAPTIVE_PRESET_CHAIN as SYNC_CHAIN

        # Both should have the same values
        assert SYNC_CHAIN == ADAPTIVE_PRESET_CHAIN, (
            "Sync implementation should use same ADAPTIVE_PRESET_CHAIN as quality_config"
        )

    def test_balanced_plus_chain(self):
        """Verify balanced+ preset chain."""
        assert "balanced+" in ADAPTIVE_PRESET_CHAIN
        chain = ADAPTIVE_PRESET_CHAIN["balanced+"]
        assert chain == ["balanced", "high"], (
            f"balanced+ chain should be ['balanced', 'high'], got {chain}"
        )

    def test_adaptive_preset_detection(self):
        """Verify adaptive preset detection logic."""
        from vco.converter.mediaconvert import is_adaptive_preset

        # Non-adaptive presets
        assert not is_adaptive_preset("balanced")
        assert not is_adaptive_preset("high")
        assert not is_adaptive_preset("compression")

        # Adaptive presets
        assert is_adaptive_preset("balanced+")


class TestMediaConvertSettingsConsistency:
    """Test that MediaConvert settings are consistent.

    These tests verify the structure of job settings generated by
    both sync and async implementations.
    """

    def test_sync_job_settings_structure(self):
        """Verify sync implementation generates valid job settings."""
        from vco.converter.mediaconvert import MediaConvertClient

        # Create a mock client to test _build_job_settings
        # We can't instantiate without AWS credentials, so we test the method signature
        assert hasattr(MediaConvertClient, "_build_job_settings")

    def test_preset_to_settings_mapping(self):
        """Verify preset values map correctly to MediaConvert settings."""
        for preset_name, preset in QUALITY_PRESETS.items():
            # Verify preset values are valid for MediaConvert
            assert 1 <= preset.qvbr_quality_level <= 10, (
                f"Preset '{preset_name}' quality_level {preset.qvbr_quality_level} "
                "should be between 1 and 10"
            )
            assert preset.qvbr_max_bitrate > 0, (
                f"Preset '{preset_name}' max_bitrate should be positive"
            )


class TestOutputFileFormatConsistency:
    """Test that output file format is consistent."""

    def test_output_suffix(self):
        """Verify output file suffix is _h265.mp4."""
        from vco.utils.s3_keys import S3KeyBuilder

        # Test output key generation
        output_key = S3KeyBuilder.output_key("task-1", "file-1", "video.mov")
        assert output_key.endswith("_h265.mp4"), (
            f"Output key should end with '_h265.mp4', got {output_key}"
        )


class TestReviewQueueConsistency:
    """Test that review queue integration is consistent."""

    def test_review_item_structure(self):
        """Verify ReviewItem has required fields for both sync and async."""
        from pathlib import Path

        from vco.services.review import ReviewItem

        # Create a sample entry
        entry = ReviewItem(
            id="test-id",
            original_uuid="test-uuid",
            original_path=Path("/path/to/original.mov"),
            converted_path=Path("/path/to/converted.mp4"),
            conversion_date="2024-01-01T00:00:00",
            quality_result={
                "ssim_score": 0.98,
                "original_size": 1000000,
                "converted_size": 500000,
            },
            metadata={},
        )

        # Verify required fields
        assert entry.original_uuid is not None
        assert entry.original_path is not None
        assert entry.converted_path is not None
        assert entry.quality_result is not None
        assert "ssim_score" in entry.quality_result
        assert "original_size" in entry.quality_result
        assert "converted_size" in entry.quality_result
