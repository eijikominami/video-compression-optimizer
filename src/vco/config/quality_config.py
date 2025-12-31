"""Quality configuration for video conversion.

This module defines all quality-related settings used by both
sync and async conversion modes. It serves as the single source
of truth for quality presets, SSIM thresholds, and MediaConvert
job settings.

Requirements: 1.1, 1.2, 1.3, 1.6
"""

from dataclasses import dataclass
from typing import Literal

# SSIM threshold for quality acceptance (Requirement 1.2)
SSIM_THRESHOLD: float = 0.95

# Quality preset names
PresetName = Literal["balanced", "high", "compression", "balanced+", "high+"]


@dataclass(frozen=True)
class QualityPreset:
    """Quality preset configuration.

    Attributes:
        name: Preset identifier
        qvbr_max_bitrate: Maximum bitrate in bits per second (MediaConvert QVBR)
        qvbr_quality_level: QVBR quality level (1-10)
        description: Human-readable description
        is_adaptive: Whether this preset supports adaptive quality fallback
    """

    name: str
    qvbr_max_bitrate: int
    qvbr_quality_level: int
    description: str = ""
    is_adaptive: bool = False


# Quality presets definition (Requirement 1.1)
QUALITY_PRESETS: dict[str, QualityPreset] = {
    "balanced": QualityPreset(
        name="balanced",
        qvbr_max_bitrate=20_000_000,
        qvbr_quality_level=7,
        description="Balanced (QVBR 6-7)",
    ),
    "high": QualityPreset(
        name="high",
        qvbr_max_bitrate=50_000_000,
        qvbr_quality_level=9,
        description="High quality (QVBR 8-9)",
    ),
    "compression": QualityPreset(
        name="compression",
        qvbr_max_bitrate=10_000_000,
        qvbr_quality_level=5,
        description="High compression (QVBR 4-5)",
    ),
    "balanced+": QualityPreset(
        name="balanced+",
        qvbr_max_bitrate=20_000_000,
        qvbr_quality_level=7,
        description="Balanced with adaptive fallback",
        is_adaptive=True,
    ),
    "high+": QualityPreset(
        name="high+",
        qvbr_max_bitrate=50_000_000,
        qvbr_quality_level=9,
        description="High quality with adaptive fallback",
        is_adaptive=True,
    ),
}

# Adaptive preset chain - fallback order when SSIM threshold not met
# Maps adaptive preset name to list of presets to try
ADAPTIVE_PRESET_CHAIN: dict[str, list[str]] = {
    "balanced+": ["balanced", "high"],
}


def get_preset(name: str) -> QualityPreset:
    """Get quality preset by name.

    Args:
        name: Preset name (balanced, high, compression, balanced+, high+)

    Returns:
        QualityPreset configuration

    Raises:
        ValueError: If preset name is unknown
    """
    if name not in QUALITY_PRESETS:
        raise ValueError(f"Unknown preset: {name}. Valid presets: {list(QUALITY_PRESETS.keys())}")
    return QUALITY_PRESETS[name]


def get_mediaconvert_settings(preset: QualityPreset) -> dict:
    """Get MediaConvert job settings for a preset.

    Args:
        preset: QualityPreset configuration

    Returns:
        Dictionary with max_bitrate and quality_level for MediaConvert
    """
    return {
        "max_bitrate": preset.qvbr_max_bitrate,
        "quality_level": preset.qvbr_quality_level,
    }


def is_adaptive_preset(name: str) -> bool:
    """Check if a preset name is adaptive.

    Args:
        name: Preset name

    Returns:
        True if preset is in ADAPTIVE_PRESET_CHAIN
    """
    return name in ADAPTIVE_PRESET_CHAIN


def get_base_preset_name(name: str) -> str:
    """Get base preset name without adaptive suffix.

    Args:
        name: Preset name (e.g., "balanced+" or "balanced")

    Returns:
        Base preset name (e.g., "balanced")
    """
    return name.rstrip("+")
