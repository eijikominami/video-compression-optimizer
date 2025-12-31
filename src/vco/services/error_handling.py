"""Error handling logic for async workflow.

Provides error classification and retry decision logic for:
- MediaConvert errors (transient vs config/input errors)
- SSIM quality failures (adaptive vs non-adaptive presets)

Requirements: 5.2, 5.3, 5.4
"""

from dataclasses import dataclass
from enum import Enum


class ErrorCategory(Enum):
    """Error category classification."""

    TRANSIENT = "transient"
    CONFIG_OR_INPUT = "config_or_input"
    PERMISSION = "permission"
    UNKNOWN = "unknown"


class SSIMAction(Enum):
    """Action to take for SSIM result."""

    ACCEPT = "accept"
    RETRY_WITH_HIGHER_PRESET = "retry_with_higher_preset"
    FAIL = "fail"


@dataclass
class MediaConvertErrorResult:
    """Result of MediaConvert error classification."""

    error_code: int
    is_retryable: bool
    category: str


@dataclass
class SSIMActionResult:
    """Result of SSIM action determination."""

    action: str
    next_preset: str | None = None


# MediaConvert error codes
# Transient errors: temporary issues that may resolve on retry
TRANSIENT_ERRORS = {1517, 1522, 1550, 1999}

# Config/input errors: issues with job configuration or input files
CONFIG_ERRORS = {1010, 1030, 1040}

# Permission errors: IAM or access issues
PERMISSION_ERRORS = {1401, 1432, 1433}

# Preset chain for adaptive quality
PRESET_CHAIN = ["balanced", "high"]


def classify_mediaconvert_error(error_code: int) -> MediaConvertErrorResult:
    """Classify MediaConvert error code.

    Args:
        error_code: MediaConvert error code (1000-2000 range)

    Returns:
        MediaConvertErrorResult with classification

    Error categories:
    - transient (1517, 1522, 1550, 1999): Temporary issues, can retry
    - config_or_input (1010, 1030, 1040): Job config or input issues, no retry
    - permission (1401, 1432, 1433): IAM/access issues, no retry
    - unknown: Unrecognized error code, no retry (safe default)

    Requirements: 5.2
    """
    if error_code in TRANSIENT_ERRORS:
        return MediaConvertErrorResult(
            error_code=error_code,
            is_retryable=True,
            category=ErrorCategory.TRANSIENT.value,
        )

    if error_code in CONFIG_ERRORS:
        return MediaConvertErrorResult(
            error_code=error_code,
            is_retryable=False,
            category=ErrorCategory.CONFIG_OR_INPUT.value,
        )

    if error_code in PERMISSION_ERRORS:
        return MediaConvertErrorResult(
            error_code=error_code,
            is_retryable=False,
            category=ErrorCategory.PERMISSION.value,
        )

    # Unknown error: default to non-retryable for safety
    return MediaConvertErrorResult(
        error_code=error_code,
        is_retryable=False,
        category=ErrorCategory.UNKNOWN.value,
    )


def determine_ssim_action(
    preset: str,
    ssim_score: float,
    threshold: float = 0.95,
) -> SSIMActionResult:
    """Determine action based on SSIM score and preset type.

    Args:
        preset: Quality preset (e.g., "balanced", "high", "balanced+")
        ssim_score: SSIM score (0.0 to 1.0)
        threshold: SSIM threshold for acceptance (default 0.95)

    Returns:
        SSIMActionResult with action and optional next preset

    Behavior:
    - If SSIM >= threshold: accept
    - If SSIM < threshold and adaptive preset (ends with +):
      - Try next preset in chain if available
      - Fail if no more presets
    - If SSIM < threshold and non-adaptive: fail immediately

    Requirements: 5.3, 5.4
    """
    # Accept if SSIM meets threshold
    if ssim_score >= threshold:
        return SSIMActionResult(action=SSIMAction.ACCEPT.value)

    # Check if adaptive preset
    is_adaptive = preset.endswith("+")

    if not is_adaptive:
        # Non-adaptive: fail immediately
        return SSIMActionResult(action=SSIMAction.FAIL.value)

    # Adaptive: try next preset in chain
    base_preset = preset.rstrip("+")

    try:
        current_index = PRESET_CHAIN.index(base_preset)
    except ValueError:
        # Unknown preset, treat as first in chain
        current_index = 0

    if current_index < len(PRESET_CHAIN) - 1:
        next_preset = PRESET_CHAIN[current_index + 1]  # high (without +)
        return SSIMActionResult(
            action=SSIMAction.RETRY_WITH_HIGHER_PRESET.value,
            next_preset=next_preset,
        )

    # No more presets to try
    return SSIMActionResult(action=SSIMAction.FAIL.value)


def is_adaptive_preset(preset: str) -> bool:
    """Check if preset is adaptive (ends with +).

    Args:
        preset: Quality preset name

    Returns:
        True if adaptive, False otherwise
    """
    return preset.endswith("+")


def get_next_preset(current_preset: str) -> str | None:
    """Get next preset in the chain for adaptive retry.

    Args:
        current_preset: Current preset (with or without +)

    Returns:
        Next preset (without + suffix), or None if at end of chain.
        balanced+ -> high (not high+)
    """
    base_preset = current_preset.rstrip("+")

    try:
        current_index = PRESET_CHAIN.index(base_preset)
    except ValueError:
        return None

    if current_index < len(PRESET_CHAIN) - 1:
        return PRESET_CHAIN[current_index + 1]  # high (without +)

    return None
