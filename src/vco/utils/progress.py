"""Progress calculation utility.

Centralizes progress calculation logic for consistency
between CLI and Lambda functions.

Requirements: 6.1, 6.2, 6.5
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

# Progress mapping constants
PROGRESS_PENDING = 0
PROGRESS_CONVERTING_MIDPOINT = 15  # Midpoint of 0-30% range
PROGRESS_CONVERTING_MAX = 30
PROGRESS_VERIFYING = 65
PROGRESS_COMPLETED = 100


def calculate_progress(
    files: list[dict[str, Any]],
    get_mediaconvert_progress: Callable[[str], int] | None = None,
) -> tuple[int, str]:
    """Calculate progress percentage and current step from file statuses.

    Progress is calculated dynamically at query time (not stored in DynamoDB).

    File status to progress mapping:
    - PENDING: 0%
    - CONVERTING: 0-30% (scaled from MediaConvert jobPercentComplete)
    - VERIFYING: 65% (fixed, SSIM calculation has no progress API)
    - COMPLETED/FAILED: 100%

    Task progress is the average of all file progress percentages.

    Args:
        files: List of file dictionaries with 'status' and optionally 'mediaconvert_job_id'
        get_mediaconvert_progress: Optional callback to get MediaConvert job progress.
            If None, uses midpoint value (15%) for CONVERTING status.

    Returns:
        Tuple of (progress_percentage, current_step)

    Requirements: 6.1, 6.2, 6.5
    """
    if not files:
        return 0, "pending"

    total_progress = 0
    current_step = "pending"

    for f in files:
        status = f.get("status", "PENDING")

        if status == "PENDING":
            # 0%
            pass
        elif status == "CONVERTING":
            # 0-30% range
            if get_mediaconvert_progress:
                job_id = f.get("mediaconvert_job_id")
                if job_id:
                    mc_progress = get_mediaconvert_progress(job_id)
                    # Scale 0-100% to 0-30%
                    total_progress += int(mc_progress * 0.3)
                # else: 0%
            else:
                # Use midpoint when no callback provided
                total_progress += PROGRESS_CONVERTING_MIDPOINT
            current_step = "converting"
        elif status == "VERIFYING":
            # 65% fixed (SSIM calculation has no progress API)
            total_progress += PROGRESS_VERIFYING
            current_step = "verifying"
        elif status in ("COMPLETED", "FAILED"):
            # 100%
            total_progress += PROGRESS_COMPLETED

    # Calculate average progress
    progress = int(total_progress / len(files))

    # Determine current step (most advanced active state)
    completed_count = sum(1 for f in files if f.get("status") in ("COMPLETED", "FAILED"))
    if completed_count == len(files):
        current_step = "completed"

    return progress, current_step


def calculate_progress_simple(files: list[dict[str, Any]]) -> tuple[int, str]:
    """Calculate progress percentage without MediaConvert API calls.

    Used for list view where we don't want to make API calls for each task.
    Uses fixed values for CONVERTING status instead of querying MediaConvert.

    File status to progress mapping:
    - PENDING: 0%
    - CONVERTING: 15% (midpoint of 0-30% range)
    - VERIFYING: 65% (fixed)
    - COMPLETED/FAILED: 100%

    Args:
        files: List of file dictionaries with 'status'

    Returns:
        Tuple of (progress_percentage, current_step)

    Requirements: 6.1, 6.2
    """
    return calculate_progress(files, get_mediaconvert_progress=None)
