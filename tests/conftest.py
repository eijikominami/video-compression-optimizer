"""Pytest configuration and fixtures for Video Compression Optimizer tests."""

import sys
from datetime import datetime
from pathlib import Path

import pytest

from vco.models.types import VideoInfo


@pytest.fixture
def sample_video_info():
    """Create a sample VideoInfo for testing."""
    return VideoInfo(
        uuid="ABC123-DEF456-GHI789",
        filename="test_video.mov",
        path=Path("/tmp/test_video.mov"),
        codec="h264",
        resolution=(1920, 1080),
        bitrate=25000000,
        duration=120.5,
        frame_rate=30.0,
        file_size=375000000,
        capture_date=datetime(2020, 7, 15, 14, 30, 0),
        creation_date=datetime(2020, 7, 15, 14, 30, 0),
        albums=["Vacation 2020", "Family"],
        is_in_icloud=False,
        is_local=True,
    )


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for tests."""
    return tmp_path


@pytest.fixture(autouse=True)
def clean_cli_modules():
    """Clean up CLI modules before and after each test to prevent state pollution.

    This fixture runs automatically before and after each test and removes any
    cached CLI modules from sys.modules to ensure clean state for
    subsequent tests that use mocking.
    """
    # Clean up CLI modules before test
    modules_to_remove = [mod for mod in list(sys.modules.keys()) if mod.startswith("vco.cli")]
    for mod in modules_to_remove:
        sys.modules.pop(mod, None)

    yield

    # Clean up CLI modules after test
    modules_to_remove = [mod for mod in list(sys.modules.keys()) if mod.startswith("vco.cli")]
    for mod in modules_to_remove:
        sys.modules.pop(mod, None)
