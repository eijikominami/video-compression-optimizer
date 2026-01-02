"""Compatibility tests between Swift and Python Photos implementations.

These tests compare the output of SwiftBridge and PhotosAccessManager
to ensure they produce consistent results.

Note: Integration tests require actual Photos library access and cannot run in CI.
Run manually with: python3.11 -m pytest tests/integration/test_swift_python_compatibility.py -v -m "not skip"
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest


@dataclass
class FieldComparison:
    """Result of comparing a single field between implementations."""

    field_name: str
    swift_value: object
    python_value: object
    matches: bool
    tolerance_used: str | None = None


@dataclass
class VideoComparison:
    """Result of comparing a video between implementations."""

    uuid: str
    filename: str
    all_match: bool
    field_comparisons: list[FieldComparison]


class CompatibilityChecker:
    """Compares VideoInfo objects from Swift and Python implementations."""

    # Fields that must match exactly
    EXACT_MATCH_FIELDS = [
        "uuid",
        "filename",
        "codec",
        "resolution",
        "file_size",
        "albums",
        "is_in_icloud",
        "is_local",
    ]

    # Fields with allowed tolerance
    TOLERANCE_FIELDS = {
        "bitrate": 0.01,  # 1% tolerance
        "frame_rate": 0.01,  # 1% tolerance
        "duration": 0.001,  # 0.1% tolerance
    }

    # Date fields with 1 second tolerance
    DATE_FIELDS = ["capture_date", "creation_date"]

    def compare_videos(self, swift_video, python_video) -> VideoComparison:
        """Compare two VideoInfo objects from different implementations.

        Args:
            swift_video: VideoInfo from SwiftBridge
            python_video: VideoInfo from PhotosAccessManager

        Returns:
            VideoComparison with detailed field-by-field results
        """
        comparisons = []

        # Check exact match fields
        for field in self.EXACT_MATCH_FIELDS:
            swift_val = getattr(swift_video, field, None)
            python_val = getattr(python_video, field, None)
            matches = swift_val == python_val
            comparisons.append(
                FieldComparison(
                    field_name=field,
                    swift_value=swift_val,
                    python_value=python_val,
                    matches=matches,
                )
            )

        # Check tolerance fields
        for field, tolerance in self.TOLERANCE_FIELDS.items():
            swift_val = getattr(swift_video, field, 0)
            python_val = getattr(python_video, field, 0)

            if python_val == 0:
                matches = swift_val == 0
            else:
                diff = abs(swift_val - python_val) / python_val
                matches = diff <= tolerance

            comparisons.append(
                FieldComparison(
                    field_name=field,
                    swift_value=swift_val,
                    python_value=python_val,
                    matches=matches,
                    tolerance_used=f"±{tolerance * 100}%",
                )
            )

        # Check date fields
        for field in self.DATE_FIELDS:
            swift_val = getattr(swift_video, field, None)
            python_val = getattr(python_video, field, None)

            if swift_val is None and python_val is None:
                matches = True
            elif swift_val is None or python_val is None:
                matches = False
            else:
                # Remove timezone info for comparison
                swift_naive = swift_val.replace(tzinfo=None) if swift_val.tzinfo else swift_val
                python_naive = python_val.replace(tzinfo=None) if python_val.tzinfo else python_val
                diff = abs((swift_naive - python_naive).total_seconds())
                matches = diff <= 1.0  # 1 second tolerance

            comparisons.append(
                FieldComparison(
                    field_name=field,
                    swift_value=swift_val,
                    python_value=python_val,
                    matches=matches,
                    tolerance_used="±1 second",
                )
            )

        all_match = all(c.matches for c in comparisons)

        return VideoComparison(
            uuid=swift_video.uuid,
            filename=swift_video.filename,
            all_match=all_match,
            field_comparisons=comparisons,
        )

    def generate_report(self, comparisons: list[VideoComparison]) -> str:
        """Generate a human-readable report of comparison results.

        Args:
            comparisons: List of VideoComparison results

        Returns:
            Formatted report string
        """
        lines = []
        lines.append("=" * 60)
        lines.append("Swift vs Python Compatibility Report")
        lines.append("=" * 60)

        total = len(comparisons)
        matching = sum(1 for c in comparisons if c.all_match)
        mismatched = total - matching

        lines.append(f"\nTotal videos compared: {total}")
        lines.append(f"Fully matching: {matching}")
        lines.append(f"With differences: {mismatched}")

        if mismatched > 0:
            lines.append("\n" + "-" * 60)
            lines.append("MISMATCHED VIDEOS:")
            lines.append("-" * 60)

            for comp in comparisons:
                if not comp.all_match:
                    lines.append(f"\n{comp.filename} ({comp.uuid})")
                    for fc in comp.field_comparisons:
                        if not fc.matches:
                            tolerance_info = f" [{fc.tolerance_used}]" if fc.tolerance_used else ""
                            lines.append(f"  {fc.field_name}{tolerance_info}:")
                            lines.append(f"    Swift:  {fc.swift_value}")
                            lines.append(f"    Python: {fc.python_value}")

        lines.append("\n" + "=" * 60)
        return "\n".join(lines)


class TestSwiftPythonCompatibility:
    """Integration tests for Swift/Python compatibility.

    These tests require actual Photos library access and should be run manually.
    """

    pytestmark = pytest.mark.skip(reason="Requires actual Photos library access - run manually")

    @pytest.fixture
    def swift_bridge(self):
        """Create SwiftBridge instance."""
        from vco.photos.swift_bridge import SwiftBridge

        return SwiftBridge()

    @pytest.fixture
    def photos_manager(self):
        """Create PhotosAccessManager instance."""
        from vco.photos.manager import PhotosAccessManager

        return PhotosAccessManager()

    @pytest.fixture
    def checker(self):
        """Create CompatibilityChecker instance."""
        return CompatibilityChecker()

    def test_uuid_set_comparison(self, swift_bridge, photos_manager):
        """Test that both implementations return the same set of video UUIDs."""
        swift_videos = swift_bridge.get_all_videos()
        python_videos = photos_manager.get_all_videos()

        swift_uuids = {v.uuid for v in swift_videos}
        python_uuids = {v.uuid for v in python_videos}

        # Check for missing videos
        only_in_swift = swift_uuids - python_uuids
        only_in_python = python_uuids - swift_uuids

        assert not only_in_swift, f"Videos only in Swift: {only_in_swift}"
        assert not only_in_python, f"Videos only in Python: {only_in_python}"
        assert swift_uuids == python_uuids

    def test_video_count_match(self, swift_bridge, photos_manager):
        """Test that both implementations return the same number of videos."""
        swift_count = len(swift_bridge.get_all_videos())
        python_count = len(photos_manager.get_all_videos())

        assert swift_count == python_count, (
            f"Video count mismatch: Swift={swift_count}, Python={python_count}"
        )

    def test_field_compatibility(self, swift_bridge, photos_manager, checker):
        """Test that video fields match between implementations."""
        swift_videos = swift_bridge.get_all_videos()
        python_videos = photos_manager.get_all_videos()

        # Create lookup by UUID
        python_by_uuid = {v.uuid: v for v in python_videos}

        comparisons = []
        for swift_video in swift_videos:
            python_video = python_by_uuid.get(swift_video.uuid)
            if python_video:
                comparison = checker.compare_videos(swift_video, python_video)
                comparisons.append(comparison)

        # Generate and print report
        report = checker.generate_report(comparisons)
        print(report)

        # Assert all videos match
        mismatched = [c for c in comparisons if not c.all_match]
        assert not mismatched, f"{len(mismatched)} videos have field mismatches"

    def test_date_range_filter_compatibility(self, swift_bridge, photos_manager):
        """Test that date range filtering produces consistent results."""
        # Use a date range that should include some videos
        from_date = datetime(2024, 1, 1)
        to_date = datetime(2024, 12, 31)

        swift_videos = swift_bridge.get_videos_by_date_range(from_date, to_date)
        python_videos = photos_manager.get_videos_by_date_range(from_date, to_date)

        swift_uuids = {v.uuid for v in swift_videos}
        python_uuids = {v.uuid for v in python_videos}

        assert swift_uuids == python_uuids, (
            f"Date range filter mismatch. "
            f"Only in Swift: {swift_uuids - python_uuids}, "
            f"Only in Python: {python_uuids - swift_uuids}"
        )


class TestCompatibilityCheckerUnit:
    """Unit tests for CompatibilityChecker (can run without Photos access)."""

    @pytest.fixture
    def checker(self):
        """Create CompatibilityChecker instance."""
        return CompatibilityChecker()

    def test_exact_match_comparison(self, checker):
        """Test exact match field comparison."""
        from pathlib import Path

        from vco.models.types import VideoInfo

        video1 = VideoInfo(
            uuid="test-uuid",
            filename="test.mov",
            path=Path("/test"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=datetime(2024, 1, 1, 12, 0, 0),
            creation_date=datetime(2024, 1, 1, 12, 0, 0),
            albums=["Album1"],
            is_in_icloud=False,
            is_local=True,
        )

        video2 = VideoInfo(
            uuid="test-uuid",
            filename="test.mov",
            path=Path("/test"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=datetime(2024, 1, 1, 12, 0, 0),
            creation_date=datetime(2024, 1, 1, 12, 0, 0),
            albums=["Album1"],
            is_in_icloud=False,
            is_local=True,
        )

        result = checker.compare_videos(video1, video2)
        assert result.all_match

    def test_tolerance_comparison(self, checker):
        """Test tolerance field comparison."""
        from pathlib import Path

        from vco.models.types import VideoInfo

        video1 = VideoInfo(
            uuid="test-uuid",
            filename="test.mov",
            path=Path("/test"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=datetime(2024, 1, 1, 12, 0, 0),
            creation_date=datetime(2024, 1, 1, 12, 0, 0),
            albums=[],
            is_in_icloud=False,
            is_local=True,
        )

        # Bitrate within 1% tolerance
        video2 = VideoInfo(
            uuid="test-uuid",
            filename="test.mov",
            path=Path("/test"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=10050000,  # 0.5% difference
            duration=60.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=datetime(2024, 1, 1, 12, 0, 0),
            creation_date=datetime(2024, 1, 1, 12, 0, 0),
            albums=[],
            is_in_icloud=False,
            is_local=True,
        )

        result = checker.compare_videos(video1, video2)
        assert result.all_match

    def test_date_tolerance_comparison(self, checker):
        """Test date field comparison with tolerance."""
        from pathlib import Path

        from vco.models.types import VideoInfo

        video1 = VideoInfo(
            uuid="test-uuid",
            filename="test.mov",
            path=Path("/test"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=datetime(2024, 1, 1, 12, 0, 0),
            creation_date=datetime(2024, 1, 1, 12, 0, 0),
            albums=[],
            is_in_icloud=False,
            is_local=True,
        )

        # Date within 1 second tolerance
        video2 = VideoInfo(
            uuid="test-uuid",
            filename="test.mov",
            path=Path("/test"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=datetime(2024, 1, 1, 12, 0, 0) + timedelta(milliseconds=500),
            creation_date=datetime(2024, 1, 1, 12, 0, 0),
            albums=[],
            is_in_icloud=False,
            is_local=True,
        )

        result = checker.compare_videos(video1, video2)
        assert result.all_match

    def test_mismatch_detection(self, checker):
        """Test that mismatches are properly detected."""
        from pathlib import Path

        from vco.models.types import VideoInfo

        video1 = VideoInfo(
            uuid="test-uuid",
            filename="test.mov",
            path=Path("/test"),
            codec="hevc",
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=datetime(2024, 1, 1, 12, 0, 0),
            creation_date=datetime(2024, 1, 1, 12, 0, 0),
            albums=[],
            is_in_icloud=False,
            is_local=True,
        )

        # Different codec
        video2 = VideoInfo(
            uuid="test-uuid",
            filename="test.mov",
            path=Path("/test"),
            codec="h264",  # Different!
            resolution=(1920, 1080),
            bitrate=10000000,
            duration=60.0,
            frame_rate=30.0,
            file_size=100000000,
            capture_date=datetime(2024, 1, 1, 12, 0, 0),
            creation_date=datetime(2024, 1, 1, 12, 0, 0),
            albums=[],
            is_in_icloud=False,
            is_local=True,
        )

        result = checker.compare_videos(video1, video2)
        assert not result.all_match

        # Find the codec comparison
        codec_comp = next(c for c in result.field_comparisons if c.field_name == "codec")
        assert not codec_comp.matches
        assert codec_comp.swift_value == "hevc"
        assert codec_comp.python_value == "h264"
