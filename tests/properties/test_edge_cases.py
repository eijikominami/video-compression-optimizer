"""Property-based tests for edge case processing.

Feature: video-compression-optimizer, Property 9: Edge case processing accuracy
Validates: Requirements 10.1, 10.2, 10.4
"""

from datetime import datetime
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.analyzer.analyzer import CompressionAnalyzer
from vco.models.types import VideoInfo, VideoStatus


def create_video(
    duration: float,
    codec: str = "h264",
    file_size: int = 100_000_000,
    is_local: bool = True,
    is_in_icloud: bool = False,
) -> VideoInfo:
    """Create a VideoInfo object with specified parameters."""
    return VideoInfo(
        uuid=f"test_{duration}_{codec}",
        filename=f"test_{codec}.mov",
        path=Path(f"/tmp/test_{codec}.mov"),
        codec=codec,
        resolution=(1920, 1080),
        bitrate=25000000,
        duration=duration,
        frame_rate=30.0,
        file_size=file_size,
        capture_date=datetime(2020, 7, 15, 14, 30, 0),
        creation_date=datetime(2020, 7, 15, 14, 30, 0),
        albums=[],
        is_in_icloud=is_in_icloud,
        is_local=is_local,
    )


class TestEdgeCaseProcessing:
    """Property tests for edge case processing accuracy.

    Property 9: Edge case processing
    - Videos shorter than 1 second are skipped
    - Videos longer than 4 hours trigger a warning
    - Corrupted/inaccessible files are handled gracefully

    Validates: Requirements 10.1, 10.2, 10.4
    """

    # Duration thresholds
    MIN_DURATION = 1.0  # 1 second
    LONG_DURATION = 4 * 60 * 60  # 4 hours in seconds

    @given(duration=st.floats(min_value=0.0, max_value=0.99, allow_nan=False))
    @settings(max_examples=100)
    def test_videos_shorter_than_1_second_are_skipped(self, duration: float):
        """Videos with duration < 1 second are skipped from conversion.

        Requirement 10.1: Skip videos shorter than 1 second
        """
        analyzer = CompressionAnalyzer()
        video = create_video(duration=duration, codec="h264")

        should_skip, reason = analyzer.should_skip(video)

        assert should_skip is True, f"Video with duration {duration}s should be skipped"
        assert reason is not None
        assert "duration" in reason.lower() or "short" in reason.lower()

    @given(duration=st.floats(min_value=1.0, max_value=14400.0, allow_nan=False))
    @settings(max_examples=100)
    def test_videos_1_second_or_longer_not_skipped_for_duration(self, duration: float):
        """Videos with duration >= 1 second are not skipped due to duration.

        Requirement 10.1: Only skip videos shorter than 1 second
        """
        analyzer = CompressionAnalyzer()
        video = create_video(duration=duration, codec="h264")

        should_skip, reason = analyzer.should_skip(video)

        # If skipped, it should NOT be due to duration
        if should_skip:
            assert reason is not None
            assert "duration" not in reason.lower() or "short" not in reason.lower()

    @given(duration=st.floats(min_value=14401.0, max_value=100000.0, allow_nan=False))
    @settings(max_examples=50)
    def test_videos_longer_than_4_hours_flagged_as_long(self, duration: float):
        """Videos longer than 4 hours are flagged as long videos.

        Requirement 10.2: Warn for videos longer than 4 hours
        """
        analyzer = CompressionAnalyzer()
        video = create_video(duration=duration, codec="h264")

        is_long = analyzer.is_long_video(video)

        assert is_long is True, (
            f"Video with duration {duration}s ({duration / 3600:.2f}h) should be flagged as long"
        )

    @given(duration=st.floats(min_value=1.0, max_value=14400.0, allow_nan=False))
    @settings(max_examples=50)
    def test_videos_4_hours_or_shorter_not_flagged_as_long(self, duration: float):
        """Videos 4 hours or shorter are not flagged as long.

        Requirement 10.2: Only warn for videos longer than 4 hours
        """
        analyzer = CompressionAnalyzer()
        video = create_video(duration=duration, codec="h264")

        is_long = analyzer.is_long_video(video)

        assert is_long is False, (
            f"Video with duration {duration}s ({duration / 3600:.2f}h) should not be flagged as long"
        )

    def test_inaccessible_file_is_skipped(self):
        """Files that are not local and not in iCloud are skipped.

        Requirement 10.4: Handle corrupted/inaccessible files gracefully
        """
        analyzer = CompressionAnalyzer()
        video = create_video(
            duration=120.0,
            codec="h264",
            is_local=False,
            is_in_icloud=False,
        )

        should_skip, reason = analyzer.should_skip(video)

        assert should_skip is True
        assert reason is not None
        assert "accessible" in reason.lower() or "not" in reason.lower()

    def test_local_file_not_skipped_for_accessibility(self):
        """Local files are not skipped due to accessibility."""
        analyzer = CompressionAnalyzer()
        video = create_video(
            duration=120.0,
            codec="h264",
            is_local=True,
            is_in_icloud=False,
        )

        should_skip, reason = analyzer.should_skip(video)

        # Should not be skipped (h264 is inefficient codec)
        assert should_skip is False
        assert reason is None

    def test_icloud_file_not_skipped_for_accessibility(self):
        """iCloud files are not skipped due to accessibility."""
        analyzer = CompressionAnalyzer()
        video = create_video(
            duration=120.0,
            codec="h264",
            is_local=False,
            is_in_icloud=True,
        )

        should_skip, reason = analyzer.should_skip(video)

        # Should not be skipped (h264 is inefficient codec)
        assert should_skip is False
        assert reason is None

    @given(duration=st.floats(min_value=0.0, max_value=0.99, allow_nan=False))
    @settings(max_examples=50)
    def test_short_video_analysis_returns_skipped_status(self, duration: float):
        """Short videos analyzed return appropriate skip status.

        Tests state consistency: skip_reason and status must be consistent.
        """
        analyzer = CompressionAnalyzer()
        video = create_video(duration=duration, codec="h264")

        candidate = analyzer.analyze_video(video)

        # Short videos should have skip_reason set AND status should be SKIPPED
        assert candidate.skip_reason is not None
        assert "duration" in candidate.skip_reason.lower()
        # State consistency check: skip_reason implies status == SKIPPED
        assert candidate.status == VideoStatus.SKIPPED, (
            f"Video with skip_reason '{candidate.skip_reason}' should have status SKIPPED, "
            f"but got {candidate.status}"
        )

    def test_boundary_duration_exactly_1_second(self):
        """Video with exactly 1 second duration is not skipped."""
        analyzer = CompressionAnalyzer()
        video = create_video(duration=1.0, codec="h264")

        should_skip, reason = analyzer.should_skip(video)

        assert should_skip is False, "Video with exactly 1s duration should not be skipped"
        assert reason is None

    def test_boundary_duration_exactly_4_hours(self):
        """Video with exactly 4 hours duration is not flagged as long."""
        analyzer = CompressionAnalyzer()
        video = create_video(duration=4 * 60 * 60, codec="h264")  # Exactly 4 hours

        is_long = analyzer.is_long_video(video)

        assert is_long is False, "Video with exactly 4h duration should not be flagged as long"

    def test_boundary_duration_just_over_4_hours(self):
        """Video just over 4 hours is flagged as long."""
        analyzer = CompressionAnalyzer()
        video = create_video(duration=4 * 60 * 60 + 1, codec="h264")  # 4 hours + 1 second

        is_long = analyzer.is_long_video(video)

        assert is_long is True, "Video just over 4h should be flagged as long"

    @given(
        duration=st.floats(min_value=0.001, max_value=0.999, allow_nan=False),
        codec=st.sampled_from(["h264", "mpeg2video", "wmv3"]),
    )
    @settings(max_examples=50)
    def test_short_video_with_inefficient_codec_still_skipped(self, duration: float, codec: str):
        """Short videos are skipped regardless of codec efficiency.

        Duration check takes precedence over codec classification.
        """
        analyzer = CompressionAnalyzer()
        video = create_video(duration=duration, codec=codec)

        should_skip, reason = analyzer.should_skip(video)

        assert should_skip is True
        assert "duration" in reason.lower()

    def test_zero_duration_video_is_skipped(self):
        """Video with zero duration is skipped."""
        analyzer = CompressionAnalyzer()
        video = create_video(duration=0.0, codec="h264")

        should_skip, reason = analyzer.should_skip(video)

        assert should_skip is True
        assert reason is not None
