"""Property-based tests for date range filtering.

Feature: video-compression-optimizer, Property 2: Date range filter accuracy
Validates: Requirements 1.1.1, 1.1.2, 1.1.3, 1.1.5
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.models.types import VideoInfo
from vco.photos.manager import PhotosAccessManager


def create_video_info(
    uuid: str,
    capture_date: datetime | None,
    creation_date: datetime,
) -> VideoInfo:
    """Create a VideoInfo object for testing."""
    return VideoInfo(
        uuid=uuid,
        filename=f"{uuid}.mov",
        path=Path(f"/tmp/{uuid}.mov"),
        codec="h264",
        resolution=(1920, 1080),
        bitrate=25000000,
        duration=120.0,
        frame_rate=30.0,
        file_size=375000000,
        capture_date=capture_date,
        creation_date=creation_date,
        albums=[],
        is_in_icloud=False,
        is_local=True,
    )


# Strategy for generating datetime objects (2015-2025 range)
datetime_strategy = st.datetimes(
    min_value=datetime(2015, 1, 1),
    max_value=datetime(2025, 12, 31),
)


# Strategy for generating video lists
@st.composite
def video_list_strategy(draw):
    """Generate a list of VideoInfo objects with various dates."""
    num_videos = draw(st.integers(min_value=0, max_value=20))
    videos = []

    for i in range(num_videos):
        capture_date = draw(st.one_of(st.none(), datetime_strategy))
        creation_date = draw(datetime_strategy)

        video = create_video_info(
            uuid=f"video_{i}",
            capture_date=capture_date,
            creation_date=creation_date,
        )
        videos.append(video)

    return videos


class TestDateRangeFilter:
    """Property tests for date range filter accuracy.

    Property 2: For any date range filter (from_date, to_date) and video set,
    the filtered result contains only videos with capture_date (or creation_date
    depending on filter type) within the specified range (inclusive).
    When no filter is specified, all videos are included.

    Validates: Requirements 1.1.1, 1.1.2, 1.1.3, 1.1.5
    """

    @given(videos=video_list_strategy())
    @settings(max_examples=100)
    def test_no_filter_returns_all_videos(self, videos: list[VideoInfo]):
        """When no date filter is specified, all videos are returned."""
        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(None, None)

        assert len(result) == len(videos)
        assert set(v.uuid for v in result) == set(v.uuid for v in videos)

    @given(
        videos=video_list_strategy(),
        from_date=datetime_strategy,
        to_date=datetime_strategy,
    )
    @settings(max_examples=100)
    def test_capture_date_filter_only_includes_videos_in_range(
        self,
        videos: list[VideoInfo],
        from_date: datetime,
        to_date: datetime,
    ):
        """All returned videos have capture_date within the specified range."""
        # Ensure from_date <= to_date
        if from_date > to_date:
            from_date, to_date = to_date, from_date

        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(from_date, to_date, date_type="capture")

        for video in result:
            assert video.capture_date is not None
            assert from_date <= video.capture_date <= to_date

    @given(
        videos=video_list_strategy(),
        from_date=datetime_strategy,
        to_date=datetime_strategy,
    )
    @settings(max_examples=100)
    def test_creation_date_filter_only_includes_videos_in_range(
        self,
        videos: list[VideoInfo],
        from_date: datetime,
        to_date: datetime,
    ):
        """All returned videos have creation_date within the specified range."""
        # Ensure from_date <= to_date
        if from_date > to_date:
            from_date, to_date = to_date, from_date

        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(from_date, to_date, date_type="creation")

        for video in result:
            assert from_date <= video.creation_date <= to_date

    @given(
        videos=video_list_strategy(),
        from_date=datetime_strategy,
    )
    @settings(max_examples=100)
    def test_from_date_only_filter(self, videos: list[VideoInfo], from_date: datetime):
        """When only from_date is specified, all returned videos are on or after that date."""
        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(from_date, None, date_type="capture")

        for video in result:
            assert video.capture_date is not None
            assert video.capture_date >= from_date

    @given(
        videos=video_list_strategy(),
        to_date=datetime_strategy,
    )
    @settings(max_examples=100)
    def test_to_date_only_filter(self, videos: list[VideoInfo], to_date: datetime):
        """When only to_date is specified, all returned videos are on or before that date."""
        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(None, to_date, date_type="capture")

        for video in result:
            assert video.capture_date is not None
            assert video.capture_date <= to_date

    @given(videos=video_list_strategy())
    @settings(max_examples=100)
    def test_videos_without_capture_date_excluded_from_capture_filter(
        self, videos: list[VideoInfo]
    ):
        """Videos without capture_date are excluded when filtering by capture date."""
        # Use a very wide date range to include all dated videos
        from_date = datetime(2015, 1, 1)
        to_date = datetime(2025, 12, 31)

        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(from_date, to_date, date_type="capture")

        # All returned videos should have capture_date
        for video in result:
            assert video.capture_date is not None

        # Videos without capture_date should not be in result
        videos_without_date = [v for v in videos if v.capture_date is None]
        result_uuids = {v.uuid for v in result}
        for video in videos_without_date:
            assert video.uuid not in result_uuids

    @given(
        videos=video_list_strategy(),
        from_date=datetime_strategy,
        to_date=datetime_strategy,
    )
    @settings(max_examples=100)
    def test_filter_is_inclusive(
        self,
        videos: list[VideoInfo],
        from_date: datetime,
        to_date: datetime,
    ):
        """Date range filter is inclusive on both ends."""
        # Ensure from_date <= to_date
        if from_date > to_date:
            from_date, to_date = to_date, from_date

        # Create videos exactly at boundaries
        boundary_videos = [
            create_video_info("at_from", from_date, from_date),
            create_video_info("at_to", to_date, to_date),
        ]

        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=boundary_videos):
            result = manager.get_videos_by_date_range(from_date, to_date, date_type="capture")

        # Both boundary videos should be included
        result_uuids = {v.uuid for v in result}
        assert "at_from" in result_uuids
        assert "at_to" in result_uuids

    @given(
        videos=video_list_strategy(),
        from_date=datetime_strategy,
        to_date=datetime_strategy,
    )
    @settings(max_examples=100)
    def test_filter_excludes_videos_outside_range(
        self,
        videos: list[VideoInfo],
        from_date: datetime,
        to_date: datetime,
    ):
        """Videos outside the date range are excluded."""
        # Ensure from_date <= to_date
        if from_date > to_date:
            from_date, to_date = to_date, from_date

        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(from_date, to_date, date_type="capture")

        result_uuids = {v.uuid for v in result}

        # Check that videos outside range are not in result
        for video in videos:
            if video.capture_date is not None:
                if video.capture_date < from_date or video.capture_date > to_date:
                    assert video.uuid not in result_uuids

    @given(
        videos=video_list_strategy(),
        from_date=datetime_strategy,
        to_date=datetime_strategy,
    )
    @settings(max_examples=100)
    def test_filter_result_is_subset_of_input(
        self,
        videos: list[VideoInfo],
        from_date: datetime,
        to_date: datetime,
    ):
        """Filtered result is always a subset of the input."""
        manager = PhotosAccessManager()

        with patch.object(manager, "get_all_videos", return_value=videos):
            result = manager.get_videos_by_date_range(from_date, to_date)

        input_uuids = {v.uuid for v in videos}
        result_uuids = {v.uuid for v in result}

        assert result_uuids.issubset(input_uuids)
