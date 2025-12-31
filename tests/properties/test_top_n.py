"""Property-based tests for Top-N selection feature.

Validates:
- Property 13: Top-N selection accuracy
- Property 14: Top-N total calculation accuracy

Requirements: 11.1.1, 11.1.2, 11.1.3, 11.1.4
"""

from datetime import datetime
from pathlib import Path

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from vco.analyzer.analyzer import ConversionCandidate
from vco.models.types import VideoStatus
from vco.photos.manager import VideoInfo
from vco.services.scan import ScanService


def create_video_info(
    uuid: str = "test-uuid",
    filename: str = "test.mov",
    file_size: int = 1000000,
    codec: str = "h264",
    duration: float = 60.0,
    capture_date: datetime = None,
) -> VideoInfo:
    """Create a VideoInfo instance for testing."""
    return VideoInfo(
        uuid=uuid,
        filename=filename,
        path=Path(f"/test/{filename}"),
        codec=codec,
        resolution=(1920, 1080),
        bitrate=10000000,
        duration=duration,
        frame_rate=30.0,
        file_size=file_size,
        capture_date=capture_date or datetime(2020, 1, 1),
        creation_date=datetime(2020, 1, 1),
        albums=[],
        is_in_icloud=False,
        is_local=True,
        location=None,
    )


def create_candidate(
    uuid: str = "test-uuid",
    filename: str = "test.mov",
    file_size: int = 1000000,
    estimated_savings_bytes: int = 500000,
) -> ConversionCandidate:
    """Create a ConversionCandidate instance for testing."""
    video = create_video_info(uuid=uuid, filename=filename, file_size=file_size)
    return ConversionCandidate(
        video=video,
        estimated_savings_bytes=estimated_savings_bytes,
        estimated_savings_percent=(estimated_savings_bytes / file_size * 100)
        if file_size > 0
        else 0,
        status=VideoStatus.PENDING,
        skip_reason=None,
    )


class TestTopNSelectionAccuracy:
    """Property 13: Top-N selection accuracy.

    For any candidate list and positive integer N, the result of --top-n N:
    - Is sorted by file size in descending order
    - Has count equal to min(N, original candidate count)
    - When combined with date filter, date filter is applied first
    """

    @given(
        file_sizes=st.lists(
            st.integers(min_value=1, max_value=10_000_000_000), min_size=1, max_size=50
        ),
        n=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100)
    def test_result_sorted_by_file_size_descending(self, file_sizes: list[int], n: int):
        """Top-N result is sorted by file size in descending order."""
        # Create candidates with given file sizes
        candidates = [
            create_candidate(
                uuid=f"uuid-{i}",
                filename=f"video_{i}.mov",
                file_size=size,
                estimated_savings_bytes=size // 2,
            )
            for i, size in enumerate(file_sizes)
        ]

        scan_service = ScanService()
        result = scan_service.select_top_n(candidates, n)

        # Verify sorted in descending order
        for i in range(len(result) - 1):
            assert result[i].video.file_size >= result[i + 1].video.file_size, (
                f"Result not sorted: {result[i].video.file_size} < {result[i + 1].video.file_size}"
            )

    @given(
        file_sizes=st.lists(
            st.integers(min_value=1, max_value=10_000_000_000), min_size=1, max_size=50
        ),
        n=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100)
    def test_result_count_equals_min_n_or_total(self, file_sizes: list[int], n: int):
        """Top-N result count equals min(N, original candidate count)."""
        candidates = [
            create_candidate(
                uuid=f"uuid-{i}",
                filename=f"video_{i}.mov",
                file_size=size,
                estimated_savings_bytes=size // 2,
            )
            for i, size in enumerate(file_sizes)
        ]

        scan_service = ScanService()
        result = scan_service.select_top_n(candidates, n)

        expected_count = min(n, len(candidates))
        assert len(result) == expected_count, (
            f"Expected {expected_count} candidates, got {len(result)}"
        )

    @given(
        file_sizes=st.lists(
            st.integers(min_value=1, max_value=10_000_000_000), min_size=1, max_size=50
        ),
        n=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100)
    def test_result_contains_largest_files(self, file_sizes: list[int], n: int):
        """Top-N result contains the N largest files."""
        candidates = [
            create_candidate(
                uuid=f"uuid-{i}",
                filename=f"video_{i}.mov",
                file_size=size,
                estimated_savings_bytes=size // 2,
            )
            for i, size in enumerate(file_sizes)
        ]

        scan_service = ScanService()
        result = scan_service.select_top_n(candidates, n)

        # Get expected largest sizes
        sorted_sizes = sorted(file_sizes, reverse=True)
        expected_sizes = sorted_sizes[: min(n, len(file_sizes))]

        # Get actual sizes from result
        result_sizes = [c.video.file_size for c in result]

        assert result_sizes == expected_sizes, (
            f"Expected sizes {expected_sizes}, got {result_sizes}"
        )

    def test_n_must_be_positive(self):
        """select_top_n raises ValueError for non-positive n."""
        candidates = [create_candidate()]
        scan_service = ScanService()

        with pytest.raises(ValueError, match="n must be a positive integer"):
            scan_service.select_top_n(candidates, 0)

        with pytest.raises(ValueError, match="n must be a positive integer"):
            scan_service.select_top_n(candidates, -1)

    def test_empty_candidates_returns_empty(self):
        """select_top_n returns empty list for empty candidates."""
        scan_service = ScanService()
        result = scan_service.select_top_n([], 10)
        assert result == []

    @given(n=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_n_larger_than_candidates_returns_all(self, n: int):
        """When N > candidate count, all candidates are returned."""
        # Create fewer candidates than N
        num_candidates = max(1, n // 2)
        candidates = [
            create_candidate(
                uuid=f"uuid-{i}", filename=f"video_{i}.mov", file_size=(i + 1) * 1000000
            )
            for i in range(num_candidates)
        ]

        scan_service = ScanService()
        result = scan_service.select_top_n(candidates, n)

        assert len(result) == num_candidates


class TestTopNTotalCalculation:
    """Property 14: Top-N total calculation accuracy.

    For any Top-N selection result:
    - Total file size equals sum of selected candidates' file_size
    - Estimated savings equals sum of selected candidates' estimated_savings_bytes
    """

    @given(
        file_sizes=st.lists(
            st.integers(min_value=1, max_value=10_000_000_000), min_size=1, max_size=50
        ),
        n=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100)
    def test_total_size_equals_sum_of_file_sizes(self, file_sizes: list[int], n: int):
        """Total file size equals sum of selected candidates' file_size."""
        candidates = [
            create_candidate(
                uuid=f"uuid-{i}",
                filename=f"video_{i}.mov",
                file_size=size,
                estimated_savings_bytes=size // 2,
            )
            for i, size in enumerate(file_sizes)
        ]

        scan_service = ScanService()
        selected = scan_service.select_top_n(candidates, n)
        summary = scan_service.calculate_top_n_summary(selected)

        expected_total = sum(c.video.file_size for c in selected)
        assert summary["total_size"] == expected_total, (
            f"Expected total_size {expected_total}, got {summary['total_size']}"
        )

    @given(
        file_sizes=st.lists(
            st.integers(min_value=1, max_value=10_000_000_000), min_size=1, max_size=50
        ),
        n=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100)
    def test_estimated_savings_equals_sum_of_savings(self, file_sizes: list[int], n: int):
        """Estimated savings equals sum of selected candidates' estimated_savings_bytes."""
        candidates = [
            create_candidate(
                uuid=f"uuid-{i}",
                filename=f"video_{i}.mov",
                file_size=size,
                estimated_savings_bytes=size // 2,
            )
            for i, size in enumerate(file_sizes)
        ]

        scan_service = ScanService()
        selected = scan_service.select_top_n(candidates, n)
        summary = scan_service.calculate_top_n_summary(selected)

        expected_savings = sum(c.estimated_savings_bytes for c in selected)
        assert summary["estimated_savings"] == expected_savings, (
            f"Expected estimated_savings {expected_savings}, got {summary['estimated_savings']}"
        )

    @given(
        file_sizes=st.lists(
            st.integers(min_value=1, max_value=10_000_000_000), min_size=1, max_size=50
        ),
        n=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100)
    def test_count_equals_selected_count(self, file_sizes: list[int], n: int):
        """Count in summary equals number of selected candidates."""
        candidates = [
            create_candidate(
                uuid=f"uuid-{i}",
                filename=f"video_{i}.mov",
                file_size=size,
                estimated_savings_bytes=size // 2,
            )
            for i, size in enumerate(file_sizes)
        ]

        scan_service = ScanService()
        selected = scan_service.select_top_n(candidates, n)
        summary = scan_service.calculate_top_n_summary(selected)

        assert summary["count"] == len(selected), (
            f"Expected count {len(selected)}, got {summary['count']}"
        )

    @given(
        file_sizes=st.lists(
            st.integers(min_value=1, max_value=10_000_000_000), min_size=1, max_size=50
        ),
        savings_ratios=st.lists(st.floats(min_value=0.1, max_value=0.9), min_size=1, max_size=50),
        n=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=100)
    def test_savings_percent_calculation(
        self, file_sizes: list[int], savings_ratios: list[float], n: int
    ):
        """Savings percent is correctly calculated."""
        # Ensure same length
        min_len = min(len(file_sizes), len(savings_ratios))
        assume(min_len > 0)

        candidates = [
            create_candidate(
                uuid=f"uuid-{i}",
                filename=f"video_{i}.mov",
                file_size=file_sizes[i],
                estimated_savings_bytes=int(file_sizes[i] * savings_ratios[i]),
            )
            for i in range(min_len)
        ]

        scan_service = ScanService()
        selected = scan_service.select_top_n(candidates, n)
        summary = scan_service.calculate_top_n_summary(selected)

        total_size = sum(c.video.file_size for c in selected)
        total_savings = sum(c.estimated_savings_bytes for c in selected)

        if total_size > 0:
            expected_percent = round((total_savings / total_size) * 100, 1)
            assert summary["estimated_savings_percent"] == expected_percent, (
                f"Expected {expected_percent}%, got {summary['estimated_savings_percent']}%"
            )
        else:
            assert summary["estimated_savings_percent"] == 0.0

    def test_empty_candidates_summary(self):
        """Summary for empty candidates has zero values."""
        scan_service = ScanService()
        summary = scan_service.calculate_top_n_summary([])

        assert summary["count"] == 0
        assert summary["total_size"] == 0
        assert summary["estimated_savings"] == 0
        assert summary["estimated_savings_percent"] == 0.0
