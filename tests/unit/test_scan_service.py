"""Unit tests for ScanService.

Tests for scan service functionality.
Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from vco.analyzer.analyzer import ConversionCandidate
from vco.models.types import VideoInfo, VideoStatus
from vco.services.scan import ScanResult, ScanService, ScanSummary


def create_test_video(
    uuid: str,
    codec: str = "avc1",
    duration: float = 120.0,
    file_size: int = 100000000,
    capture_date: datetime = None,
) -> VideoInfo:
    """Create a test VideoInfo object."""
    return VideoInfo(
        uuid=uuid,
        filename=f"{uuid}.mov",
        path=Path(f"/tmp/{uuid}.mov"),
        codec=codec,
        resolution=(1920, 1080),
        bitrate=25000000,
        duration=duration,
        frame_rate=30.0,
        file_size=file_size,
        capture_date=capture_date or datetime(2020, 7, 15),
        creation_date=datetime(2020, 7, 15),
        albums=[],
        is_in_icloud=False,
        is_local=True,
    )


class TestBuildSummary:
    """Tests for _build_summary method."""

    def test_build_summary_with_pending_candidates(self):
        """Test summary building with pending candidates."""
        service = ScanService()

        candidates = [
            ConversionCandidate(
                video=create_test_video("video1", codec="avc1"),
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status=VideoStatus.PENDING,
                skip_reason=None,
            ),
            ConversionCandidate(
                video=create_test_video("video2", codec="h264"),
                estimated_savings_bytes=30000000,
                estimated_savings_percent=50.0,
                status=VideoStatus.PENDING,
                skip_reason=None,
            ),
        ]

        summary = service._build_summary(candidates)

        assert summary.total_videos == 2
        assert summary.conversion_candidates == 2
        assert summary.already_optimized == 0
        assert summary.professional == 0
        assert summary.skipped == 0
        assert summary.estimated_total_savings_bytes == 80000000

    def test_build_summary_with_mixed_statuses(self):
        """Test summary building with mixed status candidates."""
        service = ScanService()

        candidates = [
            ConversionCandidate(
                video=create_test_video("video1", codec="avc1"),
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status=VideoStatus.PENDING,
                skip_reason=None,
            ),
            ConversionCandidate(
                video=create_test_video("video2", codec="hevc"),
                estimated_savings_bytes=0,
                estimated_savings_percent=0.0,
                status=VideoStatus.OPTIMIZED,
                skip_reason="Already optimized",
            ),
            ConversionCandidate(
                video=create_test_video("video3", codec="prores"),
                estimated_savings_bytes=0,
                estimated_savings_percent=0.0,
                status=VideoStatus.PROFESSIONAL,
                skip_reason="Professional format",
            ),
            ConversionCandidate(
                video=create_test_video("video4", codec="avc1", duration=0.5),
                estimated_savings_bytes=0,
                estimated_savings_percent=0.0,
                status=VideoStatus.SKIPPED,
                skip_reason="Duration too short",
            ),
        ]

        summary = service._build_summary(candidates)

        assert summary.total_videos == 4
        assert summary.conversion_candidates == 1
        assert summary.already_optimized == 1
        assert summary.professional == 1
        assert summary.skipped == 1
        assert summary.estimated_total_savings_bytes == 50000000

    def test_build_summary_empty_candidates(self):
        """Test summary building with empty candidates list."""
        service = ScanService()

        summary = service._build_summary([])

        assert summary.total_videos == 0
        assert summary.conversion_candidates == 0
        assert summary.estimated_total_savings_bytes == 0
        assert summary.estimated_total_savings_percent == 0.0


class TestScanMethod:
    """Tests for scan method."""

    def test_scan_returns_pending_candidates(self):
        """Test that scan returns only pending candidates."""
        service = ScanService()

        # Create test videos
        test_videos = [
            create_test_video("video1", codec="avc1"),
            create_test_video("video2", codec="hevc"),  # Will be optimized
        ]

        with patch.object(service.photos_manager, "get_all_videos", return_value=test_videos):
            result = service.scan()

        # Only pending candidates should be in result.candidates
        assert len(result.candidates) == 1
        assert result.candidates[0].video.uuid == "video1"
        assert result.candidates[0].status == VideoStatus.PENDING

    def test_scan_with_date_filter(self):
        """Test scan with date range filter."""
        service = ScanService()

        test_videos = [
            create_test_video("video1", codec="avc1", capture_date=datetime(2022, 6, 15)),
            create_test_video("video2", codec="avc1", capture_date=datetime(2024, 3, 20)),
        ]

        with patch.object(
            service.photos_manager, "get_videos_by_date_range", return_value=[test_videos[0]]
        ):
            result = service.scan(
                from_date=datetime(2022, 1, 1),
                to_date=datetime(2023, 12, 31),
            )

        assert result.summary.total_videos == 1
        assert result.filter is not None
        assert result.filter["from_date"] is not None


class TestCandidateSerialization:
    """Tests for candidate serialization/deserialization."""

    def test_candidate_to_dict_converts_status_to_string(self):
        """Test that status enum is converted to string in dict."""
        candidate = ConversionCandidate(
            video=create_test_video("video1", codec="avc1"),
            estimated_savings_bytes=50000000,
            estimated_savings_percent=50.0,
            status=VideoStatus.PENDING,
            skip_reason=None,
        )

        result = ScanResult(candidates=[candidate])
        result_dict = result.to_dict()

        # Status should be a string, not enum
        assert result_dict["candidates"][0]["status"] == "pending"
        assert isinstance(result_dict["candidates"][0]["status"], str)

    def test_save_and_load_candidates(self):
        """Test saving and loading candidates preserves data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            service = ScanService(output_dir=Path(tmpdir))

            # Create a result with candidates
            candidate = ConversionCandidate(
                video=create_test_video("video1", codec="avc1"),
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status=VideoStatus.PENDING,
                skip_reason=None,
            )

            result = ScanResult(
                summary=ScanSummary(
                    total_videos=1,
                    conversion_candidates=1,
                    estimated_total_savings_bytes=50000000,
                ),
                candidates=[candidate],
            )

            # Save
            output_path = service.save_candidates(result)
            assert output_path.exists()

            # Load
            loaded = service.load_candidates()

            assert loaded is not None
            assert loaded.summary.total_videos == 1
            assert loaded.summary.conversion_candidates == 1
            assert len(loaded.candidates) == 1
            assert loaded.candidates[0].video.uuid == "video1"
            assert loaded.candidates[0].status == VideoStatus.PENDING

    def test_load_candidates_converts_status_string_to_enum(self):
        """Test that status string is converted to enum when loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            service = ScanService(output_dir=Path(tmpdir))

            # Create a JSON file with string status
            data = {
                "schema_version": "1.0",
                "scan_date": datetime.now().isoformat(),
                "filter": None,
                "summary": {
                    "total_videos": 1,
                    "conversion_candidates": 1,
                    "already_optimized": 0,
                    "professional": 0,
                    "skipped": 0,
                    "estimated_total_savings_bytes": 50000000,
                    "estimated_total_savings_percent": 50.0,
                },
                "candidates": [
                    {
                        "uuid": "video1",
                        "filename": "video1.mov",
                        "path": "/tmp/video1.mov",
                        "codec": "avc1",
                        "resolution": [1920, 1080],
                        "bitrate": 25000000,
                        "duration": 120.0,
                        "frame_rate": 30.0,
                        "file_size": 100000000,
                        "capture_date": "2020-07-15T00:00:00",
                        "creation_date": "2020-07-15T00:00:00",
                        "albums": [],
                        "is_in_icloud": False,
                        "estimated_savings_bytes": 50000000,
                        "estimated_savings_percent": 50.0,
                        "status": "pending",  # String status
                        "skip_reason": None,
                    }
                ],
            }

            output_path = Path(tmpdir) / "candidates.json"
            with open(output_path, "w") as f:
                json.dump(data, f)

            # Load
            loaded = service.load_candidates()

            assert loaded is not None
            assert loaded.candidates[0].status == VideoStatus.PENDING
            assert isinstance(loaded.candidates[0].status, VideoStatus)


class TestVideoStatusEnum:
    """Tests for VideoStatus enum usage."""

    def test_video_status_has_correct_values(self):
        """Test that VideoStatus enum has expected values."""
        assert VideoStatus.PENDING.value == "pending"
        assert VideoStatus.OPTIMIZED.value == "optimized"
        assert VideoStatus.PROFESSIONAL.value == "professional"
        assert VideoStatus.SKIPPED.value == "skipped"

    def test_video_status_comparison(self):
        """Test VideoStatus enum comparison."""
        status = VideoStatus.PENDING

        assert status == VideoStatus.PENDING
        assert status != VideoStatus.OPTIMIZED
        assert status != "pending"  # Enum != string
