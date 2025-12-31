"""Integration tests for scan and convert workflow.

Tests the integration between ScanService and ConvertService,
verifying the complete workflow from scanning to conversion.

検証対象:
- 要件 1.1: 動画ファイル検出
- 要件 1.4: 非効率コーデックの変換候補マーク
- 要件 3.1: AWS MediaConvert による変換
- 要件 5.1: バッチ処理
"""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

from vco.analyzer.analyzer import CompressionAnalyzer, ConversionCandidate
from vco.models.types import VideoStatus
from vco.photos.manager import VideoInfo
from vco.services.convert import ConvertService
from vco.services.scan import ScanService


def create_mock_video(
    uuid: str = "test-uuid",
    filename: str = "test_video.mp4",
    codec: str = "h264",
    file_size: int = 100_000_000,
    duration: float = 60.0,
    resolution: tuple = (1920, 1080),
    capture_date: datetime = None,
    is_local: bool = True,
    is_in_icloud: bool = False,
    path: Path = None,
) -> VideoInfo:
    """Create a mock VideoInfo for testing."""
    return VideoInfo(
        uuid=uuid,
        filename=filename,
        path=path or Path(f"/tmp/test/{filename}"),
        codec=codec,
        resolution=resolution,
        bitrate=10_000_000,
        duration=duration,
        frame_rate=30.0,
        file_size=file_size,
        capture_date=capture_date or datetime(2024, 6, 15),
        creation_date=datetime(2024, 6, 15),
        albums=["Test Album"],
        is_in_icloud=is_in_icloud,
        is_local=is_local,
    )


class TestScanServiceIntegration:
    """Integration tests for ScanService with real analyzer."""

    def test_scan_with_real_analyzer(self):
        """ScanService correctly uses CompressionAnalyzer to classify videos."""
        # Create mock photos manager
        mock_photos_manager = Mock()
        mock_videos = [
            create_mock_video(uuid="1", filename="h264_video.mp4", codec="h264"),
            create_mock_video(uuid="2", filename="h265_video.mp4", codec="hevc"),
            create_mock_video(uuid="3", filename="prores_video.mov", codec="prores"),
        ]
        mock_photos_manager.get_all_videos.return_value = mock_videos

        # Use real analyzer
        analyzer = CompressionAnalyzer()

        # Create service
        service = ScanService(photos_manager=mock_photos_manager, analyzer=analyzer)

        # Execute scan
        result = service.scan()

        # Verify results
        assert result.summary.total_videos == 3
        assert result.summary.conversion_candidates == 1  # Only h264
        assert result.summary.already_optimized == 1  # hevc
        assert result.summary.professional == 1  # prores

        # Verify only pending candidates are in the result
        assert len(result.candidates) == 1
        assert result.candidates[0].video.codec == "h264"

    def test_scan_with_date_filter(self):
        """ScanService correctly filters videos by date range."""
        mock_photos_manager = Mock()
        mock_videos = [
            create_mock_video(uuid="1", capture_date=datetime(2024, 1, 15)),
            create_mock_video(uuid="2", capture_date=datetime(2024, 6, 15)),
        ]
        mock_photos_manager.get_videos_by_date_range.return_value = [mock_videos[1]]

        service = ScanService(photos_manager=mock_photos_manager, analyzer=CompressionAnalyzer())

        result = service.scan(from_date=datetime(2024, 6, 1), to_date=datetime(2024, 6, 30))

        # Verify filter was applied
        mock_photos_manager.get_videos_by_date_range.assert_called_once()
        assert result.filter is not None
        assert result.filter["from_date"] is not None

    def test_scan_result_serialization_roundtrip(self):
        """ScanResult can be saved and loaded correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            mock_photos_manager = Mock()
            mock_photos_manager.get_all_videos.return_value = [
                create_mock_video(uuid="test-1", codec="h264")
            ]

            service = ScanService(
                photos_manager=mock_photos_manager,
                analyzer=CompressionAnalyzer(),
                output_dir=output_dir,
            )

            # Scan and save
            result = service.scan()
            saved_path = service.save_candidates(result)

            assert saved_path.exists()

            # Load and verify
            loaded = service.load_candidates()

            assert loaded is not None
            assert loaded.summary.conversion_candidates == result.summary.conversion_candidates
            assert len(loaded.candidates) == len(result.candidates)
            assert loaded.candidates[0].video.uuid == result.candidates[0].video.uuid


class TestConvertServiceIntegration:
    """Integration tests for ConvertService with mocked AWS services."""

    def test_convert_batch_with_disk_space_check(self):
        """ConvertService checks disk space before batch conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_folder = Path(tmpdir) / "converted"

            # Create mock dependencies
            mock_mediaconvert = Mock()
            mock_quality_checker = Mock()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                staging_folder=staging_folder,
            )

            # Create test file
            test_file = Path(tmpdir) / "test_video.mp4"
            test_file.write_bytes(b"x" * 1000)

            candidates = [
                ConversionCandidate(
                    video=create_mock_video(uuid="1", file_size=1000, path=test_file),
                    estimated_savings_bytes=500,
                    estimated_savings_percent=50.0,
                    status=VideoStatus.PENDING,
                )
            ]

            # Should not raise with sufficient disk space
            # (dry_run to avoid actual conversion)
            result = service.convert_batch(candidates, dry_run=True)

            assert result.total == 1

    def test_convert_batch_error_resilience(self):
        """ConvertService continues processing after individual failures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_folder = Path(tmpdir) / "converted"

            # Create test files
            file1 = Path(tmpdir) / "video1.mp4"
            file2 = Path(tmpdir) / "video2.mp4"
            file1.write_bytes(b"x" * 1000)
            file2.write_bytes(b"x" * 1000)

            # Mock MediaConvert to fail on first, succeed on second
            mock_mediaconvert = Mock()
            mock_mediaconvert.upload_to_s3.side_effect = [
                Exception("Upload failed"),  # First call fails
                None,  # Second call succeeds
            ]

            mock_quality_checker = Mock()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                staging_folder=staging_folder,
            )

            candidates = [
                ConversionCandidate(
                    video=create_mock_video(uuid="1", filename="video1.mp4", path=file1),
                    estimated_savings_bytes=500,
                    estimated_savings_percent=50.0,
                    status=VideoStatus.PENDING,
                ),
                ConversionCandidate(
                    video=create_mock_video(uuid="2", filename="video2.mp4", path=file2),
                    estimated_savings_bytes=500,
                    estimated_savings_percent=50.0,
                    status=VideoStatus.PENDING,
                ),
            ]

            result = service.convert_batch(candidates, skip_disk_check=True)

            # Both should be processed
            assert result.total == 2
            assert result.failed >= 1  # At least first one failed
            assert len(result.errors) >= 1
            assert "video1.mp4" in result.errors[0]

    def test_convert_single_file_not_found(self):
        """ConvertService handles missing files gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_folder = Path(tmpdir) / "converted"

            mock_mediaconvert = Mock()
            mock_quality_checker = Mock()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                staging_folder=staging_folder,
            )

            # Video with non-existent path
            candidate = ConversionCandidate(
                video=create_mock_video(uuid="1", path=Path("/nonexistent/video.mp4")),
                estimated_savings_bytes=500,
                estimated_savings_percent=50.0,
                status=VideoStatus.PENDING,
            )

            result = service.convert_single(candidate)

            assert not result.success
            assert "not found" in result.error_message.lower()


class TestScanConvertWorkflow:
    """End-to-end integration tests for scan → convert workflow."""

    def test_scan_then_convert_workflow(self):
        """Complete workflow: scan videos, then convert candidates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            staging_folder = output_dir / "converted"

            # Create test video file
            test_video = output_dir / "test_h264.mp4"
            test_video.write_bytes(b"x" * 10000)

            # Setup scan service with mock photos manager
            mock_photos_manager = Mock()
            mock_photos_manager.get_all_videos.return_value = [
                create_mock_video(
                    uuid="scan-test-1", filename="test_h264.mp4", codec="h264", path=test_video
                )
            ]

            scan_service = ScanService(
                photos_manager=mock_photos_manager,
                analyzer=CompressionAnalyzer(),
                output_dir=output_dir,
            )

            # Step 1: Scan
            scan_result = scan_service.scan()

            assert scan_result.summary.conversion_candidates == 1

            # Save candidates
            scan_service.save_candidates(scan_result)

            # Step 2: Load candidates for conversion
            loaded_result = scan_service.load_candidates()

            assert loaded_result is not None
            assert len(loaded_result.candidates) == 1

            # Step 3: Convert (dry run)
            mock_mediaconvert = Mock()
            mock_quality_checker = Mock()

            convert_service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                staging_folder=staging_folder,
            )

            convert_result = convert_service.convert_batch(loaded_result.candidates, dry_run=True)

            assert convert_result.total == 1

    def test_candidate_status_preserved_through_workflow(self):
        """Candidate status is correctly preserved through save/load cycle."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            mock_photos_manager = Mock()
            mock_photos_manager.get_all_videos.return_value = [
                create_mock_video(uuid="1", codec="h264"),
                create_mock_video(uuid="2", codec="hevc"),
            ]

            service = ScanService(
                photos_manager=mock_photos_manager,
                analyzer=CompressionAnalyzer(),
                output_dir=output_dir,
            )

            # Scan
            result = service.scan()

            # Only pending candidates should be in result
            assert len(result.candidates) == 1
            assert result.candidates[0].status == VideoStatus.PENDING

            # Save and load
            service.save_candidates(result)
            loaded = service.load_candidates()

            # Status should be preserved
            assert loaded.candidates[0].status == VideoStatus.PENDING

    def test_cost_estimation_for_batch(self):
        """Cost estimation works for batch of candidates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging_folder = Path(tmpdir) / "converted"

            mock_mediaconvert = Mock()
            # Mock cost estimation: $0.015/min for HD
            mock_mediaconvert.estimate_cost.return_value = 0.015

            mock_quality_checker = Mock()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                staging_folder=staging_folder,
            )

            candidates = [
                ConversionCandidate(
                    video=create_mock_video(uuid="1", duration=60.0),
                    estimated_savings_bytes=500,
                    estimated_savings_percent=50.0,
                    status=VideoStatus.PENDING,
                ),
                ConversionCandidate(
                    video=create_mock_video(uuid="2", duration=120.0),
                    estimated_savings_bytes=500,
                    estimated_savings_percent=50.0,
                    status=VideoStatus.PENDING,
                ),
            ]

            cost = service.estimate_batch_cost(candidates)

            assert cost == 0.03  # 2 * $0.015
            assert mock_mediaconvert.estimate_cost.call_count == 2
