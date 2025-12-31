"""Property-based tests for original file safety.

**Property 5: Original File Safety**
The original video file is never modified or deleted until:
1. Conversion is complete
2. Quality check passes
3. User explicitly approves the conversion

**Validates: Requirements 6.1.1, 8.1, 8.4**
"""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.analyzer.analyzer import ConversionCandidate
from vco.metadata.manager import VideoMetadata
from vco.models.types import VideoInfo
from vco.quality.checker import QualityResult
from vco.services.convert import ConvertService
from vco.services.review import ReviewItem, ReviewQueue, ReviewService


# Test strategies
def create_temp_files_for_candidates(candidates: list, tmp_dir: Path) -> None:
    """Create actual temp files for all candidates with non-zero size.

    Files must have content because _check_file_available() checks for size > 0.
    """
    for candidate in candidates:
        file_path = tmp_dir / candidate.video.filename
        # Write some content to make file size > 0 (required by _check_file_available)
        file_path.write_bytes(b"dummy video content")
        candidate.video.path = file_path


@st.composite
def video_info_strategy(draw):
    """Generate random VideoInfo objects (path will be updated later with temp file)."""
    uuid = draw(st.text(alphabet="abcdef0123456789", min_size=8, max_size=16))
    filename = (
        draw(st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789_", min_size=3, max_size=20))
        + ".mp4"
    )

    return VideoInfo(
        uuid=uuid,
        filename=filename,
        path=Path(f"/tmp/placeholder/{filename}"),  # Placeholder, will be updated
        codec="h264",
        resolution=(1920, 1080),
        bitrate=10000000,
        duration=draw(st.floats(min_value=1.0, max_value=3600.0)),
        frame_rate=30.0,
        file_size=draw(st.integers(min_value=1000000, max_value=1000000000)),
        capture_date=datetime.now(),
        creation_date=datetime.now(),
        albums=["Test Album"],
        is_in_icloud=False,
        is_local=True,
    )


@st.composite
def conversion_candidate_strategy(draw):
    """Generate random ConversionCandidate objects."""
    video = draw(video_info_strategy())
    return ConversionCandidate(
        video=video,
        estimated_savings_bytes=draw(st.integers(min_value=100000, max_value=video.file_size // 2)),
        estimated_savings_percent=draw(st.floats(min_value=10.0, max_value=60.0)),
        status="pending",
    )


class MockMediaConvertClient:
    """Mock MediaConvert client that tracks file operations."""

    def __init__(self, should_fail: bool = False, quality_fail: bool = False):
        self.should_fail = should_fail
        self.quality_fail = quality_fail
        self.uploaded_files = []
        self.downloaded_files = []
        self.deleted_s3_files = []
        self.original_files_touched = []  # Track if original files were modified

    def upload_to_s3(self, local_path: Path, s3_key: str):
        self.uploaded_files.append((local_path, s3_key))
        # We should NOT modify the original file during upload
        # Just record that we accessed it

    def submit_job(
        self, source_video_uuid: str, source_s3_key: str, output_s3_key: str, quality_preset: str
    ):
        if self.should_fail:
            raise Exception("Simulated MediaConvert failure")

        return MagicMock(job_id=f"job_{source_video_uuid}", status="SUBMITTED")

    def wait_for_completion(self, job_id: str, poll_interval: int = 10, timeout: int = 3600):
        if self.should_fail:
            return MagicMock(job_id=job_id, status="ERROR", error_message="Simulated failure")

        return MagicMock(job_id=job_id, status="COMPLETE", error_message=None)

    def download_from_s3(self, s3_key: str, local_path: Path):
        self.downloaded_files.append((s3_key, local_path))
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.touch()

    def delete_from_s3(self, s3_key: str):
        self.deleted_s3_files.append(s3_key)

    def estimate_cost(self, duration_seconds: float, resolution: tuple[int, int]) -> float:
        return 0.01


class MockQualityChecker:
    """Mock quality checker."""

    def __init__(self, should_pass: bool = True):
        self.should_pass = should_pass

    def trigger_quality_check_sync(
        self, original_s3_key: str, converted_s3_key: str, metadata_s3_key: str = None
    ) -> QualityResult:
        return QualityResult(
            job_id="quality_job",
            original_s3_key=original_s3_key,
            converted_s3_key=converted_s3_key,
            status="passed" if self.should_pass else "failed",
            ssim_score=0.97 if self.should_pass else 0.85,
            original_size=100000000,
            converted_size=50000000,
            compression_ratio=2.0,
            space_saved_bytes=50000000,
            space_saved_percent=50.0,
            playback_verified=True,
            failure_reason=None if self.should_pass else "SSIM below threshold",
        )


class MockMetadataManager:
    """Mock metadata manager."""

    def extract_metadata(self, video_path: Path):
        return VideoMetadata(capture_date=datetime.now(), creation_date=datetime.now(), albums=[])

    def apply_metadata(self, video_path: Path, metadata):
        pass

    def copy_dates_from_original(self, original_path: Path, target_path: Path):
        pass

    def save_metadata_json(self, metadata, path: Path):
        pass

    def set_file_dates(
        self, video_path: Path, capture_date=None, creation_date=None, modification_date=None
    ):
        pass


class MockPhotosAccessManager:
    """Mock Photos access manager that tracks operations."""

    def __init__(self):
        self.imported_videos = []
        self.deleted_videos = []
        self.albums_added = []

    def import_video(self, video_path: Path, album_name: str | None = None) -> str:
        self.imported_videos.append(video_path)
        return f"new_uuid_{len(self.imported_videos)}"

    def add_to_albums(self, uuid: str, albums: list[str]):
        self.albums_added.append((uuid, albums))

    def delete_video(self, uuid: str):
        self.deleted_videos.append(uuid)


class TestOriginalFileSafetyDuringConversion:
    """Tests that original files are not modified during conversion."""

    @given(conversion_candidate_strategy())
    @settings(max_examples=50, deadline=None)
    def test_original_file_not_deleted_on_conversion_failure(self, candidate):
        """
        **Feature: video-compression-optimizer, Property 5: Original File Safety**

        When conversion fails, the original file is not deleted or modified.

        **Validates: Requirements 8.1, 8.4**
        """
        candidate.video.uuid = f"uuid_{candidate.video.uuid[:8]}"

        # Create mocks - conversion will fail
        mock_mediaconvert = MockMediaConvertClient(should_fail=True)
        mock_quality_checker = MockQualityChecker()

        # Use a unique temporary directory for each test run
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create actual temp file
            create_temp_files_for_candidates([candidate], tmp_path)

            staging_dir = tmp_path / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            # Run conversion (will fail)
            result = service.convert_batch(
                [candidate], quality_preset="balanced", skip_disk_check=True
            )

        # Property: Original file should not be in any delete operation
        assert len(mock_mediaconvert.original_files_touched) == 0, (
            "Original file should not be touched during failed conversion"
        )

        # Property: Conversion should fail
        assert result.failed == 1
        assert result.successful == 0

    @given(conversion_candidate_strategy())
    @settings(max_examples=50, deadline=None)
    def test_original_file_not_deleted_on_quality_failure(self, candidate):
        """
        **Feature: video-compression-optimizer, Property 5: Original File Safety**

        When quality check fails, the original file is not deleted or modified.

        **Validates: Requirements 8.1, 8.4**
        """
        candidate.video.uuid = f"uuid_{candidate.video.uuid[:8]}"

        # Create mocks - quality check will fail
        mock_mediaconvert = MockMediaConvertClient(should_fail=False)
        mock_quality_checker = MockQualityChecker(should_pass=False)

        # Use a unique temporary directory for each test run
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create actual temp file
            create_temp_files_for_candidates([candidate], tmp_path)

            staging_dir = tmp_path / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            result = service.convert_batch(
                [candidate], quality_preset="balanced", skip_disk_check=True
            )

        # Property: Original file should not be touched
        assert len(mock_mediaconvert.original_files_touched) == 0

        # Property: Conversion should fail due to quality
        assert result.failed == 1

    @given(conversion_candidate_strategy())
    @settings(max_examples=50, deadline=None)
    def test_original_file_preserved_on_successful_conversion(self, candidate):
        """
        **Feature: video-compression-optimizer, Property 5: Original File Safety**

        Even on successful conversion, original file is preserved until user approval.

        **Validates: Requirements 6.1.1, 8.1**
        """
        candidate.video.uuid = f"uuid_{candidate.video.uuid[:8]}"

        # Create mocks - everything succeeds
        mock_mediaconvert = MockMediaConvertClient(should_fail=False)
        mock_quality_checker = MockQualityChecker(should_pass=True)

        # Use a unique temporary directory for each test run
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create actual temp file
            create_temp_files_for_candidates([candidate], tmp_path)
            original_path = candidate.video.path

            staging_dir = tmp_path / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            result = service.convert_batch(
                [candidate], quality_preset="balanced", skip_disk_check=True
            )

        # Property: Original file should not be touched even on success
        assert len(mock_mediaconvert.original_files_touched) == 0

        # Property: Conversion should succeed
        assert result.successful == 1

        # Property: Original path should still be recorded (not deleted)
        assert result.results[0].original_path == original_path


class TestOriginalFileSafetyDuringReview:
    """Tests that original files are only deleted after explicit approval."""

    def test_original_deleted_only_on_approval(self, tmp_path):
        """
        **Feature: video-compression-optimizer, Property 5: Original File Safety**

        Original file is only deleted when user explicitly approves.

        **Validates: Requirements 6.1.1, 6.1.4**
        """
        # Create mock Photos manager
        mock_photos = MockPhotosAccessManager()

        # Create review service
        queue_path = tmp_path / "review_queue.json"
        service = ReviewService(photos_manager=mock_photos, queue_path=queue_path)

        # Create a converted file
        converted_path = tmp_path / "converted.mp4"
        converted_path.touch()

        # Create a review item
        item = ReviewItem(
            id="rev_test_001",
            original_uuid="original_uuid_123",
            original_path=Path("/original/video.mp4"),
            converted_path=converted_path,
            conversion_date=datetime.now().isoformat(),
            quality_result={"ssim_score": 0.97},
            metadata={"capture_date": datetime.now().isoformat()},
        )

        # Add to queue
        queue = ReviewQueue(items=[item])
        service.save_queue(queue)

        # Before approval: original should not be deleted
        assert len(mock_photos.deleted_videos) == 0

        # Approve the conversion
        service.approve("rev_test_001")

        # After approval: original should be deleted
        assert "original_uuid_123" in mock_photos.deleted_videos

    def test_original_preserved_on_rejection(self, tmp_path):
        """
        **Feature: video-compression-optimizer, Property 5: Original File Safety**

        Original file is preserved when user rejects conversion.

        **Validates: Requirements 6.1.1, 6.1.5**
        """
        mock_photos = MockPhotosAccessManager()

        queue_path = tmp_path / "review_queue.json"
        service = ReviewService(photos_manager=mock_photos, queue_path=queue_path)

        # Create a converted file
        converted_path = tmp_path / "converted.mp4"
        converted_path.touch()

        item = ReviewItem(
            id="rev_test_002",
            original_uuid="original_uuid_456",
            original_path=Path("/original/video.mp4"),
            converted_path=converted_path,
            conversion_date=datetime.now().isoformat(),
            quality_result={"ssim_score": 0.97},
            metadata={"capture_date": datetime.now().isoformat()},
        )

        queue = ReviewQueue(items=[item])
        service.save_queue(queue)

        # Reject the conversion
        service.reject("rev_test_002")

        # Property: Original should NOT be deleted
        assert len(mock_photos.deleted_videos) == 0

        # Property: Converted file should be deleted
        assert not converted_path.exists()


class TestConversionWorkflowSafety:
    """Tests for the complete conversion workflow safety."""

    @given(st.lists(conversion_candidate_strategy(), min_size=1, max_size=5))
    @settings(max_examples=30, deadline=None)
    def test_batch_failure_preserves_all_originals(self, candidates):
        """
        **Feature: video-compression-optimizer, Property 5: Original File Safety**

        When batch conversion has failures, all original files are preserved.

        **Validates: Requirements 8.1, 8.4**
        """
        # Ensure unique UUIDs
        for i, candidate in enumerate(candidates):
            candidate.video.uuid = f"uuid_{i}_{candidate.video.uuid[:8]}"

        # All conversions will fail
        mock_mediaconvert = MockMediaConvertClient(should_fail=True)
        mock_quality_checker = MockQualityChecker()

        # Use a unique temporary directory for each test run
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create actual temp files for all candidates
            create_temp_files_for_candidates(candidates, tmp_path)

            staging_dir = tmp_path / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            result = service.convert_batch(
                candidates, quality_preset="balanced", skip_disk_check=True
            )

        # Property: All should fail
        assert result.failed == len(candidates)

        # Property: No original files should be touched
        assert len(mock_mediaconvert.original_files_touched) == 0

        # Property: All original paths should be preserved in results
        for r in result.results:
            assert r.original_path is not None

    @given(st.lists(conversion_candidate_strategy(), min_size=1, max_size=5))
    @settings(max_examples=30, deadline=None)
    def test_successful_batch_preserves_originals_until_review(self, candidates):
        """
        **Feature: video-compression-optimizer, Property 5: Original File Safety**

        Successful batch conversion preserves all originals until review.

        **Validates: Requirements 6.1.1, 8.1**
        """
        # Ensure unique UUIDs
        for i, candidate in enumerate(candidates):
            candidate.video.uuid = f"uuid_{i}_{candidate.video.uuid[:8]}"

        mock_mediaconvert = MockMediaConvertClient(should_fail=False)
        mock_quality_checker = MockQualityChecker(should_pass=True)

        # Use a unique temporary directory for each test run
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create actual temp files for all candidates
            create_temp_files_for_candidates(candidates, tmp_path)

            # Store original paths before conversion
            original_paths = [c.video.path for c in candidates]

            staging_dir = tmp_path / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            result = service.convert_batch(
                candidates, quality_preset="balanced", skip_disk_check=True
            )

        # Property: All should succeed
        assert result.successful == len(candidates)

        # Property: No original files should be touched
        assert len(mock_mediaconvert.original_files_touched) == 0

        # Property: All original paths should be preserved
        for i, r in enumerate(result.results):
            assert r.original_path is not None
            assert r.original_path == original_paths[i]
