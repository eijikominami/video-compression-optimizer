"""Property-based tests for batch processing error resilience.

**Property 8: Batch Processing Error Resilience**
For any batch of videos being processed, if an error occurs during conversion
of one video, the system continues processing remaining videos and includes
all errors in the final report.

**Validates: Requirements 5.5, 2.4**
"""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from vco.analyzer.analyzer import ConversionCandidate
from vco.models.types import VideoInfo
from vco.quality.checker import QualityResult
from vco.services.convert import ConvertService


# Test strategies
def create_video_info_with_temp_file(
    tmp_dir: Path, uuid: str, filename: str, duration: float, file_size: int
) -> VideoInfo:
    """Create VideoInfo with an actual temp file."""
    file_path = tmp_dir / filename
    file_path.touch()

    return VideoInfo(
        uuid=uuid,
        filename=filename,
        path=file_path,
        codec="h264",
        resolution=(1920, 1080),
        bitrate=10000000,
        duration=duration,
        frame_rate=30.0,
        file_size=file_size,
        capture_date=datetime.now(),
        creation_date=datetime.now(),
        albums=["Test Album"],
        is_in_icloud=False,
        is_local=True,
    )


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
def batch_with_failures_strategy(draw):
    """Generate a batch of candidates with specified failure indices."""
    batch_size = draw(st.integers(min_value=2, max_value=10))
    candidates = [draw(conversion_candidate_strategy()) for _ in range(batch_size)]

    # Ensure unique UUIDs
    for i, candidate in enumerate(candidates):
        candidate.video.uuid = f"uuid_{i}_{candidate.video.uuid[:8]}"

    # Select which indices should fail (at least one, but not all)
    num_failures = draw(st.integers(min_value=1, max_value=max(1, batch_size - 1)))
    failure_indices = draw(
        st.lists(
            st.integers(min_value=0, max_value=batch_size - 1),
            min_size=num_failures,
            max_size=num_failures,
            unique=True,
        )
    )

    return candidates, set(failure_indices)


class MockMediaConvertClient:
    """Mock MediaConvert client for testing."""

    def __init__(self, failure_uuids: set[str] = None):
        self.failure_uuids = failure_uuids or set()
        self.uploaded_files = []
        self.submitted_jobs = []
        self.deleted_files = []

    def upload_to_s3(self, local_path: Path, s3_key: str):
        self.uploaded_files.append(s3_key)

    def submit_job(
        self, source_video_uuid: str, source_s3_key: str, output_s3_key: str, quality_preset: str
    ):
        self.submitted_jobs.append(source_video_uuid)

        if source_video_uuid in self.failure_uuids:
            raise Exception(f"Simulated MediaConvert failure for {source_video_uuid}")

        return MagicMock(job_id=f"job_{source_video_uuid}", status="SUBMITTED")

    def wait_for_completion(self, job_id: str, poll_interval: int = 10, timeout: int = 3600):
        return MagicMock(job_id=job_id, status="COMPLETE", error_message=None)

    def download_from_s3(self, s3_key: str, local_path: Path):
        # Create a dummy file
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.touch()

    def delete_from_s3(self, s3_key: str):
        self.deleted_files.append(s3_key)

    def estimate_cost(self, duration_seconds: float, resolution: tuple[int, int]) -> float:
        return 0.01


class MockMetadataManager:
    """Mock metadata manager for testing."""

    def extract_metadata(self, video_path: Path):
        """Return mock metadata without checking file existence."""
        from vco.metadata.manager import VideoMetadata

        return VideoMetadata(capture_date=datetime.now(), creation_date=datetime.now(), albums=[])

    def apply_metadata(self, video_path: Path, metadata):
        """Mock apply metadata - do nothing."""
        pass

    def copy_dates_from_original(self, original_path: Path, target_path: Path):
        """Mock copy dates - do nothing."""
        pass

    def save_metadata_json(self, metadata, path: Path):
        """Mock save metadata JSON - do nothing."""
        pass

    def set_file_dates(
        self, video_path: Path, capture_date=None, creation_date=None, modification_date=None
    ):
        """Mock set file dates - do nothing."""
        pass


class MockQualityChecker:
    """Mock quality checker for testing."""

    def __init__(self, failure_uuids: set[str] = None):
        self.failure_uuids = failure_uuids or set()

    def trigger_quality_check_sync(
        self, original_s3_key: str, converted_s3_key: str, metadata_s3_key: str = None
    ) -> QualityResult:
        # Extract UUID from S3 key
        uuid = original_s3_key.split("/")[1]

        if uuid in self.failure_uuids:
            return QualityResult(
                job_id=f"quality_{uuid}",
                original_s3_key=original_s3_key,
                converted_s3_key=converted_s3_key,
                status="failed",  # Use 'failed' status instead of is_acceptable=False
                ssim_score=0.90,  # Below threshold
                original_size=100000000,
                converted_size=90000000,
                compression_ratio=1.11,
                space_saved_bytes=10000000,
                space_saved_percent=10.0,
                playback_verified=True,
                failure_reason="SSIM score below threshold",
            )

        return QualityResult(
            job_id=f"quality_{uuid}",
            original_s3_key=original_s3_key,
            converted_s3_key=converted_s3_key,
            status="passed",  # Use 'passed' status for acceptable
            ssim_score=0.97,
            original_size=100000000,
            converted_size=50000000,
            compression_ratio=2.0,
            space_saved_bytes=50000000,
            space_saved_percent=50.0,
            playback_verified=True,
        )


class TestBatchErrorResilience:
    """Property tests for batch processing error resilience."""

    @given(batch_with_failures_strategy())
    @settings(max_examples=100, deadline=None)
    def test_continues_after_mediaconvert_failure(self, batch_data):
        """
        **Feature: video-compression-optimizer, Property 8: Batch Processing Error Resilience**

        For any batch of videos, if MediaConvert fails for some videos,
        the system continues processing remaining videos.

        **Validates: Requirements 5.5, 2.4**
        """
        candidates, failure_indices = batch_data
        assume(len(candidates) >= 2)
        assume(len(failure_indices) < len(candidates))  # At least one should succeed

        # Get UUIDs that should fail
        failure_uuids = {candidates[i].video.uuid for i in failure_indices}

        # Create mocks
        mock_mediaconvert = MockMediaConvertClient(failure_uuids=failure_uuids)
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

            # Run batch conversion
            result = service.convert_batch(
                candidates, quality_preset="balanced", skip_disk_check=True
            )

        # Property: Total results should equal total candidates
        assert result.total == len(candidates), (
            f"Total should be {len(candidates)}, got {result.total}"
        )

        # Property: All candidates should have a result
        assert len(result.results) == len(candidates), (
            f"Should have {len(candidates)} results, got {len(result.results)}"
        )

        # Property: Failed count should match expected failures
        assert result.failed == len(failure_indices), (
            f"Failed should be {len(failure_indices)}, got {result.failed}"
        )

        # Property: Successful count should be total minus failures
        expected_success = len(candidates) - len(failure_indices)
        assert result.successful == expected_success, (
            f"Successful should be {expected_success}, got {result.successful}"
        )

        # Property: All errors should be recorded
        assert len(result.errors) == len(failure_indices), (
            f"Should have {len(failure_indices)} errors, got {len(result.errors)}"
        )

        # Property: Each failed video should have an error message
        for r in result.results:
            if not r.success:
                assert r.error_message is not None, (
                    f"Failed result for {r.uuid} should have error message"
                )

    @given(batch_with_failures_strategy())
    @settings(max_examples=100, deadline=None)
    def test_continues_after_quality_check_failure(self, batch_data):
        """
        **Feature: video-compression-optimizer, Property 8: Batch Processing Error Resilience**

        For any batch of videos, if quality check fails for some videos,
        the system continues processing remaining videos.

        **Validates: Requirements 5.5, 2.4**
        """
        candidates, failure_indices = batch_data
        assume(len(candidates) >= 2)
        assume(len(failure_indices) < len(candidates))

        # Get UUIDs that should fail quality check
        failure_uuids = {candidates[i].video.uuid for i in failure_indices}

        # Create mocks - MediaConvert succeeds, quality check fails
        mock_mediaconvert = MockMediaConvertClient()
        mock_quality_checker = MockQualityChecker(failure_uuids=failure_uuids)

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

        # Property: All candidates should be processed
        assert result.total == len(candidates)
        assert len(result.results) == len(candidates)

        # Property: Failed count should match quality failures
        assert result.failed == len(failure_indices)

        # Property: Successful count should be total minus failures
        expected_success = len(candidates) - len(failure_indices)
        assert result.successful == expected_success

    @given(st.lists(conversion_candidate_strategy(), min_size=1, max_size=5))
    @settings(max_examples=50, deadline=None)
    def test_all_success_no_errors(self, candidates):
        """
        **Feature: video-compression-optimizer, Property 8: Batch Processing Error Resilience**

        For any batch where all conversions succeed, there should be no errors.

        **Validates: Requirements 5.5**
        """
        # Ensure unique UUIDs
        for i, candidate in enumerate(candidates):
            candidate.video.uuid = f"uuid_{i}_{candidate.video.uuid[:8]}"

        # Create mocks with no failures
        mock_mediaconvert = MockMediaConvertClient()
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

        # Property: All should succeed
        assert result.successful == len(candidates)
        assert result.failed == 0
        assert len(result.errors) == 0

        # Property: All results should be successful
        for r in result.results:
            assert r.success, f"Result for {r.uuid} should be successful"

    @given(st.lists(conversion_candidate_strategy(), min_size=1, max_size=5))
    @settings(max_examples=50, deadline=None)
    def test_all_fail_all_errors_recorded(self, candidates):
        """
        **Feature: video-compression-optimizer, Property 8: Batch Processing Error Resilience**

        For any batch where all conversions fail, all errors should be recorded.

        **Validates: Requirements 5.5, 2.4**
        """
        # Ensure unique UUIDs
        for i, candidate in enumerate(candidates):
            candidate.video.uuid = f"uuid_{i}_{candidate.video.uuid[:8]}"

        # All UUIDs should fail
        all_uuids = {c.video.uuid for c in candidates}

        mock_mediaconvert = MockMediaConvertClient(failure_uuids=all_uuids)
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
        assert result.successful == 0

        # Property: All errors should be recorded
        assert len(result.errors) == len(candidates)

        # Property: All results should have error messages
        for r in result.results:
            assert not r.success
            assert r.error_message is not None

    @given(batch_with_failures_strategy())
    @settings(max_examples=50, deadline=None)
    def test_result_counts_are_consistent(self, batch_data):
        """
        **Feature: video-compression-optimizer, Property 8: Batch Processing Error Resilience**

        For any batch, successful + failed should equal total.

        **Validates: Requirements 5.5**
        """
        candidates, failure_indices = batch_data

        # Ensure unique UUIDs
        for i, candidate in enumerate(candidates):
            candidate.video.uuid = f"uuid_{i}_{candidate.video.uuid[:8]}"

        failure_uuids = {candidates[i].video.uuid for i in failure_indices}

        mock_mediaconvert = MockMediaConvertClient(failure_uuids=failure_uuids)
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

        # Property: Counts should be consistent
        assert result.successful + result.failed == result.total, (
            f"successful ({result.successful}) + failed ({result.failed}) != total ({result.total})"
        )

        # Property: Results count should match total
        assert len(result.results) == result.total

    @given(batch_with_failures_strategy())
    @settings(max_examples=50, deadline=None)
    def test_error_messages_contain_filename(self, batch_data):
        """
        **Feature: video-compression-optimizer, Property 8: Batch Processing Error Resilience**

        For any failed conversion, the error message should contain the filename.

        **Validates: Requirements 5.5, 2.4**
        """
        candidates, failure_indices = batch_data
        assume(len(failure_indices) > 0)

        # Ensure unique UUIDs
        for i, candidate in enumerate(candidates):
            candidate.video.uuid = f"uuid_{i}_{candidate.video.uuid[:8]}"

        failure_uuids = {candidates[i].video.uuid for i in failure_indices}
        failed_filenames = {candidates[i].video.filename for i in failure_indices}

        mock_mediaconvert = MockMediaConvertClient(failure_uuids=failure_uuids)
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

        # Property: Each error should contain the filename
        for error in result.errors:
            found_filename = any(fn in error for fn in failed_filenames)
            assert found_filename, f"Error '{error}' should contain one of the failed filenames"


class TestDryRunMode:
    """Tests for dry run mode."""

    @given(st.lists(conversion_candidate_strategy(), min_size=1, max_size=5))
    @settings(max_examples=50, deadline=None)
    def test_dry_run_does_not_process(self, candidates):
        """
        **Feature: video-compression-optimizer, Property 8: Batch Processing Error Resilience**

        In dry run mode, no actual processing should occur.

        **Validates: Requirements 5.5**
        """
        # Ensure unique UUIDs
        for i, candidate in enumerate(candidates):
            candidate.video.uuid = f"uuid_{i}_{candidate.video.uuid[:8]}"

        mock_mediaconvert = MockMediaConvertClient()
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

            result = service.convert_batch(candidates, quality_preset="balanced", dry_run=True)

        # Property: No files should be uploaded
        assert len(mock_mediaconvert.uploaded_files) == 0

        # Property: No jobs should be submitted
        assert len(mock_mediaconvert.submitted_jobs) == 0

        # Property: Result should show total but no processing
        assert result.total == len(candidates)
        assert result.successful == 0
        assert result.failed == 0
