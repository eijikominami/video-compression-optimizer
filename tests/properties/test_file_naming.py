"""Property-based tests for file naming conventions.

**Property 12: File Naming Convention**
Converted files follow the naming convention: {original_name}_h265.{extension}
This ensures converted files are easily identifiable and don't overwrite originals.

**Validates: Requirements 8.3**
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

# Valid filename characters (excluding extension)
VALID_FILENAME_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"


@st.composite
def video_info_strategy(draw, stem: str = None, extension: str = ".mp4"):
    """Generate random VideoInfo objects with optional specific stem."""
    if stem is None:
        stem = draw(st.text(alphabet=VALID_FILENAME_CHARS, min_size=1, max_size=20))

    uuid = draw(st.text(alphabet="abcdef0123456789", min_size=8, max_size=16))
    filename = f"{stem}{extension}"

    return VideoInfo(
        uuid=uuid,
        filename=filename,
        path=Path(f"/tmp/test/{filename}"),
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
def conversion_candidate_strategy(draw, stem: str = None, extension: str = ".mp4"):
    """Generate random ConversionCandidate objects."""
    video = draw(video_info_strategy(stem=stem, extension=extension))
    return ConversionCandidate(
        video=video,
        estimated_savings_bytes=draw(st.integers(min_value=100000, max_value=video.file_size // 2)),
        estimated_savings_percent=draw(st.floats(min_value=10.0, max_value=60.0)),
        status="pending",
    )


class MockMediaConvertClient:
    """Mock MediaConvert client that tracks S3 keys and file paths."""

    def __init__(self):
        self.uploaded_files = []
        self.submitted_jobs = []
        self.downloaded_files = []
        self.output_s3_keys = []

    def upload_to_s3(self, local_path: Path, s3_key: str):
        self.uploaded_files.append((local_path, s3_key))

    def submit_job(
        self, source_video_uuid: str, source_s3_key: str, output_s3_key: str, quality_preset: str
    ):
        self.submitted_jobs.append(
            {
                "uuid": source_video_uuid,
                "source_s3_key": source_s3_key,
                "output_s3_key": output_s3_key,
                "quality_preset": quality_preset,
            }
        )
        self.output_s3_keys.append(output_s3_key)

        return MagicMock(job_id=f"job_{source_video_uuid}", status="SUBMITTED")

    def wait_for_completion(self, job_id: str, poll_interval: int = 10, timeout: int = 3600):
        return MagicMock(job_id=job_id, status="COMPLETE", error_message=None)

    def download_from_s3(self, s3_key: str, local_path: Path):
        self.downloaded_files.append((s3_key, local_path))
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.touch()

    def delete_from_s3(self, s3_key: str):
        pass

    def estimate_cost(self, duration_seconds: float, resolution: tuple[int, int]) -> float:
        return 0.01


class MockQualityChecker:
    """Mock quality checker."""

    def trigger_quality_check_sync(
        self, original_s3_key: str, converted_s3_key: str, metadata_s3_key: str = None
    ) -> QualityResult:
        return QualityResult(
            job_id="quality_job",
            original_s3_key=original_s3_key,
            converted_s3_key=converted_s3_key,
            status="passed",
            ssim_score=0.97,
            original_size=100000000,
            converted_size=50000000,
            compression_ratio=2.0,
            space_saved_bytes=50000000,
            space_saved_percent=50.0,
            playback_verified=True,
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


class TestFileNamingInConvertService:
    """Property tests for file naming convention in ConvertService."""

    @given(
        stem=st.text(alphabet=VALID_FILENAME_CHARS, min_size=1, max_size=20),
        extension=st.sampled_from([".mp4", ".mov", ".m4v"]),
    )
    @settings(max_examples=50, deadline=None)
    def test_output_s3_key_has_h265_suffix(self, stem: str, extension: str):
        """
        **Feature: video-compression-optimizer, Property 12: File Naming Convention**

        ConvertService generates output S3 key with _h265 suffix.

        **Validates: Requirements 8.3**
        """
        mock_mediaconvert = MockMediaConvertClient()
        mock_quality_checker = MockQualityChecker()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create actual test file with content (required by _check_file_available)
            test_file = Path(tmp_dir) / f"{stem}{extension}"
            test_file.write_bytes(b"dummy video content")

            video = VideoInfo(
                uuid=f"uuid_{stem[:8]}",
                filename=f"{stem}{extension}",
                path=test_file,
                codec="h264",
                resolution=(1920, 1080),
                bitrate=10000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime.now(),
                creation_date=datetime.now(),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

            candidate = ConversionCandidate(
                video=video,
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status="pending",
            )

            staging_dir = Path(tmp_dir) / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            service.convert_batch([candidate], quality_preset="balanced", skip_disk_check=True)

        # Property: Output S3 key should contain _h265.mp4
        assert len(mock_mediaconvert.output_s3_keys) == 1
        output_key = mock_mediaconvert.output_s3_keys[0]
        assert "_h265.mp4" in output_key, f"Output S3 key '{output_key}' should contain '_h265.mp4'"

    @given(
        stem=st.text(alphabet=VALID_FILENAME_CHARS, min_size=1, max_size=20),
        extension=st.sampled_from([".mp4", ".mov", ".m4v"]),
    )
    @settings(max_examples=50, deadline=None)
    def test_output_s3_key_preserves_original_stem(self, stem: str, extension: str):
        """
        **Feature: video-compression-optimizer, Property 12: File Naming Convention**

        ConvertService preserves original file stem in output S3 key.

        **Validates: Requirements 8.3**
        """
        mock_mediaconvert = MockMediaConvertClient()
        mock_quality_checker = MockQualityChecker()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create actual test file with content (required by _check_file_available)
            test_file = Path(tmp_dir) / f"{stem}{extension}"
            test_file.write_bytes(b"dummy video content")

            video = VideoInfo(
                uuid=f"uuid_{stem[:8]}",
                filename=f"{stem}{extension}",
                path=test_file,
                codec="h264",
                resolution=(1920, 1080),
                bitrate=10000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime.now(),
                creation_date=datetime.now(),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

            candidate = ConversionCandidate(
                video=video,
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status="pending",
            )

            staging_dir = Path(tmp_dir) / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            service.convert_batch([candidate], quality_preset="balanced", skip_disk_check=True)

        # Property: Output S3 key should contain original stem
        assert len(mock_mediaconvert.output_s3_keys) == 1
        output_key = mock_mediaconvert.output_s3_keys[0]
        assert stem in output_key, (
            f"Output S3 key '{output_key}' should contain original stem '{stem}'"
        )

    @given(
        stem=st.text(alphabet=VALID_FILENAME_CHARS, min_size=1, max_size=20),
        extension=st.sampled_from([".mp4", ".mov", ".m4v"]),
    )
    @settings(max_examples=50, deadline=None)
    def test_local_output_path_has_h265_suffix(self, stem: str, extension: str):
        """
        **Feature: video-compression-optimizer, Property 12: File Naming Convention**

        ConvertService generates local output path with _h265 suffix.

        **Validates: Requirements 8.3**
        """
        mock_mediaconvert = MockMediaConvertClient()
        mock_quality_checker = MockQualityChecker()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create actual test file with content (required by _check_file_available)
            test_file = Path(tmp_dir) / f"{stem}{extension}"
            test_file.write_bytes(b"dummy video content")

            video = VideoInfo(
                uuid=f"uuid_{stem[:8]}",
                filename=f"{stem}{extension}",
                path=test_file,
                codec="h264",
                resolution=(1920, 1080),
                bitrate=10000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime.now(),
                creation_date=datetime.now(),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

            candidate = ConversionCandidate(
                video=video,
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status="pending",
            )

            staging_dir = Path(tmp_dir) / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            service.convert_batch([candidate], quality_preset="balanced", skip_disk_check=True)

        # Property: Downloaded file path should have _h265.mp4
        assert len(mock_mediaconvert.downloaded_files) == 1
        _, local_path = mock_mediaconvert.downloaded_files[0]
        assert local_path.name == f"{stem}_h265.mp4", (
            f"Local path '{local_path}' should be '{stem}_h265.mp4'"
        )

    @given(
        stem=st.text(alphabet=VALID_FILENAME_CHARS, min_size=1, max_size=20),
        extension=st.sampled_from([".mp4", ".mov", ".m4v"]),
    )
    @settings(max_examples=50, deadline=None)
    def test_local_output_path_in_staging_folder(self, stem: str, extension: str):
        """
        **Feature: video-compression-optimizer, Property 12: File Naming Convention**

        ConvertService places converted file in staging folder.

        **Validates: Requirements 8.3**
        """
        mock_mediaconvert = MockMediaConvertClient()
        mock_quality_checker = MockQualityChecker()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create actual test file with content (required by _check_file_available)
            test_file = Path(tmp_dir) / f"{stem}{extension}"
            test_file.write_bytes(b"dummy video content")

            video = VideoInfo(
                uuid=f"uuid_{stem[:8]}",
                filename=f"{stem}{extension}",
                path=test_file,
                codec="h264",
                resolution=(1920, 1080),
                bitrate=10000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime.now(),
                creation_date=datetime.now(),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

            candidate = ConversionCandidate(
                video=video,
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status="pending",
            )

            staging_folder = Path(tmp_dir) / "staging"
            staging_folder.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_folder,
            )

            service.convert_batch([candidate], quality_preset="balanced", skip_disk_check=True)

            # Property: Downloaded file should be in staging folder
            assert len(mock_mediaconvert.downloaded_files) == 1
            _, local_path = mock_mediaconvert.downloaded_files[0]
            assert local_path.parent == staging_folder, (
                f"Local path parent '{local_path.parent}' should be staging folder '{staging_folder}'"
            )


class TestFileNamingEdgeCases:
    """Tests for edge cases in file naming."""

    def test_already_has_h265_suffix(self):
        """
        **Feature: video-compression-optimizer, Property 12: File Naming Convention**

        Files already ending in _h265 get another _h265 suffix.

        **Validates: Requirements 8.3**
        """
        mock_mediaconvert = MockMediaConvertClient()
        mock_quality_checker = MockQualityChecker()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create actual test file with content (required by _check_file_available)
            test_file = Path(tmp_dir) / "video_h265.mp4"
            test_file.write_bytes(b"dummy video content")

            video = VideoInfo(
                uuid="uuid_test",
                filename="video_h265.mp4",
                path=test_file,
                codec="h264",
                resolution=(1920, 1080),
                bitrate=10000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime.now(),
                creation_date=datetime.now(),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

            candidate = ConversionCandidate(
                video=video,
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status="pending",
            )

            staging_dir = Path(tmp_dir) / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            service.convert_batch([candidate], quality_preset="balanced", skip_disk_check=True)

        # Property: Should add another _h265
        assert len(mock_mediaconvert.downloaded_files) == 1
        _, local_path = mock_mediaconvert.downloaded_files[0]
        assert local_path.name == "video_h265_h265.mp4"

    def test_numeric_filename(self):
        """
        **Feature: video-compression-optimizer, Property 12: File Naming Convention**

        Numeric filenames are handled correctly.

        **Validates: Requirements 8.3**
        """
        mock_mediaconvert = MockMediaConvertClient()
        mock_quality_checker = MockQualityChecker()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create actual test file with content (required by _check_file_available)
            test_file = Path(tmp_dir) / "12345.mov"
            test_file.write_bytes(b"dummy video content")

            video = VideoInfo(
                uuid="uuid_12345",
                filename="12345.mov",
                path=test_file,
                codec="h264",
                resolution=(1920, 1080),
                bitrate=10000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime.now(),
                creation_date=datetime.now(),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

            candidate = ConversionCandidate(
                video=video,
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status="pending",
            )

            staging_dir = Path(tmp_dir) / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            service.convert_batch([candidate], quality_preset="balanced", skip_disk_check=True)

        assert len(mock_mediaconvert.downloaded_files) == 1
        _, local_path = mock_mediaconvert.downloaded_files[0]
        assert local_path.name == "12345_h265.mp4"

    def test_single_character_filename(self):
        """
        **Feature: video-compression-optimizer, Property 12: File Naming Convention**

        Single character filenames are handled correctly.

        **Validates: Requirements 8.3**
        """
        mock_mediaconvert = MockMediaConvertClient()
        mock_quality_checker = MockQualityChecker()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create actual test file with content (required by _check_file_available)
            test_file = Path(tmp_dir) / "a.mp4"
            test_file.write_bytes(b"dummy video content")

            video = VideoInfo(
                uuid="uuid_a",
                filename="a.mp4",
                path=test_file,
                codec="h264",
                resolution=(1920, 1080),
                bitrate=10000000,
                duration=60.0,
                frame_rate=30.0,
                file_size=100000000,
                capture_date=datetime.now(),
                creation_date=datetime.now(),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

            candidate = ConversionCandidate(
                video=video,
                estimated_savings_bytes=50000000,
                estimated_savings_percent=50.0,
                status="pending",
            )

            staging_dir = Path(tmp_dir) / "staging"
            staging_dir.mkdir()

            service = ConvertService(
                mediaconvert_client=mock_mediaconvert,
                quality_checker=mock_quality_checker,
                metadata_manager=MockMetadataManager(),
                staging_folder=staging_dir,
            )

            service.convert_batch([candidate], quality_preset="balanced", skip_disk_check=True)

        assert len(mock_mediaconvert.downloaded_files) == 1
        _, local_path = mock_mediaconvert.downloaded_files[0]
        assert local_path.name == "a_h265.mp4"
