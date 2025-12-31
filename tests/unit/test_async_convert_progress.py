"""Tests for async convert progress reporting consistency.

This test verifies that progress callbacks use consistent filenames,
specifically testing the case where video.filename differs from video.path.name.

In Apple Photos library, the original filename (e.g., "MVI_8155.MOV") is stored
separately from the internal path (e.g., ".../9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov").
Progress reporting should always use the user-friendly original filename.

Requirements: Testing Guidelines - テストデータの現実性
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from vco.models.types import ConversionCandidate, VideoInfo, VideoStatus
from vco.services.async_convert import AsyncConvertCommand, UploadProgress


def create_photos_library_video(
    uuid: str = "9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3",
    original_filename: str = "MVI_8155.MOV",
    internal_path_name: str | None = None,
) -> VideoInfo:
    """Create a VideoInfo that mimics Apple Photos library behavior.

    In Photos library:
    - filename: Original filename from camera (e.g., "MVI_8155.MOV")
    - path: Internal Photos library path using UUID (e.g., ".../9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov")

    These are often different, which is a realistic data pattern that tests should cover.

    Args:
        uuid: Photos library UUID
        original_filename: Original filename from camera
        internal_path_name: Internal path filename (defaults to UUID-based name)

    Returns:
        VideoInfo with realistic Photos library data pattern
    """
    if internal_path_name is None:
        # Photos library uses UUID as internal filename
        internal_path_name = f"{uuid}.mov"

    return VideoInfo(
        uuid=uuid,
        filename=original_filename,  # Original name from camera
        path=Path(
            f"/Users/test/Pictures/Photos Library.photoslibrary/originals/{internal_path_name}"
        ),
        codec="h264",
        resolution=(1920, 1080),
        bitrate=25000000,
        duration=120.5,
        frame_rate=30.0,
        file_size=375000000,
        capture_date=datetime(2020, 7, 15, 14, 30, 0),
        creation_date=datetime(2020, 7, 15, 14, 30, 0),
        albums=["Vacation 2020"],
        is_in_icloud=False,
        is_local=True,
    )


class TestProgressCallbackFilenameConsistency:
    """Test that progress callbacks use consistent filenames.

    This addresses the bug where progress display showed different filenames
    because _upload_file used path.name instead of video.filename.
    """

    def test_progress_callback_uses_original_filename_not_path_name(self, tmp_path):
        """Verify progress callback uses video.filename, not video.path.name.

        This is the key test for the bug where:
        - video.filename = "MVI_8155.MOV" (original camera name)
        - video.path.name = "9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov" (internal UUID)

        Progress should always show "MVI_8155.MOV" to the user.
        """
        # Track all filenames reported in progress callbacks
        reported_filenames: list[str] = []

        def progress_callback(progress: UploadProgress):
            reported_filenames.append(progress.filename)

        # Create a real file for the test with UUID-based name
        test_file = tmp_path / "9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov"
        test_file.write_bytes(b"x" * 1000)

        # Create video with different filename and path.name (realistic Photos data)
        video = VideoInfo(
            uuid="9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3",
            filename="MVI_8155.MOV",  # Original camera name
            path=test_file,  # Path has UUID-based name
            codec="h264",
            resolution=(1920, 1080),
            bitrate=25000000,
            duration=120.5,
            frame_rate=30.0,
            file_size=1000,
            capture_date=datetime(2020, 7, 15, 14, 30, 0),
            creation_date=datetime(2020, 7, 15, 14, 30, 0),
            albums=["Vacation 2020"],
            is_in_icloud=False,
            is_local=True,
        )

        # Verify mismatch exists - this is the realistic pattern
        assert video.filename == "MVI_8155.MOV"
        assert video.path.name == "9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov"
        assert video.filename != video.path.name

        candidate = ConversionCandidate(
            video=video,
            status=VideoStatus.PENDING,
            estimated_savings_bytes=100000000,
            estimated_savings_percent=30.0,
        )

        # Mock S3 client to simulate upload with progress callbacks
        mock_s3_client = MagicMock()

        def mock_upload_file(local_path, bucket, key, Callback=None):  # noqa: N803
            if Callback:
                Callback(500)
                Callback(1000)

        mock_s3_client.upload_file = mock_upload_file

        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3_client
        mock_session.get_credentials.return_value = MagicMock(
            access_key="test", secret_key="test", token=None
        )

        with patch("boto3.Session", return_value=mock_session):
            with patch("requests.post") as mock_post:
                mock_response = MagicMock()
                mock_response.json.return_value = {"task_id": "test-task"}
                mock_response.raise_for_status = MagicMock()
                mock_post.return_value = mock_response

                command = AsyncConvertCommand(
                    api_url="https://test.execute-api.ap-northeast-1.amazonaws.com",
                    s3_bucket="test-bucket",
                    progress_callback=progress_callback,
                )

                command.execute([candidate])

        # All progress callbacks should use the original filename
        assert len(reported_filenames) > 0, "Progress callback should have been called"

        for filename in reported_filenames:
            assert filename == "MVI_8155.MOV", (
                f"Progress callback should use original filename 'MVI_8155.MOV', "
                f"not internal path name. Got: '{filename}'"
            )

        # Specifically verify it's NOT using the path name
        for filename in reported_filenames:
            assert filename != "9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov", (
                "Progress callback should NOT use internal UUID-based path name"
            )

    def test_progress_callback_consistency_across_multiple_files(self, tmp_path):
        """Verify all files in a batch use their original filenames consistently."""
        reported_filenames: list[str] = []

        def progress_callback(progress: UploadProgress):
            reported_filenames.append(progress.filename)

        # Create multiple videos with different filename/path patterns
        test_cases = [
            ("MVI_8155.MOV", "9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov"),
            ("IMG_0001.MOV", "A1B2C3D4-E5F6-7890-ABCD-EF1234567890.mov"),
            ("vacation_2020.mp4", "DEADBEEF-CAFE-BABE-FEED-FACE12345678.mp4"),
        ]

        candidates = []
        for i, (original_name, internal_name) in enumerate(test_cases):
            test_file = tmp_path / internal_name
            test_file.write_bytes(b"x" * 1000)

            video = VideoInfo(
                uuid=f"uuid-{i}",
                filename=original_name,
                path=test_file,
                codec="h264",
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=120.5,
                frame_rate=30.0,
                file_size=1000,
                capture_date=datetime(2020, 7, 15),
                creation_date=datetime(2020, 7, 15),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

            candidates.append(
                ConversionCandidate(
                    video=video,
                    status=VideoStatus.PENDING,
                    estimated_savings_bytes=100,
                    estimated_savings_percent=10.0,
                )
            )

        # Mock S3 client
        mock_s3_client = MagicMock()

        def mock_upload_file(local_path, bucket, key, Callback=None):  # noqa: N803
            if Callback:
                Callback(500)
                Callback(1000)

        mock_s3_client.upload_file = mock_upload_file

        mock_session = MagicMock()
        mock_session.client.return_value = mock_s3_client
        mock_session.get_credentials.return_value = MagicMock(
            access_key="test", secret_key="test", token=None
        )

        with patch("boto3.Session", return_value=mock_session):
            with patch("requests.post") as mock_post:
                mock_response = MagicMock()
                mock_response.json.return_value = {"task_id": "test-task"}
                mock_response.raise_for_status = MagicMock()
                mock_post.return_value = mock_response

                command = AsyncConvertCommand(
                    api_url="https://test.execute-api.ap-northeast-1.amazonaws.com",
                    s3_bucket="test-bucket",
                    progress_callback=progress_callback,
                )

                command.execute(candidates)

        # Verify only original filenames are used
        expected_filenames = {"MVI_8155.MOV", "IMG_0001.MOV", "vacation_2020.mp4"}
        unexpected_filenames = {
            "9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov",
            "A1B2C3D4-E5F6-7890-ABCD-EF1234567890.mov",
            "DEADBEEF-CAFE-BABE-FEED-FACE12345678.mp4",
        }

        for filename in reported_filenames:
            assert filename in expected_filenames, (
                f"Unexpected filename in progress: '{filename}'. "
                f"Expected one of: {expected_filenames}"
            )
            assert filename not in unexpected_filenames, (
                f"Progress should not use internal path name: '{filename}'"
            )


class TestPhotosLibraryDataPatterns:
    """Tests verifying realistic Photos library data patterns are handled correctly.

    These tests document the expected behavior when filename and path.name differ,
    which is common in Apple Photos library.
    """

    def test_video_info_can_have_different_filename_and_path_name(self):
        """Document that VideoInfo.filename can differ from VideoInfo.path.name.

        This is the realistic data pattern from Apple Photos library that
        caused the original bug.
        """
        video = create_photos_library_video(
            original_filename="MVI_8155.MOV",
            internal_path_name="9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov",
        )

        # These should be different - this is the realistic pattern
        assert video.filename == "MVI_8155.MOV"
        assert video.path.name == "9FCB3EC2-B8F5-452F-AC8F-E3159936A4E3.mov"
        assert video.filename != video.path.name

    def test_filename_is_user_friendly_name(self):
        """Verify filename is the user-friendly original name."""
        video = create_photos_library_video(original_filename="Family_Vacation_2020.MOV")

        # filename should be the human-readable name
        assert "Family_Vacation" in video.filename
        assert "UUID" not in video.filename.upper()

    def test_path_name_may_be_uuid_based(self):
        """Verify path.name may be UUID-based internal name."""
        video = create_photos_library_video(
            uuid="ABCD1234-5678-90EF-GHIJ-KLMNOPQRSTUV",
            internal_path_name="ABCD1234-5678-90EF-GHIJ-KLMNOPQRSTUV.mov",
        )

        # path.name may be UUID-based
        assert "ABCD1234" in video.path.name
