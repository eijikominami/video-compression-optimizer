"""Unit tests for AsyncConvertCommand.

Tests: Task 9.3 - AsyncConvertCommand unit tests
Requirements: 1.1, 1.2, 1.4
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from vco.services.async_convert import AsyncConvertCommand


@dataclass
class MockVideo:
    """Mock video object for testing."""

    uuid: str = "video-uuid-123"
    filename: str = "test_video.mov"
    path: Path = Path("/tmp/test_video.mov")
    file_size: int = 1024 * 1024
    capture_date: datetime = None
    creation_date: datetime = None
    location: tuple = None
    albums: list = None


@dataclass
class MockCandidate:
    """Mock conversion candidate for testing."""

    video: MockVideo = None

    def __post_init__(self):
        if self.video is None:
            self.video = MockVideo()


class TestAsyncConvertCommandInit:
    """Tests for AsyncConvertCommand initialization."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        with patch("boto3.Session"):
            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
            )
            assert cmd.api_url == "https://api.example.com"
            assert cmd.s3_bucket == "test-bucket"
            assert cmd.region == "ap-northeast-1"

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped from API URL."""
        with patch("boto3.Session"):
            cmd = AsyncConvertCommand(
                api_url="https://api.example.com/",
                s3_bucket="test-bucket",
            )
            assert cmd.api_url == "https://api.example.com"


class TestExecuteEmptyCandidates:
    """Tests for execute with empty candidates."""

    def test_empty_candidates_returns_error(self):
        """Test that empty candidates returns error result."""
        with patch("boto3.Session"):
            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
            )
            result = cmd.execute(candidates=[])
            assert result.status == "ERROR"
            assert result.file_count == 0
            assert "No candidates" in result.message


class TestExecuteICloudOnly:
    """Tests for execute with iCloud-only files."""

    def test_all_icloud_files_returns_error(self):
        """Test that all iCloud files returns error."""
        with patch("boto3.Session"):
            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
            )
            cmd._check_file_available = MagicMock(return_value=False)
            candidates = [MockCandidate()]
            result = cmd.execute(candidates)
            assert result.status == "ERROR"
            assert "iCloud" in result.error_message


class TestExecuteWithCandidates:
    """Tests for execute with valid candidates."""

    def test_successful_submission(self, tmp_path):
        """Test successful task submission."""
        test_file = tmp_path / "test.mov"
        test_file.write_bytes(b"x" * 1024)

        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_session.return_value.client.return_value = mock_s3

            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
            )
            cmd._submit_task = MagicMock(return_value={"task_id": "t1"})
            cmd.metadata_manager = MagicMock()
            cmd.metadata_manager.extract_metadata.return_value = MagicMock(
                capture_date=None, creation_date=None, location=None, albums=[]
            )

            video = MockVideo(path=test_file, file_size=1024)
            candidate = MockCandidate(video=video)

            result = cmd.execute([candidate], quality_preset="balanced")

            assert result.status == "PENDING"
            assert result.file_count == 1
            assert result.task_id != ""

    def test_icloud_only_files_skipped(self, tmp_path):
        """Test that iCloud-only files are skipped."""
        with patch("boto3.Session"):
            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
            )

            video = MockVideo(path=Path("/nonexistent/file.mov"))
            candidate = MockCandidate(video=video)

            result = cmd.execute([candidate])

            assert result.status == "ERROR"
            assert "iCloud" in result.error_message

    def test_upload_error_triggers_cleanup(self, tmp_path):
        """Test that upload error triggers cleanup."""
        test_file = tmp_path / "test.mov"
        test_file.write_bytes(b"x" * 1024)

        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.upload_file.side_effect = Exception("Upload failed")
            mock_session.return_value.client.return_value = mock_s3

            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
            )
            cmd._cleanup_task_files = MagicMock()

            video = MockVideo(path=test_file)
            result = cmd.execute([MockCandidate(video=video)])

            assert result.status == "ERROR"
            cmd._cleanup_task_files.assert_called_once()

    def test_api_error_triggers_cleanup(self, tmp_path):
        """Test that API error triggers cleanup."""
        test_file = tmp_path / "test.mov"
        test_file.write_bytes(b"x" * 1024)

        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_session.return_value.client.return_value = mock_s3

            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
            )
            cmd._submit_task = MagicMock(side_effect=Exception("API error"))
            cmd._cleanup_task_files = MagicMock()
            cmd.metadata_manager = MagicMock()
            cmd.metadata_manager.extract_metadata.return_value = MagicMock(
                capture_date=None, creation_date=None, location=None, albums=[]
            )

            video = MockVideo(path=test_file, file_size=1024)
            result = cmd.execute([MockCandidate(video=video)])

            assert result.status == "ERROR"
            cmd._cleanup_task_files.assert_called_once()


class TestHelperMethods:
    """Tests for helper methods."""

    def test_check_file_available_exists(self, tmp_path):
        """Test _check_file_available with existing file."""
        test_file = tmp_path / "test.mov"
        test_file.write_bytes(b"x" * 100)
        with patch("boto3.Session"):
            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="b",
            )
            assert cmd._check_file_available(test_file) is True

    def test_check_file_available_not_exists(self):
        """Test _check_file_available with non-existing file."""
        with patch("boto3.Session"):
            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="b",
            )
            assert cmd._check_file_available(Path("/nonexistent")) is False

    def test_get_machine_id_returns_string(self):
        """Test _get_machine_id returns valid string."""
        with patch("boto3.Session"):
            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="b",
            )
            machine_id = cmd._get_machine_id()
            assert isinstance(machine_id, str)
            assert len(machine_id) == 32

    def test_cleanup_task_files(self):
        """Test _cleanup_task_files deletes S3 objects."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_paginator = MagicMock()
            mock_paginator.paginate.return_value = [
                {"Contents": [{"Key": "async/t1/f1"}, {"Key": "async/t1/f2"}]}
            ]
            mock_s3.get_paginator.return_value = mock_paginator
            mock_session.return_value.client.return_value = mock_s3

            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="b",
            )
            cmd._cleanup_task_files("t1")

            mock_s3.delete_objects.assert_called_once()


class TestProgressCallback:
    """Tests for progress callback functionality."""

    def test_progress_callback_called(self, tmp_path):
        """Test that progress callback is called during execution."""
        test_file = tmp_path / "test.mov"
        test_file.write_bytes(b"x" * 1024)
        progress_updates = []

        def callback(progress):
            progress_updates.append(progress)

        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_session.return_value.client.return_value = mock_s3

            cmd = AsyncConvertCommand(
                api_url="https://api.example.com",
                s3_bucket="b",
                progress_callback=callback,
            )
            cmd._submit_task = MagicMock(return_value={"task_id": "t1"})
            cmd.metadata_manager = MagicMock()
            cmd.metadata_manager.extract_metadata.return_value = MagicMock(
                capture_date=None, creation_date=None, location=None, albums=[]
            )

            video = MockVideo(path=test_file, file_size=1024)
            cmd.execute([MockCandidate(video=video)])

            assert len(progress_updates) > 0
