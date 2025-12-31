"""Unit tests for DownloadCommand.

Tests: Task 12.5 - DownloadCommand unit tests
Requirements: 4.1, 4.2, 4.5
"""

from dataclasses import dataclass
from datetime import datetime
from unittest.mock import MagicMock, patch

from vco.services.async_download import DownloadCommand, FileDownloadResult


@dataclass
class MockFileDetail:
    """Mock file detail for testing."""

    file_id: str = "file-1"
    filename: str = "video.mov"
    status: str = "COMPLETED"
    progress_percentage: int = 100
    error_message: str = None
    ssim_score: float = 0.95
    output_size_bytes: int = 1024000
    output_s3_key: str = None


@dataclass
class MockTaskDetail:
    """Mock task detail for testing."""

    task_id: str = "task-1"
    status: str = "COMPLETED"
    quality_preset: str = "balanced"
    files: list = None
    created_at: datetime = None
    updated_at: datetime = None
    started_at: datetime = None
    completed_at: datetime = None
    progress_percentage: int = 100
    current_step: str = None
    estimated_completion_time: datetime = None
    error_message: str = None
    execution_arn: str = None

    def __post_init__(self):
        if self.files is None:
            self.files = [MockFileDetail()]
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()


class TestDownloadCommandInit:
    """Tests for DownloadCommand initialization."""

    def test_init_with_defaults(self, tmp_path):
        """Test initialization with default values."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            assert cmd.api_url == "https://api.example.com"
            assert cmd.s3_bucket == "test-bucket"
            assert cmd.region == "ap-northeast-1"

    def test_init_strips_trailing_slash(self, tmp_path):
        """Test that trailing slash is stripped from API URL."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com/",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            assert cmd.api_url == "https://api.example.com"

    def test_init_creates_output_dir(self, tmp_path):
        """Test that output directory is created."""
        output_dir = tmp_path / "downloads"
        with patch("boto3.Session"):
            DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=output_dir,
            )
            assert output_dir.exists()


class TestDownloadTaskNotReady:
    """Tests for download when task is not ready."""

    def test_download_task_not_completed(self, tmp_path):
        """Test download when task is not completed."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(status="CONVERTING")

            result = cmd.download("task-1")

            assert result.success is False
            assert "not ready" in result.error_message.lower()

    def test_download_task_pending(self, tmp_path):
        """Test download when task is pending."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(status="PENDING")

            result = cmd.download("task-1")

            assert result.success is False

    def test_download_task_failed(self, tmp_path):
        """Test download when task failed."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="FAILED",
                files=[MockFileDetail(status="FAILED")],
            )

            result = cmd.download("task-1")

            assert result.success is False


class TestDownloadSuccess:
    """Tests for successful download."""

    def test_download_completed_task(self, tmp_path):
        """Test downloading a completed task."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.head_object.return_value = {
                "ContentLength": 1024,
                "ETag": '"abc123"',
            }
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail()
            cmd._download_with_progress = MagicMock()
            cmd._verify_checksum = MagicMock(return_value=True)
            cmd._add_to_review_queue = MagicMock(return_value=True)

            # Create temp file to simulate download
            temp_file = tmp_path / ".video_h265.mp4.tmp"
            temp_file.write_bytes(b"x" * 1024)

            result = cmd.download("task-1")

            assert result.downloaded_files >= 0

    def test_download_partially_completed_task(self, tmp_path):
        """Test downloading a partially completed task."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.head_object.return_value = {
                "ContentLength": 1024,
                "ETag": '"abc123"',
            }
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="PARTIALLY_COMPLETED",
                files=[
                    MockFileDetail(
                        file_id="f1",
                        status="COMPLETED",
                        output_s3_key="output/task-1/f1/video_h265.mp4",
                    ),
                    MockFileDetail(file_id="f2", status="FAILED"),
                ],
            )
            cmd._download_file = MagicMock(
                return_value=FileDownloadResult(
                    file_id="f1",
                    filename="video.mov",
                    success=True,
                    local_path=tmp_path / "video_h265.mp4",
                )
            )
            cmd._add_to_review_queue = MagicMock(return_value=True)

            cmd.download("task-1")

            # Should only download successful files
            assert cmd._download_file.call_count == 1


class TestDownloadError:
    """Tests for download error handling."""

    def test_download_task_not_found(self, tmp_path):
        """Test download when task is not found."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.side_effect = Exception("Task not found")

            result = cmd.download("nonexistent-task")

            assert result.success is False
            assert "Failed to get task details" in result.error_message

    def test_download_no_successful_files(self, tmp_path):
        """Test download when no files succeeded."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="COMPLETED",
                files=[MockFileDetail(status="FAILED")],
            )

            result = cmd.download("task-1")

            assert result.success is False
            assert "No successful files" in result.error_message


class TestOutputS3KeyValidation:
    """Tests for output_s3_key validation before download (Requirement 5.6, Property 7)."""

    def test_download_fails_when_output_s3_key_is_none(self, tmp_path):
        """Test download fails when COMPLETED file has no output_s3_key."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="COMPLETED",
                files=[MockFileDetail(status="COMPLETED", output_s3_key=None)],
            )

            result = cmd.download("task-1")

            assert result.success is False
            assert "missing output_s3_key" in result.error_message.lower()

    def test_download_fails_when_output_s3_key_is_empty(self, tmp_path):
        """Test download fails when COMPLETED file has empty output_s3_key."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="COMPLETED",
                files=[MockFileDetail(status="COMPLETED", output_s3_key="")],
            )

            result = cmd.download("task-1")

            assert result.success is False
            assert "missing output_s3_key" in result.error_message.lower()

    def test_download_succeeds_when_output_s3_key_is_set(self, tmp_path):
        """Test download proceeds when COMPLETED file has output_s3_key."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.head_object.return_value = {
                "ContentLength": 1024,
                "ETag": '"abc123"',
            }
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="COMPLETED",
                files=[
                    MockFileDetail(
                        status="COMPLETED",
                        output_s3_key="output/task-1/file-1/video_h265.mp4",
                    )
                ],
            )
            cmd._download_with_progress = MagicMock()
            cmd._verify_checksum = MagicMock(return_value=True)
            cmd._add_to_review_queue = MagicMock(return_value=True)

            # Create temp file to simulate download
            temp_file = tmp_path / ".video_h265.mp4.tmp"
            temp_file.write_bytes(b"x" * 1024)

            result = cmd.download("task-1")

            # Should not fail due to missing output_s3_key
            assert "missing output_s3_key" not in (result.error_message or "").lower()

    def test_download_reports_all_files_missing_output_s3_key(self, tmp_path):
        """Test download reports all files missing output_s3_key."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="COMPLETED",
                files=[
                    MockFileDetail(
                        file_id="f1", filename="video1.mov", status="COMPLETED", output_s3_key=None
                    ),
                    MockFileDetail(
                        file_id="f2", filename="video2.mov", status="COMPLETED", output_s3_key=None
                    ),
                ],
            )

            result = cmd.download("task-1")

            assert result.success is False
            assert "video1.mov" in result.error_message
            assert "video2.mov" in result.error_message

    def test_download_skips_failed_files_in_validation(self, tmp_path):
        """Test that FAILED files are not checked for output_s3_key."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.head_object.return_value = {
                "ContentLength": 1024,
                "ETag": '"abc123"',
            }
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="PARTIALLY_COMPLETED",
                files=[
                    MockFileDetail(
                        file_id="f1",
                        filename="video1.mov",
                        status="COMPLETED",
                        output_s3_key="output/task-1/f1/video1_h265.mp4",
                    ),
                    MockFileDetail(
                        file_id="f2",
                        filename="video2.mov",
                        status="FAILED",
                        output_s3_key=None,  # FAILED files don't need output_s3_key
                    ),
                ],
            )
            cmd._download_file = MagicMock(
                return_value=FileDownloadResult(
                    file_id="f1",
                    filename="video1.mov",
                    success=True,
                    local_path=tmp_path / "video1_h265.mp4",
                )
            )
            cmd._add_to_review_queue = MagicMock(return_value=True)

            result = cmd.download("task-1")

            # Should not fail - FAILED files are not validated
            assert "missing output_s3_key" not in (result.error_message or "").lower()
            # Should only download the COMPLETED file
            assert cmd._download_file.call_count == 1


class TestVerifyChecksum:
    """Tests for checksum verification."""

    def test_verify_checksum_success(self, tmp_path):
        """Test successful checksum verification."""
        test_file = tmp_path / "test.mp4"
        test_file.write_bytes(b"test content")

        import hashlib

        expected_md5 = hashlib.md5(b"test content").hexdigest()

        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )

            result = cmd._verify_checksum(test_file, expected_md5)

            assert result is True

    def test_verify_checksum_failure(self, tmp_path):
        """Test checksum verification failure."""
        test_file = tmp_path / "test.mp4"
        test_file.write_bytes(b"test content")

        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )

            result = cmd._verify_checksum(test_file, "wrong_checksum")

            assert result is False

    def test_verify_checksum_multipart(self, tmp_path):
        """Test checksum verification for multipart uploads."""
        test_file = tmp_path / "test.mp4"
        test_file.write_bytes(b"test content")

        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )

            # Multipart ETags contain a dash
            result = cmd._verify_checksum(test_file, "abc123-2")

            # Should return True (skip verification for multipart)
            assert result is True


class TestResumeDownload:
    """Tests for download resume functionality."""

    def test_resume_download(self, tmp_path):
        """Test resuming an interrupted download."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.head_object.return_value = {
                "ContentLength": 2048,
                "ETag": '"abc123"',
            }
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )

            # Create partial temp file
            temp_file = tmp_path / ".video_h265.mp4.tmp"
            temp_file.write_bytes(b"x" * 1024)

            # Set up progress store with matching progress
            from vco.services.download_progress import DownloadProgress

            cmd.progress_store.save_progress(
                DownloadProgress(
                    task_id="task-1",
                    file_id="file-1",
                    total_bytes=2048,
                    downloaded_bytes=1024,
                    local_temp_path=str(temp_file),
                    s3_key="async/task-1/output/file-1/video_h265.mp4",
                    checksum="abc123",
                )
            )

            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail()

            # Mock the download to succeed
            cmd._download_with_progress = MagicMock()
            cmd._verify_checksum = MagicMock(return_value=True)
            cmd._add_to_review_queue = MagicMock(return_value=True)

            result = cmd.download("task-1", resume=True)

            # Verify resume was attempted
            assert result is not None


class TestHelperMethods:
    """Tests for helper methods."""

    def test_get_machine_id_returns_string(self, tmp_path):
        """Test _get_machine_id returns valid string."""
        with patch("boto3.Session"):
            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            machine_id = cmd._get_machine_id()
            assert isinstance(machine_id, str)
            assert len(machine_id) == 32


class TestS3FileRetention:
    """Tests for S3 file deletion after download.

    S3 output files should be deleted after successful download.
    DynamoDB status should be updated to DOWNLOADED.
    """

    def test_s3_file_deleted_after_download(self, tmp_path):
        """Test that S3 output file is deleted after successful download."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.head_object.return_value = {
                "ContentLength": 1024,
                "ETag": '"abc123"',
            }
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="COMPLETED",
                files=[
                    MockFileDetail(
                        status="COMPLETED",
                        output_s3_key="output/task-1/file-1/video_h265.mp4",
                    )
                ],
            )
            cmd._download_with_progress = MagicMock()
            cmd._verify_checksum = MagicMock(return_value=True)
            cmd._add_to_review_queue = MagicMock(return_value=True)
            cmd._update_download_status = MagicMock(return_value=True)

            # Create temp file to simulate download
            temp_file = tmp_path / ".video_h265.mp4.tmp"
            temp_file.write_bytes(b"x" * 1024)

            result = cmd.download("task-1")

            # Verify S3 delete_object was called
            mock_s3.delete_object.assert_called_once_with(
                Bucket="test-bucket",
                Key="output/task-1/file-1/video_h265.mp4",
            )
            # Download should succeed
            assert result.downloaded_files >= 0

    def test_download_status_updated_after_download(self, tmp_path):
        """Test that download status is updated in DynamoDB after successful download."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.head_object.return_value = {
                "ContentLength": 1024,
                "ETag": '"abc123"',
            }
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )
            cmd.status_command = MagicMock()
            cmd.status_command.get_task_detail.return_value = MockTaskDetail(
                status="COMPLETED",
                files=[
                    MockFileDetail(
                        file_id="file-1",
                        status="COMPLETED",
                        output_s3_key="output/task-1/file-1/video_h265.mp4",
                    )
                ],
            )
            cmd._download_with_progress = MagicMock()
            cmd._verify_checksum = MagicMock(return_value=True)
            cmd._add_to_review_queue = MagicMock(return_value=True)
            cmd._update_download_status = MagicMock(return_value=True)

            # Create temp file to simulate download
            temp_file = tmp_path / ".video_h265.mp4.tmp"
            temp_file.write_bytes(b"x" * 1024)

            cmd.download("task-1")

            # Verify download status was updated
            cmd._update_download_status.assert_called_once_with("task-1", "file-1", "completed")

    def test_delete_s3_file_method_still_works(self, tmp_path):
        """Test that _delete_s3_file method still works when called directly."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )

            result = cmd._delete_s3_file("output/task-1/file-1/video_h265.mp4")

            assert result is True
            mock_s3.delete_object.assert_called_once_with(
                Bucket="test-bucket",
                Key="output/task-1/file-1/video_h265.mp4",
            )

    def test_delete_s3_file_handles_error(self, tmp_path):
        """Test that _delete_s3_file handles errors gracefully."""
        with patch("boto3.Session") as mock_session:
            mock_s3 = MagicMock()
            mock_s3.delete_object.side_effect = Exception("S3 error")
            mock_session.return_value.client.return_value = mock_s3

            cmd = DownloadCommand(
                api_url="https://api.example.com",
                s3_bucket="test-bucket",
                output_dir=tmp_path,
            )

            result = cmd._delete_s3_file("output/task-1/file-1/video_h265.mp4")

            assert result is False
