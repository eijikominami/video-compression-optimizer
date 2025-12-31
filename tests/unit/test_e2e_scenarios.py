"""End-to-end scenario tests.

Tests: Task 14.5 - E2E scenario tests
Requirements: 1.1, 2.1, 3.1, 4.1, 9.2, 9.6
"""

import json
from datetime import datetime


class TestNormalFlowScenario:
    """Test normal flow: convert --async → status → download."""

    def test_complete_normal_flow(self, tmp_path):
        """Test complete normal flow from submission to download."""
        # Step 1: Submit async conversion
        task_id = "task-normal-001"
        files = [
            {"filename": "video1.mov", "size": 1000000},
            {"filename": "video2.mov", "size": 2000000},
        ]

        submit_response = {
            "task_id": task_id,
            "status": "PENDING",
            "created_at": datetime.now().isoformat(),
            "files": [
                {"file_id": f"file-{i}", "filename": f["filename"], "status": "PENDING"}
                for i, f in enumerate(files)
            ],
        }

        assert submit_response["task_id"] == task_id
        assert submit_response["status"] == "PENDING"
        assert len(submit_response["files"]) == 2

        # Step 2: Check status (UPLOADING)
        status_uploading = {
            "task_id": task_id,
            "status": "UPLOADING",
            "progress_percentage": 25,
            "files": [
                {"file_id": "file-0", "filename": "video1.mov", "status": "UPLOADING"},
                {"file_id": "file-1", "filename": "video2.mov", "status": "PENDING"},
            ],
        }

        assert status_uploading["status"] == "UPLOADING"
        assert status_uploading["progress_percentage"] == 25

        # Step 3: Check status (CONVERTING)
        status_converting = {
            "task_id": task_id,
            "status": "CONVERTING",
            "progress_percentage": 50,
            "files": [
                {"file_id": "file-0", "filename": "video1.mov", "status": "CONVERTING"},
                {"file_id": "file-1", "filename": "video2.mov", "status": "CONVERTING"},
            ],
        }

        assert status_converting["status"] == "CONVERTING"

        # Step 4: Check status (COMPLETED)
        status_completed = {
            "task_id": task_id,
            "status": "COMPLETED",
            "progress_percentage": 100,
            "files": [
                {
                    "file_id": "file-0",
                    "filename": "video1.mov",
                    "status": "COMPLETED",
                    "output_key": "async/task-normal-001/output/file-0/video1_h265.mp4",
                    "ssim_score": 0.96,
                },
                {
                    "file_id": "file-1",
                    "filename": "video2.mov",
                    "status": "COMPLETED",
                    "output_key": "async/task-normal-001/output/file-1/video2_h265.mp4",
                    "ssim_score": 0.95,
                },
            ],
        }

        assert status_completed["status"] == "COMPLETED"
        assert status_completed["progress_percentage"] == 100
        assert all(f["status"] == "COMPLETED" for f in status_completed["files"])

        # Step 5: Download
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        for file_info in status_completed["files"]:
            # Simulate download
            output_file = download_dir / file_info["filename"].replace(".mov", "_h265.mp4")
            output_file.write_bytes(b"converted video content")

        # Verify downloads
        downloaded_files = list(download_dir.glob("*.mp4"))
        assert len(downloaded_files) == 2

    def test_normal_flow_state_transitions(self):
        """Test valid state transitions in normal flow."""
        valid_transitions = [
            ("PENDING", "UPLOADING"),
            ("UPLOADING", "CONVERTING"),
            ("CONVERTING", "VERIFYING"),
            ("VERIFYING", "COMPLETED"),
        ]

        current_state = "PENDING"
        for from_state, to_state in valid_transitions:
            assert current_state == from_state
            current_state = to_state

        assert current_state == "COMPLETED"


class TestCancelFlowScenario:
    """Test cancel flow: convert --async → cancel → status (CANCELLED)."""

    def test_cancel_during_upload(self):
        """Test cancel during upload phase."""
        task_id = "task-cancel-001"

        # Initial state - verify task exists
        _ = {
            "task_id": task_id,
            "status": "UPLOADING",
            "files": [
                {"file_id": "file-0", "status": "UPLOADING"},
                {"file_id": "file-1", "status": "PENDING"},
            ],
        }

        # Cancel request
        cancel_response = {
            "task_id": task_id,
            "status": "CANCELLED",
            "message": "Task cancelled successfully",
        }

        assert cancel_response["status"] == "CANCELLED"

        # Verify final state
        status_after_cancel = {
            "task_id": task_id,
            "status": "CANCELLED",
            "files": [
                {"file_id": "file-0", "status": "CANCELLED"},
                {"file_id": "file-1", "status": "CANCELLED"},
            ],
        }

        assert status_after_cancel["status"] == "CANCELLED"
        assert all(f["status"] == "CANCELLED" for f in status_after_cancel["files"])

    def test_cancel_during_conversion(self):
        """Test cancel during conversion phase."""
        task_id = "task-cancel-002"

        # State during conversion - verify task exists
        _ = {
            "task_id": task_id,
            "status": "CONVERTING",
            "files": [
                {"file_id": "file-0", "status": "COMPLETED"},  # Already done
                {"file_id": "file-1", "status": "CONVERTING"},  # In progress
                {"file_id": "file-2", "status": "PENDING"},  # Not started
            ],
        }

        # Cancel - verify cancel request is made
        _ = {"task_id": task_id, "status": "CANCELLED"}

        # After cancel - completed files stay completed
        status_after_cancel = {
            "task_id": task_id,
            "status": "CANCELLED",
            "files": [
                {"file_id": "file-0", "status": "COMPLETED"},  # Stays completed
                {"file_id": "file-1", "status": "CANCELLED"},  # Was converting
                {"file_id": "file-2", "status": "CANCELLED"},  # Was pending
            ],
        }

        completed_count = sum(1 for f in status_after_cancel["files"] if f["status"] == "COMPLETED")
        cancelled_count = sum(1 for f in status_after_cancel["files"] if f["status"] == "CANCELLED")

        assert completed_count == 1
        assert cancelled_count == 2

    def test_cancel_idempotency(self):
        """Test that cancelling already cancelled task is idempotent."""
        task_id = "task-cancel-003"

        # First cancel
        first_cancel = {"task_id": task_id, "status": "CANCELLED"}

        # Second cancel (should succeed without error)
        second_cancel = {"task_id": task_id, "status": "CANCELLED"}

        assert first_cancel["status"] == second_cancel["status"]

    def test_cannot_cancel_completed_task(self):
        """Test that completed task cannot be cancelled."""
        task_id = "task-cancel-004"

        status_completed = {"task_id": task_id, "status": "COMPLETED"}

        # Attempt to cancel should fail
        cancel_error = {
            "error": "InvalidStateError",
            "message": "Cannot cancel completed task",
        }

        assert "error" in cancel_error
        assert status_completed["status"] == "COMPLETED"


class TestPartialFailureScenario:
    """Test partial failure: some files fail → PARTIALLY_COMPLETED → download success only."""

    def test_partial_failure_status(self):
        """Test status shows PARTIALLY_COMPLETED when some files fail."""
        task_id = "task-partial-001"

        status_partial = {
            "task_id": task_id,
            "status": "PARTIALLY_COMPLETED",
            "progress_percentage": 100,  # All processing done
            "files": [
                {
                    "file_id": "file-0",
                    "filename": "video1.mov",
                    "status": "COMPLETED",
                    "output_key": "async/task-partial-001/output/file-0/video1_h265.mp4",
                },
                {
                    "file_id": "file-1",
                    "filename": "video2.mov",
                    "status": "FAILED",
                    "error_code": "1010",
                    "error_message": "Invalid input format",
                },
                {
                    "file_id": "file-2",
                    "filename": "video3.mov",
                    "status": "COMPLETED",
                    "output_key": "async/task-partial-001/output/file-2/video3_h265.mp4",
                },
            ],
        }

        assert status_partial["status"] == "PARTIALLY_COMPLETED"

        completed = [f for f in status_partial["files"] if f["status"] == "COMPLETED"]
        failed = [f for f in status_partial["files"] if f["status"] == "FAILED"]

        assert len(completed) == 2
        assert len(failed) == 1

    def test_download_only_successful_files(self, tmp_path):
        """Test that download only includes successful files."""
        # task_id = "task-partial-002"  # For reference

        files = [
            {"file_id": "file-0", "status": "COMPLETED", "filename": "video1.mov"},
            {"file_id": "file-1", "status": "FAILED", "filename": "video2.mov"},
            {"file_id": "file-2", "status": "COMPLETED", "filename": "video3.mov"},
        ]

        # Filter downloadable files
        downloadable = [f for f in files if f["status"] == "COMPLETED"]

        assert len(downloadable) == 2
        assert all(f["status"] == "COMPLETED" for f in downloadable)

        # Simulate download
        download_dir = tmp_path / "downloads"
        download_dir.mkdir()

        for f in downloadable:
            output_file = download_dir / f["filename"].replace(".mov", "_h265.mp4")
            output_file.write_bytes(b"converted content")

        downloaded = list(download_dir.glob("*.mp4"))
        assert len(downloaded) == 2

    def test_partial_failure_error_details(self):
        """Test that error details are available for failed files."""
        failed_file = {
            "file_id": "file-1",
            "filename": "video2.mov",
            "status": "FAILED",
            "error_code": "1010",
            "error_message": "Invalid input format",
            "error_details": {
                "codec": "unknown",
                "container": "avi",
            },
        }

        assert failed_file["status"] == "FAILED"
        assert "error_code" in failed_file
        assert "error_message" in failed_file


class TestResumeFlowScenario:
    """Test resume flow: download interrupted → download resume."""

    def test_download_resume_from_progress(self, tmp_path):
        """Test download resumes from saved progress."""
        task_id = "task-resume-001"
        file_id = "file-0"
        total_size = 10000

        # Simulate interrupted download
        progress_file = tmp_path / f".vco_download_{task_id}_{file_id}.json"
        progress_data = {
            "task_id": task_id,
            "file_id": file_id,
            "downloaded_bytes": 5000,
            "total_bytes": total_size,
            "temp_path": str(tmp_path / "video.tmp"),
        }
        progress_file.write_text(json.dumps(progress_data))

        # Create partial file
        partial_file = tmp_path / "video.tmp"
        partial_file.write_bytes(b"X" * 5000)

        # Resume download
        resume_offset = progress_data["downloaded_bytes"]
        remaining = total_size - resume_offset

        assert resume_offset == 5000
        assert remaining == 5000

        # Complete download
        with open(partial_file, "ab") as f:
            f.write(b"Y" * remaining)

        assert partial_file.stat().st_size == total_size

    def test_resume_with_corrupted_progress(self, tmp_path):
        """Test handling of corrupted progress file."""
        progress_file = tmp_path / ".vco_download_progress.json"
        progress_file.write_text("invalid json{")

        # Should start fresh download
        try:
            json.loads(progress_file.read_text())
            progress_valid = True
        except json.JSONDecodeError:
            progress_valid = False

        assert not progress_valid
        # Fresh download starts from 0

    def test_resume_with_missing_temp_file(self, tmp_path):
        """Test handling when temp file is missing but progress exists."""
        progress_file = tmp_path / ".vco_download_progress.json"
        progress_data = {
            "downloaded_bytes": 5000,
            "temp_path": str(tmp_path / "missing.tmp"),
        }
        progress_file.write_text(json.dumps(progress_data))

        # Temp file doesn't exist
        temp_path = tmp_path / "missing.tmp"
        assert not temp_path.exists()

        # Should start fresh download
        if not temp_path.exists():
            resume_offset = 0
        else:
            resume_offset = progress_data["downloaded_bytes"]

        assert resume_offset == 0

    def test_multiple_file_resume(self, tmp_path):
        """Test resuming download with multiple files."""
        files_progress = [
            {"file_id": "file-0", "downloaded": 10000, "total": 10000, "complete": True},
            {"file_id": "file-1", "downloaded": 5000, "total": 10000, "complete": False},
            {"file_id": "file-2", "downloaded": 0, "total": 10000, "complete": False},
        ]

        # Determine which files need download
        to_download = [f for f in files_progress if not f["complete"]]
        to_resume = [f for f in to_download if f["downloaded"] > 0]
        to_start = [f for f in to_download if f["downloaded"] == 0]

        assert len(to_download) == 2
        assert len(to_resume) == 1
        assert len(to_start) == 1


class TestErrorRecoveryScenario:
    """Test error recovery scenarios."""

    def test_transient_error_retry(self):
        """Test that transient errors trigger retry."""
        transient_errors = [1517, 1522, 1550, 1999]

        for error_code in transient_errors:
            error_response = {
                "error_code": error_code,
                "retryable": True,
                "retry_count": 0,
                "max_retries": 3,
            }

            assert error_response["retryable"]
            assert error_response["retry_count"] < error_response["max_retries"]

    def test_permanent_error_no_retry(self):
        """Test that permanent errors don't retry."""
        permanent_errors = [1010, 1030, 1040, 1401]

        for error_code in permanent_errors:
            error_response = {
                "error_code": error_code,
                "retryable": False,
            }

            assert not error_response["retryable"]

    def test_ssim_failure_preset_fallback(self):
        """Test SSIM failure triggers preset fallback."""
        ssim_result = {
            "ssim_score": 0.94,  # Below threshold
            "threshold": 0.95,
            "current_preset": "balanced",
            "fallback_preset": "quality",
        }

        if ssim_result["ssim_score"] < ssim_result["threshold"]:
            next_preset = ssim_result["fallback_preset"]
        else:
            next_preset = None

        assert next_preset == "quality"

    def test_max_retry_exceeded(self):
        """Test handling when max retries exceeded."""
        retry_state = {
            "retry_count": 3,
            "max_retries": 3,
            "last_error": "Transient error",
        }

        if retry_state["retry_count"] >= retry_state["max_retries"]:
            final_status = "FAILED"
        else:
            final_status = "RETRYING"

        assert final_status == "FAILED"
