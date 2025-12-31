"""Unit tests for async-workflow Lambda function."""

import importlib.util
import os
import sys
from unittest.mock import MagicMock

import pytest


def load_workflow_module():
    if "workflow_app" in sys.modules:
        del sys.modules["workflow_app"]
    _lambda_path = os.path.join(os.path.dirname(__file__), "../../sam-app/async-workflow/app.py")
    _spec = importlib.util.spec_from_file_location("workflow_app", _lambda_path)
    module = importlib.util.module_from_spec(_spec)
    sys.modules["workflow_app"] = module
    _spec.loader.exec_module(module)
    return module


workflow_app = load_workflow_module()


class TestValidateInput:
    def test_valid_input(self):
        event = {"task_id": "t1", "files": [{"file_id": "f1"}]}
        result = workflow_app.validate_input(event)
        assert result["valid"] is True

    def test_missing_task_id(self):
        with pytest.raises(ValueError, match="Missing task_id"):
            workflow_app.validate_input({"files": [{"file_id": "f1"}]})

    def test_empty_files(self):
        with pytest.raises(ValueError, match="No files"):
            workflow_app.validate_input({"task_id": "t1", "files": []})


class TestUpdateStatus:
    def test_update_status_basic(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        mock_table = MagicMock()
        monkeypatch.setattr(workflow_app, "get_dynamodb_table", lambda: mock_table)
        result = workflow_app.update_status({"task_id": "t1", "status": "PROCESSING"})
        assert result["updated"] is True

    def test_update_status_completed(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        mock_table = MagicMock()
        monkeypatch.setattr(workflow_app, "get_dynamodb_table", lambda: mock_table)
        workflow_app.update_status({"task_id": "t1", "status": "COMPLETED"})
        call_kwargs = mock_table.update_item.call_args[1]
        assert ":completed" in call_kwargs["ExpressionAttributeValues"]


class TestStartConversion:
    def test_start_conversion_success(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("MEDIACONVERT_ROLE_ARN", "arn:aws:iam::123:role/test")
        mock_client = MagicMock()
        mock_client.create_job.return_value = {"Job": {"Id": "job-123"}}
        monkeypatch.setattr(workflow_app, "get_mediaconvert_client", lambda: mock_client)
        monkeypatch.setattr(workflow_app, "update_file_status", MagicMock())
        event = {
            "task_id": "t1",
            "file": {"file_id": "f1", "source_s3_key": "input/t1/f1/v.mov"},
            "quality_preset": "balanced",
        }
        result = workflow_app.start_conversion(event)
        assert result["job_id"] == "job-123"


class TestCheckConversionStatus:
    def test_check_status_complete(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        module = load_workflow_module()
        mock_client = MagicMock()
        mock_client.get_job.return_value = {
            "Job": {
                "Status": "COMPLETE",
                "Settings": {
                    "Inputs": [{"FileInput": "s3://test-bucket/input/video.mov"}],
                    "OutputGroups": [
                        {
                            "OutputGroupSettings": {
                                "FileGroupSettings": {"Destination": "s3://test-bucket/out/"}
                            },
                            "Outputs": [{"NameModifier": "_h265", "Extension": "mp4"}],
                        }
                    ],
                },
            }
        }
        module.get_mediaconvert_client = lambda: mock_client
        result = module.check_conversion_status({"job_id": "job-123"})
        assert result["status"] == "COMPLETE"
        assert result["output_s3_key"] == "out/video_h265.mp4"

    def test_check_status_error(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        module = load_workflow_module()
        mock_client = MagicMock()
        mock_client.get_job.return_value = {
            "Job": {"Status": "ERROR", "ErrorCode": 1517, "ErrorMessage": "Err"}
        }
        module.get_mediaconvert_client = lambda: mock_client
        result = module.check_conversion_status({"job_id": "job-123"})
        assert result["error_code"] == 1517


class TestHandleConversionError:
    def test_transient_error_retries(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setattr(workflow_app, "update_file_status", MagicMock())
        event = {
            "task_id": "t1",
            "file": {"file_id": "f1", "retry_count": 0},
            "error_code": 1517,
            "error_message": "Transient",
        }
        result = workflow_app.handle_conversion_error(event)
        assert result["should_retry"] is True

    def test_config_error_no_retry(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setattr(workflow_app, "update_file_status", MagicMock())
        event = {
            "task_id": "t1",
            "file": {"file_id": "f1", "retry_count": 0},
            "error_code": 1010,
            "error_message": "Config",
        }
        result = workflow_app.handle_conversion_error(event)
        assert result["should_retry"] is False


class TestHandleQualityFailure:
    def test_non_adaptive_fails(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setattr(workflow_app, "update_file_status", MagicMock())
        event = {
            "task_id": "t1",
            "file": {"file_id": "f1", "preset_attempts": []},
            "quality_preset": "balanced",
            "quality_result": {"ssim": 0.94},
        }
        result = workflow_app.handle_quality_failure(event)
        assert result["should_retry"] is False

    def test_adaptive_retries(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setattr(workflow_app, "update_file_status", MagicMock())
        event = {
            "task_id": "t1",
            "file": {"file_id": "f1", "preset_attempts": []},
            "quality_preset": "balanced+",
            "quality_result": {"ssim": 0.94},
        }
        result = workflow_app.handle_quality_failure(event)
        assert result["should_retry"] is True
        assert result["next_preset"] == "high"

    def test_adaptive_preset_best_effort(self, monkeypatch):
        """Adaptive preset uses best-effort when all presets exhausted."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setattr(workflow_app, "update_file_status", MagicMock())
        event = {
            "task_id": "t1",
            "file": {"file_id": "f1", "preset_attempts": ["balanced+"]},
            "quality_preset": "high+",  # Last preset in chain, still adaptive
            "quality_result": {"ssim": 0.93},
        }
        result = workflow_app.handle_quality_failure(event)
        assert result["should_retry"] is False
        assert result["reason"] == "best_effort"
        assert result["accept_anyway"] is True

    def test_retry_from_adaptive_uses_best_effort(self, monkeypatch):
        """Retry from adaptive preset uses best-effort mode."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setattr(workflow_app, "update_file_status", MagicMock())
        event = {
            "task_id": "t1",
            "file": {"file_id": "f1", "preset_attempts": ["balanced+"]},
            "quality_preset": "high",
            "quality_result": {"ssim": 0.93},
        }
        result = workflow_app.handle_quality_failure(event)
        assert result["should_retry"] is False
        assert result["reason"] == "best_effort"
        assert result["accept_anyway"] is True


class TestAggregateResults:
    def test_all_completed(self):
        event = {
            "task_id": "t1",
            "results": [
                {"file_result": {"status": "COMPLETED"}},
                {"file_result": {"status": "COMPLETED"}},
            ],
        }
        result = workflow_app.aggregate_results(event)
        assert result["final_status"] == "COMPLETED"

    def test_all_failed(self):
        event = {
            "task_id": "t1",
            "results": [
                {"file_result": {"status": "FAILED"}},
                {"file_result": {"status": "FAILED"}},
            ],
        }
        result = workflow_app.aggregate_results(event)
        assert result["final_status"] == "FAILED"

    def test_partial(self):
        event = {
            "task_id": "t1",
            "results": [
                {"file_result": {"status": "COMPLETED"}},
                {"file_result": {"status": "FAILED"}},
            ],
        }
        result = workflow_app.aggregate_results(event)
        assert result["final_status"] == "PARTIALLY_COMPLETED"


class TestHandleError:
    def test_handle_error(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        mock_table = MagicMock()
        monkeypatch.setattr(workflow_app, "get_dynamodb_table", lambda: mock_table)
        result = workflow_app.handle_error({"task_id": "t1", "error": {"msg": "fail"}})
        assert result["handled"] is True


class TestLambdaHandler:
    def test_unknown_action(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        with pytest.raises(ValueError, match="Unknown action"):
            workflow_app.lambda_handler({"action": "unknown"}, None)

    def test_validate_input_action(self, monkeypatch):
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        event = {"action": "validate_input", "task_id": "t1", "files": [{"file_id": "f1"}]}
        result = workflow_app.lambda_handler(event, None)
        assert result["valid"] is True


class TestCreateJobSettings:
    def test_balanced_preset(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        result = workflow_app.create_job_settings("in/v.mov", "out/v.mp4", "balanced")
        video = result["OutputGroups"][0]["Outputs"][0]["VideoDescription"]
        assert video["CodecSettings"]["H265Settings"]["MaxBitrate"] == 20_000_000

    def test_high_preset(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        result = workflow_app.create_job_settings("in/v.mov", "out/v.mp4", "high")
        video = result["OutputGroups"][0]["Outputs"][0]["VideoDescription"]
        assert video["CodecSettings"]["H265Settings"]["MaxBitrate"] == 50_000_000
