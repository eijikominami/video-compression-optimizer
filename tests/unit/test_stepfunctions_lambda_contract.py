"""Step Functions と Lambda 戻り値の整合性テスト.

ASL 定義で参照されるパスが Lambda 関数の戻り値に存在することを検証する。
これにより、Step Functions 実行時の "Invalid path" エラーを事前に検出できる。

Requirements: 7.1, 7.2
"""

import importlib.util
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def load_workflow_module():
    """Load async-workflow Lambda module."""
    if "workflow_app" in sys.modules:
        del sys.modules["workflow_app"]
    if "quality_metrics" in sys.modules:
        del sys.modules["quality_metrics"]

    # Load quality_metrics first (dependency)
    qm_path = os.path.join(
        os.path.dirname(__file__), "../../sam-app/async-workflow/quality_metrics.py"
    )
    qm_spec = importlib.util.spec_from_file_location("quality_metrics", qm_path)
    qm_module = importlib.util.module_from_spec(qm_spec)
    sys.modules["quality_metrics"] = qm_module
    qm_spec.loader.exec_module(qm_module)

    # Load app module
    app_path = os.path.join(os.path.dirname(__file__), "../../sam-app/async-workflow/app.py")
    app_spec = importlib.util.spec_from_file_location("workflow_app", app_path)
    app_module = importlib.util.module_from_spec(app_spec)
    sys.modules["workflow_app"] = app_module
    app_spec.loader.exec_module(app_module)

    return app_module


class TestStepFunctionsLambdaContract:
    """Step Functions と Lambda の契約テスト."""

    @pytest.fixture
    def asl_definition(self) -> dict:
        """ASL 定義を読み込む."""
        asl_path = (
            Path(__file__).parent.parent.parent / "sam-app/statemachine/async-workflow.asl.json"
        )
        with open(asl_path) as f:
            return json.load(f)

    def test_asl_definition_exists(self, asl_definition: dict):
        """ASL 定義ファイルが存在し、読み込めることを確認."""
        assert asl_definition is not None
        assert "States" in asl_definition

    def test_check_quality_result_references_quality_passed(self, asl_definition: dict):
        """CheckQualityResult 状態が $.job_status.quality_passed を参照することを確認."""
        # ProcessFiles Map の Iterator 内の CheckQualityResult を取得
        process_files = asl_definition["States"]["ProcessFiles"]
        iterator_states = process_files["Iterator"]["States"]
        check_quality_result = iterator_states["CheckQualityResult"]

        # Choice 状態の条件を確認
        assert check_quality_result["Type"] == "Choice"
        choices = check_quality_result["Choices"]

        # quality_passed を参照する条件があることを確認
        quality_passed_refs = [
            c for c in choices if c.get("Variable") == "$.job_status.quality_passed"
        ]
        assert len(quality_passed_refs) > 0, (
            "CheckQualityResult should reference $.job_status.quality_passed"
        )

    def test_is_conversion_complete_references_status(self, asl_definition: dict):
        """IsConversionComplete 状態が $.job_status.status を参照することを確認."""
        process_files = asl_definition["States"]["ProcessFiles"]
        iterator_states = process_files["Iterator"]["States"]
        is_conversion_complete = iterator_states["IsConversionComplete"]

        assert is_conversion_complete["Type"] == "Choice"
        choices = is_conversion_complete["Choices"]

        # status を参照する条件があることを確認
        status_refs = [c for c in choices if c.get("Variable") == "$.job_status.status"]
        assert len(status_refs) >= 2, (
            "IsConversionComplete should reference $.job_status.status for COMPLETE and ERROR"
        )


class TestCheckConversionStatusReturnValue:
    """check_conversion_status 関数の戻り値テスト."""

    def test_complete_status_returns_quality_passed(self, monkeypatch):
        """COMPLETE 時に quality_passed がトップレベルで返されることを確認."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")

        module = load_workflow_module()
        qm = sys.modules["quality_metrics"]

        # Mock MediaConvert client
        mock_mc = MagicMock()
        mock_mc.get_job.return_value = {
            "Job": {
                "Id": "test-job-id",
                "Status": "COMPLETE",
                "Settings": {
                    "Inputs": [{"FileInput": "s3://test-bucket/input/test.mp4"}],
                    "OutputGroups": [
                        {
                            "OutputGroupSettings": {
                                "FileGroupSettings": {"Destination": "s3://test-bucket/output/"}
                            },
                            "Outputs": [{"NameModifier": "_h265", "Extension": "mp4"}],
                        }
                    ],
                },
            }
        }
        module.get_mediaconvert_client = lambda: mock_mc

        # Mock S3 client for file size
        mock_s3 = MagicMock()
        mock_s3.head_object.return_value = {"ContentLength": 1000000}

        # Mock QualityMetricsParser
        mock_metrics = qm.QualityMetrics(
            ssim_average=0.96,
            ssim_min=0.94,
            ssim_max=0.98,
            vmaf_average=85.0,
            vmaf_min=80.0,
            vmaf_max=90.0,
        )
        mock_parser = MagicMock()
        mock_parser.parse_metrics_from_s3.return_value = mock_metrics

        # Patch boto3.client to return mock S3
        original_boto3_client = module.boto3.client
        module.boto3.client = (
            lambda service: mock_s3 if service == "s3" else original_boto3_client(service)
        )

        # Patch QualityMetricsParser
        original_parser = qm.QualityMetricsParser
        qm.QualityMetricsParser = lambda **kwargs: mock_parser

        try:
            event = {
                "job_id": "test-job-id",
                "task_id": "test-task-id",
                "file": {"file_id": "test-file-id", "source_s3_key": "input/test.mp4"},
            }

            result = module.check_conversion_status(event)

            # Verify required fields for Step Functions
            assert "status" in result, "Result must contain 'status'"
            assert result["status"] == "COMPLETE"
            assert "quality_passed" in result, "Result must contain 'quality_passed' at top level"
            assert isinstance(result["quality_passed"], bool)
            assert "quality_result" in result, "Result must contain 'quality_result'"
            assert "output_s3_key" in result, "Result must contain 'output_s3_key'"
        finally:
            module.boto3.client = original_boto3_client
            qm.QualityMetricsParser = original_parser

    def test_error_status_returns_error_fields(self, monkeypatch):
        """ERROR 時に error_code と error_message が返されることを確認."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

        module = load_workflow_module()

        mock_mc = MagicMock()
        mock_mc.get_job.return_value = {
            "Job": {
                "Id": "test-job-id",
                "Status": "ERROR",
                "ErrorCode": 1517,
                "ErrorMessage": "Transient error occurred",
            }
        }
        module.get_mediaconvert_client = lambda: mock_mc

        event = {
            "job_id": "test-job-id",
            "task_id": "test-task-id",
            "file": {"file_id": "test-file-id"},
        }

        result = module.check_conversion_status(event)

        # Verify required fields for Step Functions HandleConversionError
        assert "status" in result
        assert result["status"] == "ERROR"
        assert "error_code" in result, "Result must contain 'error_code' for ERROR status"
        assert "error_message" in result, "Result must contain 'error_message' for ERROR status"

    def test_quality_metrics_error_raises_exception(self, monkeypatch):
        """品質メトリクス解析エラー時に例外が再送出されることを確認.

        CSV パースエラーはシステムエラーとして扱い、Step Functions の Catch ブロックで
        処理されるべき。quality_passed: false を返すとリトライロジックが誤動作する。
        """
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

        module = load_workflow_module()
        qm = sys.modules["quality_metrics"]

        mock_mc = MagicMock()
        mock_mc.get_job.return_value = {
            "Job": {
                "Id": "test-job-id",
                "Status": "COMPLETE",
                "Settings": {
                    "Inputs": [{"FileInput": "s3://test-bucket/input/test.mp4"}],
                    "OutputGroups": [
                        {
                            "OutputGroupSettings": {
                                "FileGroupSettings": {"Destination": "s3://test-bucket/output/"}
                            },
                            "Outputs": [{"NameModifier": "_h265", "Extension": "mp4"}],
                        }
                    ],
                },
            }
        }
        module.get_mediaconvert_client = lambda: mock_mc

        # Mock parser to raise error
        mock_parser = MagicMock()
        mock_parser.parse_metrics_from_s3.side_effect = qm.QualityMetricsError("CSV not found")

        original_parser = qm.QualityMetricsParser
        qm.QualityMetricsParser = lambda **kwargs: mock_parser

        try:
            event = {
                "job_id": "test-job-id",
                "task_id": "test-task-id",
                "file": {"file_id": "test-file-id", "source_s3_key": "input/test.mp4"},
            }

            # CSV parse error should raise exception for Step Functions Catch block
            with pytest.raises(qm.QualityMetricsError):
                module.check_conversion_status(event)
        finally:
            qm.QualityMetricsParser = original_parser


class TestHandleQualityFailureReturnValue:
    """handle_quality_failure 関数の戻り値テスト."""

    def test_adaptive_preset_retry_returns_required_fields(self, monkeypatch):
        """アダプティブプリセットのリトライ時に必要なフィールドが返されることを確認."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")

        module = load_workflow_module()
        module.update_file_status = MagicMock()

        event = {
            "task_id": "test-task-id",
            "file": {"file_id": "test-file-id", "preset_attempts": []},
            "quality_preset": "balanced+",
            "quality_result": {"passed": False, "ssim_score": 0.90},
        }

        result = module.handle_quality_failure(event)

        # Verify required fields for Step Functions ShouldRetryWithHigherPreset
        assert "should_retry" in result, "Result must contain 'should_retry'"
        if result["should_retry"]:
            assert "next_preset" in result, (
                "Result must contain 'next_preset' when should_retry=true"
            )
            assert "updated_file" in result, (
                "Result must contain 'updated_file' when should_retry=true"
            )

    def test_non_adaptive_preset_returns_reason(self, monkeypatch):
        """非アダプティブプリセットで reason が返されることを確認."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")

        module = load_workflow_module()
        module.update_file_status = MagicMock()

        event = {
            "task_id": "test-task-id",
            "file": {"file_id": "test-file-id", "preset_attempts": []},
            "quality_preset": "balanced",  # Non-adaptive
            "quality_result": {"passed": False, "ssim_score": 0.90},
        }

        result = module.handle_quality_failure(event)

        assert "should_retry" in result
        assert "reason" in result, "Result must contain 'reason'"


class TestHandleConversionErrorReturnValue:
    """handle_conversion_error 関数の戻り値テスト."""

    def test_retryable_error_returns_should_retry(self, monkeypatch):
        """リトライ可能エラー時に should_retry が返されることを確認."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")

        module = load_workflow_module()
        module.update_file_status = MagicMock()

        event = {
            "task_id": "test-task-id",
            "file": {"file_id": "test-file-id", "retry_count": 0},
            "error_code": 1517,  # Transient error
            "error_message": "Transient error",
        }

        result = module.handle_conversion_error(event)

        assert "should_retry" in result, "Result must contain 'should_retry'"

    def test_non_retryable_error_returns_error_code(self, monkeypatch):
        """リトライ不可エラー時に error_code が返されることを確認."""
        monkeypatch.setenv("DYNAMODB_TABLE", "test-table")

        module = load_workflow_module()
        module.update_file_status = MagicMock()

        event = {
            "task_id": "test-task-id",
            "file": {"file_id": "test-file-id", "retry_count": 0},
            "error_code": 1010,  # Config error (non-retryable)
            "error_message": "Config error",
        }

        result = module.handle_conversion_error(event)

        assert "should_retry" in result
        assert result["should_retry"] is False
        assert "error_code" in result, "Result must contain 'error_code' for non-retryable errors"
