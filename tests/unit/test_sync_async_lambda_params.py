"""Test sync/async Lambda parameter consistency.

Ensures that sync mode and async mode pass the same parameters
to shared Lambda functions (e.g., quality-checker).

This test was added after discovering that async mode was missing
metadata_s3_key parameter, causing capture dates to not be embedded.
"""

import json
from pathlib import Path


class TestQualityCheckerParameterConsistency:
    """Verify quality-checker Lambda receives same params in sync and async modes."""

    def test_async_workflow_passes_metadata_s3_key(self):
        """Step Functions must pass metadata_s3_key to quality-checker."""
        # Load Step Functions definition
        asl_path = (
            Path(__file__).parent.parent.parent
            / "sam-app"
            / "statemachine"
            / "async-workflow.asl.json"
        )

        with open(asl_path) as f:
            asl = json.load(f)

        # Find VerifyQuality state
        verify_quality = asl["States"]["ProcessFiles"]["Iterator"]["States"]["VerifyQuality"]
        params = verify_quality["Parameters"]

        # Verify required parameters are present
        assert "job_id.$" in params, "Missing job_id parameter"
        assert "original_s3_key.$" in params or "original_s3_key" in params, (
            "Missing original_s3_key parameter"
        )
        assert "converted_s3_key.$" in params or "converted_s3_key" in params, (
            "Missing converted_s3_key parameter"
        )
        assert "metadata_s3_key.$" in params or "metadata_s3_key" in params, (
            "Missing metadata_s3_key parameter - capture date will not be embedded!"
        )

    def test_sync_mode_quality_checker_params(self):
        """Sync mode must pass metadata_s3_key to quality-checker."""
        import inspect

        from vco.quality.checker import QualityChecker

        # Check trigger_quality_check_sync signature
        sig = inspect.signature(QualityChecker.trigger_quality_check_sync)
        param_names = list(sig.parameters.keys())

        assert "metadata_s3_key" in param_names, (
            "QualityChecker.trigger_quality_check_sync missing metadata_s3_key parameter"
        )

    def test_async_file_data_includes_metadata_s3_key(self):
        """Async convert must include metadata_s3_key in file data."""
        # This verifies the data structure passed to Step Functions
        # The file object must contain metadata_s3_key for the workflow to use it

        # Check that AsyncFile model has metadata_s3_key field
        import dataclasses

        from vco.models.async_task import AsyncFile

        field_names = [f.name for f in dataclasses.fields(AsyncFile)]
        assert "metadata_s3_key" in field_names, "AsyncFile model missing metadata_s3_key field"

    def test_parameter_mapping_consistency(self):
        """Verify parameter names are consistent between sync and async."""
        # Load Step Functions definition
        asl_path = (
            Path(__file__).parent.parent.parent
            / "sam-app"
            / "statemachine"
            / "async-workflow.asl.json"
        )

        with open(asl_path) as f:
            asl = json.load(f)

        verify_quality = asl["States"]["ProcessFiles"]["Iterator"]["States"]["VerifyQuality"]
        async_params = set(verify_quality["Parameters"].keys())

        # Expected parameters based on quality-checker Lambda
        expected_params = {
            "job_id.$",
            "original_s3_key.$",
            "converted_s3_key.$",
            "metadata_s3_key.$",
        }

        missing = expected_params - async_params
        assert not missing, f"Step Functions missing parameters: {missing}"
