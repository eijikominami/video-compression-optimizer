"""Property tests for MediaConvert error classification.

**Property 6: MediaConvert エラー分類の正確性**
**Validates: Requirements 5.2**

Tests that MediaConvert error codes are correctly classified:
- Transient errors (1517, 1522, 1550, 1999): retryable
- Config/input errors (1010, 1030, 1040, 1401): not retryable
- Unknown errors: not retryable (safe default)
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.services.error_handling import (
    CONFIG_ERRORS,
    PERMISSION_ERRORS,
    TRANSIENT_ERRORS,
    classify_mediaconvert_error,
)


class TestMediaConvertErrorClassification:
    """Property tests for MediaConvert error classification.

    Validates: Requirements 5.2
    - Transient errors (1517, 1522, 1550, 1999) are retryable
    - Config/input errors (1010, 1030, 1040, 1401) are not retryable
    """

    # Test data defined independently from implementation
    # Source: AWS MediaConvert error codes documentation
    EXPECTED_TRANSIENT_ERRORS = {1517, 1522, 1550, 1999}
    EXPECTED_CONFIG_ERRORS = {1010, 1030, 1040}
    EXPECTED_PERMISSION_ERRORS = {1401, 1432, 1433}

    def test_implementation_covers_all_transient_errors(self):
        """Verify implementation covers all expected transient errors."""
        for error_code in self.EXPECTED_TRANSIENT_ERRORS:
            assert error_code in TRANSIENT_ERRORS, f"Missing transient error: {error_code}"

    def test_implementation_covers_all_config_errors(self):
        """Verify implementation covers all expected config errors."""
        for error_code in self.EXPECTED_CONFIG_ERRORS:
            assert error_code in CONFIG_ERRORS, f"Missing config error: {error_code}"

    def test_implementation_covers_all_permission_errors(self):
        """Verify implementation covers all expected permission errors."""
        for error_code in self.EXPECTED_PERMISSION_ERRORS:
            assert error_code in PERMISSION_ERRORS, f"Missing permission error: {error_code}"

    @given(error_code=st.sampled_from(list(TRANSIENT_ERRORS)))
    @settings(max_examples=100)
    def test_transient_errors_are_retryable(self, error_code: int):
        """
        Property: For all transient error codes, classification returns retryable=True.

        Feature: async-workflow, Property 6: MediaConvert エラー分類の正確性
        Validates: Requirements 5.2
        """
        result = classify_mediaconvert_error(error_code)

        assert result.is_retryable is True
        assert result.category == "transient"
        assert result.error_code == error_code

    @given(error_code=st.sampled_from(list(CONFIG_ERRORS)))
    @settings(max_examples=100)
    def test_config_errors_are_not_retryable(self, error_code: int):
        """
        Property: For all config error codes, classification returns retryable=False.

        Feature: async-workflow, Property 6: MediaConvert エラー分類の正確性
        Validates: Requirements 5.2
        """
        result = classify_mediaconvert_error(error_code)

        assert result.is_retryable is False
        assert result.category == "config_or_input"
        assert result.error_code == error_code

    @given(error_code=st.sampled_from(list(PERMISSION_ERRORS)))
    @settings(max_examples=100)
    def test_permission_errors_are_not_retryable(self, error_code: int):
        """
        Property: For all permission error codes, classification returns retryable=False.

        Feature: async-workflow, Property 6: MediaConvert エラー分類の正確性
        Validates: Requirements 5.2
        """
        result = classify_mediaconvert_error(error_code)

        assert result.is_retryable is False
        assert result.category == "permission"
        assert result.error_code == error_code

    @given(error_code=st.integers(min_value=1000, max_value=2000))
    @settings(max_examples=200)
    def test_unknown_errors_are_not_retryable(self, error_code: int):
        """
        Property: For all unknown error codes, classification returns retryable=False.

        This is a safety property: unknown errors should not be retried.

        Feature: async-workflow, Property 6: MediaConvert エラー分類の正確性
        Validates: Requirements 5.2
        """
        known_errors = TRANSIENT_ERRORS | CONFIG_ERRORS | PERMISSION_ERRORS

        result = classify_mediaconvert_error(error_code)

        if error_code in known_errors:
            # Known error: check specific category
            if error_code in TRANSIENT_ERRORS:
                assert result.is_retryable is True
                assert result.category == "transient"
            else:
                assert result.is_retryable is False
        else:
            # Unknown error: must not be retryable
            assert result.is_retryable is False
            assert result.category == "unknown"

    @given(error_code=st.integers(min_value=1000, max_value=2000))
    @settings(max_examples=200)
    def test_error_code_preserved_in_result(self, error_code: int):
        """
        Property: For all error codes, the original code is preserved in result.

        Feature: async-workflow, Property 6: MediaConvert エラー分類の正確性
        Validates: Requirements 5.2
        """
        result = classify_mediaconvert_error(error_code)

        assert result.error_code == error_code

    @given(error_code=st.integers(min_value=1000, max_value=2000))
    @settings(max_examples=200)
    def test_category_is_valid(self, error_code: int):
        """
        Property: For all error codes, category is one of the valid values.

        Feature: async-workflow, Property 6: MediaConvert エラー分類の正確性
        Validates: Requirements 5.2
        """
        valid_categories = {"transient", "config_or_input", "permission", "unknown"}

        result = classify_mediaconvert_error(error_code)

        assert result.category in valid_categories

    def test_specific_transient_error_1517(self):
        """Example: Error 1517 (service unavailable) is transient."""
        result = classify_mediaconvert_error(1517)
        assert result.is_retryable is True
        assert result.category == "transient"

    def test_specific_config_error_1010(self):
        """Example: Error 1010 (invalid input) is config error."""
        result = classify_mediaconvert_error(1010)
        assert result.is_retryable is False
        assert result.category == "config_or_input"

    def test_specific_permission_error_1401(self):
        """Example: Error 1401 (access denied) is permission error."""
        result = classify_mediaconvert_error(1401)
        assert result.is_retryable is False
        assert result.category == "permission"

    def test_unknown_error_1234(self):
        """Example: Unknown error 1234 is not retryable."""
        result = classify_mediaconvert_error(1234)
        assert result.is_retryable is False
        assert result.category == "unknown"
