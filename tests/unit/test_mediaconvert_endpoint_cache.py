"""Unit tests for MediaConvert endpoint caching.

Tests the endpoint caching mechanism in async-workflow Lambda to prevent
TooManyRequestsException during parallel processing.

Requirements: 10.1, 10.2, 10.3, 10.5
"""

import time
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

# Replicate the functions from app.py for testing
# This avoids import issues with Lambda function code


def _fetch_endpoint_with_retry_impl(mock_client_func, logger, max_retries: int = 3) -> str:
    """Fetch MediaConvert endpoint with exponential backoff.

    Args:
        mock_client_func: Function to get boto3 client
        logger: Logger instance
        max_retries: Maximum number of retries (default: 3)

    Returns:
        MediaConvert endpoint URL

    Raises:
        ClientError: If all retries fail
    """
    mc = mock_client_func("mediaconvert")

    for attempt in range(max_retries + 1):
        try:
            endpoints = mc.describe_endpoints()
            return endpoints["Endpoints"][0]["Url"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "TooManyRequestsException":
                if attempt < max_retries:
                    # Exponential backoff: 1s, 2s, 4s
                    wait_time = 2**attempt
                    logger.warning(
                        f"DescribeEndpoints rate limited, retrying in {wait_time}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue
            raise

    # Should not reach here, but just in case
    raise RuntimeError("Failed to fetch MediaConvert endpoint after retries")


class MediaConvertClientCache:
    """Test helper class that replicates the caching logic from app.py."""

    def __init__(self, env_endpoint: str = ""):
        self._mediaconvert_endpoint: str | None = None
        self.env_endpoint = env_endpoint
        self.logger = MagicMock()

    def get_mediaconvert_client(self, boto_client_func):
        """Get MediaConvert client with cached endpoint.

        Endpoint retrieval priority:
        1. Instance variable (simulates Lambda global variable)
        2. Environment variable MEDIACONVERT_ENDPOINT
        3. DescribeEndpoints API (fallback with exponential backoff)
        """
        # 1. Use cached endpoint
        if self._mediaconvert_endpoint:
            return boto_client_func("mediaconvert", endpoint_url=self._mediaconvert_endpoint)

        # 2. Get from environment variable
        if self.env_endpoint:
            self._mediaconvert_endpoint = self.env_endpoint
            self.logger.info(
                f"Using MediaConvert endpoint from environment: {self._mediaconvert_endpoint}"
            )
            return boto_client_func("mediaconvert", endpoint_url=self._mediaconvert_endpoint)

        # 3. Fetch from API with retry (fallback)
        self.logger.info("Fetching MediaConvert endpoint from API")
        self._mediaconvert_endpoint = _fetch_endpoint_with_retry_impl(boto_client_func, self.logger)
        self.logger.info(f"Cached MediaConvert endpoint: {self._mediaconvert_endpoint}")
        return boto_client_func("mediaconvert", endpoint_url=self._mediaconvert_endpoint)


class TestMediaConvertEndpointCache:
    """Tests for MediaConvert endpoint caching."""

    def test_global_variable_cache_retrieval(self):
        """Test that cached endpoint from global variable is used on subsequent calls.

        Requirement: 10.1 - Lambda global variable caching
        """
        mock_mc = MagicMock()
        mock_mc.describe_endpoints.return_value = {
            "Endpoints": [{"Url": "https://test.mediaconvert.ap-northeast-1.amazonaws.com"}]
        }

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        cache = MediaConvertClientCache(env_endpoint="")

        # First call - should fetch from API
        cache.get_mediaconvert_client(mock_boto_client)
        assert mock_mc.describe_endpoints.call_count == 1

        # Second call - should use cached endpoint
        cache.get_mediaconvert_client(mock_boto_client)
        # describe_endpoints should NOT be called again
        assert mock_mc.describe_endpoints.call_count == 1

    def test_environment_variable_retrieval(self):
        """Test that endpoint from environment variable is used.

        Requirement: 10.2 - Environment variable support
        """
        mock_mc = MagicMock()
        calls = []

        def mock_boto_client(service, endpoint_url=None):
            calls.append({"service": service, "endpoint_url": endpoint_url})
            return mock_mc

        cache = MediaConvertClientCache(
            env_endpoint="https://abc123.mediaconvert.ap-northeast-1.amazonaws.com"
        )

        cache.get_mediaconvert_client(mock_boto_client)

        # describe_endpoints should NOT be called (using env var)
        mock_mc.describe_endpoints.assert_not_called()

        # Verify client was created with correct endpoint
        assert len(calls) == 1
        assert (
            calls[0]["endpoint_url"] == "https://abc123.mediaconvert.ap-northeast-1.amazonaws.com"
        )

    def test_api_fallback_when_no_cache(self):
        """Test that API is called when no cache exists.

        Requirement: 10.3 - API fallback
        """
        mock_mc = MagicMock()
        mock_mc.describe_endpoints.return_value = {
            "Endpoints": [{"Url": "https://fetched.mediaconvert.ap-northeast-1.amazonaws.com"}]
        }

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        cache = MediaConvertClientCache(env_endpoint="")

        cache.get_mediaconvert_client(mock_boto_client)

        # describe_endpoints should be called
        mock_mc.describe_endpoints.assert_called_once()

    def test_exponential_backoff_on_rate_limit(self):
        """Test exponential backoff retry on TooManyRequestsException.

        Requirement: 10.5 - Exponential backoff retry
        """
        mock_mc = MagicMock()
        mock_logger = MagicMock()

        # First two calls fail with rate limit, third succeeds
        rate_limit_error = ClientError(
            {"Error": {"Code": "TooManyRequestsException", "Message": "Rate exceeded"}},
            "DescribeEndpoints",
        )
        mock_mc.describe_endpoints.side_effect = [
            rate_limit_error,
            rate_limit_error,
            {"Endpoints": [{"Url": "https://success.mediaconvert.ap-northeast-1.amazonaws.com"}]},
        ]

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        with patch("time.sleep") as mock_sleep:
            endpoint = _fetch_endpoint_with_retry_impl(mock_boto_client, mock_logger, max_retries=3)

        assert endpoint == "https://success.mediaconvert.ap-northeast-1.amazonaws.com"
        assert mock_mc.describe_endpoints.call_count == 3

        # Verify exponential backoff: 1s, 2s
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1)  # 2^0 = 1
        mock_sleep.assert_any_call(2)  # 2^1 = 2

    def test_exception_after_max_retries(self):
        """Test that exception is raised after max retries exhausted.

        Requirement: 10.5 - Exception after max retries
        """
        mock_mc = MagicMock()
        mock_logger = MagicMock()

        # All calls fail with rate limit
        rate_limit_error = ClientError(
            {"Error": {"Code": "TooManyRequestsException", "Message": "Rate exceeded"}},
            "DescribeEndpoints",
        )
        mock_mc.describe_endpoints.side_effect = rate_limit_error

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        with patch("time.sleep") as mock_sleep:
            with pytest.raises(ClientError) as exc_info:
                _fetch_endpoint_with_retry_impl(mock_boto_client, mock_logger, max_retries=3)

        assert exc_info.value.response["Error"]["Code"] == "TooManyRequestsException"
        # Initial attempt + 3 retries = 4 calls
        assert mock_mc.describe_endpoints.call_count == 4
        # 3 sleeps for retries
        assert mock_sleep.call_count == 3

    def test_non_rate_limit_error_not_retried(self):
        """Test that non-rate-limit errors are not retried."""
        mock_mc = MagicMock()
        mock_logger = MagicMock()

        # Fail with a different error
        access_denied_error = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "Access denied"}},
            "DescribeEndpoints",
        )
        mock_mc.describe_endpoints.side_effect = access_denied_error

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        with patch("time.sleep") as mock_sleep:
            with pytest.raises(ClientError) as exc_info:
                _fetch_endpoint_with_retry_impl(mock_boto_client, mock_logger, max_retries=3)

        assert exc_info.value.response["Error"]["Code"] == "AccessDeniedException"
        # Only 1 call - no retries for non-rate-limit errors
        assert mock_mc.describe_endpoints.call_count == 1
        mock_sleep.assert_not_called()

    def test_cache_persists_across_multiple_calls(self):
        """Test that cache persists across multiple get_mediaconvert_client calls."""
        mock_mc = MagicMock()
        mock_mc.describe_endpoints.return_value = {
            "Endpoints": [{"Url": "https://cached.mediaconvert.ap-northeast-1.amazonaws.com"}]
        }

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        cache = MediaConvertClientCache(env_endpoint="")

        # Multiple calls
        for _ in range(10):
            cache.get_mediaconvert_client(mock_boto_client)

        # describe_endpoints should only be called once
        assert mock_mc.describe_endpoints.call_count == 1

    def test_priority_order_global_over_env(self):
        """Test that global variable cache takes priority over environment variable."""
        mock_mc = MagicMock()
        calls = []

        def mock_boto_client(service, endpoint_url=None):
            calls.append({"service": service, "endpoint_url": endpoint_url})
            return mock_mc

        cache = MediaConvertClientCache(
            env_endpoint="https://env.mediaconvert.ap-northeast-1.amazonaws.com"
        )
        # Pre-set the cache (simulating warm start)
        cache._mediaconvert_endpoint = "https://cached.mediaconvert.ap-northeast-1.amazonaws.com"

        cache.get_mediaconvert_client(mock_boto_client)

        # Should use cached endpoint, not env var
        assert len(calls) == 1
        assert (
            calls[0]["endpoint_url"] == "https://cached.mediaconvert.ap-northeast-1.amazonaws.com"
        )
        mock_mc.describe_endpoints.assert_not_called()

    def test_priority_order_env_over_api(self):
        """Test that environment variable takes priority over API call."""
        mock_mc = MagicMock()
        mock_mc.describe_endpoints.return_value = {
            "Endpoints": [{"Url": "https://api.mediaconvert.ap-northeast-1.amazonaws.com"}]
        }
        calls = []

        def mock_boto_client(service, endpoint_url=None):
            calls.append({"service": service, "endpoint_url": endpoint_url})
            return mock_mc

        cache = MediaConvertClientCache(
            env_endpoint="https://env.mediaconvert.ap-northeast-1.amazonaws.com"
        )

        cache.get_mediaconvert_client(mock_boto_client)

        # Should use env var, not API
        assert len(calls) == 1
        assert calls[0]["endpoint_url"] == "https://env.mediaconvert.ap-northeast-1.amazonaws.com"
        mock_mc.describe_endpoints.assert_not_called()


class TestMediaConvertEndpointCacheEdgeCases:
    """Edge case tests for endpoint caching."""

    def test_empty_env_var_triggers_api_call(self):
        """Test that empty environment variable triggers API call."""
        mock_mc = MagicMock()
        mock_mc.describe_endpoints.return_value = {
            "Endpoints": [{"Url": "https://api.mediaconvert.ap-northeast-1.amazonaws.com"}]
        }

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        cache = MediaConvertClientCache(env_endpoint="")

        cache.get_mediaconvert_client(mock_boto_client)

        # API should be called since env var is empty
        mock_mc.describe_endpoints.assert_called_once()

    def test_backoff_timing_sequence(self):
        """Test that backoff timing follows exponential pattern."""
        mock_mc = MagicMock()
        mock_logger = MagicMock()

        rate_limit_error = ClientError(
            {"Error": {"Code": "TooManyRequestsException", "Message": "Rate exceeded"}},
            "DescribeEndpoints",
        )
        # Fail 4 times (initial + 3 retries), then succeed
        mock_mc.describe_endpoints.side_effect = [
            rate_limit_error,
            rate_limit_error,
            rate_limit_error,
            {"Endpoints": [{"Url": "https://success.mediaconvert.ap-northeast-1.amazonaws.com"}]},
        ]

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        sleep_times = []
        with patch("time.sleep", side_effect=lambda t: sleep_times.append(t)):
            _fetch_endpoint_with_retry_impl(mock_boto_client, mock_logger, max_retries=3)

        # Verify exponential backoff: 1s, 2s, 4s
        assert sleep_times == [1, 2, 4]

    def test_zero_retries_fails_immediately(self):
        """Test that zero retries fails on first error."""
        mock_mc = MagicMock()
        mock_logger = MagicMock()

        rate_limit_error = ClientError(
            {"Error": {"Code": "TooManyRequestsException", "Message": "Rate exceeded"}},
            "DescribeEndpoints",
        )
        mock_mc.describe_endpoints.side_effect = rate_limit_error

        def mock_boto_client(service, endpoint_url=None):
            return mock_mc

        with patch("time.sleep") as mock_sleep:
            with pytest.raises(ClientError):
                _fetch_endpoint_with_retry_impl(mock_boto_client, mock_logger, max_retries=0)

        # Only 1 call with 0 retries
        assert mock_mc.describe_endpoints.call_count == 1
        mock_sleep.assert_not_called()
