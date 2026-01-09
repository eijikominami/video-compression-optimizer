"""Property-based tests for ParallelTransferService.

Tests correctness properties using Hypothesis.

Feature: parallel-transfer
Requirements: 1.1, 1.3, 2.1, 2.3, 3.3, 3.4, 5.1, 5.2, 5.4
"""

import time
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.services.parallel_transfer import (
    ConcurrencyConfig,
    ParallelTransferService,
    TransferResult,
    TransferSummary,
)

# =============================================================================
# Strategies for generating test data
# =============================================================================

# Concurrency limit values (including edge cases)
concurrency_limit = st.integers(min_value=-100, max_value=200)

# Valid concurrency limit values (1-10)
valid_concurrency_limit = st.integers(min_value=1, max_value=10)

# Number of items to transfer
item_count = st.integers(min_value=0, max_value=20)

# Success/failure probability
success_probability = st.floats(min_value=0.0, max_value=1.0)


# =============================================================================
# Property 6: Concurrency Limit Validation
# Feature: parallel-transfer, Property 6: Concurrency limit validation
# Validates: Requirements 3.3, 3.4
# =============================================================================


class TestConcurrencyLimitValidationProperty:
    """Property 6: Concurrency limit validation.

    *For any* concurrency limit value:
    - If value < 1, the system shall use 1
    - If value > 10, the system shall use 10
    - Otherwise, the system shall use the specified value
    """

    @given(limit=concurrency_limit)
    @settings(max_examples=100)
    def test_concurrency_config_clamps_download_limit(self, limit: int):
        """Property 6: ConcurrencyConfig clamps download_limit to 1-10.

        Feature: parallel-transfer, Property 6: Concurrency limit validation
        Validates: Requirements 3.3, 3.4
        """
        config = ConcurrencyConfig(download_limit=limit)

        # Value should be clamped to 1-10
        assert 1 <= config.download_limit <= 10

        # Verify clamping logic
        if limit < 1:
            assert config.download_limit == 1
        elif limit > 10:
            assert config.download_limit == 10
        else:
            assert config.download_limit == limit

    @given(limit=concurrency_limit)
    @settings(max_examples=100)
    def test_concurrency_config_clamps_upload_limit(self, limit: int):
        """Property 6: ConcurrencyConfig clamps upload_limit to 1-10.

        Feature: parallel-transfer, Property 6: Concurrency limit validation
        Validates: Requirements 3.3, 3.4
        """
        config = ConcurrencyConfig(upload_limit=limit)

        # Value should be clamped to 1-10
        assert 1 <= config.upload_limit <= 10

        # Verify clamping logic
        if limit < 1:
            assert config.upload_limit == 1
        elif limit > 10:
            assert config.upload_limit == 10
        else:
            assert config.upload_limit == limit

    @given(limit=concurrency_limit)
    @settings(max_examples=100)
    def test_parallel_transfer_service_clamps_limit(self, limit: int):
        """Property 6: ParallelTransferService clamps concurrency_limit to 1-10.

        Feature: parallel-transfer, Property 6: Concurrency limit validation
        Validates: Requirements 3.3, 3.4
        """
        service = ParallelTransferService(concurrency_limit=limit)

        # Value should be clamped to 1-10
        assert 1 <= service.concurrency_limit <= 10

        # Verify clamping logic
        if limit < 1:
            assert service.concurrency_limit == 1
        elif limit > 10:
            assert service.concurrency_limit == 10
        else:
            assert service.concurrency_limit == limit

    @given(
        download_limit=concurrency_limit,
        upload_limit=concurrency_limit,
    )
    @settings(max_examples=100)
    def test_both_limits_clamped_independently(self, download_limit: int, upload_limit: int):
        """Property 6: Both limits are clamped independently.

        Feature: parallel-transfer, Property 6: Concurrency limit validation
        Validates: Requirements 3.3, 3.4
        """
        config = ConcurrencyConfig(download_limit=download_limit, upload_limit=upload_limit)

        # Both should be in valid range
        assert 1 <= config.download_limit <= 10
        assert 1 <= config.upload_limit <= 10


# =============================================================================
# Property 1: Concurrency Limit Respected for Downloads
# Feature: parallel-transfer, Property 1: Concurrency limit is respected for downloads
# Validates: Requirements 1.1
# =============================================================================


class TestDownloadConcurrencyProperty:
    """Property 1: Concurrency limit is respected for downloads.

    *For any* list of videos and any concurrency limit N (1 ≤ N ≤ 10),
    at no point during parallel download execution shall more than N
    downloads be active simultaneously.
    """

    @given(
        limit=valid_concurrency_limit,
        num_items=st.integers(min_value=1, max_value=15),
    )
    @settings(max_examples=100)
    def test_download_concurrency_never_exceeds_limit(self, limit: int, num_items: int):
        """Property 1: Download concurrency never exceeds limit.

        Feature: parallel-transfer, Property 1: Concurrency limit is respected for downloads
        Validates: Requirements 1.1
        """
        service = ParallelTransferService(concurrency_limit=limit)

        # Create download functions that track concurrent execution
        def create_download_func(item_id: str):
            def download():
                time.sleep(0.01)  # Small delay to allow overlap
                return Path(f"/tmp/{item_id}")

            return download

        items = [
            (f"item-{i}", f"file-{i}.mp4", create_download_func(f"item-{i}"))
            for i in range(num_items)
        ]

        result = service.download_parallel(items)

        # Verify concurrency was never exceeded
        max_active = service.get_max_active_count()
        assert max_active <= limit, f"Max active downloads ({max_active}) exceeded limit ({limit})"

        # Verify all items were processed
        assert result.total == num_items

    @given(limit=valid_concurrency_limit)
    @settings(max_examples=50)
    def test_empty_download_list_returns_empty_summary(self, limit: int):
        """Property 1: Empty download list returns empty summary.

        Feature: parallel-transfer, Property 1: Concurrency limit is respected for downloads
        Validates: Requirements 1.1
        """
        service = ParallelTransferService(concurrency_limit=limit)
        result = service.download_parallel([])

        assert result.total == 0
        assert result.successful == 0
        assert result.failed == 0
        assert len(result.results) == 0


# =============================================================================
# Property 2: Concurrency Limit Respected for Uploads
# Feature: parallel-transfer, Property 2: Concurrency limit is respected for uploads
# Validates: Requirements 2.1
# =============================================================================


class TestUploadConcurrencyProperty:
    """Property 2: Concurrency limit is respected for uploads.

    *For any* list of files and any concurrency limit N (1 ≤ N ≤ 10),
    at no point during parallel upload execution shall more than N
    uploads be active simultaneously.
    """

    @given(
        limit=valid_concurrency_limit,
        num_items=st.integers(min_value=1, max_value=15),
    )
    @settings(max_examples=100)
    def test_upload_concurrency_never_exceeds_limit(self, limit: int, num_items: int):
        """Property 2: Upload concurrency never exceeds limit.

        Feature: parallel-transfer, Property 2: Concurrency limit is respected for uploads
        Validates: Requirements 2.1
        """
        service = ParallelTransferService(concurrency_limit=limit)

        # Create upload functions that track concurrent execution
        def create_upload_func():
            def upload():
                time.sleep(0.01)  # Small delay to allow overlap
                return True

            return upload

        items = [(f"item-{i}", f"file-{i}.mp4", create_upload_func()) for i in range(num_items)]

        result = service.upload_parallel(items)

        # Verify concurrency was never exceeded
        max_active = service.get_max_active_count()
        assert max_active <= limit, f"Max active uploads ({max_active}) exceeded limit ({limit})"

        # Verify all items were processed
        assert result.total == num_items

    @given(limit=valid_concurrency_limit)
    @settings(max_examples=50)
    def test_empty_upload_list_returns_empty_summary(self, limit: int):
        """Property 2: Empty upload list returns empty summary.

        Feature: parallel-transfer, Property 2: Concurrency limit is respected for uploads
        Validates: Requirements 2.1
        """
        service = ParallelTransferService(concurrency_limit=limit)
        result = service.upload_parallel([])

        assert result.total == 0
        assert result.successful == 0
        assert result.failed == 0
        assert len(result.results) == 0


# =============================================================================
# Property 3: Download Failures Do Not Affect Other Downloads
# Feature: parallel-transfer, Property 3: Download failures do not affect other downloads
# Validates: Requirements 1.3, 5.1
# =============================================================================


class TestDownloadFailureIsolationProperty:
    """Property 3: Download failures do not affect other downloads.

    *For any* list of videos where some downloads fail, all non-failing
    downloads shall complete successfully and be included in the result.
    """

    @given(
        num_items=st.integers(min_value=2, max_value=10),
        fail_indices=st.lists(st.integers(min_value=0, max_value=9), min_size=1, max_size=5),
    )
    @settings(max_examples=100)
    def test_successful_downloads_complete_despite_failures(
        self, num_items: int, fail_indices: list[int]
    ):
        """Property 3: Successful downloads complete despite failures.

        Feature: parallel-transfer, Property 3: Download failures do not affect other downloads
        Validates: Requirements 1.3, 5.1
        """
        # Normalize fail_indices to be within range
        fail_set = {i % num_items for i in fail_indices}

        service = ParallelTransferService(concurrency_limit=3)

        def create_download_func(item_id: str, should_fail: bool):
            def download():
                if should_fail:
                    raise Exception(f"Simulated failure for {item_id}")
                return Path(f"/tmp/{item_id}")

            return download

        items = [
            (f"item-{i}", f"file-{i}.mp4", create_download_func(f"item-{i}", i in fail_set))
            for i in range(num_items)
        ]

        result = service.download_parallel(items)

        # All items should be processed
        assert result.total == num_items

        # Count expected successes and failures
        expected_failures = len(fail_set)
        expected_successes = num_items - expected_failures

        assert result.successful == expected_successes
        assert result.failed == expected_failures

        # Verify each result
        for r in result.results:
            item_idx = int(r.item_id.split("-")[1])
            if item_idx in fail_set:
                assert r.success is False
                assert r.error_message is not None
            else:
                assert r.success is True
                assert r.local_path is not None


# =============================================================================
# Property 4: Upload Failures Do Not Affect Other Uploads
# Feature: parallel-transfer, Property 4: Upload failures do not affect other uploads
# Validates: Requirements 2.3, 5.2
# =============================================================================


class TestUploadFailureIsolationProperty:
    """Property 4: Upload failures do not affect other uploads.

    *For any* list of files where some uploads fail, all non-failing
    uploads shall complete successfully and be included in the result.
    """

    @given(
        num_items=st.integers(min_value=2, max_value=10),
        fail_indices=st.lists(st.integers(min_value=0, max_value=9), min_size=1, max_size=5),
    )
    @settings(max_examples=100)
    def test_successful_uploads_complete_despite_failures(
        self, num_items: int, fail_indices: list[int]
    ):
        """Property 4: Successful uploads complete despite failures.

        Feature: parallel-transfer, Property 4: Upload failures do not affect other uploads
        Validates: Requirements 2.3, 5.2
        """
        # Normalize fail_indices to be within range
        fail_set = {i % num_items for i in fail_indices}

        service = ParallelTransferService(concurrency_limit=3)

        def create_upload_func(item_id: str, should_fail: bool):
            def upload():
                if should_fail:
                    raise Exception(f"Simulated failure for {item_id}")
                return True

            return upload

        items = [
            (f"item-{i}", f"file-{i}.mp4", create_upload_func(f"item-{i}", i in fail_set))
            for i in range(num_items)
        ]

        result = service.upload_parallel(items)

        # All items should be processed
        assert result.total == num_items

        # Count expected successes and failures
        expected_failures = len(fail_set)
        expected_successes = num_items - expected_failures

        assert result.successful == expected_successes
        assert result.failed == expected_failures

        # Verify each result
        for r in result.results:
            item_idx = int(r.item_id.split("-")[1])
            if item_idx in fail_set:
                assert r.success is False
                assert r.error_message is not None
            else:
                assert r.success is True


# =============================================================================
# Property 5: Transfer Summary Integrity
# Feature: parallel-transfer, Property 5: Transfer summary integrity
# Validates: Requirements 1.5, 2.5, 5.4
# =============================================================================


class TestTransferSummaryIntegrityProperty:
    """Property 5: Transfer summary integrity.

    *For any* parallel transfer operation, the returned summary shall satisfy:
    - total == successful + failed
    - successful equals the count of results with success=True
    - failed equals the count of results with success=False
    - Each failed result contains a non-empty error_message
    """

    @given(
        num_items=st.integers(min_value=1, max_value=15),
        success_pattern=st.lists(st.booleans(), min_size=1, max_size=15),
    )
    @settings(max_examples=100)
    def test_download_summary_integrity(self, num_items: int, success_pattern: list[bool]):
        """Property 5: Download summary has correct counts.

        Feature: parallel-transfer, Property 5: Transfer summary integrity
        Validates: Requirements 1.5, 2.5, 5.4
        """
        # Adjust pattern to match num_items
        pattern = (success_pattern * ((num_items // len(success_pattern)) + 1))[:num_items]

        service = ParallelTransferService(concurrency_limit=3)

        def create_download_func(should_succeed: bool):
            def download():
                if should_succeed:
                    return Path("/tmp/test")
                else:
                    raise Exception("Simulated failure")

            return download

        items = [
            (f"item-{i}", f"file-{i}.mp4", create_download_func(pattern[i]))
            for i in range(num_items)
        ]

        result = service.download_parallel(items)

        # Verify total == successful + failed
        assert result.total == result.successful + result.failed

        # Verify counts match results
        actual_successful = sum(1 for r in result.results if r.success)
        actual_failed = sum(1 for r in result.results if not r.success)

        assert result.successful == actual_successful
        assert result.failed == actual_failed

        # Verify failed results have error messages
        for r in result.results:
            if not r.success:
                assert r.error_message is not None
                assert len(r.error_message) > 0

    @given(
        num_items=st.integers(min_value=1, max_value=15),
        success_pattern=st.lists(st.booleans(), min_size=1, max_size=15),
    )
    @settings(max_examples=100)
    def test_upload_summary_integrity(self, num_items: int, success_pattern: list[bool]):
        """Property 5: Upload summary has correct counts.

        Feature: parallel-transfer, Property 5: Transfer summary integrity
        Validates: Requirements 1.5, 2.5, 5.4
        """
        # Adjust pattern to match num_items
        pattern = (success_pattern * ((num_items // len(success_pattern)) + 1))[:num_items]

        service = ParallelTransferService(concurrency_limit=3)

        def create_upload_func(should_succeed: bool):
            def upload():
                if should_succeed:
                    return True
                else:
                    raise Exception("Simulated failure")

            return upload

        items = [
            (f"item-{i}", f"file-{i}.mp4", create_upload_func(pattern[i])) for i in range(num_items)
        ]

        result = service.upload_parallel(items)

        # Verify total == successful + failed
        assert result.total == result.successful + result.failed

        # Verify counts match results
        actual_successful = sum(1 for r in result.results if r.success)
        actual_failed = sum(1 for r in result.results if not r.success)

        assert result.successful == actual_successful
        assert result.failed == actual_failed

        # Verify failed results have error messages
        for r in result.results:
            if not r.success:
                assert r.error_message is not None
                assert len(r.error_message) > 0

    @given(
        total=st.integers(min_value=0, max_value=100),
        successful=st.integers(min_value=0, max_value=100),
        failed=st.integers(min_value=0, max_value=100),
    )
    @settings(max_examples=100)
    def test_transfer_summary_auto_corrects_counts(self, total: int, successful: int, failed: int):
        """Property 5: TransferSummary auto-corrects mismatched counts.

        Feature: parallel-transfer, Property 5: Transfer summary integrity
        Validates: Requirements 1.5, 2.5, 5.4
        """
        # Create results that may not match the provided counts
        results = []
        for i in range(total):
            results.append(
                TransferResult(
                    item_id=f"item-{i}",
                    filename=f"file-{i}.mp4",
                    success=(i % 2 == 0),  # Alternating success/failure
                    error_message=None if (i % 2 == 0) else "Error",
                )
            )

        summary = TransferSummary(
            total=total,
            successful=successful,  # May not match actual
            failed=failed,  # May not match actual
            results=results,
            total_time_seconds=1.0,
        )

        # After __post_init__, counts should match results
        actual_successful = sum(1 for r in results if r.success)
        actual_failed = sum(1 for r in results if not r.success)

        assert summary.successful == actual_successful
        assert summary.failed == actual_failed
        assert summary.total == len(results)
