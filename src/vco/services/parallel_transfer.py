"""Parallel transfer service for concurrent iCloud downloads and S3 uploads.

This module provides parallel file transfer capabilities using ThreadPoolExecutor
to improve overall transfer performance when processing multiple files.
"""

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class ConcurrencyConfig:
    """Configuration for parallel transfer concurrency limits.

    Attributes:
        download_limit: Maximum concurrent downloads (1-10, default: 3)
        upload_limit: Maximum concurrent uploads (1-10, default: 3)
    """

    download_limit: int = 3
    upload_limit: int = 3

    def __post_init__(self) -> None:
        """Validate and clamp concurrency limits to valid range (1-10)."""
        self.download_limit = min(max(self.download_limit, 1), 10)
        self.upload_limit = min(max(self.upload_limit, 1), 10)


@dataclass
class TransferResult:
    """Result of a single file transfer operation.

    Attributes:
        item_id: Unique identifier for the transferred item
        filename: Name of the transferred file
        success: Whether the transfer completed successfully
        local_path: Local file path (for downloads) or None
        error_message: Error message if transfer failed
        transfer_time_seconds: Time taken for the transfer in seconds
    """

    item_id: str
    filename: str
    success: bool
    local_path: Path | None = None
    error_message: str | None = None
    transfer_time_seconds: float = 0.0


@dataclass
class TransferSummary:
    """Summary of a parallel transfer operation.

    Attributes:
        total: Total number of transfers attempted
        successful: Number of successful transfers
        failed: Number of failed transfers
        results: List of individual transfer results
        total_time_seconds: Total time for all transfers in seconds
    """

    total: int
    successful: int
    failed: int
    results: list[TransferResult]
    total_time_seconds: float

    def __post_init__(self) -> None:
        """Validate summary integrity."""
        # Ensure counts match results
        actual_successful = sum(1 for r in self.results if r.success)
        actual_failed = sum(1 for r in self.results if not r.success)
        if self.successful != actual_successful:
            self.successful = actual_successful
        if self.failed != actual_failed:
            self.failed = actual_failed
        if self.total != len(self.results):
            self.total = len(self.results)


@dataclass
class TransferProgress:
    """Progress information for a single transfer.

    Attributes:
        filename: Name of the file being transferred
        progress_percent: Transfer progress (0-100)
        transferred_bytes: Number of bytes transferred
        total_bytes: Total file size in bytes
        status: Current transfer status
    """

    filename: str
    progress_percent: int
    transferred_bytes: int
    total_bytes: int
    status: str  # "downloading", "uploading", "completed", "failed"


# =============================================================================
# Parallel Transfer Service
# =============================================================================


class ParallelTransferService:
    """Service for managing parallel file transfers.

    Uses ThreadPoolExecutor to execute multiple downloads or uploads
    concurrently while respecting the configured concurrency limits.
    Supports graceful cancellation via Ctrl+C.
    """

    def __init__(
        self,
        concurrency_limit: int = 3,
        progress_callback: Callable[[str, int, int, int], None] | None = None,
    ) -> None:
        """Initialize the parallel transfer service.

        Args:
            concurrency_limit: Maximum number of concurrent transfers (1-10, default: 3)
            progress_callback: Optional callback for progress updates
                              (filename, percent, current_index, total_count)
        """
        self.concurrency_limit = min(max(concurrency_limit, 1), 10)
        self.progress_callback = progress_callback
        self._active_count = 0
        self._active_count_lock = threading.Lock()
        self._max_active_count = 0
        self._cancelled = False
        self._cancel_lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = None
        self._futures: list[Future] = []

    def cancel(self) -> None:
        """Cancel all pending transfers.

        Running transfers will complete, but no new transfers will start.
        """
        with self._cancel_lock:
            self._cancelled = True
            logger.info("Transfer cancellation requested")

        # Cancel pending futures
        for future in self._futures:
            future.cancel()

        # Shutdown executor without waiting
        if self._executor:
            self._executor.shutdown(wait=False, cancel_futures=True)

    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested.

        Returns:
            True if cancel() has been called
        """
        with self._cancel_lock:
            return self._cancelled

    def _reset_cancel_state(self) -> None:
        """Reset cancellation state for new transfer operation."""
        with self._cancel_lock:
            self._cancelled = False
        self._futures = []

    def _track_active(self, increment: bool) -> None:
        """Track the number of active transfers for testing.

        Args:
            increment: True to increment, False to decrement
        """
        with self._active_count_lock:
            if increment:
                self._active_count += 1
                self._max_active_count = max(self._max_active_count, self._active_count)
            else:
                self._active_count -= 1

    def get_max_active_count(self) -> int:
        """Get the maximum number of concurrent transfers observed.

        Returns:
            Maximum number of transfers that were active simultaneously
        """
        return self._max_active_count

    def reset_tracking(self) -> None:
        """Reset the active count tracking for testing."""
        with self._active_count_lock:
            self._active_count = 0
            self._max_active_count = 0

    def download_parallel(
        self,
        items: list[tuple[str, str, Callable[[], Path | None]]],
    ) -> TransferSummary:
        """Download multiple files in parallel.

        Args:
            items: List of (item_id, filename, download_func) tuples
                   download_func should return the local Path on success or None on failure

        Returns:
            TransferSummary with results for all downloads
        """
        start_time = time.time()
        results: list[TransferResult] = []

        if not items:
            return TransferSummary(
                total=0,
                successful=0,
                failed=0,
                results=[],
                total_time_seconds=0.0,
            )

        self.reset_tracking()

        def execute_download(
            item_id: str, filename: str, download_func: Callable[[], Path | None]
        ) -> TransferResult:
            """Execute a single download with tracking."""
            transfer_start = time.time()
            self._track_active(True)
            try:
                local_path = download_func()
                transfer_time = time.time() - transfer_start
                if local_path:
                    return TransferResult(
                        item_id=item_id,
                        filename=filename,
                        success=True,
                        local_path=local_path,
                        transfer_time_seconds=transfer_time,
                    )
                else:
                    return TransferResult(
                        item_id=item_id,
                        filename=filename,
                        success=False,
                        error_message="Download returned None",
                        transfer_time_seconds=transfer_time,
                    )
            except Exception as e:
                transfer_time = time.time() - transfer_start
                logger.error(f"Download failed for {filename}: {e}")
                return TransferResult(
                    item_id=item_id,
                    filename=filename,
                    success=False,
                    error_message=str(e),
                    transfer_time_seconds=transfer_time,
                )
            finally:
                self._track_active(False)

        with ThreadPoolExecutor(max_workers=self.concurrency_limit) as executor:
            self._executor = executor
            self._reset_cancel_state()

            futures = {
                executor.submit(execute_download, item_id, filename, download_func): (
                    item_id,
                    filename,
                )
                for item_id, filename, download_func in items
            }
            self._futures = list(futures.keys())

            completed_count = 0
            cancelled_count = 0
            for future in as_completed(futures):
                if self.is_cancelled():
                    # Mark remaining as cancelled
                    cancelled_count += 1
                    item_id, filename = futures[future]
                    results.append(
                        TransferResult(
                            item_id=item_id,
                            filename=filename,
                            success=False,
                            error_message="Cancelled by user",
                            transfer_time_seconds=0.0,
                        )
                    )
                    continue

                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    item_id, filename = futures[future]
                    results.append(
                        TransferResult(
                            item_id=item_id,
                            filename=filename,
                            success=False,
                            error_message=str(e),
                            transfer_time_seconds=0.0,
                        )
                    )
                completed_count += 1

                if self.progress_callback:
                    self.progress_callback(
                        result.filename,
                        100 if result.success else 0,
                        completed_count,
                        len(items),
                    )

            self._executor = None

        total_time = time.time() - start_time
        successful = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)

        return TransferSummary(
            total=len(results),
            successful=successful,
            failed=failed,
            results=results,
            total_time_seconds=total_time,
        )

    def upload_parallel(
        self,
        items: list[tuple[str, str, Callable[[], bool]]],
    ) -> TransferSummary:
        """Upload multiple files in parallel.

        Args:
            items: List of (item_id, filename, upload_func) tuples
                   upload_func should return True on success, False on failure

        Returns:
            TransferSummary with results for all uploads
        """
        start_time = time.time()
        results: list[TransferResult] = []

        if not items:
            return TransferSummary(
                total=0,
                successful=0,
                failed=0,
                results=[],
                total_time_seconds=0.0,
            )

        self.reset_tracking()

        def execute_upload(
            item_id: str, filename: str, upload_func: Callable[[], bool]
        ) -> TransferResult:
            """Execute a single upload with tracking."""
            transfer_start = time.time()
            self._track_active(True)
            try:
                success = upload_func()
                transfer_time = time.time() - transfer_start
                return TransferResult(
                    item_id=item_id,
                    filename=filename,
                    success=success,
                    error_message=None if success else "Upload returned False",
                    transfer_time_seconds=transfer_time,
                )
            except Exception as e:
                transfer_time = time.time() - transfer_start
                logger.error(f"Upload failed for {filename}: {e}")
                return TransferResult(
                    item_id=item_id,
                    filename=filename,
                    success=False,
                    error_message=str(e),
                    transfer_time_seconds=transfer_time,
                )
            finally:
                self._track_active(False)

        with ThreadPoolExecutor(max_workers=self.concurrency_limit) as executor:
            self._executor = executor
            self._reset_cancel_state()

            futures = {
                executor.submit(execute_upload, item_id, filename, upload_func): (
                    item_id,
                    filename,
                )
                for item_id, filename, upload_func in items
            }
            self._futures = list(futures.keys())

            completed_count = 0
            for future in as_completed(futures):
                if self.is_cancelled():
                    # Mark remaining as cancelled
                    item_id, filename = futures[future]
                    results.append(
                        TransferResult(
                            item_id=item_id,
                            filename=filename,
                            success=False,
                            error_message="Cancelled by user",
                            transfer_time_seconds=0.0,
                        )
                    )
                    continue

                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    item_id, filename = futures[future]
                    results.append(
                        TransferResult(
                            item_id=item_id,
                            filename=filename,
                            success=False,
                            error_message=str(e),
                            transfer_time_seconds=0.0,
                        )
                    )
                completed_count += 1

                if self.progress_callback:
                    self.progress_callback(
                        result.filename,
                        100 if result.success else 0,
                        completed_count,
                        len(items),
                    )

            self._executor = None

        total_time = time.time() - start_time
        successful = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)

        return TransferSummary(
            total=len(results),
            successful=successful,
            failed=failed,
            results=results,
            total_time_seconds=total_time,
        )
