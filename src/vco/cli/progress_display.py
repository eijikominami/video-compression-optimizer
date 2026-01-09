"""Progress display for parallel transfers.

This module provides Rich-based progress display for concurrent file transfers,
showing individual progress bars for each active transfer and overall progress.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


@dataclass
class TransferState:
    """State of a single transfer."""

    filename: str
    task_id: TaskID | None = None
    started_at: float = 0.0
    completed_at: float = 0.0
    is_complete: bool = False
    is_success: bool = False


@dataclass
class ParallelProgressDisplay:
    """Display progress for parallel file transfers.

    Shows:
    - Overall progress (e.g., "3/10 files completed")
    - Individual progress bars for active transfers
    - Estimated time remaining based on throughput
    """

    console: Console
    total_files: int
    operation: str = "Transferring"  # "Downloading" or "Uploading"
    _progress: Progress | None = field(default=None, init=False)
    _overall_task: TaskID | None = field(default=None, init=False)
    _active_transfers: dict[str, TransferState] = field(default_factory=dict, init=False)
    _completed_count: int = field(default=0, init=False)
    _start_time: float = field(default=0.0, init=False)
    _completed_times: list[float] = field(default_factory=list, init=False)

    def __enter__(self) -> "ParallelProgressDisplay":
        """Start the progress display."""
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console,
        )
        self._progress.start()
        self._start_time = time.time()

        # Add overall progress task
        self._overall_task = self._progress.add_task(
            f"{self.operation} 0/{self.total_files} files",
            total=self.total_files,
        )

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop the progress display."""
        if self._progress:
            # Complete the overall task
            if self._overall_task is not None:
                self._progress.update(
                    self._overall_task,
                    completed=self.total_files,
                    description=f"{self.operation} {self.total_files}/{self.total_files} files",
                )
            self._progress.stop()

    def start_transfer(self, item_id: str, filename: str) -> None:
        """Mark a transfer as started.

        Args:
            item_id: Unique identifier for the transfer
            filename: Name of the file being transferred
        """
        if self._progress is None:
            return

        state = TransferState(
            filename=filename,
            started_at=time.time(),
        )
        self._active_transfers[item_id] = state

    def complete_transfer(self, item_id: str, success: bool) -> None:
        """Mark a transfer as completed.

        Args:
            item_id: Unique identifier for the transfer
            success: Whether the transfer was successful
        """
        if self._progress is None or self._overall_task is None:
            return

        state = self._active_transfers.get(item_id)
        if state:
            state.completed_at = time.time()
            state.is_complete = True
            state.is_success = success

            # Track completion time for ETA calculation
            transfer_time = state.completed_at - state.started_at
            self._completed_times.append(transfer_time)

        self._completed_count += 1

        # Update overall progress
        self._progress.update(
            self._overall_task,
            completed=self._completed_count,
            description=f"{self.operation} {self._completed_count}/{self.total_files} files",
        )

    def get_progress_callback(self) -> Callable[[str, int, int, int], None]:
        """Get a callback function for progress updates.

        Returns:
            Callback function (filename, percent, current, total)
        """

        def callback(filename: str, percent: int, current: int, total: int) -> None:
            if self._progress is None or self._overall_task is None:
                return

            self._completed_count = current
            self._progress.update(
                self._overall_task,
                completed=current,
                description=f"{self.operation} {current}/{total} files",
            )

        return callback

    def get_estimated_remaining_seconds(self) -> float | None:
        """Calculate estimated remaining time based on throughput.

        Returns:
            Estimated seconds remaining, or None if not enough data
        """
        if not self._completed_times or self._completed_count == 0:
            return None

        # Calculate average time per file
        avg_time = sum(self._completed_times) / len(self._completed_times)

        # Estimate remaining time
        remaining_files = self.total_files - self._completed_count
        return avg_time * remaining_files
