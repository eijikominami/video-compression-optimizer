"""Download progress store for resumable downloads.

This service handles:
1. Store download progress locally
2. Resume interrupted downloads
3. Track completed downloads

Requirements: 4.5
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DownloadProgress:
    """Download progress for a single file.

    Requirements: 4.5
    """

    task_id: str
    file_id: str
    total_bytes: int
    downloaded_bytes: int
    local_temp_path: str
    s3_key: str
    checksum: str | None = None
    last_updated: datetime = field(default_factory=datetime.now)

    @property
    def is_complete(self) -> bool:
        """Check if download is complete."""
        return self.downloaded_bytes >= self.total_bytes

    @property
    def progress_percentage(self) -> int:
        """Calculate progress percentage."""
        if self.total_bytes == 0:
            return 0
        return int(self.downloaded_bytes / self.total_bytes * 100)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "task_id": self.task_id,
            "file_id": self.file_id,
            "total_bytes": self.total_bytes,
            "downloaded_bytes": self.downloaded_bytes,
            "local_temp_path": self.local_temp_path,
            "s3_key": self.s3_key,
            "checksum": self.checksum,
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DownloadProgress":
        """Create from dictionary."""
        return cls(
            task_id=data["task_id"],
            file_id=data["file_id"],
            total_bytes=data["total_bytes"],
            downloaded_bytes=data["downloaded_bytes"],
            local_temp_path=data["local_temp_path"],
            s3_key=data["s3_key"],
            checksum=data.get("checksum"),
            last_updated=datetime.fromisoformat(data["last_updated"]),
        )


class DownloadProgressStore:
    """Persistent store for download progress.

    Stores progress in a local JSON file for resumable downloads.

    Requirements: 4.5
    """

    def __init__(self, cache_dir: Path | None = None):
        """Initialize DownloadProgressStore.

        Args:
            cache_dir: Directory for cache files (default: ~/.cache/vco)
        """
        if cache_dir is None:
            cache_dir = Path.home() / ".cache" / "vco"

        self.cache_dir = cache_dir
        self.db_path = cache_dir / "download_progress.json"

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Load existing progress
        self._progress_data: dict[str, dict[str, DownloadProgress]] = {}
        self._load()

    def _load(self) -> None:
        """Load progress data from file."""
        if not self.db_path.exists():
            return

        try:
            with open(self.db_path, encoding="utf-8") as f:
                data = json.load(f)

            for task_id, files in data.items():
                # Validate that files is a dict
                if not isinstance(files, dict):
                    logger.warning(f"Invalid structure for task {task_id}, skipping")
                    continue

                self._progress_data[task_id] = {}
                for file_id, progress_data in files.items():
                    # Validate that progress_data is a dict
                    if not isinstance(progress_data, dict):
                        logger.warning(f"Invalid structure for file {file_id}, skipping")
                        continue

                    self._progress_data[task_id][file_id] = DownloadProgress.from_dict(
                        progress_data
                    )

            logger.debug(f"Loaded download progress from {self.db_path}")

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to load download progress: {e}")
            self._progress_data = {}

    def _save(self) -> None:
        """Save progress data to file."""
        try:
            data: dict[str, dict[str, Any]] = {}
            for task_id, files in self._progress_data.items():
                data[task_id] = {}
                for file_id, progress in files.items():
                    data[task_id][file_id] = progress.to_dict()

            with open(self.db_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            logger.debug(f"Saved download progress to {self.db_path}")

        except Exception as e:
            logger.warning(f"Failed to save download progress: {e}")

    def get_progress(self, task_id: str, file_id: str) -> DownloadProgress | None:
        """Get saved progress for a file.

        Args:
            task_id: Task ID
            file_id: File ID

        Returns:
            DownloadProgress if exists, None otherwise
        """
        task_progress = self._progress_data.get(task_id, {})
        return task_progress.get(file_id)

    def save_progress(self, progress: DownloadProgress) -> None:
        """Save progress for a file.

        Args:
            progress: DownloadProgress to save
        """
        if progress.task_id not in self._progress_data:
            self._progress_data[progress.task_id] = {}

        progress.last_updated = datetime.now()
        self._progress_data[progress.task_id][progress.file_id] = progress
        self._save()

    def clear_progress(self, task_id: str, file_id: str) -> None:
        """Clear progress for a completed file.

        Args:
            task_id: Task ID
            file_id: File ID
        """
        if task_id in self._progress_data:
            if file_id in self._progress_data[task_id]:
                del self._progress_data[task_id][file_id]

                # Clean up empty task entries
                if not self._progress_data[task_id]:
                    del self._progress_data[task_id]

                self._save()

    def clear_task(self, task_id: str) -> None:
        """Clear all progress for a task.

        Args:
            task_id: Task ID
        """
        if task_id in self._progress_data:
            del self._progress_data[task_id]
            self._save()

    def get_task_progress(self, task_id: str) -> dict[str, DownloadProgress]:
        """Get all progress for a task.

        Args:
            task_id: Task ID

        Returns:
            Dictionary of file_id -> DownloadProgress
        """
        return self._progress_data.get(task_id, {})

    def list_incomplete_tasks(self) -> list[str]:
        """List tasks with incomplete downloads.

        Returns:
            List of task IDs with incomplete downloads
        """
        incomplete = []
        for task_id, files in self._progress_data.items():
            for progress in files.values():
                if not progress.is_complete:
                    incomplete.append(task_id)
                    break
        return incomplete
