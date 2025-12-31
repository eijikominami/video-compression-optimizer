"""Data models module for Video Compression Optimizer."""

from vco.models.async_task import (
    AsyncFile,
    AsyncTask,
    DownloadProgress,
    FileStatus,
    TaskStatus,
    aggregate_task_status,
)
from vco.models.types import (
    BatchImportResult,
    ConversionCandidate,
    ConversionJob,
    ImportResult,
    QualityPresetName,
    QualityResult,
    ReviewItem,
    VideoInfo,
    VideoMetadata,
    VideoStatus,
)

__all__ = [
    # Async task models
    "AsyncFile",
    "AsyncTask",
    "DownloadProgress",
    "FileStatus",
    "TaskStatus",
    "aggregate_task_status",
    # Core types
    "BatchImportResult",
    "ConversionCandidate",
    "ConversionJob",
    "ImportResult",
    "QualityPresetName",
    "QualityResult",
    "ReviewItem",
    "VideoInfo",
    "VideoMetadata",
    "VideoStatus",
]
