"""Core data models for Video Compression Optimizer."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from vco.models.base import BaseVideoMetadata


class VideoStatus(Enum):
    """Status of a video in the conversion pipeline."""

    PENDING = "pending"
    OPTIMIZED = "optimized"
    SKIPPED = "skipped"
    PROFESSIONAL = "professional"
    CONVERTING = "converting"
    CONVERTED = "converted"
    APPROVED = "approved"
    REJECTED = "rejected"
    ERROR = "error"


class QualityPresetName(Enum):
    """Quality preset names for conversion."""

    HIGH = "high"
    BALANCED = "balanced"
    COMPRESSION = "compression"


@dataclass
class VideoInfo(BaseVideoMetadata):
    """Video file information from Apple Photos library.

    Inherits common fields (uuid, filename, file_size, capture_date, location)
    from BaseVideoMetadata.

    Attributes:
        path: Path to the video file
        codec: Video codec (e.g., h264, hevc, prores)
        resolution: Video resolution as (width, height)
        bitrate: Video bitrate in bits per second
        duration: Video duration in seconds
        frame_rate: Video frame rate in fps
        creation_date: File creation date
        albums: List of album names the video belongs to
        is_in_icloud: Whether the video is stored in iCloud
        is_local: Whether the video is available locally
    """

    # VideoInfo specific fields (all with defaults to avoid dataclass issues)
    path: Path = Path()
    codec: str = ""
    resolution: tuple[int, int] = (0, 0)
    bitrate: int = 0
    duration: float = 0.0
    frame_rate: float = 0.0
    creation_date: datetime = field(default_factory=datetime.now)
    albums: list[str] = field(default_factory=list)
    is_in_icloud: bool = False
    is_local: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format including base fields."""
        base_dict = super().to_dict()
        base_dict.update(
            {
                "path": str(self.path),
                "codec": self.codec,
                "resolution": list(self.resolution),
                "bitrate": self.bitrate,
                "duration": self.duration,
                "frame_rate": self.frame_rate,
                "creation_date": self.creation_date.isoformat(),
                "albums": self.albums,
                "is_in_icloud": self.is_in_icloud,
                "is_local": self.is_local,
            }
        )
        return base_dict

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VideoInfo":
        """Create instance from dictionary."""
        return cls(
            # Base fields
            uuid=data["uuid"],
            filename=data["filename"],
            file_size=data["file_size"],
            capture_date=datetime.fromisoformat(data["capture_date"])
            if data.get("capture_date")
            else None,
            location=tuple(data["location"]) if data.get("location") else None,
            # VideoInfo specific fields
            path=Path(data["path"]),
            codec=data["codec"],
            resolution=tuple(data["resolution"]),
            bitrate=data["bitrate"],
            duration=data["duration"],
            frame_rate=data["frame_rate"],
            creation_date=datetime.fromisoformat(data["creation_date"]),
            albums=data.get("albums", []),
            is_in_icloud=data.get("is_in_icloud", False),
            is_local=data.get("is_local", True),
        )


@dataclass
class ConversionCandidate:
    """A video identified as a candidate for conversion.

    Attributes:
        video: The video information
        estimated_savings_bytes: Estimated space savings in bytes
        estimated_savings_percent: Estimated space savings as percentage
        skip_reason: Reason for skipping conversion (if applicable)
        status: Current status of the candidate
    """

    video: VideoInfo
    estimated_savings_bytes: int
    estimated_savings_percent: float
    skip_reason: str | None = None
    status: VideoStatus = VideoStatus.PENDING


@dataclass
class QualityResult:
    """Result of quality verification after conversion.

    Attributes:
        job_id: Unique identifier for the quality check job
        ssim_score: Structural Similarity Index (0.0-1.0)
        original_size: Original file size in bytes
        converted_size: Converted file size in bytes
        compression_ratio: Ratio of original to converted size
        space_saved_bytes: Space saved in bytes
        space_saved_percent: Space saved as percentage
        playback_verified: Whether playback was verified successfully
        is_acceptable: Whether the conversion meets quality standards
        rejection_reason: Reason for rejection (if applicable)
        converted_metadata: Metadata extracted from converted file
    """

    job_id: str
    ssim_score: float
    original_size: int
    converted_size: int
    compression_ratio: float
    space_saved_bytes: int
    space_saved_percent: float
    playback_verified: bool
    is_acceptable: bool
    rejection_reason: str | None = None
    converted_metadata: dict | None = None

    @classmethod
    def calculate_compression_ratio(cls, original_size: int, converted_size: int) -> float:
        """Calculate compression ratio.

        Args:
            original_size: Original file size in bytes
            converted_size: Converted file size in bytes

        Returns:
            Compression ratio (original_size / converted_size)
        """
        if converted_size <= 0:
            return 0.0
        return original_size / converted_size

    @classmethod
    def calculate_space_saved_percent(cls, original_size: int, converted_size: int) -> float:
        """Calculate space saved as percentage.

        Args:
            original_size: Original file size in bytes
            converted_size: Converted file size in bytes

        Returns:
            Space saved as percentage ((original - converted) / original * 100)
        """
        if original_size <= 0:
            return 0.0
        return (original_size - converted_size) / original_size * 100


@dataclass
class VideoMetadata:
    """Metadata extracted from a video file.

    Attributes:
        capture_date: Date when the video was captured
        creation_date: File creation date
        albums: List of album names
        title: Video title (if available)
        description: Video description (if available)
        location: GPS coordinates as (latitude, longitude)
    """

    capture_date: datetime | None
    creation_date: datetime
    albums: list[str] = field(default_factory=list)
    title: str | None = None
    description: str | None = None
    location: tuple[float, float] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "capture_date": self.capture_date.isoformat() if self.capture_date else None,
            "creation_date": self.creation_date.isoformat() if self.creation_date else None,
            "albums": self.albums,
            "title": self.title,
            "description": self.description,
            "location": list(self.location) if self.location else None,
        }


@dataclass
class ConversionJob:
    """Information about a MediaConvert conversion job.

    Attributes:
        job_id: MediaConvert job ID
        source_video_uuid: UUID of the source video in Photos library
        source_path: Path to the source video
        output_path: Path where the converted video will be saved
        status: Current job status
        progress_percent: Conversion progress (0-100)
        error_message: Error message if job failed
        estimated_cost: Estimated cost in USD
    """

    job_id: str
    source_video_uuid: str
    source_path: Path
    output_path: Path
    status: str  # SUBMITTED, PROGRESSING, COMPLETE, ERROR
    progress_percent: int = 0
    error_message: str | None = None
    estimated_cost: float = 0.0


@dataclass
class ReviewItem:
    """An item in the review queue awaiting user approval.

    Attributes:
        id: Unique identifier for the review item
        original_uuid: UUID of the original video in Photos library
        original_path: Path to the original video
        converted_path: Path to the converted video
        conversion_date: Date when conversion was completed
        quality_result: Quality verification result
        metadata: Preserved metadata from original video
        status: Current review status
    """

    id: str
    original_uuid: str
    original_path: Path
    converted_path: Path
    conversion_date: datetime
    quality_result: QualityResult
    metadata: VideoMetadata
    status: str = "pending_review"  # pending_review, approved, rejected


@dataclass
class ImportResult:
    """Result of importing a single video to Photos.

    Attributes:
        success: Whether the import was successful
        review_id: ID of the review item
        original_filename: Original video filename
        converted_filename: Converted video filename
        albums: List of album names the video was added to
        error_message: Error message if import failed
    """

    success: bool
    review_id: str
    original_filename: str
    converted_filename: str
    albums: list[str] = field(default_factory=list)
    error_message: str | None = None


@dataclass
class BatchImportResult:
    """Result of batch importing multiple videos.

    Attributes:
        total: Total number of items processed
        successful: Number of successful imports
        failed: Number of failed imports
        results: List of individual import results
    """

    total: int
    successful: int
    failed: int
    results: list[ImportResult] = field(default_factory=list)


# =============================================================================
# Unified Import Models (Local + AWS)
# =============================================================================


@dataclass
class ImportableItem:
    """Unified representation of importable items from local and AWS sources.

    Attributes:
        item_id: Unique identifier (local: review_id, AWS: task_id:file_id)
        source: Source type ("local" or "aws")
        original_filename: Original video filename
        converted_filename: Converted video filename
        original_size: Original file size in bytes
        converted_size: Converted file size in bytes
        compression_ratio: Ratio of original to converted size
        ssim_score: Structural Similarity Index (0.0-1.0)
        albums: List of album names
        capture_date: Date when the video was captured
        task_id: AWS task ID (AWS only)
        file_id: AWS file ID (AWS only)
        s3_key: S3 object key (AWS only)
        converted_path: Local path to converted file (local only)
    """

    item_id: str
    source: str  # "local" or "aws"
    original_filename: str
    converted_filename: str
    original_size: int
    converted_size: int
    compression_ratio: float
    ssim_score: float
    albums: list[str] = field(default_factory=list)
    capture_date: datetime | None = None

    # AWS-specific fields
    task_id: str | None = None
    file_id: str | None = None
    s3_key: str | None = None

    # Local-specific fields
    converted_path: Path | None = None

    @property
    def display_id(self) -> str:
        """Display ID for user-facing output.

        Returns task_id:file_id for AWS items, item_id for local items.
        """
        if self.source == "aws" and self.task_id and self.file_id:
            return f"{self.task_id}:{self.file_id}"
        return self.item_id


@dataclass
class UnifiedListResult:
    """Result of listing all importable items from local and AWS sources.

    Attributes:
        local_items: List of local importable items
        aws_items: List of AWS importable items
        aws_available: Whether AWS API is available
        aws_error: Error message if AWS API failed
    """

    local_items: list[ImportableItem] = field(default_factory=list)
    aws_items: list[ImportableItem] = field(default_factory=list)
    aws_available: bool = True
    aws_error: str | None = None

    @property
    def total_count(self) -> int:
        """Total number of importable items."""
        return len(self.local_items) + len(self.aws_items)

    @property
    def all_items(self) -> list[ImportableItem]:
        """All importable items from both sources."""
        return self.local_items + self.aws_items


@dataclass
class UnifiedImportResult:
    """Result of importing a single item from local or AWS source.

    Attributes:
        success: Whether the import was successful
        item_id: ID of the imported item
        source: Source type ("local" or "aws")
        original_filename: Original video filename
        converted_filename: Converted video filename
        albums: List of album names the video was added to
        error_message: Error message if import failed
        downloaded: Whether file was downloaded from AWS
        download_resumed: Whether download was resumed from previous progress
        checksum_verified: Whether checksum was verified
        s3_deleted: Whether S3 file was deleted after import
    """

    success: bool
    item_id: str
    source: str  # "local" or "aws"
    original_filename: str
    converted_filename: str
    albums: list[str] = field(default_factory=list)
    error_message: str | None = None

    # AWS-specific fields
    downloaded: bool = False
    download_resumed: bool = False
    checksum_verified: bool = False
    s3_deleted: bool = False


@dataclass
class UnifiedBatchResult:
    """Result of batch importing items from local and AWS sources.

    Attributes:
        local_total: Total local items processed
        local_successful: Successful local imports
        local_failed: Failed local imports
        aws_total: Total AWS items processed
        aws_successful: Successful AWS imports
        aws_failed: Failed AWS imports
        results: List of individual import results
    """

    local_total: int = 0
    local_successful: int = 0
    local_failed: int = 0
    aws_total: int = 0
    aws_successful: int = 0
    aws_failed: int = 0
    results: list[UnifiedImportResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Total number of items processed."""
        return self.local_total + self.aws_total

    @property
    def successful(self) -> int:
        """Total number of successful imports."""
        return self.local_successful + self.aws_successful

    @property
    def failed(self) -> int:
        """Total number of failed imports."""
        return self.local_failed + self.aws_failed


@dataclass
class UnifiedRemoveResult:
    """Result of removing an item from local or AWS source.

    Attributes:
        success: Whether the removal was successful
        item_id: ID of the removed item
        source: Source type ("local" or "aws")
        file_deleted: Whether the file was deleted
        metadata_deleted: Whether the metadata was deleted
        s3_deleted: Whether S3 file was deleted (AWS only)
        error_message: Error message if removal failed
    """

    success: bool
    item_id: str
    source: str  # "local" or "aws"
    file_deleted: bool = False
    metadata_deleted: bool = False
    s3_deleted: bool = False
    error_message: str | None = None


@dataclass
class ClearResult:
    """Result of clearing the local queue.

    Attributes:
        success: Whether the clear operation was successful
        items_removed: Number of items removed
        files_deleted: Number of files deleted
        files_failed: Number of files that failed to delete
        error_details: List of error messages
    """

    success: bool
    items_removed: int = 0
    files_deleted: int = 0
    files_failed: int = 0
    error_details: list[str] = field(default_factory=list)


@dataclass
class UnifiedClearResult:
    """Result of clearing both local and AWS queues.

    Attributes:
        success: Whether the clear operation was successful
        local_items_removed: Number of local items removed
        local_files_deleted: Number of local files deleted
        local_files_failed: Number of local files that failed to delete
        aws_items_removed: Number of AWS items removed
        aws_files_deleted: Number of AWS S3 files deleted
        aws_files_failed: Number of AWS files that failed to delete
        error_details: List of error messages from both sources
    """

    success: bool
    local_items_removed: int = 0
    local_files_deleted: int = 0
    local_files_failed: int = 0
    aws_items_removed: int = 0
    aws_files_deleted: int = 0
    aws_files_failed: int = 0
    error_details: list[str] = field(default_factory=list)

    @property
    def total_items_removed(self) -> int:
        """Total number of items removed from both sources."""
        return self.local_items_removed + self.aws_items_removed

    @property
    def total_files_deleted(self) -> int:
        """Total number of files deleted from both sources."""
        return self.local_files_deleted + self.aws_files_deleted

    @property
    def total_files_failed(self) -> int:
        """Total number of files that failed to delete from both sources."""
        return self.local_files_failed + self.aws_files_failed


# =============================================================================
# Conversion Result Models (moved from services/convert.py)
# =============================================================================


@dataclass
class ConversionProgress:
    """Progress information for a conversion.

    Attributes:
        uuid: Unique identifier for the video
        filename: Name of the video file
        stage: Current stage of conversion
        progress_percent: Progress percentage (0-100)
        error_message: Error message if conversion failed
        job_id: MediaConvert job ID
        quality_job_id: Quality check job ID
    """

    uuid: str
    filename: str
    stage: str  # 'uploading', 'converting', 'checking', 'downloading', 'complete', 'failed'
    progress_percent: int = 0
    error_message: str | None = None
    job_id: str | None = None
    quality_job_id: str | None = None


@dataclass
class AttemptResult:
    """Result of a single preset attempt in adaptive conversion.

    Used to track each preset attempt during best-effort mode conversion,
    allowing comparison of SSIM scores to select the best result.

    Attributes:
        preset: Preset name used for this attempt
        ssim_score: SSIM score (None if conversion failed)
        quality_result: Full quality check result
        output_s3_key: S3 key for converted file
        source_s3_key: S3 key for source file
        metadata_s3_key: S3 key for metadata JSON
        success: Whether conversion itself succeeded
        error_message: Error message if conversion failed
    """

    preset: str
    ssim_score: float | None
    quality_result: QualityResult | None
    output_s3_key: str
    source_s3_key: str
    metadata_s3_key: str
    success: bool
    error_message: str | None = None


@dataclass
class ConversionResult(BaseVideoMetadata):
    """Result of a single video conversion.

    Inherits common fields (uuid, filename, file_size, capture_date, location)
    from BaseVideoMetadata.

    Attributes:
        success: Whether conversion itself succeeded
        original_path: Path to the original video file
        converted_path: Path to the converted video file
        quality_result: Quality verification result
        metadata: Preserved metadata from original video
        error_message: Error message if conversion failed
        mediaconvert_job_id: MediaConvert job ID
        quality_job_id: Quality check job ID
        best_effort: True if best-effort mode was used
        selected_preset: Preset that was selected (for adaptive presets)
    """

    # ConversionResult specific fields (all with defaults to avoid dataclass issues)
    success: bool = False
    original_path: Path = field(default_factory=Path)
    converted_path: Path | None = None
    quality_result: QualityResult | None = None
    metadata: VideoMetadata | None = None
    error_message: str | None = None
    mediaconvert_job_id: str | None = None
    quality_job_id: str | None = None
    best_effort: bool = False
    selected_preset: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format including base fields."""
        base_dict = super().to_dict()
        base_dict.update(
            {
                "success": self.success,
                "original_path": str(self.original_path),
                "converted_path": str(self.converted_path) if self.converted_path else None,
                "quality_result": self.quality_result.__dict__ if self.quality_result else None,
                "metadata": self.metadata.__dict__ if self.metadata else None,
                "error_message": self.error_message,
                "mediaconvert_job_id": self.mediaconvert_job_id,
                "quality_job_id": self.quality_job_id,
                "best_effort": self.best_effort,
                "selected_preset": self.selected_preset,
            }
        )
        return base_dict


@dataclass
class BatchConversionResult:
    """Result of batch conversion.

    Attributes:
        total: Total number of videos processed
        successful: Number of successful conversions
        failed: Number of failed conversions
        added_to_queue: Number of items added to review queue
        results: List of individual conversion results
        errors: List of error messages
    """

    total: int = 0
    successful: int = 0
    failed: int = 0
    added_to_queue: int = 0
    results: list[ConversionResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "total": self.total,
            "successful": self.successful,
            "failed": self.failed,
            "added_to_queue": self.added_to_queue,
            "results": [self._result_to_dict(r) for r in self.results],
            "errors": self.errors,
        }

    def _result_to_dict(self, result: ConversionResult) -> dict:
        """Convert a result to dictionary."""
        return result.to_dict()
