"""Core data models for Video Compression Optimizer."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


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
class VideoInfo:
    """Video file information from Apple Photos library.

    Attributes:
        uuid: Unique identifier from Photos library
        filename: Original filename
        path: Path to the video file
        codec: Video codec (e.g., h264, hevc, prores)
        resolution: Video resolution as (width, height)
        bitrate: Video bitrate in bits per second
        duration: Video duration in seconds
        frame_rate: Video frame rate in fps
        file_size: File size in bytes
        capture_date: Date when the video was captured (from metadata)
        creation_date: File creation date
        albums: List of album names the video belongs to
        is_in_icloud: Whether the video is stored in iCloud
        is_local: Whether the video is available locally
        location: GPS coordinates as (latitude, longitude) from Photos
    """

    uuid: str
    filename: str
    path: Path
    codec: str
    resolution: tuple[int, int]
    bitrate: int
    duration: float
    frame_rate: float
    file_size: int
    capture_date: datetime | None
    creation_date: datetime
    albums: list[str] = field(default_factory=list)
    is_in_icloud: bool = False
    is_local: bool = True
    location: tuple[float, float] | None = None


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
