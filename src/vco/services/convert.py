"""Convert service for batch video conversion.

This service orchestrates the conversion workflow:
1. Upload videos to S3
2. Submit MediaConvert jobs
3. Monitor progress
4. Trigger quality checks
5. Download converted videos
"""

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from vco.analyzer.analyzer import ConversionCandidate
from vco.converter import get_adaptive_preset_chain, is_adaptive_preset
from vco.converter.mediaconvert import MediaConvertClient
from vco.metadata.manager import MetadataManager, VideoMetadata
from vco.models.base import BaseVideoMetadata
from vco.photos.manager import PhotosAccessManager
from vco.quality.checker import QualityChecker, QualityResult
from vco.utils.disk import (
    InsufficientDiskSpaceError,
    check_batch_disk_space,
    format_bytes,
)

# Import ReviewService for type checking only (avoid circular import)
if TYPE_CHECKING:
    from vco.services.review import ReviewService


logger = logging.getLogger(__name__)

# MediaConvert API rate limits (ap-northeast-1)
# CreateJob: 5 requests/second, burst 100
MEDIACONVERT_RATE_LIMIT_DELAY = 0.25  # 250ms between requests (4 req/sec to be safe)


@dataclass
class ConversionProgress:
    """Progress information for a conversion."""

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
    """

    preset: str  # Preset name used for this attempt
    ssim_score: float | None  # SSIM score (None if conversion failed)
    quality_result: QualityResult | None  # Full quality check result
    output_s3_key: str  # S3 key for converted file
    source_s3_key: str  # S3 key for source file
    metadata_s3_key: str  # S3 key for metadata JSON
    success: bool  # Whether conversion itself succeeded
    error_message: str | None = None  # Error message if conversion failed


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
    original_path: Path = Path()
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
    """Result of batch conversion."""

    total: int = 0
    successful: int = 0
    failed: int = 0
    added_to_queue: int = 0  # Number of items added to review queue
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
        return {
            "uuid": result.uuid,
            "filename": result.filename,
            "success": result.success,
            "original_path": str(result.original_path),
            "converted_path": str(result.converted_path) if result.converted_path else None,
            "error_message": result.error_message,
            "mediaconvert_job_id": result.mediaconvert_job_id,
            "quality_job_id": result.quality_job_id,
            "best_effort": result.best_effort,
            "selected_preset": result.selected_preset,
        }


class ConvertService:
    """Service for batch video conversion using AWS MediaConvert."""

    def __init__(
        self,
        mediaconvert_client: MediaConvertClient,
        quality_checker: QualityChecker,
        metadata_manager: MetadataManager | None = None,
        photos_manager: PhotosAccessManager | None = None,
        staging_folder: Path | None = None,
        progress_callback: Callable[[ConversionProgress], None] | None = None,
        review_service: Optional["ReviewService"] = None,
    ):
        """Initialize ConvertService.

        Args:
            mediaconvert_client: MediaConvertClient instance
            quality_checker: QualityChecker instance
            metadata_manager: MetadataManager instance (created if not provided)
            photos_manager: PhotosAccessManager instance for iCloud downloads
            staging_folder: Folder for converted files
            progress_callback: Callback for progress updates
            review_service: ReviewService instance for auto-registration to review queue
        """
        self.mediaconvert = mediaconvert_client
        self.quality_checker = quality_checker
        self.metadata_manager = metadata_manager or MetadataManager()
        self.photos_manager = photos_manager
        self.staging_folder = (
            staging_folder or Path.home() / "Movies" / "VideoCompressionOptimizer" / "converted"
        )
        self.progress_callback = progress_callback
        self.review_service = review_service

    def convert_batch(
        self,
        candidates: list[ConversionCandidate],
        quality_preset: str = "balanced",
        max_concurrent: int = 5,
        dry_run: bool = False,
        skip_disk_check: bool = False,
    ) -> BatchConversionResult:
        """Convert a batch of videos.

        Args:
            candidates: List of conversion candidates
            quality_preset: Quality preset for conversion
            max_concurrent: Maximum concurrent conversions
            dry_run: If True, simulate without actual conversion
            skip_disk_check: If True, skip disk space check

        Returns:
            BatchConversionResult with all results

        Raises:
            InsufficientDiskSpaceError: If not enough disk space
        """
        result = BatchConversionResult(total=len(candidates))

        if dry_run:
            logger.info(f"Dry run: would convert {len(candidates)} videos")
            return result

        # Ensure staging folder exists
        self.staging_folder.mkdir(parents=True, exist_ok=True)

        # Re-check iCloud status at runtime (files may have been downloaded since scan)
        # This is important because candidates.json stores the state at scan time
        local_candidates = []
        icloud_only_candidates = []

        for candidate in candidates:
            video = candidate.video

            # Check if file is actually available locally (real-time check)
            # This handles the case where user downloaded files after scanning
            is_actually_local = self._check_file_available(video.path)

            if is_actually_local:
                local_candidates.append(candidate)
                logger.debug(f"{video.filename}: locally available")
            else:
                icloud_only_candidates.append(candidate)
                logger.debug(f"{video.filename}: iCloud only (path: {video.path})")

        # Log iCloud-only videos that will be skipped
        if icloud_only_candidates:
            logger.info(f"Skipping {len(icloud_only_candidates)} iCloud-only videos")
            for candidate in icloud_only_candidates:
                video = candidate.video
                logger.info(f"  - {video.filename}: iCloud only, download in Photos app first")

                # Add to results as skipped
                result.results.append(
                    ConversionResult(
                        uuid=video.uuid,
                        filename=video.filename,
                        success=False,
                        original_path=video.path,
                        error_message="iCloud only - download in Photos app first",
                    )
                )
                result.failed += 1
                result.errors.append(f"{video.filename}: iCloud only")

        # Update total to reflect only local candidates that will be processed
        result.total = len(candidates)  # Keep original total for reporting

        # Check disk space before starting (Requirement 8.6)
        if not skip_disk_check and local_candidates:
            total_source_size = sum(c.video.file_size for c in local_candidates)
            try:
                check_batch_disk_space(
                    total_source_size=total_source_size,
                    target_path=self.staging_folder,
                    multiplier=2.0,  # Require 2x source size
                )
                logger.info(
                    f"Disk space check passed: {format_bytes(total_source_size)} source, "
                    f"staging folder: {self.staging_folder}"
                )
            except InsufficientDiskSpaceError as e:
                logger.error(f"Disk space check failed: {e}")
                raise

        # Determine if we're using an adaptive preset
        use_adaptive = is_adaptive_preset(quality_preset)
        preset_chain = (
            get_adaptive_preset_chain(quality_preset) if use_adaptive else [quality_preset]
        )

        # Process only local candidates
        for candidate in local_candidates:
            try:
                if use_adaptive:
                    # Adaptive preset: use best-effort mode
                    conversion_result = self._convert_with_best_effort(
                        candidate=candidate, preset_chain=preset_chain
                    )
                else:
                    # Non-adaptive preset: use standard conversion
                    conversion_result = self.convert_single(
                        candidate=candidate, quality_preset=quality_preset
                    )

                result.results.append(conversion_result)

                if conversion_result.success:
                    result.successful += 1
                else:
                    result.failed += 1
                    if conversion_result.error_message:
                        result.errors.append(
                            f"{candidate.video.filename}: {conversion_result.error_message}"
                        )

            except Exception as e:
                logger.exception(f"Error converting {candidate.video.filename}")
                result.failed += 1
                result.errors.append(f"{candidate.video.filename}: {str(e)}")

                result.results.append(
                    ConversionResult(
                        uuid=candidate.video.uuid,
                        filename=candidate.video.filename,
                        success=False,
                        original_path=candidate.video.path,
                        error_message=str(e),
                    )
                )

        # Add successful conversions to review queue (Requirement 12.1)
        if self.review_service:
            for conversion_result in result.results:
                if conversion_result.success:
                    review_item = self.review_service.add_to_queue(conversion_result)
                    if review_item:
                        result.added_to_queue += 1
                        logger.info(f"Added to review queue: {conversion_result.filename}")

        return result

    def _check_file_available(self, path: Path) -> bool:
        """Check if a file is actually available locally.

        This performs a real-time check to see if the file exists and is readable,
        regardless of what was stored in candidates.json at scan time.

        Args:
            path: Path to the video file

        Returns:
            True if file exists and is readable, False otherwise
        """
        try:
            # Check if path exists and is a file
            if not path.exists():
                return False
            if not path.is_file():
                return False

            # Try to get file size to verify it's readable
            # iCloud placeholder files may exist but have 0 size or be unreadable
            size = path.stat().st_size
            if size == 0:
                return False

            return True
        except (OSError, PermissionError) as e:
            logger.debug(f"File not available: {path} - {e}")
            return False

    def _get_photos_app_link(self, uuid: str) -> str:
        """Generate Photos app link for a video.

        Note: This method is deprecated as the photos:// URL scheme does not
        reliably open specific photos in the Photos app on macOS.

        Args:
            uuid: Video UUID

        Returns:
            Empty string (feature removed)
        """
        return ""

    def convert_single(
        self, candidate: ConversionCandidate, quality_preset: str = "balanced"
    ) -> ConversionResult:
        """Convert a single video.

        Args:
            candidate: Conversion candidate
            quality_preset: Quality preset for conversion

        Returns:
            ConversionResult with conversion details

        Note:
            Only processes locally available videos. iCloud-only videos should
            be filtered out before calling this method.
        """
        video = candidate.video
        video_path = video.path

        # Check if video is iCloud-only (should be filtered before, but double-check)
        if video.is_in_icloud and not video.is_local:
            return ConversionResult(
                uuid=video.uuid,
                filename=video.filename,
                success=False,
                original_path=video.path,
                error_message="Video is in iCloud only. Download in Photos app first.",
            )

        # Verify file exists
        if not video_path.exists():
            return ConversionResult(
                uuid=video.uuid,
                filename=video.filename,
                success=False,
                original_path=video.path,
                error_message=f"Video file not found: {video_path}",
            )

        # Extract metadata before conversion
        try:
            metadata = self.metadata_manager.extract_metadata(video_path)
            metadata.albums = video.albums  # Add album info from Photos

            # Use capture_date from Photos if not found in file metadata
            # This is important for formats like MPEG-1 that don't have embedded metadata
            if metadata.capture_date is None and video.capture_date:
                metadata.capture_date = video.capture_date

            # Use creation_date from Photos if not found
            if metadata.creation_date is None and video.creation_date:
                metadata.creation_date = video.creation_date

            # Use location from Photos if not found in file metadata
            if metadata.location is None and video.location:
                metadata.location = video.location
        except Exception as e:
            logger.warning(f"Failed to extract metadata: {e}")
            metadata = VideoMetadata(
                capture_date=video.capture_date,
                creation_date=video.creation_date,
                albums=video.albums,
                location=video.location,
            )

        # Generate S3 keys
        # MediaConvert output filename: {input_filename}_h265.mp4
        # Note: video.filename is the original filename (e.g., "IMG_1234.mp4")
        # video_path.stem is the Photos library internal filename (UUID-based)
        source_s3_key = f"input/{video.uuid}/{video.filename}"
        input_filename_stem = Path(video.filename).stem
        output_s3_key = f"output/{video.uuid}/{input_filename_stem}_h265.mp4"
        metadata_s3_key = f"input/{video.uuid}/metadata.json"

        try:
            # Stage 1: Upload to S3
            self._report_progress(video.uuid, video.filename, "uploading", 0)

            self.mediaconvert.upload_to_s3(video_path, source_s3_key)

            # Upload metadata JSON to S3 for Lambda to embed
            self._upload_metadata_to_s3(metadata, metadata_s3_key)

            self._report_progress(video.uuid, video.filename, "uploading", 100)

            # Stage 2: Submit MediaConvert job (with rate limiting)
            self._report_progress(video.uuid, video.filename, "converting", 0)

            # Apply rate limiting to avoid hitting MediaConvert API limits
            time.sleep(MEDIACONVERT_RATE_LIMIT_DELAY)

            job = self.mediaconvert.submit_job(
                source_video_uuid=video.uuid,
                source_s3_key=source_s3_key,
                output_s3_key=output_s3_key,
                quality_preset=quality_preset,
            )

            # Wait for conversion to complete
            job = self.mediaconvert.wait_for_completion(
                job_id=job.job_id, poll_interval=10, timeout=3600
            )

            if job.status != "COMPLETE":
                return ConversionResult(
                    uuid=video.uuid,
                    filename=video.filename,
                    success=False,
                    original_path=video.path,
                    error_message=job.error_message
                    or f"Conversion failed with status: {job.status}",
                    mediaconvert_job_id=job.job_id,
                )

            self._report_progress(video.uuid, video.filename, "converting", 100)

            # Stage 3: Quality check
            self._report_progress(video.uuid, video.filename, "checking", 0)

            quality_result = self.quality_checker.trigger_quality_check_sync(
                original_s3_key=source_s3_key,
                converted_s3_key=output_s3_key,
                metadata_s3_key=metadata_s3_key,
            )

            self._report_progress(video.uuid, video.filename, "checking", 100)

            if not quality_result.is_acceptable:
                # Clean up S3 files
                self.mediaconvert.delete_from_s3(source_s3_key)
                self.mediaconvert.delete_from_s3(output_s3_key)

                return ConversionResult(
                    uuid=video.uuid,
                    filename=video.filename,
                    success=False,
                    original_path=video.path,
                    quality_result=quality_result,
                    error_message=quality_result.failure_reason or "Quality check failed",
                    mediaconvert_job_id=job.job_id,
                    quality_job_id=quality_result.job_id,
                )

            # Stage 4: Download converted file
            self._report_progress(video.uuid, video.filename, "downloading", 0)

            # Use original filename stem for output (Requirement 8.3)
            original_filename_stem = Path(video.filename).stem
            local_output_path = self.staging_folder / f"{original_filename_stem}_h265.mp4"
            self.mediaconvert.download_from_s3(output_s3_key, local_output_path)

            # Check if Lambda embedded metadata successfully
            # If not, apply metadata locally as fallback (Requirement 13.3)
            metadata_embedded_by_lambda = getattr(quality_result, "metadata_embedded", False)

            if not metadata_embedded_by_lambda:
                logger.info(f"Lambda did not embed metadata, applying locally for {video.filename}")
                if (
                    hasattr(quality_result, "metadata_embed_error")
                    and quality_result.metadata_embed_error
                ):
                    logger.warning(
                        f"Lambda metadata embed error: {quality_result.metadata_embed_error}"
                    )

                # Apply metadata to converted file locally
                self.metadata_manager.apply_metadata(local_output_path, metadata)
            else:
                logger.info(f"Metadata was embedded by Lambda for {video.filename}")

            # Set file dates from metadata (capture_date or creation_date)
            # Don't use original file dates as they may be wrong (e.g., iCloud download time)
            file_date = metadata.capture_date or metadata.creation_date
            if file_date:
                self.metadata_manager.set_file_dates(
                    local_output_path, creation_date=file_date, modification_date=file_date
                )

            # Save metadata JSON alongside converted file
            metadata_path = local_output_path.with_suffix(".json")
            self.metadata_manager.save_metadata_json(metadata, metadata_path)

            self._report_progress(video.uuid, video.filename, "downloading", 100)

            # Clean up S3 files
            self.mediaconvert.delete_from_s3(source_s3_key)
            self.mediaconvert.delete_from_s3(output_s3_key)
            self._delete_metadata_from_s3(metadata_s3_key)

            self._report_progress(video.uuid, video.filename, "complete", 100)

            return ConversionResult(
                uuid=video.uuid,
                filename=video.filename,
                success=True,
                original_path=video.path,
                converted_path=local_output_path,
                quality_result=quality_result,
                metadata=metadata,
                mediaconvert_job_id=job.job_id,
                quality_job_id=quality_result.job_id,
            )

        except Exception as e:
            logger.exception(f"Conversion failed for {video.filename}")

            self._report_progress(video.uuid, video.filename, "failed", 0, str(e))

            # Try to clean up S3 files (including metadata)
            try:
                self.mediaconvert.delete_from_s3(source_s3_key)
                self.mediaconvert.delete_from_s3(output_s3_key)
                self._delete_metadata_from_s3(metadata_s3_key)
            except Exception:
                pass

            return ConversionResult(
                uuid=video.uuid,
                filename=video.filename,
                success=False,
                original_path=video.path,
                error_message=str(e),
            )

    def estimate_batch_cost(self, candidates: list[ConversionCandidate]) -> float:
        """Estimate total cost for batch conversion.

        Args:
            candidates: List of conversion candidates

        Returns:
            Estimated cost in USD
        """
        total_cost = 0.0

        for candidate in candidates:
            video = candidate.video
            cost = self.mediaconvert.estimate_cost(
                duration_seconds=video.duration, resolution=video.resolution
            )
            total_cost += cost

        return round(total_cost, 2)

    def _report_progress(
        self, uuid: str, filename: str, stage: str, progress: int, error: str | None = None
    ):
        """Report progress via callback.

        Args:
            uuid: Video UUID
            filename: Video filename
            stage: Current stage
            progress: Progress percentage
            error: Error message if any
        """
        if self.progress_callback:
            self.progress_callback(
                ConversionProgress(
                    uuid=uuid,
                    filename=filename,
                    stage=stage,
                    progress_percent=progress,
                    error_message=error,
                )
            )

    def _upload_metadata_to_s3(self, metadata: VideoMetadata, s3_key: str) -> None:
        """Upload metadata JSON to S3 for Lambda to embed.

        Args:
            metadata: VideoMetadata to upload
            s3_key: S3 key for metadata JSON
        """
        try:
            metadata_dict = {
                "capture_date": metadata.capture_date.isoformat()
                if metadata.capture_date
                else None,
                "creation_date": metadata.creation_date.isoformat()
                if metadata.creation_date
                else None,
                "location": list(metadata.location) if metadata.location else None,
                "albums": metadata.albums or [],
            }

            # Upload as JSON
            import tempfile

            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(metadata_dict, f, indent=2)
                temp_path = f.name

            try:
                self.mediaconvert.upload_to_s3(Path(temp_path), s3_key)
                logger.info(f"Uploaded metadata to s3://{self.mediaconvert.s3_bucket}/{s3_key}")
            finally:
                import os

                os.unlink(temp_path)

        except Exception as e:
            logger.warning(f"Failed to upload metadata to S3: {e}")
            # Don't fail the conversion if metadata upload fails

    def _delete_metadata_from_s3(self, s3_key: str) -> None:
        """Delete metadata JSON from S3.

        Args:
            s3_key: S3 key for metadata JSON
        """
        try:
            self.mediaconvert.delete_from_s3(s3_key)
        except Exception as e:
            logger.warning(f"Failed to delete metadata from S3: {e}")

    def _cleanup_attempt(self, attempt: AttemptResult) -> None:
        """Clean up S3 files from an attempt.

        Deletes the output file from S3. Source and metadata files are shared
        across attempts and cleaned up separately.

        Args:
            attempt: Attempt to clean up
        """
        try:
            self.mediaconvert.delete_from_s3(attempt.output_s3_key)
            logger.debug(f"Cleaned up S3 file: {attempt.output_s3_key}")
        except Exception as e:
            logger.warning(f"Failed to delete {attempt.output_s3_key}: {e}")

    def _try_preset(
        self, candidate: ConversionCandidate, preset: str, source_s3_key: str, metadata_s3_key: str
    ) -> AttemptResult:
        """Try conversion with a single preset.

        Args:
            candidate: Conversion candidate
            preset: Quality preset to use
            source_s3_key: S3 key for source file (already uploaded)
            metadata_s3_key: S3 key for metadata JSON (already uploaded)

        Returns:
            AttemptResult with conversion details
        """
        video = candidate.video
        input_filename_stem = Path(video.filename).stem

        # Use preset-specific output path to avoid collisions
        output_s3_key = f"output/{video.uuid}/{preset}/{input_filename_stem}_h265.mp4"

        try:
            # Submit MediaConvert job (with rate limiting)
            self._report_progress(video.uuid, video.filename, "converting", 0)
            time.sleep(MEDIACONVERT_RATE_LIMIT_DELAY)

            job = self.mediaconvert.submit_job(
                source_video_uuid=video.uuid,
                source_s3_key=source_s3_key,
                output_s3_key=output_s3_key,
                quality_preset=preset,
            )

            # Wait for conversion to complete
            job = self.mediaconvert.wait_for_completion(
                job_id=job.job_id, poll_interval=10, timeout=3600
            )

            if job.status != "COMPLETE":
                return AttemptResult(
                    preset=preset,
                    ssim_score=None,
                    quality_result=None,
                    output_s3_key=output_s3_key,
                    source_s3_key=source_s3_key,
                    metadata_s3_key=metadata_s3_key,
                    success=False,
                    error_message=job.error_message
                    or f"Conversion failed with status: {job.status}",
                )

            self._report_progress(video.uuid, video.filename, "converting", 100)

            # Quality check
            self._report_progress(video.uuid, video.filename, "checking", 0)

            quality_result = self.quality_checker.trigger_quality_check_sync(
                original_s3_key=source_s3_key,
                converted_s3_key=output_s3_key,
                metadata_s3_key=metadata_s3_key,
            )

            self._report_progress(video.uuid, video.filename, "checking", 100)

            return AttemptResult(
                preset=preset,
                ssim_score=quality_result.ssim_score,
                quality_result=quality_result,
                output_s3_key=output_s3_key,
                source_s3_key=source_s3_key,
                metadata_s3_key=metadata_s3_key,
                success=True,
            )

        except Exception as e:
            logger.exception(f"Preset {preset} failed for {video.filename}")
            return AttemptResult(
                preset=preset,
                ssim_score=None,
                quality_result=None,
                output_s3_key=output_s3_key,
                source_s3_key=source_s3_key,
                metadata_s3_key=metadata_s3_key,
                success=False,
                error_message=str(e),
            )

    def _select_best_attempt(
        self, candidate: ConversionCandidate, attempts: list[AttemptResult]
    ) -> tuple[AttemptResult | None, bool]:
        """Select the best attempt based on SSIM score.

        Args:
            candidate: Conversion candidate
            attempts: List of all attempts

        Returns:
            Tuple of (best_attempt, is_best_effort)
            - best_attempt: The attempt with highest SSIM, or None if all failed
            - is_best_effort: True if best-effort mode was used (no attempt met threshold)
        """
        # Filter valid attempts (conversion succeeded and has SSIM score)
        valid_attempts = [a for a in attempts if a.success and a.ssim_score is not None]

        if not valid_attempts:
            # All attempts failed
            logger.warning(f"{candidate.video.filename}: All preset attempts failed")
            return None, False

        # Check if any attempt met the threshold
        threshold_met = any(
            a.quality_result and a.quality_result.is_acceptable for a in valid_attempts
        )

        if threshold_met:
            # Return the first attempt that met threshold (already handled in caller)
            # This shouldn't happen as threshold success is handled before calling this
            best = next(
                a for a in valid_attempts if a.quality_result and a.quality_result.is_acceptable
            )
            return best, False

        # Best-effort mode: select highest SSIM
        best_attempt = max(valid_attempts, key=lambda a: a.ssim_score)  # type: ignore

        logger.info(
            f"{candidate.video.filename}: Best-effort mode selected '{best_attempt.preset}' "
            f"with SSIM {best_attempt.ssim_score:.4f}"
        )

        # Log comparison if multiple valid attempts
        if len(valid_attempts) > 1:
            for attempt in valid_attempts:
                if attempt != best_attempt:
                    logger.info(
                        f"  - {attempt.preset}: SSIM {attempt.ssim_score:.4f} (not selected)"
                    )

        return best_attempt, True

    def _finalize_attempt(
        self,
        candidate: ConversionCandidate,
        attempt: AttemptResult,
        metadata: VideoMetadata,
        best_effort: bool,
    ) -> ConversionResult:
        """Finalize a successful attempt by downloading the file.

        Args:
            candidate: Conversion candidate
            attempt: Successful attempt to finalize
            metadata: Video metadata
            best_effort: Whether best-effort mode was used

        Returns:
            ConversionResult with downloaded file
        """
        video = candidate.video

        # Download converted file
        self._report_progress(video.uuid, video.filename, "downloading", 0)

        original_filename_stem = Path(video.filename).stem
        local_output_path = self.staging_folder / f"{original_filename_stem}_h265.mp4"
        self.mediaconvert.download_from_s3(attempt.output_s3_key, local_output_path)

        # Check if Lambda embedded metadata successfully
        quality_result = attempt.quality_result
        metadata_embedded_by_lambda = quality_result and getattr(
            quality_result, "metadata_embedded", False
        )

        if not metadata_embedded_by_lambda:
            logger.info(f"Lambda did not embed metadata, applying locally for {video.filename}")
            if (
                quality_result
                and hasattr(quality_result, "metadata_embed_error")
                and quality_result.metadata_embed_error
            ):
                logger.warning(
                    f"Lambda metadata embed error: {quality_result.metadata_embed_error}"
                )
            self.metadata_manager.apply_metadata(local_output_path, metadata)
        else:
            logger.info(f"Metadata was embedded by Lambda for {video.filename}")

        # Set file dates from metadata
        file_date = metadata.capture_date or metadata.creation_date
        if file_date:
            self.metadata_manager.set_file_dates(
                local_output_path, creation_date=file_date, modification_date=file_date
            )

        # Save metadata JSON alongside converted file
        metadata_path = local_output_path.with_suffix(".json")
        self.metadata_manager.save_metadata_json(metadata, metadata_path)

        self._report_progress(video.uuid, video.filename, "downloading", 100)

        # Clean up S3 files
        self.mediaconvert.delete_from_s3(attempt.source_s3_key)
        self.mediaconvert.delete_from_s3(attempt.output_s3_key)
        self._delete_metadata_from_s3(attempt.metadata_s3_key)

        self._report_progress(video.uuid, video.filename, "complete", 100)

        return ConversionResult(
            uuid=video.uuid,
            filename=video.filename,
            success=True,
            original_path=video.path,
            converted_path=local_output_path,
            quality_result=quality_result,
            metadata=metadata,
            mediaconvert_job_id=quality_result.job_id if quality_result else None,
            quality_job_id=quality_result.job_id if quality_result else None,
            best_effort=best_effort,
            selected_preset=attempt.preset,
        )

    def _convert_with_best_effort(
        self, candidate: ConversionCandidate, preset_chain: list[str]
    ) -> ConversionResult:
        """Convert with best-effort mode for adaptive presets.

        Tries each preset in the chain. If any preset meets SSIM threshold,
        returns success immediately. If all presets fail threshold, selects
        the result with highest SSIM score.

        Args:
            candidate: Conversion candidate
            preset_chain: List of presets to try in order

        Returns:
            ConversionResult with best available result
        """
        video = candidate.video
        video_path = video.path

        # Check if video is iCloud-only
        if video.is_in_icloud and not video.is_local:
            return ConversionResult(
                uuid=video.uuid,
                filename=video.filename,
                success=False,
                original_path=video.path,
                error_message="Video is in iCloud only. Download in Photos app first.",
            )

        # Verify file exists
        if not video_path.exists():
            return ConversionResult(
                uuid=video.uuid,
                filename=video.filename,
                success=False,
                original_path=video.path,
                error_message=f"Video file not found: {video_path}",
            )

        # Extract metadata before conversion
        try:
            metadata = self.metadata_manager.extract_metadata(video_path)
            metadata.albums = video.albums
            if metadata.capture_date is None and video.capture_date:
                metadata.capture_date = video.capture_date
            if metadata.creation_date is None and video.creation_date:
                metadata.creation_date = video.creation_date
            if metadata.location is None and video.location:
                metadata.location = video.location
        except Exception as e:
            logger.warning(f"Failed to extract metadata: {e}")
            metadata = VideoMetadata(
                capture_date=video.capture_date,
                creation_date=video.creation_date,
                albums=video.albums,
                location=video.location,
            )

        # Generate S3 keys (shared across attempts)
        source_s3_key = f"input/{video.uuid}/{video.filename}"
        metadata_s3_key = f"input/{video.uuid}/metadata.json"

        attempts: list[AttemptResult] = []

        try:
            # Upload source file and metadata once (shared across all attempts)
            self._report_progress(video.uuid, video.filename, "uploading", 0)
            self.mediaconvert.upload_to_s3(video_path, source_s3_key)
            self._upload_metadata_to_s3(metadata, metadata_s3_key)
            self._report_progress(video.uuid, video.filename, "uploading", 100)

            # Try each preset in the chain
            for preset_index, preset in enumerate(preset_chain):
                is_retry = preset_index > 0

                if is_retry:
                    logger.info(f"Retrying {video.filename} with higher quality preset: {preset}")

                attempt = self._try_preset(
                    candidate=candidate,
                    preset=preset,
                    source_s3_key=source_s3_key,
                    metadata_s3_key=metadata_s3_key,
                )
                attempts.append(attempt)

                # If conversion itself failed (not quality), continue to next preset
                if not attempt.success:
                    logger.warning(
                        f"{video.filename}: Preset {preset} failed: {attempt.error_message}"
                    )
                    self._cleanup_attempt(attempt)
                    continue

                # Check if SSIM threshold was met
                if attempt.quality_result and attempt.quality_result.is_acceptable:
                    logger.info(
                        f"{video.filename}: SSIM {attempt.ssim_score:.4f} meets threshold "
                        f"with preset {preset}"
                    )

                    # Clean up previous attempts
                    for prev_attempt in attempts[:-1]:
                        if prev_attempt.success:
                            self._cleanup_attempt(prev_attempt)

                    return self._finalize_attempt(
                        candidate=candidate, attempt=attempt, metadata=metadata, best_effort=False
                    )

                # Quality check failed (SSIM below threshold)
                if attempt.ssim_score is not None:
                    logger.info(
                        f"{video.filename}: SSIM {attempt.ssim_score:.4f} below threshold "
                        f"with preset {preset}"
                    )

            # All presets tried, none met threshold - use best-effort selection
            best_attempt, is_best_effort = self._select_best_attempt(candidate, attempts)

            if best_attempt is None:
                # All attempts failed completely
                return ConversionResult(
                    uuid=video.uuid,
                    filename=video.filename,
                    success=False,
                    original_path=video.path,
                    error_message="All preset attempts failed",
                )

            # Clean up non-selected attempts
            for attempt in attempts:
                if attempt != best_attempt and attempt.success:
                    self._cleanup_attempt(attempt)

            return self._finalize_attempt(
                candidate=candidate,
                attempt=best_attempt,
                metadata=metadata,
                best_effort=is_best_effort,
            )

        except Exception as e:
            logger.exception(f"Conversion failed for {video.filename}")
            self._report_progress(video.uuid, video.filename, "failed", 0, str(e))

            # Clean up all S3 files
            for attempt in attempts:
                try:
                    self._cleanup_attempt(attempt)
                except Exception:
                    pass

            try:
                self.mediaconvert.delete_from_s3(source_s3_key)
                self._delete_metadata_from_s3(metadata_s3_key)
            except Exception:
                pass

            return ConversionResult(
                uuid=video.uuid,
                filename=video.filename,
                success=False,
                original_path=video.path,
                error_message=str(e),
            )
