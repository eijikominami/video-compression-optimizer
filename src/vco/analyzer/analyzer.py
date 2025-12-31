"""Compression Analyzer for Video Compression Optimizer.

This module analyzes video files to identify conversion candidates
based on codec efficiency and estimates potential space savings.
"""

from vco.models.types import ConversionCandidate, VideoInfo, VideoStatus

# Codec classification lists
INEFFICIENT_CODECS = {
    "mpeg2video",
    "mpeg2",
    "mpeg-2",
    "mpeg1video",
    "mpeg1",
    "mpeg-1",
    "mp1v",
    "h264",
    "avc1",
    "avc",
    "x264",
    "wmv1",
    "wmv2",
    "wmv3",
    "wmv",
    "vc1",
    "mpeg4",
    "mp4v",
    "divx",
    "xvid",
    "mjpeg",
    "mjpg",
    "dmb1",  # Matrox MPEG-2 I-frame codec
}

# Image-based codecs that should be skipped (not true video)
IMAGE_CODECS = {
    "jpeg",
    "jpg",
    "photo-jpeg",
    "photo jpeg",
    "png",
    "gif",
    "bmp",
    "tiff",
}

PROFESSIONAL_CODECS = {
    "prores",
    "prores_ks",
    "prores_aw",
    "prores_lt",
    "prores_proxy",
    "dnxhd",
    "dnxhr",
    "cineform",
    "rawvideo",
    "raw",
}

OPTIMIZED_CODECS = {
    "hevc",
    "hev1",
    "hvc1",
    "h265",
    "x265",  # hvc1 is Apple's H.265 identifier
    "av1",
    "av01",
    "vp9",
}

# MediaConvert supported input codecs (from AWS documentation)
# https://docs.aws.amazon.com/mediaconvert/latest/ug/reference-codecs-containers-input.html
MEDIACONVERT_SUPPORTED_CODECS = {
    # Video codecs
    "h264",
    "avc1",
    "avc",
    "x264",
    "hevc",
    "hev1",
    "h265",
    "x265",
    "mpeg2video",
    "mpeg2",
    "mpeg-2",
    "mpeg1video",
    "mpeg1",
    "mpeg-1",
    "prores",
    "prores_ks",
    "prores_aw",
    "prores_lt",
    "prores_proxy",
    "vc1",
    "wmv3",
    "vp8",
    "vp9",
    "mjpeg",
    "mjpg",
    "dvvideo",
    "dv",
    "dvcpro",
    "mpeg4",
    "mp4v",
    "divx",
    "xvid",
    "h261",
    "h262",
    "h263",
    "jpeg2000",
    "j2k",
    "av1",
    "av01",
    "dnxhd",
    "dnxhr",
    "gif",
}

# MediaConvert supported input containers
MEDIACONVERT_SUPPORTED_CONTAINERS = {
    "mov",
    "qt",
    "quicktime",
    "mp4",
    "m4v",
    "mxf",
    "avi",
    "ts",
    "mts",
    "m2ts",
    "mpeg-ts",
    "3gp",
    "3g2",
    "flv",
    "f4v",
    "webm",
    "mkv",
    "matroska",
    "asf",
    "wmv",
    "gif",
    "imf",
}

# Codecs that MediaConvert cannot process
UNSUPPORTED_CODECS = {
    "vp6",
    "vp6f",  # Old VP6 codec
    "flv1",  # Old Flash video
    "rv10",
    "rv20",
    "rv30",
    "rv40",  # RealVideo
    "wmv1",
    "wmv2",  # Old WMV versions (only wmv3/vc1 supported)
    "svq1",
    "svq3",  # Sorenson Video
    "cinepak",
    "indeo2",
    "indeo3",
    "indeo4",
    "indeo5",
}


class CompressionAnalyzer:
    """Analyzes video files for compression optimization potential.

    Identifies videos using inefficient codecs and estimates
    potential space savings from H.265 conversion.
    """

    # Estimated compression ratios for different quality presets
    # These are conservative estimates based on typical H.264 to H.265 conversion
    COMPRESSION_ESTIMATES = {
        "high": 0.35,  # ~35% size reduction (high quality)
        "balanced": 0.50,  # ~50% size reduction (balanced)
        "compression": 0.65,  # ~65% size reduction (high compression)
    }

    # Duration thresholds
    MIN_DURATION_SECONDS = 1.0
    LONG_DURATION_WARNING_SECONDS = 4 * 60 * 60  # 4 hours

    def __init__(self, quality_preset: str = "balanced"):
        """Initialize CompressionAnalyzer.

        Args:
            quality_preset: Quality preset for estimation (high, balanced, compression)
        """
        if quality_preset not in self.COMPRESSION_ESTIMATES:
            raise ValueError(f"Invalid quality preset: {quality_preset}")
        self.quality_preset = quality_preset

    def classify_codec(self, codec: str) -> VideoStatus:
        """Classify a codec into status category.

        Args:
            codec: Codec name (case-insensitive)

        Returns:
            VideoStatus indicating codec classification
        """
        codec_lower = codec.lower().strip()

        if codec_lower in OPTIMIZED_CODECS:
            return VideoStatus.OPTIMIZED

        if codec_lower in PROFESSIONAL_CODECS:
            return VideoStatus.PROFESSIONAL

        if codec_lower in IMAGE_CODECS:
            return VideoStatus.SKIPPED

        if codec_lower in INEFFICIENT_CODECS:
            return VideoStatus.PENDING

        # Unknown codec - treat as potential candidate
        return VideoStatus.PENDING

    def should_skip(self, video: VideoInfo) -> tuple[bool, str | None]:
        """Determine if a video should be skipped from conversion.

        Args:
            video: VideoInfo object to check

        Returns:
            Tuple of (should_skip, reason)
        """
        # Check duration
        if video.duration < self.MIN_DURATION_SECONDS:
            return (
                True,
                f"Duration too short ({video.duration:.2f}s < {self.MIN_DURATION_SECONDS}s)",
            )

        # Check codec classification (only if codec is known)
        if video.codec and video.codec != "unknown":
            codec_lower = video.codec.lower().strip()
            status = self.classify_codec(video.codec)

            if status == VideoStatus.OPTIMIZED:
                return True, "Already using efficient codec (H.265/HEVC)"

            if status == VideoStatus.PROFESSIONAL:
                return True, "Professional format - manual review recommended"

            if status == VideoStatus.SKIPPED:
                return True, "Image-based codec - not a true video format"

            # Check if codec is supported by MediaConvert
            if codec_lower in UNSUPPORTED_CODECS:
                return True, f"Codec '{video.codec}' is not supported by MediaConvert"

            # Check if codec is in the known supported list
            # If not in any known list, allow it (MediaConvert may still support it)
            all_known_codecs = (
                INEFFICIENT_CODECS
                | PROFESSIONAL_CODECS
                | OPTIMIZED_CODECS
                | IMAGE_CODECS
                | MEDIACONVERT_SUPPORTED_CODECS
                | UNSUPPORTED_CODECS
            )
            if codec_lower not in all_known_codecs:
                # Unknown codec - log warning but allow processing
                pass

        # iCloud files are valid candidates - they will be downloaded when needed
        # Only skip if file is neither local nor in iCloud
        if not video.is_local and not video.is_in_icloud:
            return True, "File not accessible"

        return False, None

    def estimate_savings(
        self, video: VideoInfo, quality_preset: str | None = None
    ) -> tuple[int, float]:
        """Estimate space savings from conversion.

        Args:
            video: VideoInfo object
            quality_preset: Override quality preset (optional)

        Returns:
            Tuple of (estimated_savings_bytes, estimated_savings_percent)
        """
        preset = quality_preset or self.quality_preset
        compression_ratio = self.COMPRESSION_ESTIMATES.get(preset, 0.50)

        estimated_savings_bytes = int(video.file_size * compression_ratio)
        estimated_savings_percent = compression_ratio * 100

        return estimated_savings_bytes, estimated_savings_percent

    def analyze_video(self, video: VideoInfo) -> ConversionCandidate:
        """Analyze a single video for conversion potential.

        Args:
            video: VideoInfo object to analyze

        Returns:
            ConversionCandidate with analysis results
        """
        # Check if should skip
        should_skip, skip_reason = self.should_skip(video)

        if should_skip:
            # Determine appropriate status based on codec
            if video.codec:
                status = self.classify_codec(video.codec)
            else:
                status = VideoStatus.SKIPPED

            # Override to SKIPPED if duration is too short or file not accessible
            if skip_reason and (
                "Duration too short" in skip_reason or "not accessible" in skip_reason
            ):
                status = VideoStatus.SKIPPED

            return ConversionCandidate(
                video=video,
                estimated_savings_bytes=0,
                estimated_savings_percent=0.0,
                skip_reason=skip_reason,
                status=status,
            )

        # Estimate savings
        savings_bytes, savings_percent = self.estimate_savings(video)

        return ConversionCandidate(
            video=video,
            estimated_savings_bytes=savings_bytes,
            estimated_savings_percent=savings_percent,
            skip_reason=None,
            status=VideoStatus.PENDING,
        )

    def analyze(self, videos: list[VideoInfo]) -> list[ConversionCandidate]:
        """Analyze a list of videos for conversion potential.

        Args:
            videos: List of VideoInfo objects to analyze

        Returns:
            List of ConversionCandidate objects
        """
        return [self.analyze_video(video) for video in videos]

    def get_conversion_candidates(self, videos: list[VideoInfo]) -> list[ConversionCandidate]:
        """Get only videos that are candidates for conversion.

        Args:
            videos: List of VideoInfo objects

        Returns:
            List of ConversionCandidate objects that should be converted
        """
        candidates = self.analyze(videos)
        return [c for c in candidates if c.status == VideoStatus.PENDING]

    def is_long_video(self, video: VideoInfo) -> bool:
        """Check if video exceeds the long duration threshold.

        Args:
            video: VideoInfo object

        Returns:
            True if video duration exceeds 4 hours
        """
        return video.duration > self.LONG_DURATION_WARNING_SECONDS

    def generate_summary(self, candidates: list[ConversionCandidate]) -> dict:
        """Generate a summary of analysis results.

        Args:
            candidates: List of ConversionCandidate objects

        Returns:
            Dictionary with summary statistics
        """
        total = len(candidates)
        pending = sum(1 for c in candidates if c.status == VideoStatus.PENDING)
        optimized = sum(1 for c in candidates if c.status == VideoStatus.OPTIMIZED)
        skipped = sum(1 for c in candidates if c.status == VideoStatus.SKIPPED)
        professional = sum(1 for c in candidates if c.status == VideoStatus.PROFESSIONAL)

        total_savings_bytes = sum(
            c.estimated_savings_bytes for c in candidates if c.status == VideoStatus.PENDING
        )

        total_original_size = sum(
            c.video.file_size for c in candidates if c.status == VideoStatus.PENDING
        )

        avg_savings_percent = (
            (total_savings_bytes / total_original_size * 100) if total_original_size > 0 else 0.0
        )

        return {
            "total_videos": total,
            "conversion_candidates": pending,
            "already_optimized": optimized,
            "skipped": skipped,
            "professional_format": professional,
            "estimated_total_savings_bytes": total_savings_bytes,
            "estimated_total_savings_percent": avg_savings_percent,
        }
