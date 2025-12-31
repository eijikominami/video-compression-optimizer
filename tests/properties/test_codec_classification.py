"""Property-based tests for codec classification.

Feature: video-compression-optimizer, Property 1: Codec classification accuracy
Validates: Requirements 1.4, 1.5, 10.3
"""

from datetime import datetime
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from vco.analyzer.analyzer import (
    IMAGE_CODECS,
    INEFFICIENT_CODECS,
    OPTIMIZED_CODECS,
    PROFESSIONAL_CODECS,
    UNSUPPORTED_CODECS,
    CompressionAnalyzer,
)
from vco.models.types import VideoInfo, VideoStatus

# =============================================================================
# EXPECTED CODEC DEFINITIONS (Independent from implementation)
# These are defined from external specifications, NOT from implementation code.
# =============================================================================

# H.265/HEVC variants
# Sources:
# - FFmpeg libavcodec documentation
# - Apple Technical Note TN2224 (hvc1 is Apple's H.265 in MOV/MP4)
# - ISO/IEC 23008-2 (HEVC standard)
EXPECTED_H265_CODECS = ["hevc", "hev1", "hvc1", "h265", "x265"]

# H.264/AVC variants
# Sources:
# - FFmpeg libavcodec documentation
# - ISO/IEC 14496-10 (AVC standard)
EXPECTED_H264_CODECS = ["h264", "avc1", "avc", "x264"]

# ProRes variants
# Source: Apple ProRes White Paper
EXPECTED_PRORES_CODECS = ["prores", "prores_ks", "prores_aw", "prores_lt", "prores_proxy"]

# AV1 variants
# Source: AOM AV1 specification
EXPECTED_AV1_CODECS = ["av1", "av01"]

# VP9 variants
# Source: Google VP9 specification
EXPECTED_VP9_CODECS = ["vp9"]


# Inefficient codecs that are also supported by MediaConvert
CONVERTIBLE_INEFFICIENT_CODECS = INEFFICIENT_CODECS - UNSUPPORTED_CODECS


def create_video_with_codec(codec: str, duration: float = 120.0) -> VideoInfo:
    """Create a VideoInfo object with specified codec."""
    return VideoInfo(
        uuid=f"test_{codec}",
        filename=f"test_{codec}.mov",
        path=Path(f"/tmp/test_{codec}.mov"),
        codec=codec,
        resolution=(1920, 1080),
        bitrate=25000000,
        duration=duration,
        frame_rate=30.0,
        file_size=375000000,
        capture_date=datetime(2020, 7, 15, 14, 30, 0),
        creation_date=datetime(2020, 7, 15, 14, 30, 0),
        albums=[],
        is_in_icloud=False,
        is_local=True,
    )


class TestCodecSpecificationCoverage:
    """Verify implementation covers all codec variants from external specifications.

    These tests ensure the implementation's codec lists include all known variants
    defined in external specifications (FFmpeg, Apple, ISO standards).
    """

    def test_optimized_codecs_include_all_h265_variants(self):
        """Implementation must include all H.265 variants from specification."""
        for codec in EXPECTED_H265_CODECS:
            assert codec in OPTIMIZED_CODECS, (
                f"OPTIMIZED_CODECS missing H.265 variant '{codec}' (Source: FFmpeg/Apple TN2224)"
            )

    def test_optimized_codecs_include_all_av1_variants(self):
        """Implementation must include all AV1 variants from specification."""
        for codec in EXPECTED_AV1_CODECS:
            assert codec in OPTIMIZED_CODECS, (
                f"OPTIMIZED_CODECS missing AV1 variant '{codec}' (Source: AOM AV1 spec)"
            )

    def test_optimized_codecs_include_all_vp9_variants(self):
        """Implementation must include all VP9 variants from specification."""
        for codec in EXPECTED_VP9_CODECS:
            assert codec in OPTIMIZED_CODECS, (
                f"OPTIMIZED_CODECS missing VP9 variant '{codec}' (Source: Google VP9 spec)"
            )

    def test_inefficient_codecs_include_all_h264_variants(self):
        """Implementation must include all H.264 variants from specification."""
        for codec in EXPECTED_H264_CODECS:
            assert codec in INEFFICIENT_CODECS, (
                f"INEFFICIENT_CODECS missing H.264 variant '{codec}' (Source: FFmpeg/ISO 14496-10)"
            )

    def test_professional_codecs_include_all_prores_variants(self):
        """Implementation must include all ProRes variants from specification."""
        for codec in EXPECTED_PRORES_CODECS:
            assert codec in PROFESSIONAL_CODECS, (
                f"PROFESSIONAL_CODECS missing ProRes variant '{codec}' "
                f"(Source: Apple ProRes White Paper)"
            )


class TestCodecClassification:
    """Property tests for codec classification accuracy.

    Property 1: For any video file, when the system analyzes it:
    - If codec is in inefficient list (MPEG-2, H.264, WMV), mark as conversion candidate
    - If codec is H.265, mark as "already optimized"
    - If codec is in professional list (ProRes, DNxHD), mark as "professional format"

    Validates: Requirements 1.4, 1.5, 10.3
    """

    @given(codec=st.sampled_from(list(INEFFICIENT_CODECS)))
    @settings(max_examples=100)
    def test_inefficient_codecs_marked_as_pending(self, codec: str):
        """Inefficient codecs are marked as conversion candidates (PENDING)."""
        analyzer = CompressionAnalyzer()
        status = analyzer.classify_codec(codec)
        assert status == VideoStatus.PENDING, (
            f"Codec '{codec}' should be marked as PENDING (conversion candidate)"
        )

    @given(codec=st.sampled_from(list(OPTIMIZED_CODECS)))
    @settings(max_examples=100)
    def test_optimized_codecs_marked_as_optimized(self, codec: str):
        """Optimized codecs (H.265, AV1, VP9) are marked as OPTIMIZED."""
        analyzer = CompressionAnalyzer()
        status = analyzer.classify_codec(codec)
        assert status == VideoStatus.OPTIMIZED, f"Codec '{codec}' should be marked as OPTIMIZED"

    @given(codec=st.sampled_from(list(PROFESSIONAL_CODECS)))
    @settings(max_examples=100)
    def test_professional_codecs_marked_as_professional(self, codec: str):
        """Professional codecs (ProRes, DNxHD) are marked as PROFESSIONAL."""
        analyzer = CompressionAnalyzer()
        status = analyzer.classify_codec(codec)
        assert status == VideoStatus.PROFESSIONAL, (
            f"Codec '{codec}' should be marked as PROFESSIONAL"
        )

    def test_h265_variants_all_optimized(self):
        """All H.265 codec variants from specification are marked as optimized.

        Source: FFmpeg documentation, Apple Technical Note TN2224
        Note: hvc1 is Apple's H.265 identifier used in MOV/MP4 containers.
        """
        analyzer = CompressionAnalyzer()
        # Test both lowercase and uppercase variants
        for codec in EXPECTED_H265_CODECS:
            for variant in [codec, codec.upper()]:
                status = analyzer.classify_codec(variant)
                assert status == VideoStatus.OPTIMIZED, (
                    f"H.265 variant '{variant}' should be marked as OPTIMIZED"
                )

    def test_h264_variants_all_inefficient(self):
        """All H.264 codec variants from specification are marked as inefficient.

        Source: FFmpeg documentation, ISO/IEC 14496-10
        """
        analyzer = CompressionAnalyzer()
        # Test both lowercase and uppercase variants
        for codec in EXPECTED_H264_CODECS:
            for variant in [codec, codec.upper()]:
                status = analyzer.classify_codec(variant)
                assert status == VideoStatus.PENDING, (
                    f"H.264 variant '{variant}' should be marked as PENDING"
                )

    def test_prores_variants_all_professional(self):
        """All ProRes codec variants from specification are marked as professional.

        Source: Apple ProRes White Paper
        """
        analyzer = CompressionAnalyzer()
        # Test both lowercase and uppercase variants
        for codec in EXPECTED_PRORES_CODECS:
            for variant in [codec, codec.upper()]:
                status = analyzer.classify_codec(variant)
                assert status == VideoStatus.PROFESSIONAL, (
                    f"ProRes variant '{variant}' should be marked as PROFESSIONAL"
                )

    @given(codec=st.sampled_from(list(CONVERTIBLE_INEFFICIENT_CODECS)))
    @settings(max_examples=100)
    def test_inefficient_codec_video_is_conversion_candidate(self, codec: str):
        """Videos with inefficient codecs (supported by MediaConvert) are conversion candidates."""
        analyzer = CompressionAnalyzer()
        video = create_video_with_codec(codec)
        candidate = analyzer.analyze_video(video)

        assert candidate.status == VideoStatus.PENDING
        assert candidate.skip_reason is None
        assert candidate.estimated_savings_bytes > 0

    @given(codec=st.sampled_from(list(UNSUPPORTED_CODECS)))
    @settings(max_examples=50)
    def test_unsupported_codec_video_is_skipped(self, codec: str):
        """Videos with codecs not supported by MediaConvert are skipped.

        Validates: External service constraint - MediaConvert supported codecs
        """
        analyzer = CompressionAnalyzer()
        video = create_video_with_codec(codec)
        candidate = analyzer.analyze_video(video)

        assert candidate.skip_reason is not None
        assert (
            "not supported" in candidate.skip_reason.lower()
            or "mediaconvert" in candidate.skip_reason.lower()
        )

    @given(codec=st.sampled_from(list(OPTIMIZED_CODECS)))
    @settings(max_examples=100)
    def test_optimized_codec_video_is_skipped(self, codec: str):
        """Videos with optimized codecs are skipped."""
        analyzer = CompressionAnalyzer()
        video = create_video_with_codec(codec)
        candidate = analyzer.analyze_video(video)

        assert candidate.status == VideoStatus.OPTIMIZED
        assert candidate.skip_reason is not None
        assert (
            "efficient" in candidate.skip_reason.lower() or "h.265" in candidate.skip_reason.lower()
        )

    @given(codec=st.sampled_from(list(PROFESSIONAL_CODECS)))
    @settings(max_examples=100)
    def test_professional_codec_video_is_skipped(self, codec: str):
        """Videos with professional codecs are skipped with manual review recommendation."""
        analyzer = CompressionAnalyzer()
        video = create_video_with_codec(codec)
        candidate = analyzer.analyze_video(video)

        assert candidate.status == VideoStatus.PROFESSIONAL
        assert candidate.skip_reason is not None
        assert "professional" in candidate.skip_reason.lower()
        assert (
            "manual" in candidate.skip_reason.lower() or "review" in candidate.skip_reason.lower()
        )

    def test_codec_classification_is_case_insensitive(self):
        """Codec classification is case-insensitive."""
        analyzer = CompressionAnalyzer()
        test_cases = [
            ("H264", VideoStatus.PENDING),
            ("h264", VideoStatus.PENDING),
            ("HEVC", VideoStatus.OPTIMIZED),
            ("hevc", VideoStatus.OPTIMIZED),
            ("ProRes", VideoStatus.PROFESSIONAL),
            ("prores", VideoStatus.PROFESSIONAL),
        ]

        for codec, expected_status in test_cases:
            status = analyzer.classify_codec(codec)
            assert status == expected_status, (
                f"Codec '{codec}' should be classified as {expected_status}"
            )

    def test_unknown_codec_treated_as_candidate(self):
        """Unknown codecs are treated as potential conversion candidates."""
        analyzer = CompressionAnalyzer()
        unknown_codecs = ["unknown_codec", "custom_codec", "xyz123"]

        for codec in unknown_codecs:
            status = analyzer.classify_codec(codec)
            assert status == VideoStatus.PENDING, (
                f"Unknown codec '{codec}' should be treated as PENDING"
            )

    @given(codec=st.sampled_from(list(IMAGE_CODECS)))
    @settings(max_examples=100)
    def test_image_codecs_marked_as_skipped(self, codec: str):
        """Image-based codecs (jpeg, png, gif) are marked as SKIPPED.

        These are not true video codecs and should not be converted.
        Validates: Requirement 10.6
        """
        analyzer = CompressionAnalyzer()
        status = analyzer.classify_codec(codec)
        assert status == VideoStatus.SKIPPED, f"Image codec '{codec}' should be marked as SKIPPED"

    @given(codec=st.sampled_from(list(IMAGE_CODECS)))
    @settings(max_examples=50)
    def test_image_codec_video_is_skipped_with_reason(self, codec: str):
        """Videos with image-based codecs are skipped with appropriate reason.

        Tests state consistency: skip_reason and status must be consistent.
        Validates: Requirement 10.6
        """
        analyzer = CompressionAnalyzer()
        video = create_video_with_codec(codec)
        candidate = analyzer.analyze_video(video)

        # State consistency check
        assert candidate.status == VideoStatus.SKIPPED, (
            f"Image codec '{codec}' video should have status SKIPPED"
        )
        assert candidate.skip_reason is not None, (
            f"Image codec '{codec}' video should have skip_reason set"
        )
        assert "image" in candidate.skip_reason.lower(), (
            f"Skip reason should mention 'image', got: {candidate.skip_reason}"
        )

    @given(
        inefficient_codecs=st.lists(
            st.sampled_from(list(CONVERTIBLE_INEFFICIENT_CODECS)),
            min_size=1,
            max_size=10,
        ),
        optimized_codecs=st.lists(
            st.sampled_from(list(OPTIMIZED_CODECS)),
            min_size=1,
            max_size=10,
        ),
    )
    @settings(max_examples=50)
    def test_batch_analysis_correctly_classifies_mixed_codecs(
        self,
        inefficient_codecs: list[str],
        optimized_codecs: list[str],
    ):
        """Batch analysis correctly classifies videos with mixed codecs."""
        analyzer = CompressionAnalyzer()
        videos = []
        for i, codec in enumerate(inefficient_codecs):
            videos.append(create_video_with_codec(f"{codec}_{i}"))
            videos[-1] = VideoInfo(
                uuid=f"inefficient_{i}",
                filename=f"inefficient_{i}.mov",
                path=Path(f"/tmp/inefficient_{i}.mov"),
                codec=codec,
                resolution=(1920, 1080),
                bitrate=25000000,
                duration=120.0,
                frame_rate=30.0,
                file_size=375000000,
                capture_date=datetime(2020, 7, 15),
                creation_date=datetime(2020, 7, 15),
                albums=[],
                is_in_icloud=False,
                is_local=True,
            )

        for i, codec in enumerate(optimized_codecs):
            videos.append(
                VideoInfo(
                    uuid=f"optimized_{i}",
                    filename=f"optimized_{i}.mov",
                    path=Path(f"/tmp/optimized_{i}.mov"),
                    codec=codec,
                    resolution=(1920, 1080),
                    bitrate=25000000,
                    duration=120.0,
                    frame_rate=30.0,
                    file_size=375000000,
                    capture_date=datetime(2020, 7, 15),
                    creation_date=datetime(2020, 7, 15),
                    albums=[],
                    is_in_icloud=False,
                    is_local=True,
                )
            )

        candidates = analyzer.analyze(videos)

        # Count by status
        pending_count = sum(1 for c in candidates if c.status == VideoStatus.PENDING)
        optimized_count = sum(1 for c in candidates if c.status == VideoStatus.OPTIMIZED)

        assert pending_count == len(inefficient_codecs)
        assert optimized_count == len(optimized_codecs)
