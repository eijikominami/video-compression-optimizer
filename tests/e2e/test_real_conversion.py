"""End-to-end tests for real video conversion.

These tests use actual Photos library and AWS services.
They are marked with pytest markers to allow selective execution.

実行方法:
    # スキャンのみ（AWS 不要）
    python3.11 -m pytest tests/e2e/test_real_conversion.py -v -m "not aws"

    # AWS を含む全テスト
    python3.11 -m pytest tests/e2e/test_real_conversion.py -v

検証対象:
- 要件 1.1: Photos ライブラリからの動画検出
- 要件 1.4: コーデック分析
- 要件 3.1: AWS MediaConvert による変換
- 要件 4.1: 品質検証
"""

import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# Skip all tests if not on macOS
pytestmark = pytest.mark.skipif(
    os.uname().sysname != "Darwin", reason="E2E tests require macOS with Photos library"
)


class TestPhotosLibraryScan:
    """E2E tests for scanning Photos library."""

    def test_scan_photos_library(self):
        """Scan actual Photos library and verify results.

        This test requires:
        - macOS with Photos app
        - At least one video in Photos library
        """
        from vco.analyzer.analyzer import CompressionAnalyzer
        from vco.photos.manager import PhotosAccessManager
        from vco.services.scan import ScanService

        # Create services
        photos_manager = PhotosAccessManager()
        analyzer = CompressionAnalyzer()

        with tempfile.TemporaryDirectory() as tmpdir:
            service = ScanService(
                photos_manager=photos_manager, analyzer=analyzer, output_dir=Path(tmpdir)
            )

            # Scan with date filter (last 30 days to limit scope)
            to_date = datetime.now()
            from_date = to_date - timedelta(days=30)

            result = service.scan(from_date=from_date, to_date=to_date)

            # Verify scan completed
            assert result is not None
            assert result.summary is not None

            # Log results for manual verification
            print("\n=== Scan Results ===")
            print(f"Total videos: {result.summary.total_videos}")
            print(f"Conversion candidates: {result.summary.conversion_candidates}")
            print(f"Already optimized: {result.summary.already_optimized}")
            print(f"Professional: {result.summary.professional}")
            print(f"Skipped: {result.summary.skipped}")

            if result.candidates:
                print("\n=== First 5 Candidates ===")
                for c in result.candidates[:5]:
                    print(
                        f"  - {c.video.filename}: {c.video.codec}, "
                        f"{c.video.file_size / 1024 / 1024:.1f} MB, "
                        f"savings: {c.estimated_savings_percent:.1f}%"
                    )

    def test_scan_all_videos(self):
        """Scan all videos without date filter.

        Warning: This may take a long time for large libraries.
        """
        from vco.analyzer.analyzer import CompressionAnalyzer
        from vco.photos.manager import PhotosAccessManager
        from vco.services.scan import ScanService

        photos_manager = PhotosAccessManager()
        analyzer = CompressionAnalyzer()

        with tempfile.TemporaryDirectory() as tmpdir:
            service = ScanService(
                photos_manager=photos_manager, analyzer=analyzer, output_dir=Path(tmpdir)
            )

            result = service.scan()

            print("\n=== Full Library Scan ===")
            print(f"Total videos: {result.summary.total_videos}")
            print(f"Conversion candidates: {result.summary.conversion_candidates}")
            print(
                f"Estimated savings: {result.summary.estimated_total_savings_bytes / 1024 / 1024 / 1024:.2f} GB"
            )
            print(f"Estimated savings: {result.summary.estimated_total_savings_percent:.1f}%")

            # Save results
            saved_path = service.save_candidates(result)
            print(f"Results saved to: {saved_path}")


@pytest.mark.aws
class TestAWSConversion:
    """E2E tests for AWS MediaConvert conversion.

    These tests require:
    - AWS credentials configured
    - Deployed VCO infrastructure (S3 bucket, Lambda, MediaConvert role)
    """

    @pytest.fixture
    def aws_config(self):
        """Load AWS configuration."""
        from vco.config.manager import ConfigManager

        config_manager = ConfigManager()
        config = config_manager.config

        # Verify AWS is configured
        if not config.aws.s3_bucket:
            pytest.skip("AWS S3 bucket not configured")
        if not config.aws.role_arn:
            pytest.skip("AWS MediaConvert role ARN not configured")

        return config.aws

    @pytest.fixture
    def mediaconvert_client(self, aws_config):
        """Create MediaConvert client."""
        from vco.converter.mediaconvert import MediaConvertClient

        return MediaConvertClient(
            region=aws_config.region,
            s3_bucket=aws_config.s3_bucket,
            role_arn=aws_config.role_arn,
            profile_name=aws_config.profile,
        )

    @pytest.fixture
    def quality_checker(self, aws_config):
        """Create quality checker."""
        from vco.quality.checker import QualityChecker

        return QualityChecker(
            s3_bucket=aws_config.s3_bucket,
            lambda_function_name="vco-quality-checker-dev",
            region=aws_config.region,
            profile_name=aws_config.profile,
        )

    def test_convert_single_video(self, aws_config, mediaconvert_client, quality_checker):
        """Convert a single video using AWS MediaConvert.

        This test:
        1. Scans Photos library for a conversion candidate
        2. Uploads to S3
        3. Converts using MediaConvert
        4. Runs quality check
        5. Downloads result
        """
        from vco.analyzer.analyzer import CompressionAnalyzer
        from vco.photos.manager import PhotosAccessManager
        from vco.services.convert import ConvertService
        from vco.services.scan import ScanService

        # Step 1: Find a conversion candidate
        photos_manager = PhotosAccessManager()
        analyzer = CompressionAnalyzer()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            staging_folder = output_dir / "converted"

            scan_service = ScanService(
                photos_manager=photos_manager, analyzer=analyzer, output_dir=output_dir
            )

            # Scan videos from last 2 years (to include more candidates)
            to_date = datetime.now()
            from_date = to_date - timedelta(days=730)

            scan_result = scan_service.scan(from_date=from_date, to_date=to_date)

            if not scan_result.candidates:
                pytest.skip("No conversion candidates found in Photos library")

            # Find smallest LOCAL candidate for faster test (skip iCloud-only files)
            local_candidates = [
                c
                for c in scan_result.candidates
                if c.video.is_local and not (c.video.is_in_icloud and not c.video.is_local)
            ]

            if not local_candidates:
                pytest.skip("No local conversion candidates found (all in iCloud)")

            candidates = sorted(local_candidates, key=lambda c: c.video.file_size)
            candidate = candidates[0]

            print("\n=== Converting ===")
            print(f"File: {candidate.video.filename}")
            print(f"Codec: {candidate.video.codec}")
            print(f"Size: {candidate.video.file_size / 1024 / 1024:.1f} MB")
            print(f"Duration: {candidate.video.duration:.1f} seconds")

            # Skip if file is too large (> 500 MB)
            if candidate.video.file_size > 500 * 1024 * 1024:
                pytest.skip(
                    f"Smallest candidate is too large: {candidate.video.file_size / 1024 / 1024:.1f} MB"
                )

            # Step 2: Convert
            convert_service = ConvertService(
                mediaconvert_client=mediaconvert_client,
                quality_checker=quality_checker,
                staging_folder=staging_folder,
            )

            # Estimate cost
            cost = convert_service.estimate_batch_cost([candidate])
            print(f"Estimated cost: ${cost:.4f}")

            # Convert
            result = convert_service.convert_single(candidate=candidate, quality_preset="balanced")

            print("\n=== Result ===")
            print(f"Success: {result.success}")

            if result.success:
                print(f"Converted path: {result.converted_path}")
                print(f"Quality result: SSIM={result.quality_result.ssim_score:.4f}")

                # Verify converted file exists
                assert result.converted_path.exists()

                # Verify quality
                assert result.quality_result.is_acceptable
                assert result.quality_result.ssim_score >= 0.95
            else:
                print(f"Error: {result.error_message}")
                pytest.fail(f"Conversion failed: {result.error_message}")

    def test_cost_estimation(self, aws_config, mediaconvert_client, quality_checker):
        """Test cost estimation for batch conversion."""
        from vco.analyzer.analyzer import CompressionAnalyzer
        from vco.photos.manager import PhotosAccessManager
        from vco.services.convert import ConvertService
        from vco.services.scan import ScanService

        photos_manager = PhotosAccessManager()
        analyzer = CompressionAnalyzer()

        with tempfile.TemporaryDirectory() as tmpdir:
            scan_service = ScanService(
                photos_manager=photos_manager, analyzer=analyzer, output_dir=Path(tmpdir)
            )

            # Scan all videos
            scan_result = scan_service.scan()

            if not scan_result.candidates:
                pytest.skip("No conversion candidates found")

            convert_service = ConvertService(
                mediaconvert_client=mediaconvert_client,
                quality_checker=quality_checker,
                staging_folder=Path(tmpdir) / "converted",
            )

            # Estimate cost for all candidates
            total_cost = convert_service.estimate_batch_cost(scan_result.candidates)

            print("\n=== Cost Estimation ===")
            print(f"Total candidates: {len(scan_result.candidates)}")
            print(f"Estimated total cost: ${total_cost:.2f}")

            # Calculate per-video average
            if scan_result.candidates:
                avg_cost = total_cost / len(scan_result.candidates)
                print(f"Average cost per video: ${avg_cost:.4f}")


class TestCLICommands:
    """E2E tests for CLI commands."""

    def test_cli_scan_command(self):
        """Test vco scan command."""
        import subprocess

        result = subprocess.run(
            ["python3.11", "-m", "vco.cli.main", "scan", "--help"],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent.parent),
        )

        assert result.returncode == 0
        assert "scan" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_cli_config_command(self):
        """Test vco config command."""
        import subprocess

        result = subprocess.run(
            ["python3.11", "-m", "vco.cli.main", "config", "--help"],
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent.parent.parent),
        )

        assert result.returncode == 0
