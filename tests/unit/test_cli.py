"""Unit tests for CLI commands.

Tests CLI command parsing, output formatting, and basic functionality.
Target coverage: 30%+ (UI)
"""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from vco.cli.main import (
    format_duration,
    format_size,
    parse_date,
)


class TestFormatSize:
    """Tests for format_size helper function."""

    def test_bytes(self):
        """Test formatting bytes."""
        assert format_size(500) == "500.0 B"

    def test_kilobytes(self):
        """Test formatting kilobytes."""
        assert format_size(1024) == "1.0 KB"
        assert format_size(2048) == "2.0 KB"

    def test_megabytes(self):
        """Test formatting megabytes."""
        assert format_size(1024 * 1024) == "1.0 MB"
        assert format_size(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        """Test formatting gigabytes."""
        assert format_size(1024 * 1024 * 1024) == "1.0 GB"

    def test_terabytes(self):
        """Test formatting terabytes."""
        assert format_size(1024 * 1024 * 1024 * 1024) == "1.0 TB"

    def test_zero(self):
        """Test formatting zero bytes."""
        assert format_size(0) == "0.0 B"


class TestFormatDuration:
    """Tests for format_duration helper function."""

    def test_seconds_only(self):
        """Test formatting seconds only."""
        assert format_duration(45) == "0:45"

    def test_minutes_and_seconds(self):
        """Test formatting minutes and seconds."""
        assert format_duration(125) == "2:05"
        assert format_duration(600) == "10:00"

    def test_hours_minutes_seconds(self):
        """Test formatting hours, minutes, and seconds."""
        assert format_duration(3661) == "1:01:01"
        assert format_duration(7200) == "2:00:00"

    def test_zero(self):
        """Test formatting zero duration."""
        assert format_duration(0) == "0:00"


class TestParseDate:
    """Tests for parse_date helper function."""

    def test_valid_date(self):
        """Test parsing valid YYYY-MM date."""
        result = parse_date("2024-06")
        assert result == datetime(2024, 6, 1)

    def test_empty_string(self):
        """Test parsing empty string returns None."""
        result = parse_date("")
        assert result is None

    def test_none(self):
        """Test parsing None returns None."""
        result = parse_date(None)
        assert result is None

    def test_invalid_format(self):
        """Test parsing invalid format raises BadParameter."""
        from click import BadParameter

        with pytest.raises(BadParameter, match="Invalid date format"):
            parse_date("2024/06/15")

    def test_invalid_date(self):
        """Test parsing invalid date raises BadParameter."""
        from click import BadParameter

        with pytest.raises(BadParameter, match="Invalid date format"):
            parse_date("invalid")


class TestCliGroup:
    """Tests for CLI group command."""

    def test_cli_help(self):
        """Test CLI help message."""
        from vco.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "Video Compression Optimizer" in result.output

    def test_cli_no_command(self):
        """Test CLI with no command shows usage."""
        from vco.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, [])

        # CLI group without command shows usage (exit code 0 or 2 depending on click version)
        assert result.exit_code in [0, 2]


class TestScanCommand:
    """Tests for scan command."""

    @patch("vco.cli.main.PhotosAccessManager")
    @patch("vco.cli.main.CompressionAnalyzer")
    @patch("vco.cli.main.ScanService")
    @patch("vco.cli.main.ConfigManager")
    def test_scan_basic(self, mock_config, mock_scan_service, mock_analyzer, mock_photos):
        """Test basic scan command."""
        from vco.cli.main import cli

        # Setup mocks
        mock_config_instance = MagicMock()
        mock_config_instance.get.return_value = "balanced"
        mock_config.return_value = mock_config_instance

        mock_scan_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.candidates = []
        mock_result.summary = MagicMock(
            total_videos=10,
            conversion_candidates=0,
            already_optimized=8,
            professional=1,
            skipped=1,
            estimated_total_savings_bytes=0,
            estimated_total_savings_percent=0.0,
        )
        mock_scan_instance.scan.return_value = mock_result
        mock_scan_service.return_value = mock_scan_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["scan"])

        assert result.exit_code == 0
        assert "Scan Summary" in result.output or "No conversion candidates" in result.output

    @patch("vco.cli.main.PhotosAccessManager")
    @patch("vco.cli.main.CompressionAnalyzer")
    @patch("vco.cli.main.ScanService")
    @patch("vco.cli.main.ConfigManager")
    def test_scan_with_date_range(self, mock_config, mock_scan_service, mock_analyzer, mock_photos):
        """Test scan command with date range."""
        from vco.cli.main import cli

        mock_config_instance = MagicMock()
        mock_config_instance.get.return_value = "balanced"
        mock_config.return_value = mock_config_instance

        mock_scan_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.candidates = []
        mock_result.summary = MagicMock(
            total_videos=5,
            conversion_candidates=0,
            already_optimized=5,
            professional=0,
            skipped=0,
            estimated_total_savings_bytes=0,
            estimated_total_savings_percent=0.0,
        )
        mock_scan_instance.scan.return_value = mock_result
        mock_scan_service.return_value = mock_scan_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["scan", "--from", "2024-01", "--to", "2024-06"])

        assert result.exit_code == 0

    @patch("vco.cli.main.PhotosAccessManager")
    @patch("vco.cli.main.CompressionAnalyzer")
    @patch("vco.cli.main.ScanService")
    @patch("vco.cli.main.ConfigManager")
    def test_scan_json_output(self, mock_config, mock_scan_service, mock_analyzer, mock_photos):
        """Test scan command with JSON output."""
        from vco.cli.main import cli

        mock_config_instance = MagicMock()
        mock_config_instance.get.return_value = "balanced"
        mock_config.return_value = mock_config_instance

        mock_scan_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.candidates = []
        mock_result.to_dict.return_value = {"candidates": [], "summary": {}}
        mock_scan_instance.scan.return_value = mock_result
        mock_scan_service.return_value = mock_scan_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["scan", "--json"])

        assert result.exit_code == 0
        # Should be valid JSON
        output_data = json.loads(result.output)
        assert "candidates" in output_data


class TestConfigCommand:
    """Tests for config command."""

    @patch("vco.cli.main.ConfigManager")
    def test_config_show(self, mock_config):
        """Test config show command."""
        from vco.cli.main import cli

        mock_config_instance = MagicMock()
        mock_config_instance.get_all.return_value = {
            "schema_version": "1.0",
            "aws": {
                "region": "ap-northeast-1",
                "s3_bucket": "test-bucket",
                "role_arn": "arn:aws:iam::123456789012:role/test",
            },
            "conversion": {
                "quality_preset": "balanced",
                "max_concurrent": 5,
                "staging_folder": "/tmp/staging",
            },
        }
        mock_config.return_value = mock_config_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["config"])

        assert result.exit_code == 0
        assert "Current Configuration" in result.output
        assert "aws.region" in result.output

    @patch("vco.cli.main.ConfigManager")
    def test_config_show_json(self, mock_config):
        """Test config show with JSON output."""
        from vco.cli.main import cli

        mock_config_instance = MagicMock()
        mock_config_instance.get_all.return_value = {
            "schema_version": "1.0",
            "aws": {"region": "ap-northeast-1"},
            "conversion": {"quality_preset": "balanced"},
        }
        mock_config.return_value = mock_config_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["config", "--json"])

        assert result.exit_code == 0
        output_data = json.loads(result.output)
        assert "aws" in output_data

    @patch("vco.cli.main.ConfigManager")
    def test_config_set(self, mock_config):
        """Test config set command."""
        from vco.cli.main import cli

        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["config", "set", "aws.region", "us-west-2"])

        assert result.exit_code == 0
        assert "Set aws.region" in result.output
        mock_config_instance.set.assert_called_once_with("aws.region", "us-west-2")

    @patch("vco.cli.main.ConfigManager")
    def test_config_set_boolean_true(self, mock_config):
        """Test config set with boolean true value."""
        from vco.cli.main import cli

        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance

        runner = CliRunner()
        runner.invoke(cli, ["config", "set", "some.key", "true"])

        # Should convert 'true' to True
        mock_config_instance.set.assert_called_once_with("some.key", True)

    @patch("vco.cli.main.ConfigManager")
    def test_config_set_integer(self, mock_config):
        """Test config set with integer value."""
        from vco.cli.main import cli

        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance

        runner = CliRunner()
        runner.invoke(cli, ["config", "set", "conversion.max_concurrent", "8"])

        # Should convert '8' to 8
        mock_config_instance.set.assert_called_once_with("conversion.max_concurrent", 8)

    @patch("vco.cli.main.ConfigManager")
    def test_config_set_invalid_value(self, mock_config):
        """Test config set with invalid value."""
        from vco.cli.main import cli

        mock_config_instance = MagicMock()
        mock_config_instance.set.side_effect = ValueError("Invalid value")
        mock_config.return_value = mock_config_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["config", "set", "conversion.quality_preset", "invalid"])

        assert result.exit_code == 1
        assert "Error" in result.output


class TestConvertCommand:
    """Tests for convert command."""

    @patch("vco.cli.main.ScanService")
    @patch("vco.cli.main.ConfigManager")
    def test_convert_no_candidates(self, mock_config, mock_scan):
        """Test convert command with no candidates."""
        from vco.cli.main import cli

        mock_config.return_value = MagicMock()

        mock_scan_instance = MagicMock()
        mock_scan_instance.load_candidates.return_value = None
        mock_scan.return_value = mock_scan_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["convert"])

        assert result.exit_code == 1
        assert "No candidates found" in result.output

    @patch("vco.cli.main.ScanService")
    @patch("vco.cli.main.ConfigManager")
    def test_convert_dry_run(self, mock_config, mock_scan):
        """Test convert command with dry-run flag."""
        from vco.cli.main import cli

        mock_config.return_value = MagicMock()

        # Create mock candidate
        mock_video = MagicMock()
        mock_video.filename = "test.mp4"
        mock_video.file_size = 1000000

        mock_candidate = MagicMock()
        mock_candidate.video = mock_video
        mock_candidate.estimated_savings_bytes = 500000

        mock_result = MagicMock()
        mock_result.candidates = [mock_candidate]

        mock_scan_instance = MagicMock()
        mock_scan_instance.load_candidates.return_value = mock_result
        mock_scan.return_value = mock_scan_instance

        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "--dry-run"])

        assert result.exit_code == 0
        assert "Dry run mode" in result.output
        assert "Would Convert" in result.output
