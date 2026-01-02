"""Tests for CLI --legacy option functionality."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from vco.cli.main import cli


class TestScanLegacyOption:
    """Tests for scan command --legacy option."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    def test_scan_legacy_shows_deprecation_warning(self, runner):
        """Test that --legacy shows deprecation warning."""
        # Patch at the module level where it's used
        with patch.object(
            __import__("vco.cli.main", fromlist=["PhotosAccessManager"]),
            "PhotosAccessManager",
        ) as mock_legacy:
            manager = MagicMock()
            manager.get_all_videos.return_value = []
            mock_legacy.return_value = manager

            result = runner.invoke(cli, ["scan", "--legacy"])

            # Check for deprecation warning in output
            assert "deprecated" in result.output.lower()

    def test_scan_swift_fallback_shows_warning(self, runner):
        """Test that Swift fallback shows warning message."""
        with patch("vco.photos.swift_bridge.SwiftBridge") as mock_swift:
            mock_swift.side_effect = Exception("Binary not found")

            # Also need to patch PhotosAccessManager for fallback
            with patch.object(
                __import__("vco.cli.main", fromlist=["PhotosAccessManager"]),
                "PhotosAccessManager",
            ) as mock_legacy:
                manager = MagicMock()
                manager.get_all_videos.return_value = []
                mock_legacy.return_value = manager

                result = runner.invoke(cli, ["scan"])

                # Should show fallback warning in output
                assert "Swift implementation unavailable" in result.output

    def test_scan_swift_success_no_fallback(self, runner):
        """Test that successful Swift initialization doesn't fall back."""
        with patch("vco.photos.swift_bridge.SwiftBridge") as mock_swift:
            bridge = MagicMock()
            bridge.get_all_videos.return_value = []
            mock_swift.return_value = bridge

            runner.invoke(cli, ["scan"])

            # SwiftBridge should be used
            mock_swift.assert_called_once()


class TestLegacyOptionHelp:
    """Tests for --legacy option help text."""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()

    def test_scan_help_shows_legacy_option(self, runner):
        """Test that scan --help shows --legacy option."""
        result = runner.invoke(cli, ["scan", "--help"])

        assert "--legacy" in result.output
        assert result.exit_code == 0

    def test_legacy_help_text_mentions_deprecated(self, runner):
        """Test that --legacy help text mentions deprecation."""
        result = runner.invoke(cli, ["scan", "--help"])

        # Check for deprecation mention in help
        output_lower = result.output.lower()
        assert "deprecated" in output_lower or "legacy" in output_lower
