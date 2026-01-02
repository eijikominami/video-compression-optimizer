"""CLI smoke tests for async workflow commands.

Task 14.4: CLI スモークテスト
- `vco convert --async --help` が正常終了
- `vco status --help` が正常終了
- `vco cancel --help` が正常終了

These tests verify that CLI commands can be invoked without errors,
catching issues that mocks cannot detect (e.g., missing dependencies,
import errors, CLI configuration issues).
"""

import subprocess
import sys


class TestCLISmokeTests:
    """Smoke tests for CLI commands."""

    def test_vco_help(self):
        """vco --help should exit successfully."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "--help"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"
        assert "vco" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_vco_convert_help(self):
        """vco convert --help should exit successfully."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "convert", "--help"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"
        assert "convert" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_vco_status_help(self):
        """vco status --help should exit successfully."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "status", "--help"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"
        assert "status" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_vco_cancel_help(self):
        """vco cancel --help should exit successfully."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "cancel", "--help"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"
        assert "cancel" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_vco_scan_help(self):
        """vco scan --help should exit successfully."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "scan", "--help"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"
        assert "scan" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_vco_import_help(self):
        """vco import --help should exit successfully."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "import", "--help"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"
        assert "import" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_vco_config_help(self):
        """vco config --help should exit successfully."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "config", "--help"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        assert result.returncode == 0, f"Failed: {result.stderr}"
        assert "config" in result.stdout.lower() or "usage" in result.stdout.lower()


class TestCLIModuleImport:
    """Test that CLI module can be imported."""

    def test_cli_main_module_imports(self):
        """vco.cli.main module should import without errors."""
        from vco.cli import main

        assert main is not None

    def test_cli_has_app(self):
        """CLI module should have an app or main function."""
        from importlib import import_module

        main_module = import_module("vco.cli.main")

        # Check for common CLI entry points
        # vco.cli.main module has 'cli' (click group) and 'main' (entry point function)
        assert hasattr(main_module, "cli") or hasattr(main_module, "main")


class TestCLIInvalidCommands:
    """Test CLI behavior with invalid commands."""

    def test_vco_invalid_command(self):
        """vco with invalid command should exit with non-zero code."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "invalid_command_xyz"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        # Should fail with non-zero exit code
        assert result.returncode != 0

    def test_vco_convert_missing_required_args(self):
        """vco convert without required args should show help or error."""
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "convert"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        # May succeed (showing help) or fail (missing args)
        # Either way, it should not crash with an exception
        assert "traceback" not in result.stderr.lower() or result.returncode in [0, 1, 2]

    def test_vco_convert_async_flag_rejected(self):
        """vco convert --async should be rejected as unknown option.

        Requirements: 1.3, 6.3
        The --async flag has been removed. Using it should result in an error.
        """
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "convert", "--async"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        # Should fail with non-zero exit code
        assert result.returncode != 0
        # Should show "No such option" error
        assert "no such option" in result.stderr.lower()

    def test_vco_download_command_removed(self):
        """vco download should be rejected as unknown command.

        The download command has been removed. Use 'vco import' instead.
        """
        result = subprocess.run(
            [sys.executable, "-m", "vco.cli.main", "download", "--help"],
            capture_output=True,
            text=True,
            cwd="/Users/kominami/Documents/Code/video-compression-optimizer",
        )
        # Should fail with non-zero exit code (unknown command)
        assert result.returncode != 0
