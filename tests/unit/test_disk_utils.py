"""Unit tests for disk utilities.

Tests for utils/disk.py.
Target coverage: 50%+ (ユーティリティ)
"""

from unittest.mock import MagicMock, patch

import pytest

import vco.utils.disk as disk_module
from vco.utils.disk import (
    InsufficientDiskSpaceError,
    check_batch_disk_space,
    check_disk_space,
    format_bytes,
    get_available_disk_space,
)


class TestInsufficientDiskSpaceError:
    """Tests for InsufficientDiskSpaceError exception."""

    def test_error_message(self, tmp_path):
        """Test error message formatting."""
        error = InsufficientDiskSpaceError(
            required_bytes=10 * 1024**3,  # 10 GB
            available_bytes=5 * 1024**3,  # 5 GB
            path=tmp_path,
        )

        assert "10.00 GB" in str(error)
        assert "5.00 GB" in str(error)
        assert str(tmp_path) in str(error)

    def test_error_attributes(self, tmp_path):
        """Test error attributes are set correctly."""
        error = InsufficientDiskSpaceError(
            required_bytes=1000000, available_bytes=500000, path=tmp_path
        )

        assert error.required_bytes == 1000000
        assert error.available_bytes == 500000
        assert error.path == tmp_path

    def test_error_inheritance(self, tmp_path):
        """Test InsufficientDiskSpaceError inherits from Exception."""
        error = InsufficientDiskSpaceError(required_bytes=1000, available_bytes=500, path=tmp_path)
        assert isinstance(error, Exception)


class TestGetAvailableDiskSpace:
    """Tests for get_available_disk_space function."""

    def test_existing_path(self, tmp_path):
        """Test getting disk space for existing path."""
        space = get_available_disk_space(tmp_path)

        assert isinstance(space, int)
        assert space > 0

    def test_nonexistent_path_uses_parent(self, tmp_path):
        """Test nonexistent path uses parent directory."""
        nonexistent = tmp_path / "nonexistent" / "deep" / "path"

        space = get_available_disk_space(nonexistent)

        assert isinstance(space, int)
        assert space > 0

    @patch("shutil.disk_usage")
    def test_returns_free_space(self, mock_disk_usage, tmp_path):
        """Test returns free space from disk_usage."""
        mock_disk_usage.return_value = MagicMock(
            total=100 * 1024**3, used=60 * 1024**3, free=40 * 1024**3
        )

        space = get_available_disk_space(tmp_path)

        assert space == 40 * 1024**3


class TestCheckDiskSpace:
    """Tests for check_disk_space function."""

    def test_sufficient_space(self, tmp_path):
        """Test returns True when sufficient space available."""
        with patch.object(disk_module, "get_available_disk_space", return_value=10 * 1024**3):
            result = check_disk_space(
                source_file_size=1 * 1024**3,  # 1 GB
                target_path=tmp_path,
            )

            assert result is True

    def test_insufficient_space(self, tmp_path):
        """Test raises error when insufficient space."""
        with patch.object(disk_module, "get_available_disk_space", return_value=1 * 1024**3):
            with pytest.raises(InsufficientDiskSpaceError) as exc_info:
                check_disk_space(
                    source_file_size=2 * 1024**3,  # 2 GB (needs 4 GB with multiplier)
                    target_path=tmp_path,
                )

            assert exc_info.value.required_bytes == 4 * 1024**3
            assert exc_info.value.available_bytes == 1 * 1024**3

    def test_custom_multiplier(self, tmp_path):
        """Test custom multiplier is applied."""
        with patch.object(disk_module, "get_available_disk_space", return_value=3 * 1024**3):
            # With multiplier 1.5, 2 GB source needs 3 GB
            result = check_disk_space(
                source_file_size=2 * 1024**3, target_path=tmp_path, multiplier=1.5
            )

            assert result is True

    def test_exact_space_available(self, tmp_path):
        """Test exact space available passes."""
        with patch.object(disk_module, "get_available_disk_space", return_value=2 * 1024**3):
            result = check_disk_space(
                source_file_size=1 * 1024**3,  # 1 GB (needs 2 GB)
                target_path=tmp_path,
            )

            assert result is True


class TestCheckBatchDiskSpace:
    """Tests for check_batch_disk_space function."""

    def test_batch_sufficient_space(self, tmp_path):
        """Test batch check with sufficient space."""
        with patch.object(disk_module, "get_available_disk_space", return_value=20 * 1024**3):
            result = check_batch_disk_space(
                total_source_size=5 * 1024**3,  # 5 GB total
                target_path=tmp_path,
            )

            assert result is True

    def test_batch_insufficient_space(self, tmp_path):
        """Test batch check with insufficient space."""
        with patch.object(disk_module, "get_available_disk_space", return_value=5 * 1024**3):
            with pytest.raises(InsufficientDiskSpaceError):
                check_batch_disk_space(
                    total_source_size=10 * 1024**3,  # 10 GB (needs 20 GB)
                    target_path=tmp_path,
                )


class TestFormatBytes:
    """Tests for format_bytes function."""

    def test_bytes(self):
        """Test formatting bytes."""
        assert format_bytes(500) == "500 B"
        assert format_bytes(0) == "0 B"
        assert format_bytes(1023) == "1023 B"

    def test_kilobytes(self):
        """Test formatting kilobytes."""
        assert format_bytes(1024) == "1.0 KB"
        assert format_bytes(1536) == "1.5 KB"
        assert format_bytes(10 * 1024) == "10.0 KB"

    def test_megabytes(self):
        """Test formatting megabytes."""
        assert format_bytes(1024**2) == "1.0 MB"
        assert format_bytes(1.5 * 1024**2) == "1.5 MB"
        assert format_bytes(500 * 1024**2) == "500.0 MB"

    def test_gigabytes(self):
        """Test formatting gigabytes."""
        assert format_bytes(1024**3) == "1.00 GB"
        assert format_bytes(1.5 * 1024**3) == "1.50 GB"
        assert format_bytes(10 * 1024**3) == "10.00 GB"

    def test_large_values(self):
        """Test formatting large values."""
        assert format_bytes(100 * 1024**3) == "100.00 GB"
        assert format_bytes(1024**4) == "1024.00 GB"  # 1 TB shown as GB
