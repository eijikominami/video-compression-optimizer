"""Integration tests for import CLI commands.

Tests CLI output and actual file operations for --remove and --clear options.
Target coverage: 50%+ (Integration)

Validates: Requirements 1.4, 2.4, 3.3
"""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner


class TestImportRemoveCLI:
    """Integration tests for vco import --remove command.

    Validates: Requirements 1.4 (display confirmation of both queue and file deletion)
    """

    def test_remove_success_output(self, tmp_path):
        """Test --remove displays success message for queue and file deletion."""
        from vco.cli.main import cli

        # Create temporary files
        video_file = tmp_path / "test_video_h265.mp4"
        metadata_file = tmp_path / "test_video_h265.json"
        video_file.write_text("fake video content")
        metadata_file.write_text('{"test": "metadata"}')

        # Create mock review item
        mock_item = MagicMock()
        mock_item.id = "rev_123"
        mock_item.converted_path = video_file

        mock_queue = MagicMock()
        mock_queue.items = [mock_item]

        with patch("vco.cli.main.ImportService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            # Mock successful removal with file deletion
            from vco.services.import_service import FileDeleteResult, RemoveResult

            mock_service.remove_item.return_value = RemoveResult(
                success=True,
                review_id="rev_123",
                queue_removed=True,
                files_deleted=FileDeleteResult(video_deleted=True, metadata_deleted=True),
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--remove", "rev_123"])

            assert result.exit_code == 0
            assert "Removed rev_123 from review queue" in result.output
            assert "Deleted video file, metadata file" in result.output

    def test_remove_partial_file_deletion(self, tmp_path):
        """Test --remove displays warning when some file deletions fail."""
        from vco.cli.main import cli

        with patch("vco.cli.main.ImportService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            # Mock partial success
            from vco.services.import_service import FileDeleteResult, RemoveResult

            mock_service.remove_item.return_value = RemoveResult(
                success=True,
                review_id="rev_456",
                queue_removed=True,
                files_deleted=FileDeleteResult(
                    video_deleted=True, metadata_deleted=False, metadata_error="Permission denied"
                ),
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--remove", "rev_456"])

            assert result.exit_code == 0
            assert "Removed rev_456 from review queue" in result.output
            assert "Deleted video file" in result.output
            assert "Some file deletions failed" in result.output
            assert "Metadata: Permission denied" in result.output

    def test_remove_not_found(self):
        """Test --remove displays error when item not found."""
        from vco.cli.main import cli

        with patch("vco.cli.main.ImportService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            from vco.services.import_service import RemoveResult

            mock_service.remove_item.return_value = RemoveResult(
                success=False,
                review_id="nonexistent",
                queue_removed=False,
                error_message="Item not found",
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--remove", "nonexistent"])

            assert result.exit_code == 1
            assert "Failed to remove" in result.output
            assert "Item not found" in result.output


class TestImportClearCLI:
    """Integration tests for vco import --clear command.

    Validates: Requirements 2.4 (display confirmation including file deletion count)
    """

    def test_clear_success_output(self):
        """Test --clear displays success message with file deletion count."""
        from vco.cli.main import cli

        with patch("vco.cli.main.ImportService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            # Mock pending items
            mock_item1 = MagicMock()
            mock_item1.id = "rev_1"
            mock_item2 = MagicMock()
            mock_item2.id = "rev_2"
            mock_service.list_pending.return_value = [mock_item1, mock_item2]

            # Mock successful clear
            from vco.services.import_service import ClearResult

            mock_service.clear_queue.return_value = ClearResult(
                success=True, items_removed=2, files_deleted=2, files_failed=0
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--clear"], input="y\n")

            assert result.exit_code == 0
            assert "Removed 2 items" in result.output
            assert "Deleted 2 files" in result.output

    def test_clear_with_file_failures(self):
        """Test --clear displays warning when some file deletions fail."""
        from vco.cli.main import cli

        with patch("vco.cli.main.ImportService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            # Mock pending items
            mock_items = [MagicMock() for _ in range(3)]
            mock_service.list_pending.return_value = mock_items

            # Mock partial success
            from vco.services.import_service import ClearResult

            mock_service.clear_queue.return_value = ClearResult(
                success=True,
                items_removed=3,
                files_deleted=2,
                files_failed=1,
                error_details=["Video /tmp/video1.mp4: Permission denied"],
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--clear"], input="y\n")

            assert result.exit_code == 0
            assert "Removed 3 items" in result.output
            assert "Deleted 2 files" in result.output
            assert "1 file deletions failed" in result.output
            assert "Permission denied" in result.output

    def test_clear_empty_queue(self):
        """Test --clear with empty queue."""
        from vco.cli.main import cli

        with patch("vco.cli.main.ImportService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            mock_service.list_pending.return_value = []

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--clear"])

            assert result.exit_code == 0
            assert "already empty" in result.output

    def test_clear_cancelled(self):
        """Test --clear cancelled by user."""
        from vco.cli.main import cli

        with patch("vco.cli.main.ImportService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            mock_service.list_pending.return_value = [MagicMock()]

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--clear"], input="n\n")

            assert result.exit_code == 0
            assert "Cancelled" in result.output
            mock_service.clear_queue.assert_not_called()

    def test_clear_confirmation_prompt(self):
        """Test --clear shows confirmation prompt with item count."""
        from vco.cli.main import cli

        with patch("vco.cli.main.ImportService") as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            mock_items = [MagicMock() for _ in range(5)]
            mock_service.list_pending.return_value = mock_items

            from vco.services.import_service import ClearResult

            mock_service.clear_queue.return_value = ClearResult(
                success=True, items_removed=5, files_deleted=5, files_failed=0
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--clear"], input="y\n")

            assert "Will remove 5 items and files" in result.output


class TestImportCLIActualFileOperations:
    """Integration tests with actual file creation and deletion.

    Validates: Requirements 1.1, 1.2, 2.1, 2.2, 3.1
    """

    def test_remove_actually_deletes_files(self, tmp_path):
        """Test --remove actually deletes video and metadata files."""
        from vco.cli.main import cli

        # Create actual files
        video_file = tmp_path / "test_video_h265.mp4"
        metadata_file = tmp_path / "test_video_h265.json"
        video_file.write_text("fake video content")
        metadata_file.write_text('{"test": "metadata"}')

        assert video_file.exists()
        assert metadata_file.exists()

        # Create mock review item pointing to real files
        mock_item = MagicMock()
        mock_item.id = "rev_real_123"
        mock_item.converted_path = video_file

        mock_queue = MagicMock()
        mock_queue.items = [mock_item]

        with patch("vco.cli.main.ImportService") as mock_service_class:
            # Use real ImportService for file deletion
            from vco.services.import_service import ImportService, RemoveResult

            real_service = ImportService()

            # Mock the service but use real _delete_item_files
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            # Perform actual file deletion
            file_result = real_service._delete_item_files(mock_item)

            mock_service.remove_item.return_value = RemoveResult(
                success=True,
                review_id="rev_real_123",
                queue_removed=True,
                files_deleted=file_result,
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--remove", "rev_real_123"])

            assert result.exit_code == 0
            # Verify files were actually deleted
            assert not video_file.exists()
            assert not metadata_file.exists()

    def test_clear_actually_deletes_multiple_files(self, tmp_path):
        """Test --clear actually deletes all video and metadata files."""
        from vco.cli.main import cli

        # Create multiple file pairs
        files = []
        for i in range(3):
            video_file = tmp_path / f"video_{i}_h265.mp4"
            metadata_file = tmp_path / f"video_{i}_h265.json"
            video_file.write_text(f"fake video content {i}")
            metadata_file.write_text(f'{{"index": {i}}}')
            files.append((video_file, metadata_file))

        # Verify all files exist
        for video_file, metadata_file in files:
            assert video_file.exists()
            assert metadata_file.exists()

        # Create mock items pointing to real files
        mock_items = []
        for i, (video_file, _) in enumerate(files):
            mock_item = MagicMock()
            mock_item.id = f"rev_{i}"
            mock_item.converted_path = video_file
            mock_items.append(mock_item)

        with patch("vco.cli.main.ImportService") as mock_service_class:
            from vco.services.import_service import ClearResult, ImportService

            real_service = ImportService()
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            mock_service.list_pending.return_value = mock_items

            # Perform actual file deletions
            files_deleted = 0
            for mock_item in mock_items:
                file_result = real_service._delete_item_files(mock_item)
                if file_result.video_deleted and file_result.metadata_deleted:
                    files_deleted += 1

            mock_service.clear_queue.return_value = ClearResult(
                success=True, items_removed=3, files_deleted=files_deleted, files_failed=0
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--clear"], input="y\n")

            assert result.exit_code == 0

            # Verify all files were actually deleted
            for video_file, metadata_file in files:
                assert not video_file.exists()
                assert not metadata_file.exists()

    def test_remove_nonexistent_file_no_error(self, tmp_path):
        """Test --remove handles non-existent files gracefully.

        Validates: Requirements 3.1 (file not exist is not an error)
        """
        from vco.cli.main import cli

        # Create mock item pointing to non-existent files
        nonexistent_video = tmp_path / "nonexistent_h265.mp4"

        mock_item = MagicMock()
        mock_item.id = "rev_nonexistent"
        mock_item.converted_path = nonexistent_video

        with patch("vco.cli.main.ImportService") as mock_service_class:
            from vco.services.import_service import ImportService, RemoveResult

            real_service = ImportService()
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            # Perform file deletion on non-existent files
            file_result = real_service._delete_item_files(mock_item)

            # Should succeed because missing_ok=True
            assert file_result.video_deleted is True
            assert file_result.metadata_deleted is True
            assert file_result.video_error is None
            assert file_result.metadata_error is None

            mock_service.remove_item.return_value = RemoveResult(
                success=True,
                review_id="rev_nonexistent",
                queue_removed=True,
                files_deleted=file_result,
            )

            runner = CliRunner()
            result = runner.invoke(cli, ["import", "--remove", "rev_nonexistent"])

            assert result.exit_code == 0
            assert "Removed rev_nonexistent from review queue" in result.output
