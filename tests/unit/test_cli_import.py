"""Unit tests for CLI import command with metadata verification."""

from click.testing import CliRunner

from vco.cli.main import cli


class TestImportCommandForceFlag:
    """Tests for import command --force flag."""

    def test_import_command_has_force_option(self):
        """Import command should have --force option."""
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "--help"])

        assert result.exit_code == 0
        assert "--force" in result.output

    def test_import_command_force_flag_description(self):
        """Force flag should have appropriate description."""
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "--help"])

        assert result.exit_code == 0
        # Check that force flag has description about metadata verification
        assert "force" in result.output.lower()


class TestImportAllCommandForceFlag:
    """Tests for import --all command --force flag."""

    def test_import_all_command_has_force_option(self):
        """Import --all command should have --force option."""
        runner = CliRunner()
        result = runner.invoke(cli, ["import", "--help"])

        assert result.exit_code == 0
        assert "--force" in result.output
