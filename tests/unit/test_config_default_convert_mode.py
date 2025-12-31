"""Unit tests for default_convert_mode configuration.

Task 15.2: 設定機能テスト
- default_convert_mode の読み書き
- 設定ファイル破損時のデフォルト値
- 設定変更後の CLI 動作確認

Requirements: 8.4
"""

import json

from vco.config.manager import ConfigManager, ConversionConfig


class TestDefaultConvertModeReadWrite:
    """Tests for default_convert_mode read/write operations."""

    def test_default_value_is_sync(self, tmp_path):
        """Default value for default_convert_mode is 'sync'."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)

        result = manager.get("conversion.default_convert_mode")

        assert result == "sync"

    def test_set_to_async(self, tmp_path):
        """Can set default_convert_mode to 'async'."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)

        manager.set("conversion.default_convert_mode", "async")

        assert manager.get("conversion.default_convert_mode") == "async"

    def test_set_to_sync(self, tmp_path):
        """Can set default_convert_mode to 'sync'."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)
        manager.set("conversion.default_convert_mode", "async")

        manager.set("conversion.default_convert_mode", "sync")

        assert manager.get("conversion.default_convert_mode") == "sync"

    def test_invalid_value_raises_error(self, tmp_path):
        """Setting invalid value raises ValueError."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)

        try:
            manager.set("conversion.default_convert_mode", "invalid")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "default_convert_mode" in str(e)
            assert "sync" in str(e)
            assert "async" in str(e)

    def test_persists_after_save(self, tmp_path):
        """Setting persists after save and reload."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)
        manager.set("conversion.default_convert_mode", "async")
        manager.save()

        # Create new manager instance
        manager2 = ConfigManager(config_path=config_path)

        assert manager2.get("conversion.default_convert_mode") == "async"

    def test_get_all_includes_default_convert_mode(self, tmp_path):
        """get_all() includes default_convert_mode."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)
        manager.set("conversion.default_convert_mode", "async")

        all_config = manager.get_all()

        assert all_config["conversion"]["default_convert_mode"] == "async"


class TestCorruptedConfigFile:
    """Tests for handling corrupted config files."""

    def test_corrupted_json_uses_default(self, tmp_path):
        """Corrupted JSON file falls back to default values."""
        config_path = tmp_path / "config.json"
        config_path.write_text("{ invalid json }")

        manager = ConfigManager(config_path=config_path)

        assert manager.get("conversion.default_convert_mode") == "sync"

    def test_missing_default_convert_mode_uses_default(self, tmp_path):
        """Missing default_convert_mode field uses default value."""
        config_path = tmp_path / "config.json"
        config_data = {
            "schema_version": "1.0",
            "aws": {"region": "us-east-1"},
            "conversion": {
                "quality_preset": "balanced",
                "max_concurrent": 5,
                # default_convert_mode is missing
            },
        }
        config_path.write_text(json.dumps(config_data))

        manager = ConfigManager(config_path=config_path)

        assert manager.get("conversion.default_convert_mode") == "sync"

    def test_invalid_default_convert_mode_in_file_raises_error(self, tmp_path):
        """Invalid default_convert_mode in file raises error on load."""
        config_path = tmp_path / "config.json"
        config_data = {
            "schema_version": "1.0",
            "aws": {"region": "us-east-1"},
            "conversion": {
                "quality_preset": "balanced",
                "max_concurrent": 5,
                "default_convert_mode": "invalid_mode",
            },
        }
        config_path.write_text(json.dumps(config_data))

        try:
            ConfigManager(config_path=config_path)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "default_convert_mode" in str(e)

    def test_empty_file_uses_default(self, tmp_path):
        """Empty config file falls back to default values."""
        config_path = tmp_path / "config.json"
        config_path.write_text("")

        manager = ConfigManager(config_path=config_path)

        assert manager.get("conversion.default_convert_mode") == "sync"

    def test_can_save_after_corrupted_load(self, tmp_path):
        """Can save new config after loading corrupted file."""
        config_path = tmp_path / "config.json"
        config_path.write_text("{ invalid json }")

        manager = ConfigManager(config_path=config_path)
        manager.set("conversion.default_convert_mode", "async")
        manager.save()

        # Verify saved correctly
        manager2 = ConfigManager(config_path=config_path)
        assert manager2.get("conversion.default_convert_mode") == "async"


class TestConversionConfigValidation:
    """Tests for ConversionConfig validation."""

    def test_valid_sync_mode(self):
        """'sync' is a valid mode."""
        config = ConversionConfig(default_convert_mode="sync")
        assert config.default_convert_mode == "sync"

    def test_valid_async_mode(self):
        """'async' is a valid mode."""
        config = ConversionConfig(default_convert_mode="async")
        assert config.default_convert_mode == "async"

    def test_invalid_mode_raises_error(self):
        """Invalid mode raises ValueError."""
        try:
            ConversionConfig(default_convert_mode="invalid")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "default_convert_mode" in str(e)

    def test_empty_mode_raises_error(self):
        """Empty mode raises ValueError."""
        try:
            ConversionConfig(default_convert_mode="")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "default_convert_mode" in str(e)


class TestResetToDefaults:
    """Tests for reset_to_defaults method."""

    def test_reset_restores_default_convert_mode(self, tmp_path):
        """reset_to_defaults restores default_convert_mode to 'sync'."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)
        manager.set("conversion.default_convert_mode", "async")

        manager.reset_to_defaults()

        assert manager.get("conversion.default_convert_mode") == "sync"


class TestConfigIntegration:
    """Integration tests for config with other settings."""

    def test_default_convert_mode_independent_of_quality_preset(self, tmp_path):
        """default_convert_mode is independent of quality_preset."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)

        manager.set("conversion.quality_preset", "high")
        manager.set("conversion.default_convert_mode", "async")

        assert manager.get("conversion.quality_preset") == "high"
        assert manager.get("conversion.default_convert_mode") == "async"

    def test_default_convert_mode_independent_of_max_concurrent(self, tmp_path):
        """default_convert_mode is independent of max_concurrent."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)

        manager.set("conversion.max_concurrent", 10)
        manager.set("conversion.default_convert_mode", "async")

        assert manager.get("conversion.max_concurrent") == 10
        assert manager.get("conversion.default_convert_mode") == "async"

    def test_all_conversion_settings_persist_together(self, tmp_path):
        """All conversion settings persist together."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)

        manager.set("conversion.quality_preset", "compression")
        manager.set("conversion.max_concurrent", 3)
        manager.set("conversion.default_convert_mode", "async")
        manager.save()

        manager2 = ConfigManager(config_path=config_path)

        assert manager2.get("conversion.quality_preset") == "compression"
        assert manager2.get("conversion.max_concurrent") == 3
        assert manager2.get("conversion.default_convert_mode") == "async"
