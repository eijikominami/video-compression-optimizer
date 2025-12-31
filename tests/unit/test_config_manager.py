"""Unit tests for ConfigManager.

Tests configuration loading, saving, and validation.
Target coverage: 50%+ (設定管理)
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from vco.config.manager import (
    AWSConfig,
    Config,
    ConfigManager,
    ConversionConfig,
)


class TestAWSConfig:
    """Tests for AWSConfig dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        config = AWSConfig()
        assert config.region == "ap-northeast-1"
        assert config.s3_bucket == ""
        assert config.role_arn == ""
        assert config.profile == ""
        assert config.quality_checker_function == "vco-quality-checker-dev"

    def test_custom_values(self):
        """Test custom values are set correctly."""
        config = AWSConfig(
            region="us-west-2",
            s3_bucket="my-bucket",
            role_arn="arn:aws:iam::123456789012:role/test",
            profile="my-profile",
            quality_checker_function="my-function",
        )
        assert config.region == "us-west-2"
        assert config.s3_bucket == "my-bucket"
        assert config.role_arn == "arn:aws:iam::123456789012:role/test"
        assert config.profile == "my-profile"
        assert config.quality_checker_function == "my-function"


class TestConversionConfig:
    """Tests for ConversionConfig dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        config = ConversionConfig()
        assert config.quality_preset == "balanced"
        assert config.max_concurrent == 5
        assert "VideoCompressionOptimizer" in config.staging_folder

    def test_valid_quality_presets(self):
        """Test all valid quality presets."""
        for preset in ["high", "balanced", "compression"]:
            config = ConversionConfig(quality_preset=preset)
            assert config.quality_preset == preset

    def test_invalid_quality_preset_raises_error(self):
        """Test invalid quality preset raises ValueError."""
        with pytest.raises(ValueError, match="Invalid quality_preset"):
            ConversionConfig(quality_preset="invalid")

    def test_valid_max_concurrent_range(self):
        """Test valid max_concurrent values (1-10)."""
        for value in [1, 5, 10]:
            config = ConversionConfig(max_concurrent=value)
            assert config.max_concurrent == value

    def test_invalid_max_concurrent_too_low(self):
        """Test max_concurrent below 1 raises ValueError."""
        with pytest.raises(ValueError, match="Invalid max_concurrent"):
            ConversionConfig(max_concurrent=0)

    def test_invalid_max_concurrent_too_high(self):
        """Test max_concurrent above 10 raises ValueError."""
        with pytest.raises(ValueError, match="Invalid max_concurrent"):
            ConversionConfig(max_concurrent=11)

    def test_staging_folder_path_property(self):
        """Test staging_folder_path returns Path object."""
        config = ConversionConfig(staging_folder="/tmp/test")
        assert isinstance(config.staging_folder_path, Path)
        assert config.staging_folder_path == Path("/tmp/test")

    def test_staging_folder_path_expands_user(self):
        """Test staging_folder_path expands ~ to home directory."""
        config = ConversionConfig(staging_folder="~/test")
        assert config.staging_folder_path == Path.home() / "test"


class TestConfig:
    """Tests for Config dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        config = Config()
        assert isinstance(config.aws, AWSConfig)
        assert isinstance(config.conversion, ConversionConfig)
        assert config.schema_version == "1.0"

    def test_custom_nested_configs(self):
        """Test custom nested configs are set correctly."""
        aws = AWSConfig(region="eu-west-1")
        conversion = ConversionConfig(quality_preset="high")
        config = Config(aws=aws, conversion=conversion, schema_version="2.0")

        assert config.aws.region == "eu-west-1"
        assert config.conversion.quality_preset == "high"
        assert config.schema_version == "2.0"


class TestConfigManager:
    """Tests for ConfigManager class."""

    def test_init_with_default_path(self, tmp_path):
        """Test initialization with default path."""
        with patch.object(ConfigManager, "DEFAULT_CONFIG_PATH", tmp_path / "config.json"):
            manager = ConfigManager()
            assert manager.config_path == tmp_path / "config.json"

    def test_init_with_custom_path(self, tmp_path):
        """Test initialization with custom path."""
        custom_path = tmp_path / "custom_config.json"
        manager = ConfigManager(config_path=custom_path)
        assert manager.config_path == custom_path

    def test_load_default_config_when_file_not_exists(self, tmp_path):
        """Test default config is loaded when file doesn't exist."""
        config_path = tmp_path / "nonexistent.json"
        manager = ConfigManager(config_path=config_path)

        assert manager.config.aws.region == "ap-northeast-1"
        assert manager.config.conversion.quality_preset == "balanced"

    def test_load_config_from_file(self, tmp_path):
        """Test config is loaded from existing file."""
        config_path = tmp_path / "config.json"
        config_data = {
            "schema_version": "1.0",
            "aws": {
                "region": "us-east-1",
                "s3_bucket": "test-bucket",
                "role_arn": "arn:aws:iam::123456789012:role/test",
                "profile": "test-profile",
                "quality_checker_function": "test-function",
            },
            "conversion": {
                "quality_preset": "high",
                "max_concurrent": 3,
                "staging_folder": "/tmp/staging",
            },
        }
        config_path.write_text(json.dumps(config_data))

        manager = ConfigManager(config_path=config_path)

        assert manager.config.aws.region == "us-east-1"
        assert manager.config.aws.s3_bucket == "test-bucket"
        assert manager.config.conversion.quality_preset == "high"
        assert manager.config.conversion.max_concurrent == 3

    def test_load_corrupted_config_returns_default(self, tmp_path):
        """Test corrupted config file returns default config."""
        config_path = tmp_path / "config.json"
        config_path.write_text("invalid json {{{")

        manager = ConfigManager(config_path=config_path)

        # Should return default config
        assert manager.config.aws.region == "ap-northeast-1"

    def test_save_config(self, tmp_path):
        """Test config is saved to file."""
        config_path = tmp_path / "config.json"
        manager = ConfigManager(config_path=config_path)

        manager.config.aws.s3_bucket = "new-bucket"
        manager.save()

        # Verify file was created and contains correct data
        assert config_path.exists()
        saved_data = json.loads(config_path.read_text())
        assert saved_data["aws"]["s3_bucket"] == "new-bucket"

    def test_save_creates_parent_directories(self, tmp_path):
        """Test save creates parent directories if needed."""
        config_path = tmp_path / "nested" / "dir" / "config.json"
        manager = ConfigManager(config_path=config_path)

        manager.save()

        assert config_path.exists()

    def test_get_top_level_key(self, tmp_path):
        """Test get with top-level key."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        aws = manager.get("aws")
        assert isinstance(aws, AWSConfig)

    def test_get_nested_key(self, tmp_path):
        """Test get with nested key (dot notation)."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        region = manager.get("aws.region")
        assert region == "ap-northeast-1"

    def test_get_unknown_key_raises_error(self, tmp_path):
        """Test get with unknown key raises KeyError."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        with pytest.raises(KeyError, match="Unknown configuration key"):
            manager.get("unknown")

    def test_get_unknown_nested_key_raises_error(self, tmp_path):
        """Test get with unknown nested key raises KeyError."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        with pytest.raises(KeyError, match="Unknown configuration key"):
            manager.get("aws.unknown")

    def test_get_invalid_key_format_raises_error(self, tmp_path):
        """Test get with invalid key format raises KeyError."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        with pytest.raises(KeyError, match="Invalid configuration key format"):
            manager.get("a.b.c")

    def test_set_nested_key(self, tmp_path):
        """Test set with nested key."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        manager.set("aws.region", "eu-west-1")
        assert manager.config.aws.region == "eu-west-1"

    def test_set_invalid_key_format_raises_error(self, tmp_path):
        """Test set with invalid key format raises KeyError."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        with pytest.raises(KeyError, match="Invalid configuration key format"):
            manager.set("single_key", "value")

    def test_set_unknown_section_raises_error(self, tmp_path):
        """Test set with unknown section raises KeyError."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        with pytest.raises(KeyError, match="Unknown configuration section"):
            manager.set("unknown.key", "value")

    def test_set_unknown_key_raises_error(self, tmp_path):
        """Test set with unknown key raises KeyError."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        with pytest.raises(KeyError, match="Unknown configuration key"):
            manager.set("aws.unknown", "value")

    def test_set_invalid_quality_preset_raises_error(self, tmp_path):
        """Test set invalid quality_preset raises ValueError."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        with pytest.raises(ValueError, match="Invalid quality_preset"):
            manager.set("conversion.quality_preset", "invalid")

    def test_set_invalid_max_concurrent_raises_error(self, tmp_path):
        """Test set invalid max_concurrent raises ValueError."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        with pytest.raises(ValueError, match="Invalid max_concurrent"):
            manager.set("conversion.max_concurrent", 100)

    def test_set_max_concurrent_converts_to_int(self, tmp_path):
        """Test set max_concurrent converts string to int."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        manager.set("conversion.max_concurrent", "7")
        assert manager.config.conversion.max_concurrent == 7

    def test_get_all(self, tmp_path):
        """Test get_all returns complete config as dict."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        all_config = manager.get_all()

        assert "schema_version" in all_config
        assert "aws" in all_config
        assert "conversion" in all_config
        assert all_config["aws"]["region"] == "ap-northeast-1"

    def test_reset_to_defaults(self, tmp_path):
        """Test reset_to_defaults restores default config."""
        manager = ConfigManager(config_path=tmp_path / "config.json")

        # Modify config
        manager.set("aws.region", "us-west-2")
        manager.set("conversion.quality_preset", "high")

        # Reset
        manager.reset_to_defaults()

        assert manager.config.aws.region == "ap-northeast-1"
        assert manager.config.conversion.quality_preset == "balanced"

    def test_ensure_staging_folder_creates_directory(self, tmp_path):
        """Test ensure_staging_folder creates directory if not exists."""
        staging_path = tmp_path / "staging" / "nested"
        manager = ConfigManager(config_path=tmp_path / "config.json")
        manager.config.conversion.staging_folder = str(staging_path)

        result = manager.ensure_staging_folder()

        assert result == staging_path
        assert staging_path.exists()
        assert staging_path.is_dir()

    def test_ensure_staging_folder_returns_existing_directory(self, tmp_path):
        """Test ensure_staging_folder returns existing directory."""
        staging_path = tmp_path / "existing"
        staging_path.mkdir()

        manager = ConfigManager(config_path=tmp_path / "config.json")
        manager.config.conversion.staging_folder = str(staging_path)

        result = manager.ensure_staging_folder()

        assert result == staging_path


class TestConfigRoundtrip:
    """Tests for config serialization roundtrip."""

    def test_config_roundtrip(self, tmp_path):
        """Test config can be saved and loaded correctly."""
        config_path = tmp_path / "config.json"

        # Create and save config
        manager1 = ConfigManager(config_path=config_path)
        manager1.set("aws.region", "eu-central-1")
        manager1.set("aws.s3_bucket", "test-bucket")
        manager1.set("conversion.quality_preset", "compression")
        manager1.set("conversion.max_concurrent", 8)
        manager1.save()

        # Load config in new manager
        manager2 = ConfigManager(config_path=config_path)

        assert manager2.config.aws.region == "eu-central-1"
        assert manager2.config.aws.s3_bucket == "test-bucket"
        assert manager2.config.conversion.quality_preset == "compression"
        assert manager2.config.conversion.max_concurrent == 8
