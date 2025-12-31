"""Configuration manager for Video Compression Optimizer."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class AWSConfig:
    """AWS configuration settings.

    Attributes:
        region: AWS region for MediaConvert
        s3_bucket: S3 bucket for temporary file storage
        role_arn: IAM role ARN for MediaConvert
        profile: AWS profile name (optional)
        quality_checker_function: Lambda function name for quality checking
    """

    region: str = "ap-northeast-1"
    s3_bucket: str = ""
    role_arn: str = ""
    profile: str = ""
    quality_checker_function: str = "vco-quality-checker-dev"


@dataclass
class ConversionConfig:
    """Conversion configuration settings.

    Attributes:
        quality_preset: Quality preset (high, balanced, compression)
        max_concurrent: Maximum concurrent conversions (1-10)
        staging_folder: Folder for converted files awaiting review
        default_convert_mode: Default conversion mode (sync or async)
    """

    quality_preset: str = "balanced"
    max_concurrent: int = 5
    staging_folder: str = field(
        default_factory=lambda: str(
            Path.home() / "Movies" / "VideoCompressionOptimizer" / "converted"
        )
    )
    default_convert_mode: str = "sync"

    def __post_init__(self):
        """Validate configuration values."""
        valid_presets = ("high", "balanced", "balanced+", "compression")
        if self.quality_preset not in valid_presets:
            raise ValueError(
                f"Invalid quality_preset: {self.quality_preset}. "
                f"Must be one of: {', '.join(valid_presets)}"
            )
        if not 1 <= self.max_concurrent <= 10:
            raise ValueError(
                f"Invalid max_concurrent: {self.max_concurrent}. Must be between 1 and 10"
            )
        valid_modes = ("sync", "async")
        if self.default_convert_mode not in valid_modes:
            raise ValueError(
                f"Invalid default_convert_mode: {self.default_convert_mode}. "
                f"Must be one of: {', '.join(valid_modes)}"
            )

    @property
    def staging_folder_path(self) -> Path:
        """Get staging folder as Path object."""
        return Path(self.staging_folder).expanduser()


@dataclass
class Config:
    """Main configuration container.

    Attributes:
        aws: AWS configuration settings
        conversion: Conversion configuration settings
        schema_version: Configuration schema version
    """

    aws: AWSConfig = field(default_factory=AWSConfig)
    conversion: ConversionConfig = field(default_factory=ConversionConfig)
    schema_version: str = "1.0"


class ConfigManager:
    """Manages configuration loading, saving, and access.

    Configuration is stored in ~/.config/vco/config.json
    """

    DEFAULT_CONFIG_PATH = Path.home() / ".config" / "vco" / "config.json"

    def __init__(self, config_path: Path | None = None):
        """Initialize ConfigManager.

        Args:
            config_path: Custom path for config file (default: ~/.config/vco/config.json)
        """
        self.config_path = config_path or self.DEFAULT_CONFIG_PATH
        self.config = self._load_config()

    def _load_config(self) -> Config:
        """Load configuration from file or create default.

        Returns:
            Config object with loaded or default values
        """
        if self.config_path.exists():
            try:
                with open(self.config_path, encoding="utf-8") as f:
                    data = json.load(f)
                return self._dict_to_config(data)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                # If config is corrupted, return default
                print(f"Warning: Could not load config from {self.config_path}: {e}")
                return Config()
        return Config()

    def _dict_to_config(self, data: dict) -> Config:
        """Convert dictionary to Config object.

        Args:
            data: Dictionary with configuration data

        Returns:
            Config object
        """
        aws_data = data.get("aws", {})
        conversion_data = data.get("conversion", {})

        aws_config = AWSConfig(
            region=aws_data.get("region", "ap-northeast-1"),
            s3_bucket=aws_data.get("s3_bucket", ""),
            role_arn=aws_data.get("role_arn", ""),
            profile=aws_data.get("profile", ""),
            quality_checker_function=aws_data.get(
                "quality_checker_function", "vco-quality-checker-dev"
            ),
        )

        conversion_config = ConversionConfig(
            quality_preset=conversion_data.get("quality_preset", "balanced"),
            max_concurrent=conversion_data.get("max_concurrent", 5),
            staging_folder=conversion_data.get(
                "staging_folder",
                str(Path.home() / "Movies" / "VideoCompressionOptimizer" / "converted"),
            ),
            default_convert_mode=conversion_data.get("default_convert_mode", "sync"),
        )

        return Config(
            aws=aws_config,
            conversion=conversion_config,
            schema_version=data.get("schema_version", "1.0"),
        )

    def _config_to_dict(self, config: Config) -> dict:
        """Convert Config object to dictionary.

        Args:
            config: Config object

        Returns:
            Dictionary representation
        """
        return {
            "schema_version": config.schema_version,
            "aws": {
                "region": config.aws.region,
                "s3_bucket": config.aws.s3_bucket,
                "role_arn": config.aws.role_arn,
                "profile": config.aws.profile,
                "quality_checker_function": config.aws.quality_checker_function,
            },
            "conversion": {
                "quality_preset": config.conversion.quality_preset,
                "max_concurrent": config.conversion.max_concurrent,
                "staging_folder": config.conversion.staging_folder,
                "default_convert_mode": config.conversion.default_convert_mode,
            },
        }

    def save(self) -> None:
        """Save current configuration to file."""
        # Ensure directory exists
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

        data = self._config_to_dict(self.config)
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def get(self, key: str) -> Any:
        """Get configuration value by dot-notation key.

        Args:
            key: Configuration key (e.g., "aws.region", "conversion.quality_preset")

        Returns:
            Configuration value

        Raises:
            KeyError: If key is not found
        """
        parts = key.split(".")

        if len(parts) == 1:
            # Top-level key
            if hasattr(self.config, key):
                return getattr(self.config, key)
            raise KeyError(f"Unknown configuration key: {key}")

        if len(parts) == 2:
            section, name = parts
            if hasattr(self.config, section):
                section_obj = getattr(self.config, section)
                if hasattr(section_obj, name):
                    return getattr(section_obj, name)
            raise KeyError(f"Unknown configuration key: {key}")

        raise KeyError(f"Invalid configuration key format: {key}")

    def set(self, key: str, value: Any) -> None:
        """Set configuration value by dot-notation key.

        Args:
            key: Configuration key (e.g., "aws.region", "conversion.quality_preset")
            value: Value to set

        Raises:
            KeyError: If key is not found
            ValueError: If value is invalid
        """
        parts = key.split(".")

        if len(parts) != 2:
            raise KeyError(f"Invalid configuration key format: {key}")

        section, name = parts

        if not hasattr(self.config, section):
            raise KeyError(f"Unknown configuration section: {section}")

        section_obj = getattr(self.config, section)

        if not hasattr(section_obj, name):
            raise KeyError(f"Unknown configuration key: {key}")

        # Validate specific fields
        if section == "conversion":
            valid_presets = ("high", "balanced", "balanced+", "compression")
            if name == "quality_preset" and value not in valid_presets:
                raise ValueError(
                    f"Invalid quality_preset: {value}. Must be one of: {', '.join(valid_presets)}"
                )
            if name == "max_concurrent":
                value = int(value)
                if not 1 <= value <= 10:
                    raise ValueError(f"Invalid max_concurrent: {value}. Must be between 1 and 10")
            valid_modes = ("sync", "async")
            if name == "default_convert_mode" and value not in valid_modes:
                raise ValueError(
                    f"Invalid default_convert_mode: {value}. Must be one of: {', '.join(valid_modes)}"
                )

        setattr(section_obj, name, value)

    def get_all(self) -> dict:
        """Get all configuration as dictionary.

        Returns:
            Dictionary with all configuration values
        """
        return self._config_to_dict(self.config)

    def reset_to_defaults(self) -> None:
        """Reset configuration to default values."""
        self.config = Config()

    def ensure_staging_folder(self) -> Path:
        """Ensure staging folder exists and return its path.

        Returns:
            Path to staging folder
        """
        staging_path = self.config.conversion.staging_folder_path
        staging_path.mkdir(parents=True, exist_ok=True)
        return staging_path
