"""Application settings with Pydantic validation.

This module provides centralized configuration for the data engineering pipeline.
Settings can be customized via environment variables with the ENV_ prefix, an '.env' file,
or default values.

Key Environment Variables:
    ENV_LOGGING_LEVEL: Logger verbosity (DEBUG, INFO, etc. Default: INFO)
    ENV_AIRFLOW_LOGGER_NAME: Logger name for Airflow UI (Default: MY_LOGGER)
    AIRFLOW_HOME: Standard Airflow path (No 'ENV_' prefix. Default: /opt/airflow)

    ENV_DOWNLOAD_CHUNK_SIZE: Streaming download chunk size (Default: 1024 * 1024)
    ENV_DOWNLOAD_TIMEOUT_TOTAL: Total download timeout (Default: 600)
    ENV_DOWNLOAD_TIMEOUT_CONNECT: Connection timeout in seconds (default: 10)
    ENV_DOWNLOAD_TIMEOUT_SOCK_READ: Socket read timeout in seconds (default: 30)

    ENV_HASH_ALGORITHM: Hashing algorithm (Default: sha256)
    ENV_HASH_CHUNK_SIZE: Chunk size for file hashing (Default: 1024 * 128)

Example:
    >>> from de_projet_perso.core.settings import settings
    ... print(settings.data_dir_path)

    # Override via environment
    export ENV_DOWNLOAD_CHUNK_SIZE=2097152
    export ENV_RETRY_MAX_ATTEMPTS=5
"""

from pathlib import Path
from typing import Literal, Self

from pydantic import DirectoryPath, Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support.

    All settings can be overridden via environment variables with prefix ENV_
    Example: ENV_DOWNLOAD_CHUNK_SIZE=2097152

    Settings are immutable (frozen=True) to prevent accidental modifications.
    """

    # =========================================================================
    # General config
    # =========================================================================
    # TODO: LOG_LEVEL should be defined via Docker for consistency.
    logging_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="The logger verbosity level"
    )

    # =========================================================================
    # Airflow config
    # =========================================================================
    airflow_logger_name: str = Field(
        default="MY_LOGGER", description="The logger name displayed in Airflow interface"
    )

    airflow_home: Path = Field(
        default=Path("/opt/airflow"),
        validation_alias="AIRFLOW_HOME",  # the variable doesn't have the ENV prefix
        description="The Airflow home directory. Uses standard AIRFLOW_HOME env var",
    )

    @computed_field
    @property
    def is_running_on_airflow(self) -> bool:
        """Determine if running inside an Airflow environment by checking for airflow.cfg."""
        return (self.airflow_home / "airflow.cfg").exists()

    # =========================================================================
    # Paths (computed from root_dir, not configurable via env)
    # =========================================================================
    root_dir: Path = Field(
        default=Path(__file__).resolve().parent.parent.parent.parent,
        description="Project root directory (computed, not configurable)",
        exclude=True,  # Don't expose in env vars
    )

    @computed_field
    @property
    def data_dir_path(self) -> DirectoryPath:
        """Data directory (computed from root_dir)."""
        return self.root_dir / "data"

    @computed_field
    @property
    def data_catalog_file_path(self) -> Path:
        """Path to data catalog YAML file."""
        return self.data_dir_path / "catalog.yaml"

    @computed_field
    @property
    def data_state_dir_path(self) -> DirectoryPath:
        """Path to pipeline state directory."""
        return self.data_dir_path / "_state"

    # =========================================================================
    # Download Settings
    # =========================================================================
    download_chunk_size: int = Field(
        default=1024 * 1024,  # 1 MB
        description="Chunk size for streaming downloads (bytes)",
        gt=0,
        le=10 * 1024 * 1024,  # Max 10 MB
    )

    download_timeout_total: int = Field(
        default=600,
        description="Maximum time for entire download (seconds)",
        gt=0,
        le=3600,  # Max 1 hour
    )

    download_timeout_connect: int = Field(
        default=10,
        description="Maximum time to establish connection (seconds)",
        gt=0,
        le=60,
    )

    download_timeout_sock_read: int = Field(
        default=30,
        description="Maximum time between data packets (seconds)",
        gt=0,
        le=300,
    )

    # =========================================================================
    # Retries settings TODO: setup for Airflow
    # =========================================================================
    # retry_max_attempts: int = Field(
    #     default=3,
    #     description="Maximum number of retry attempts",
    #     ge=1,
    #     le=10,
    # )
    #
    # retry_initial_delay: float = Field(
    #     default=1.0,
    #     description="Initial delay between retries (seconds)",
    #     gt=0,
    #     le=60.0,
    # )
    #
    # retry_backoff_factor: float = Field(
    #     default=2.0,
    #     description="Multiplier for delay after each retry",
    #     ge=1.0,
    #     le=10.0,
    # )

    # =========================================================================
    # Hash Settings
    # =========================================================================
    hash_algorithm: Literal["sha256", "sha512", "sha1", "md5"] = Field(
        default="sha256",
        description="Hashing algorithm for integrity checks (recommended: sha256)",
    )

    hash_chunk_size: int = Field(
        default=1024 * 128,  # 128 KB
        description="Chunk size for file hashing (bytes)",
        gt=0,
        le=1024 * 1024,  # Max 1 MB
    )

    # =========================================================================
    # Pydantic Config
    # =========================================================================
    model_config = SettingsConfigDict(
        env_prefix="ENV_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",  # TODO: more strict with 'forbid' fails because of PYTHONPATH
        frozen=True,  # Immutable settings
    )

    @model_validator(mode="after")
    def validate_timeout_hierarchy(self) -> Self:
        """Validate that total timeout is greater than component timeouts.

        Ensures logical consistency: total download timeout must exceed
        both connection establishment and socket read timeouts.

        Returns:
            Self (for method chaining)

        Raises:
            ValueError: If timeout hierarchy is invalid
        """
        if self.download_timeout_total <= self.download_timeout_connect:
            raise ValueError("download_timeout_total must be > download_timeout_connect")

        if self.download_timeout_total <= self.download_timeout_sock_read:
            raise ValueError("download_timeout_total must be > download_timeout_sock_read")

        return self


# Singleton instance
settings = Settings()

__all__ = ["settings"]
