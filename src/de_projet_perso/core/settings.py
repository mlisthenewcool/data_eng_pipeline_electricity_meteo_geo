"""Application settings with Pydantic validation.

This module provides centralized configuration for the data engineering pipeline.
Settings can be customized via environment variables with the ENV_ prefix.

Environment Variables:
    ENV_DOWNLOAD_CHUNK_SIZE: Download chunk size in bytes (default: 1048576)
    ENV_DOWNLOAD_TIMEOUT_TOTAL: Total download timeout in seconds (default: 600)
    ENV_DOWNLOAD_TIMEOUT_CONNECT: Connection timeout in seconds (default: 10)
    ENV_DOWNLOAD_TIMEOUT_SOCK_READ: Socket read timeout in seconds (default: 30)
    ENV_RETRY_MAX_ATTEMPTS: Maximum retry attempts (default: 3)
    ENV_RETRY_INITIAL_DELAY: Initial retry delay in seconds (default: 1.0)
    ENV_RETRY_BACKOFF_FACTOR: Retry backoff multiplier (default: 2.0)
    ENV_HASH_ALGORITHM: Hash algorithm (default: sha256)
    ENV_HASH_CHUNK_SIZE: Hash chunk size in bytes (default: 131072)

Example:
    # Use default settings
    from de_projet_perso.core.settings import settings
    print(settings.data_dir)

    # Override via environment
    export ENV_DOWNLOAD_CHUNK_SIZE=2097152
    export ENV_RETRY_MAX_ATTEMPTS=5
"""

from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support.

    All settings can be overridden via environment variables with prefix ENV_
    Example: ENV_DOWNLOAD_CHUNK_SIZE=2097152

    Settings are immutable (frozen=True) to prevent accidental modifications.
    """

    # =========================================================================
    # Paths (computed from root_dir, not configurable via env)
    # =========================================================================
    root_dir: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent.parent,
        description="Project root directory (computed, not configurable)",
        exclude=True,  # Don't expose in env vars
    )

    @property
    def data_dir_path(self) -> Path:
        """Data directory (computed from root_dir)."""
        return self.root_dir / "data"

    @property
    def data_catalog_file_path(self) -> Path:
        """Path to data catalog YAML file."""
        return self.data_dir_path / "catalog.yaml"

    @property
    def data_state_dir_path(self) -> Path:
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
    # Retry Settings (for future use)
    # =========================================================================
    retry_max_attempts: int = Field(
        default=3,
        description="Maximum number of retry attempts",
        ge=1,
        le=10,
    )

    retry_initial_delay: float = Field(
        default=1.0,
        description="Initial delay between retries (seconds)",
        gt=0,
        le=60.0,
    )

    retry_backoff_factor: float = Field(
        default=2.0,
        description="Multiplier for delay after each retry",
        ge=1.0,
        le=10.0,
    )

    # =========================================================================
    # Hash Settings
    # =========================================================================
    hash_algorithm: Literal["sha256", "sha512", "sha1", "md5"] = Field(
        default="sha256",
        description="Hashing algorithm to use (recommended: sha256)",
    )

    hash_chunk_size: int = Field(
        default=131072,  # 128 KB
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
        case_sensitive=False,
        extra="ignore",
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
            raise ValueError(
                f"download_timeout_total ({self.download_timeout_total}) must be greater than "
                f"download_timeout_connect ({self.download_timeout_connect})"
            )

        if self.download_timeout_total <= self.download_timeout_sock_read:
            raise ValueError(
                f"download_timeout_total ({self.download_timeout_total}) must be greater than "
                f"download_timeout_sock_read ({self.download_timeout_sock_read})"
            )

        return self


# =============================================================================
# Singleton instance
# =============================================================================
settings = Settings()

__all__ = [
    # "Settings",
    "settings",
]
