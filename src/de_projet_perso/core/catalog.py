"""Data catalog configuration and state management.

This module provides a centralized configuration system for data sources and ingestion
policies, enabling reliable data pipeline management with validation and error handling.

Key features:
- Dataset source configuration with URL and format validation
- Ingestion scheduling with frequency controls
- Storage path templating with version substitution
- YAML-based catalog loading with comprehensive validation

Example:
    >>> catalog = DataCatalog.load(Path("catalog.yaml"))
    >>> dataset = catalog.get_dataset("ign_contours_iris")
    >>> path = dataset.get_storage_path()
"""

from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Self

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    HttpUrl,
    ValidationError,
    field_validator,
    model_validator,
)

from de_projet_perso.core.exceptions import DatasetNotFoundError, InvalidCatalogError


class StrictModel(BaseModel):
    """Base Pydantic model that rejects unknown fields."""

    model_config = ConfigDict(extra="forbid")


class SourceFormat(StrEnum):
    """Supported source file formats."""

    SEVEN_Z = "7z"
    PARQUET = "parquet"
    JSON = "json"

    @property
    def is_archive(self) -> bool:
        """Return True if the format is a compressed archive."""
        return self in [SourceFormat.SEVEN_Z]


class IngestionFrequency(StrEnum):
    """Expected data update frequency from the source."""

    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"
    UNKNOWN = "unknown"

    @property
    def airflow_schedule(self) -> str | None:
        """Map the frequency to a valid Airflow schedule expression."""
        mapping = {
            IngestionFrequency.HOURLY: "@hourly",
            IngestionFrequency.DAILY: "@daily",
            IngestionFrequency.WEEKLY: "@weekly",
            IngestionFrequency.MONTHLY: "@monthly",
            IngestionFrequency.YEARLY: "@yearly",
            IngestionFrequency.UNKNOWN: None,
        }
        return mapping.get(self)


class Source(StrictModel):
    """Data source configuration defining where and how to fetch data."""

    provider: str
    url: HttpUrl
    format: SourceFormat
    inner_file: str | None = None

    @model_validator(mode="after")
    def inner_file_is_required_for_archive_formats_only(self) -> Self:
        """Ensure inner_file is specified for archive formats like 7z."""
        if self.format.is_archive and self.inner_file is None:
            raise ValueError(f"inner_file is required for archive format: {self.format}")

        if not self.format.is_archive and self.inner_file is not None:
            raise ValueError(f"inner_file cannot be defined for non-archive format: {self.format}")

        return self


class Ingestion(StrictModel):
    """Data ingestion configuration defining which version and when to fetch data."""

    version: str
    frequency: IngestionFrequency

    @field_validator("version")
    @classmethod
    def validate_version_format(cls, v: str) -> str:
        """Ensure version follows YYYY_MM_DD format."""
        try:
            datetime.strptime(v, "%Y_%m_%d")
        except ValueError:
            raise ValueError(f"Version must follow YYYY_MM_DD format, got: {v}")
        return v


class Dataset(StrictModel):
    """Complete dataset configuration combining source, ingestion, storage information."""

    description: str
    source: Source
    ingestion: Ingestion
    storage: str

    @field_validator("storage")
    @classmethod
    def must_contain_version_placeholder(cls, v: str) -> str:
        """Ensure the storage path contains the {version} placeholder."""
        if "{version}" not in v:
            raise ValueError("storage path must contain '{version}' placeholder")
        return v

    def get_storage_path(self) -> Path:
        """Return storage path with version placeholder substituted."""
        return Path(self.storage.format(version=self.ingestion.version))


def format_pydantic_errors(pydantic_errors: ValidationError) -> dict[str, str]:
    """TODO: move to appropriate location to generalize."""
    return {
        ".".join(str(item) for item in err["loc"]): err["msg"] for err in pydantic_errors.errors()
    }


class DataCatalog(StrictModel):
    """Root catalog model containing all dataset configurations.

    Loaded from data_catalog.yaml and validated against this schema.
    """

    datasets: dict[str, Dataset]

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load and validate the data catalog from YAML.

        Args:
            path: Path to the YAML catalog file.

        Returns:
            Validated DataCatalog instance.

        Raises:
            InvalidCatalogError: If the catalog file doesn't exist or the YAML
                doesn't match the expected schema.
        """
        if not path.exists():
            raise InvalidCatalogError(path=path, reason="file doesn't exist")

        try:
            with path.open() as f:
                data = yaml.safe_load(f)
            return cls.model_validate(data)
        except yaml.YAMLError as e:
            raise InvalidCatalogError(path=path, reason=str(e)) from e
        except ValidationError as validation_errors:
            raise InvalidCatalogError(
                path=path,
                reason="Pydantic validation error",
                validation_errors=format_pydantic_errors(validation_errors),
            ) from None

    def get_dataset(self, name: str) -> Dataset:
        """Retrieve a dataset configuration by name.

        Args:
            name: Dataset identifier (e.g., "ign_contours_iris").

        Returns:
            The dataset configuration.

        Raises:
            KeyError: If the dataset doesn't exist in the catalog.
        """
        dataset = self.datasets.get(name)

        if not dataset:
            raise DatasetNotFoundError(name=name, available_datasets=list(self.datasets.keys()))

        return dataset
