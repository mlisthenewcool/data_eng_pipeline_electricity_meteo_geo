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

from functools import cache
from pathlib import Path
from typing import Literal, Self

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


class Source(StrictModel):
    """Data source configuration defining where and how to fetch data."""

    provider: str
    url: HttpUrl
    format: Literal["7z", "parquet", "json"]
    inner_file: str | None = None

    @model_validator(mode="after")
    def inner_file_is_required_for_archive_formats_only(self) -> Self:
        """Ensure inner_file is specified for archive formats like 7z."""
        _archive_formats: list[str] = ["7z"]

        if (self.format in _archive_formats) and (self.inner_file is None):
            raise ValueError(f"inner_file is required when format is one of {_archive_formats}")

        if (self.format not in _archive_formats) and (self.inner_file is not None):
            raise ValueError("inner_file shouldn't be defined when format is not an archive")
        return self


class Ingestion(StrictModel):
    """Data ingestion configuration defining which version and when to fetch data."""

    version: str
    frequency: Literal["hourly", "daily", "weekly", "monthly", "yearly", "unknown"]


class Dataset(StrictModel):
    """Complete dataset configuration combining source, ingestion, storage information."""

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


class DataCatalog(StrictModel):
    """Root catalog model containing all dataset configurations.

    Loaded from data_catalog.yaml and validated against this schema.
    """

    datasets: dict[str, Dataset]

    @classmethod
    @cache  # NOTE: is it a bad idea if we need to reload file for any reason ?
    def load(cls, path: Path) -> Self:
        """Load and validate the data catalog from YAML.

        Args:
            path: Path to the YAML catalog file.

        Returns:
            Validated DataCatalog instance.

        Raises:
            FileNotFoundError: If the catalog file doesn't exist.
            ValidationError: If the YAML doesn't match the expected schema.
        """
        if not path.exists():
            raise InvalidCatalogError(path=path, reason="file doesn't exist")

        try:
            with path.open() as f:
                data = yaml.safe_load(f)
            return cls.model_validate(data)
        except (yaml.YAMLError, ValidationError) as e:
            raise InvalidCatalogError(path=path, reason=str(e)) from e

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
