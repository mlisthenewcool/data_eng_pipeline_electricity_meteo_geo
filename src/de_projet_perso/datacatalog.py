"""Data catalog configuration for managing dataset sources and ingestion policies.

This module provides a declarative, YAML-based catalog system for defining:
- Where to fetch data (source URLs, formats, providers)
- When to fetch data (ingestion frequency, versioning)
- Where to store data (storage paths with templating)

The catalog is validated using Pydantic models, ensuring type safety and catching
configuration errors at load time rather than runtime.

Architecture:
    DataCatalog
        └── datasets: dict[str, Dataset]
            ├── description: str
            ├── source: Source (provider, URL, format, inner_file)
            ├── ingestion: Ingestion (version, frequency, mode)
            └── storage: str (path template with {layer} and {version})

Example:
    Load catalog and retrieve dataset configuration:

    >>> from pathlib import Path
    >>> catalog = DataCatalog.load(Path("data/data_catalog.yaml"))
    >>> dataset = catalog.get_dataset("ign_contours_iris")
    >>> bronze_path = dataset.get_storage_path(layer="bronze")
    >>> print(bronze_path)
    data/bronze/ign_contours_iris/2025_01_15/file.parquet

Validation Rules:
    - Archive formats (7z) must specify inner_file
    - Non-archive formats must NOT specify inner_file
    - Version must follow YYYY_MM_DD format with valid dates
    - Storage paths must contain {layer} and {version} placeholders
"""

import re
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

# Compiled regex for version validation (performance optimization)
_VERSION_PATTERN = re.compile(r"\d{4}_\d{2}_\d{2}")

# Valid medallion architecture layers
VALID_LAYERS = frozenset({"landing", "bronze", "silver", "gold"})


class StrictModel(BaseModel):
    """Base Pydantic model that forbids extra fields.

    This model enforces strict validation by rejecting any fields not
    explicitly defined in the schema, preventing typos and configuration drift.
    """

    model_config = ConfigDict(extra="forbid")


class SourceFormat(StrEnum):
    """Supported source file formats for dataset ingestion.

    Attributes:
        SEVEN_Z: 7-Zip compressed archive format
        PARQUET: Apache Parquet columnar storage format
        JSON: JSON data interchange format
    """

    SEVEN_Z = "7z"
    PARQUET = "parquet"
    JSON = "json"

    @property
    def is_archive(self) -> bool:
        """Check if the format is a compressed archive requiring extraction.

        Returns:
            True if format is an archive (7z), False otherwise.

        Example:
            >>> SourceFormat.SEVEN_Z.is_archive
            True
            >>> SourceFormat.PARQUET.is_archive
            False
        """
        return self == SourceFormat.SEVEN_Z


class IngestionFrequency(StrEnum):
    """Expected data update frequency from the source provider.

    This enum defines how often the data source is expected to publish
    new data, which influences the Airflow scheduling strategy.

    Each enum member combines two pieces of information:
    - The frequency value (e.g., "hourly", "daily") used in YAML configs
    - The corresponding Airflow schedule expression (e.g., "@hourly", "@daily")

    This design ensures that adding a new frequency automatically includes
    its Airflow schedule, preventing inconsistencies between the two.

    Attributes:
        HOURLY: Data updated every hour (Airflow: @hourly)
        DAILY: Data updated daily (Airflow: @daily)
        WEEKLY: Data updated weekly (Airflow: @weekly)
        MONTHLY: Data updated monthly (Airflow: @monthly)
        YEARLY: Data updated yearly (Airflow: @yearly)
        UNKNOWN: Update frequency is unknown or irregular (Airflow: None)

    Implementation Note:
        This enum uses a custom __new__ method to store both the string value
        and the Airflow schedule as instance attributes. This pattern ensures
        strong coupling between frequencies and their schedules, making it
        impossible to forget to define a schedule when adding a new frequency.

    Example:
        >>> freq = IngestionFrequency.DAILY
        >>> print(freq)  # String value
        'daily'
        >>> print(freq.airflow_schedule)  # Airflow schedule
        '@daily'
        >>> print(freq.value)  # Also accessible via .value
        'daily'
    """

    # Each member is defined as (value, airflow_schedule)
    # The tuple is unpacked in __new__ to set both attributes
    HOURLY = ("hourly", "@hourly")
    DAILY = ("daily", "@daily")
    WEEKLY = ("weekly", "@weekly")
    MONTHLY = ("monthly", "@monthly")
    YEARLY = ("yearly", "@yearly")
    UNKNOWN = ("unknown", None)

    def __new__(cls, value: str, airflow_schedule: str | None):
        """Create enum member with both value and Airflow schedule.

        This custom __new__ method allows each enum member to store two
        pieces of information: the string value (used in configs) and the
        corresponding Airflow schedule expression.

        Args:
            value: Frequency string value (e.g., "hourly", "daily")
            airflow_schedule: Airflow cron preset (e.g., "@hourly") or None

        Returns:
            Enum member with both value and schedule attributes.

        Note:
            This pattern is the recommended way to add extra data to enum
            members in Python. The str.__new__ call ensures StrEnum behavior
            is preserved (enum members behave like strings).
        """
        obj = str.__new__(cls, value)
        obj._value_ = value
        # Store schedule in private attribute (dynamic attribute on enum)
        obj._airflow_schedule = airflow_schedule
        return obj

    @property
    def airflow_schedule(self) -> str | None:
        """Get the Airflow schedule expression for this frequency.

        Returns:
            Airflow cron preset string (e.g., "@daily") or None if frequency
            is unknown. This value is stored at enum member creation time.

        Example:
            >>> IngestionFrequency.DAILY.airflow_schedule
            '@daily'
            >>> IngestionFrequency.UNKNOWN.airflow_schedule
            None
        """
        # Use getattr to satisfy type checkers (attribute set dynamically in __new__)
        return getattr(self, "_airflow_schedule", None)


class Source(StrictModel):
    """Data source configuration defining where and how to fetch raw data.

    This model specifies the origin of the data, including the provider,
    download URL, file format, and optional inner file for archives.

    Attributes:
        provider: Data provider name (e.g., "IGN", "ODRE", "INSEE")
        url: HTTP(S) URL to download the source file
        format: Source file format (7z, parquet, JSON, etc.)
        inner_file: For archive formats only - the target file to extract
            from the archive (e.g., "data.gpkg" inside "archive.7z").
            Must be None for non-archive formats.

    Validation Rules:
        - Archive formats (7z) MUST specify inner_file
        - Non-archive formats (parquet, JSON) MUST NOT specify inner_file

    Example:
        Archive source requiring extraction:

        >>> data = {
        ...     "provider": "IGN",
        ...     "url": "https://data.ign.fr/contours.7z",
        ...     "format": SourceFormat.SEVEN_Z,
        ...     "inner_file": "iris.gpkg"
        ... }
        ... source = Source.model_validate(data)

        Direct file source (no extraction):

        >>> data = {
        ...     "provider": "ODRE",
        ...     "url": "https://data.odre.fr/installations.parquet",
        ...     "format": SourceFormat.PARQUET,
        ...     "inner_file": None
        ... }
        >>> source = Source.model_validate(data)

    """

    provider: str
    url: HttpUrl
    format: SourceFormat
    inner_file: str | None = None

    @model_validator(mode="after")
    def inner_file_is_required_for_archive_formats_only(self) -> Self:
        """Validate inner_file consistency with format type.

        Archive formats (7z) require inner_file to specify which file
        to extract from the archive. Non-archive formats must not have
        inner_file set since there is no extraction step.

        Returns:
            Self instance if validation passes.

        Raises:
            ValueError: If inner_file is missing for archive format or
                present for non-archive format.
        """
        if self.format.is_archive and self.inner_file is None:
            raise ValueError(f"inner_file is required for archive format: {self.format}")

        if not self.format.is_archive and self.inner_file is not None:
            raise ValueError(f"inner_file must not be set for non-archive format: {self.format}")

        return self

    @property
    def url_as_str(self) -> str:
        """Convert HttpUrl to string for compatibility with download functions.

        Pydantic's HttpUrl type provides validation but download functions
        expect plain strings. This property provides the conversion.

        Returns:
            String representation of the URL.

        Example:
            >>> source.url_as_str # noqa
            'https://data.ign.fr/file.7z'
        """
        return str(self.url)


class IngestionMode(StrEnum):
    """Data ingestion mode defining how updates are processed.

    Attributes:
        SNAPSHOT: Full dataset replacement on each ingestion
        INCREMENTAL: Only new/changed records are ingested (requires incremental_key)

    Note:
        Incremental mode is not yet fully implemented. The incremental_key
        field will be added in a future version.
    """

    SNAPSHOT = "snapshot"
    INCREMENTAL = "incremental"


class Ingestion(StrictModel):
    """Data ingestion configuration defining version, frequency, and mode.

    This model controls when and how data should be fetched from the source.
    The version follows a date-based format to enable reproducibility and
    historical data tracking.

    Attributes:
        version: Dataset version in YYYY_MM_DD format (e.g., "2025_01_15")
        frequency: Expected update frequency from the source
        mode: Ingestion mode (snapshot or incremental)

    Validation Rules:
        - version must follow YYYY_MM_DD format with leading zeros
        - version must be a valid calendar date

    Example:
        >>> ingestion = Ingestion(
        ...     version="2025_01_15",
        ...     frequency=IngestionFrequency.MONTHLY,
        ...     mode=IngestionMode.SNAPSHOT
        ... )

    Note:
        Future versions will add incremental_key field for incremental mode,
        with validation requiring it when mode=INCREMENTAL.
    """

    version: str
    frequency: IngestionFrequency
    mode: IngestionMode
    # incremental_key: str  # TODO: Add validation -> required if mode=INCREMENTAL

    @field_validator("version")
    @classmethod
    def validate_version_format(cls, v: str) -> str:
        """Validate version follows YYYY_MM_DD format and is a valid date.

        Args:
            v: Version string to validate.

        Returns:
            Validated version string.

        Raises:
            ValueError: If version doesn't match YYYY_MM_DD format or
                represents an invalid date.

        Example:
            >>> Ingestion.validate_version_format("2025_01_15")
            '2025_01_15'
            >>> Ingestion.validate_version_format("2025_13_01")
            ValueError: Version '2025_13_01' has correct format but invalid date: ...
        """
        if not _VERSION_PATTERN.fullmatch(v):
            raise ValueError(
                f"Version must follow YYYY_MM_DD format (with leading zeros), got: {v}"
            )

        try:
            datetime.strptime(v, "%Y_%m_%d")
        except ValueError as e:
            raise ValueError(f"Version '{v}' has correct format but invalid date: {e}") from e
        return v


class Dataset(StrictModel):
    """Complete dataset configuration combining source, ingestion, and storage.

    This model represents a full dataset definition from the catalog,
    including where to get the data, how often to fetch it, and where
    to store it in the data lakehouse layers.

    Attributes:
        description: Human-readable dataset description
        source: Source configuration (provider, URL, format)
        ingestion: Ingestion configuration (version, frequency, mode)
        storage: Storage path template with {layer} and {version} placeholders

    Validation Rules:
        - storage must contain {layer} placeholder for medallion architecture
        - storage must contain {version} placeholder for versioning

    Example:
        >>> dataset = Dataset(
        ...     description="IGN administrative boundaries",
        ...     source=Source(...),
        ...     ingestion=Ingestion(version="2025_01_15", ...),
        ...     storage="data/{layer}/contours_iris/{version}/file.parquet"
        ... )
        >>> dataset.get_storage_path(layer="bronze")
        Path('data/bronze/contours_iris/2025_01_15/file.parquet')
    """

    description: str
    source: Source
    ingestion: Ingestion
    storage: str

    @field_validator("storage")
    @classmethod
    def must_contain_version_placeholder(cls, v: str) -> str:
        """Validate storage path contains required placeholders.

        Ensures the storage path template includes both {layer} and {version}
        placeholders needed for the medallion architecture (bronze/silver/gold)
        and version management.

        Args:
            v: Storage path template to validate.

        Returns:
            Validated storage path template.

        Raises:
            ValueError: If storage path is missing {layer} or {version} placeholder.

        Example:
            >>> Dataset.must_contain_version_placeholder(
            ...     "data/{layer}/dataset/{version}/file.parquet"
            ... )
            'data/{layer}/dataset/{version}/file.parquet'
        """
        for placeholder in ["{layer}", "{version}"]:
            if placeholder not in v:
                raise ValueError(f"storage path must contain '{placeholder}' placeholder")
        return v

    def get_storage_path(self, layer: str) -> Path:
        """Generate storage path for a specific medallion layer.

        Substitutes the {layer} and {version} placeholders in the storage
        template with actual values.

        Args:
            layer: Medallion architecture layer (e.g., "landing", "bronze", "silver")

        Returns:
            Resolved storage path with placeholders substituted.

        Raises:
            ValueError: If layer is not one of the valid medallion layers.

        Example:
            >>> dataset.storage = "{layer}/ign/contours_iris_{version}.parquet" # noqa
            >>> dataset.ingestion.version = "2025_01_15" # noqa
            >>> dataset.get_storage_path("bronze") # noqa
            Path('data/bronze/ign/contours_iris_2025_01_15.parquet')

            >>> dataset.get_storage_path("invalid_layer") # noqa
            ValueError: Invalid layer 'invalid_layer'. Must be one of: bronze, gold, landing, silver
        """
        if layer not in VALID_LAYERS:
            raise ValueError(
                f"Invalid layer '{layer}'. Must be one of: {', '.join(sorted(VALID_LAYERS))}"
            )
        return Path(self.storage.format(layer=layer, version=self.ingestion.version))


def format_pydantic_errors(pydantic_errors: ValidationError) -> dict[str, str]:
    """Convert Pydantic validation errors to a structured dictionary.

    This utility function transforms Pydantic's error format into a simpler
    dict for logging and error messages.

    Args:
        pydantic_errors: Pydantic ValidationError exception.

    Returns:
        Dictionary mapping error location (dot-separated path) to error message.

    Example:
        >>> errors = format_pydantic_errors(validation_error) # noqa
        {'datasets.ign_contours.source.url': 'invalid URL format'}

    Note:
        This function should be moved to a common utilities module for reuse
        across the project.
    """
    return {
        ".".join(str(item) for item in err["loc"]): err["msg"] for err in pydantic_errors.errors()
    }


class DataCatalog(StrictModel):
    """Root catalog model containing all dataset configurations.

    This is the top-level model loaded from data_catalog.yaml. It provides
    access to all registered datasets with validation and type safety.

    Attributes:
        datasets: Dictionary mapping dataset names to Dataset configurations

    Example:
        Load and access catalog:

        >>> catalog = DataCatalog.load(Path("data/catalog.yaml"))
        >>> print(catalog.datasets.keys())
        dict_keys(['ign_contours_iris', 'odre_installations'])
        >>> dataset = catalog.get_dataset("ign_contours_iris")
    """

    datasets: dict[str, Dataset]

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load and validate the data catalog from YAML file.

        Reads the YAML catalog file, validates it against the schema using
        Pydantic, and returns a fully validated DataCatalog instance.

        Args:
            path: Path to the YAML catalog file.

        Returns:
            Validated DataCatalog instance with all datasets loaded.

        Raises:
            InvalidCatalogError: If the catalog file doesn't exist, contains
                invalid YAML, or fails Pydantic validation.

        Example:
            >>> catalog = DataCatalog.load(Path("data/catalog.yaml"))
            >>> len(catalog.datasets)
            5
        """
        if not path.exists():
            raise InvalidCatalogError(path=path, reason="file doesn't exist")

        try:
            with path.open() as f:
                data = yaml.safe_load(f)

            if data is None:
                raise InvalidCatalogError(
                    path=path, reason="catalog file is empty or contains only comments"
                )

            return cls.model_validate(data)
        except yaml.YAMLError as e:
            raise InvalidCatalogError(path=path, reason=f"error parsing YAML: {e}") from e
        except ValidationError as pydantic_errors:
            raise InvalidCatalogError(
                path=path,
                reason="Pydantic validation error",
                validation_errors=format_pydantic_errors(pydantic_errors),
            ) from pydantic_errors

    def get_dataset(self, name: str) -> Dataset:
        """Retrieve a dataset configuration by name.

        Args:
            name: Dataset identifier (e.g., "ign_contours_iris").

        Returns:
            Dataset configuration for the requested dataset.

        Raises:
            DatasetNotFoundError: If the dataset doesn't exist in the catalog.
                The error includes a list of available datasets.

        Example:
            >>> dataset = catalog.get_dataset("ign_contours_iris") # noqa
            >>> dataset.description
            'IGN administrative boundaries and IRIS zones'

            >>> catalog.get_dataset("unknown_dataset") # noqa
            DatasetNotFoundError: Dataset 'unknown_dataset' not found.
            Available: ['ign_contours_iris', 'odre_installations']
        """
        dataset = self.datasets.get(name)

        if not dataset:
            raise DatasetNotFoundError(name=name, available_datasets=list(self.datasets.keys()))

        return dataset
