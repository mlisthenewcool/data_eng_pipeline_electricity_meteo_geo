"""Data catalog configuration for managing dataset sources and ingestion policies.

This module provides a declarative, YAML-based catalog system for defining:
- Where to fetch data (source URLs, formats, providers)
- When to fetch data (ingestion frequency, mode)
- How data is versioned (frequency-based: daily, hourly, etc.)

The catalog is validated using Pydantic models, ensuring type safety and catching
configuration errors at load time rather than runtime.

Architecture:
    DataCatalog
        └── datasets: dict[str, Dataset]
            ├── name: str (auto-injected from YAML key)
            ├── description: str
            ├── source: Source (provider, URL, format, inner_file)
            └── ingestion: Ingestion (frequency, mode)

Example:
    Load catalog and retrieve dataset configuration:

    >>> from pathlib import Path
    ... from de_projet_perso.core.data_catalog import DataCatalog # noqa
    ... from de_projet_perso.core.path_resolver import PathResolver # noqa
    ... from datetime import datetime

    >>> catalog = DataCatalog.load(Path("data/catalog.yaml"))
    ... dataset = catalog.get_dataset("ign_contours_iris")

    >>> # Generate version from frequency
    ... run_version = dataset.ingestion.frequency.format_datetime_as_version(
    ...     datetime.now(), no_dash=True
    ... )

    >>> # Use PathResolver for path construction
    ... resolver = PathResolver(dataset_name=dataset.name, run_version=run_version)
    ... bronze_path = resolver.bronze_path()
    ... print(bronze_path)
    'data/bronze/ign_contours_iris/20260121.parquet'

Validation Rules:
    - Archive formats (7z) must specify inner_file
    - Non-archive formats must NOT specify inner_file
    - Dataset names are auto-injected from YAML keys (no need to specify in values)

Note:
    Path resolution is handled by PathResolver, not Dataset directly.
    See path_resolver.py for medallion architecture path management.
"""

from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Self

import yaml
from pydantic import (
    HttpUrl,
    ValidationError,
    model_validator,
)

from de_projet_perso.core.exceptions import (
    AirflowContextError,
    DatasetNotFoundError,
    InvalidCatalogError,
)
from de_projet_perso.core.models import StrictModel
from de_projet_perso.core.settings import settings
from de_projet_perso.utils.pydantic import format_pydantic_errors


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
        NEVER: Never automatically update (Airflow: None)

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
    NEVER = ("never", None)

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
            is 'never'. This value is stored at enum member creation time.

        Example:
            >>> IngestionFrequency.DAILY.airflow_schedule
            '@daily'
            >>> IngestionFrequency.NEVER.airflow_schedule
            None
        """
        # Use getattr to satisfy type checkers (attribute set dynamically in __new__)
        return getattr(self, "_airflow_schedule", None)

    def get_airflow_version_template(self, no_dash: bool = False) -> str:
        """Get Airflow template variable based on ingestion frequency.

        Returns {{ ts_nodash }} for hourly datasets, {{ ds_nodash }} otherwise.
        This is used to pass the correct template to tasks.

        Args:
            no_dash: If True, return nodash version ({{ ds_nodash }} or {{ ts_nodash }}).
                     If False, return version with separators ({{ ds }} or {{ ts }}).
                     Default: False.

        Returns:
            Airflow template string that will be replaced at runtime.
            - Hourly: "{{ ts_nodash }}" (20260121T143022) or "{{ ts }}" (2026-01-21T14:30:22)
            - Others: "{{ ds_nodash }}" (20260121) or "{{ ds }}" (2026-01-21)

        Raises:
            Exception: If not running on Airflow (check settings.is_running_on_airflow)

        Example:
            >>> freq = IngestionFrequency.DAILY
            >>> freq.get_airflow_version_template(no_dash=True)
            '{{ ds_nodash }}'  # For daily/weekly/monthly datasets
            >>> freq = IngestionFrequency.HOURLY
            >>> freq.get_airflow_version_template(no_dash=True)
            '{{ ts_nodash }}'  # For hourly datasets
        """
        if not settings.is_running_on_airflow:
            raise AirflowContextError(
                operation="`get_airflow_version_template`",
                expected_context="airflow",
                actual_context="not airflow",
                suggestion="use `format_datetime_as_version` instead",
            )

        if self == IngestionFrequency.HOURLY:
            return "{{ ts_nodash }}" if no_dash else "{{ ts }}"

        return "{{ ds_nodash }}" if no_dash else "{{ ds }}"

    def format_datetime_as_version(self, dt: datetime, no_dash: bool = False) -> str:
        """Format datetime as version string matching Airflow template format.

        This method generates a version string from a datetime object that matches
        the format Airflow would produce from its template variables.
        Use this in non-Airflow contexts (scripts, notebooks, tests).

        Args:
            dt: Datetime to format as version string
            no_dash: If True, return compact format without separators.
                     If False, return format with dashes/colons.
                     Default: False.

        Returns:
            Version string formatted according to frequency:
            - Hourly (no_dash=True): "20260121T143022" (YYYYMMDDTHHmmss)
            - Hourly (no_dash=False): "2026-01-21-T-14-30-22"
            - Daily/Weekly/Monthly/Yearly/Never (no_dash=True): "20260121" (YYYYMMDD)
            - Daily/Weekly/Monthly/Yearly/Never (no_dash=False): "2026-01-21" (YYYY-MM-DD)

        Raises:
            Exception: If running on Airflow (should use get_airflow_version_template instead)

        Example:
            >>> from datetime import datetime
            >>> freq = IngestionFrequency.DAILY
            >>> freq.format_datetime_as_version(datetime(2026, 1, 21), no_dash=True)
            '20260121'
            >>> freq = IngestionFrequency.HOURLY
            >>> freq.format_datetime_as_version(datetime(2026, 1, 21, 14, 30, 22), no_dash=True)
            '20260121T143022'
        """
        if settings.is_running_on_airflow:
            raise AirflowContextError(
                operation="format_datetime_as_version()",
                expected_context="not airflow",
                actual_context="airflow",
                suggestion="use get_airflow_version_template() instead",
            )

        if self == IngestionFrequency.HOURLY:
            return dt.strftime("%Y%m%dT%H%M%S" if no_dash else "%Y-%m-%d-T-%H-%M-%S")

        # Daily/Weekly/Monthly/Yearly/Never
        return dt.strftime("%Y%m%d" if no_dash else "%Y-%m-%d")


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
    """Data ingestion configuration defining frequency and mode.

    This model controls when and how data should be fetched from the source.
    Versioning is handled dynamically based on frequency (daily, hourly, etc.)
    via IngestionFrequency.format_datetime_as_version() or Airflow templates.

    Attributes:
        frequency: Expected update frequency from the source (e.g., daily, hourly).
                   Determines version format: daily → "YYYYMMDD", hourly → "YYYYMMDDTHHMMSS"
        mode: Ingestion mode (snapshot or incremental)

    Example:
        >>> ingestion = Ingestion(
        ...     frequency=IngestionFrequency.MONTHLY,
        ...     mode=IngestionMode.SNAPSHOT
        ... )
        >>>
        >>> # Version generation (outside Airflow)
        >>> from datetime import datetime
        >>> version = ingestion.frequency.format_datetime_as_version(
        ...     datetime.now(), no_dash=True
        ... )
        >>> print(version)  # "20260121" for monthly/daily

    Note:
        Future versions will add incremental_key field for incremental mode,
        with validation requiring it when mode=INCREMENTAL.
    """

    frequency: IngestionFrequency
    mode: IngestionMode
    # incremental_key: str  # TODO: Add validation -> required if mode=INCREMENTAL


class Dataset(StrictModel):
    """Complete dataset configuration combining source and ingestion policies.

    This model represents a full dataset definition from the catalog,
    including where to get the data and how often to fetch it.
    Path resolution is delegated to PathResolver for better separation of concerns.

    Attributes:
        name: Dataset identifier (e.g., "ign_contours_iris")
        description: Human-readable dataset description
        source: Source configuration (provider, URL, format)
        ingestion: Ingestion configuration (frequency, mode)

    Example:
        >>> dataset = Dataset(
        ...     name="ign_contours_iris",
        ...     description="IGN administrative boundaries",
        ...     source=Source(...),
        ...     ingestion=Ingestion(...),
        ... )
    """

    name: str
    description: str
    source: Source
    ingestion: Ingestion


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

    @model_validator(mode="before")
    @classmethod
    def inject_names_into_datasets(cls, data: dict) -> dict:
        """Inject the YAML dictionary key into the 'name' field of each dataset."""
        if isinstance(data, dict) and "datasets" in data:
            for name, config in data["datasets"].items():
                if isinstance(config, dict):
                    config["name"] = name
        return data

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
        except yaml.YAMLError as yaml_error:
            raise InvalidCatalogError(
                path=path, reason=f"error parsing YAML: {yaml_error}"
            ) from yaml_error
        except ValidationError as pydantic_errors:
            raise InvalidCatalogError(
                path=path,
                reason="Pydantic validation errors",
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


if __name__ == "__main__":
    import sys

    from de_projet_perso.core.logger import logger

    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        logger.exception(message=str(e), extra=e.validation_errors)
        sys.exit(1)

    logger.info(message=f"Catalog loaded: found {len(_catalog.datasets)} dataset(s)")

    for _name, _dataset in _catalog.datasets.items():
        logger.info(
            message=f"dataset: {_name}",
            extra=_dataset.model_dump(),
            # extra={
            #     "name": _dataset.name,
            #     "description": _dataset.description,
            #     "provider": _dataset.source.provider,
            #     "url": _dataset.source.url_as_str,
            #     "format": _dataset.source.format.value,
            #     "frequency": _dataset.ingestion.frequency.value,
            #     "mode": _dataset.ingestion.mode.value,
            #     "airflow_schedule": _dataset.ingestion.frequency.airflow_schedule,
            # },
        )
