"""Centralized path resolution for medallion architecture.

This module provides pure path resolution logic, completely independent of Dataset.
It implements the medallion architecture (landing → bronze → silver → gold) with:
- Landing: Temporary storage with original filenames
- Bronze: Versioned history with 'latest.parquet' symlink
- Silver: Current + backup files for fast rollback
- Gold: Current + backup files for fast rollback

Key Design Decisions:
- No dependency on Dataset class (uses primitives only)
- Supports both Airflow ({{ ds }} and {{ ts }}) and non-Airflow contexts
- Bronze versions retained for 1 year (cleanup via maintenance DAG)
- Silver uses current.parquet + backup.parquet strategy
- Gold uses current.parquet + backup.parquet strategy

Example:
    >>> # Airflow context (uses Airflow template variables)
    ... from de_projet_perso.core.data_catalog import DataCatalog
    ... from de_projet_perso.core.settings import settings
    ...
    ... catalog = DataCatalog.load(settings.data_catalog_file_path)
    ... dataset = catalog.get_dataset("ign_contours_iris")
    ...
    ... resolver = PathResolver(dataset_name=dataset.name)
    ...
    ... # Generate version from IngestionFrequency
    ... version = dataset.ingestion.frequency.get_airflow_version_template(no_dash=True)
    ... # → "{{ ds_nodash }}" for daily or "{{ ts_nodash }}" for hourly
    ... bronze = resolver.bronze_path(version)  # Airflow replaces template at runtime

    >>> # Non-Airflow context (generates version from datetime)
    ... from datetime import datetime
    ...
    ... catalog = DataCatalog.load(settings.data_catalog_file_path)
    ... dataset = catalog.get_dataset("ign_contours_iris")
    ...
    ... # Generate version from IngestionFrequency
    ... version = dataset.ingestion.frequency.format_datetime_as_version(
    ...     datetime.now(),
    ...     no_dash=True
    ... )
    ... # → "20260121" for daily or "20260121T143022" for hourly
    ...
    ... resolver = PathResolver(dataset_name=dataset.name)
    ... bronze = resolver.bronze_path(version)  # Uses generated version
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings

# Compiled regex for version validation (YYYY-MM-DD format)
# NOTE: Currently unused
# _VERSION_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

# Valid medallion architecture layers
# NOTE: Currently unused - kept for future validation logic
# VALID_LAYERS = frozenset({"landing", "bronze", "silver", "gold"})


@dataclass(frozen=True)
class PathResolver:
    """Pure path resolution logic for medallion architecture.

    This class is completely independent of Dataset configuration.
    It only needs minimal metadata to construct paths.

    Attributes:
        dataset_name: Dataset identifier (e.g., "ign_contours_iris")
        base_dir: Base directory for data storage (defaults to settings.data_dir_path)
    """

    dataset_name: str
    base_dir: Path = settings.data_dir_path

    def __post_init__(self):
        """Validate PathResolver parameters."""
        if not self.dataset_name or not self.dataset_name.strip():
            raise ValueError("dataset_name must be a non-empty string")

    # =========================================================================
    # Landing Layer (Temporary)
    # =========================================================================

    @property
    def landing_dir(self) -> Path:
        """Get landing directory (preserves original filenames).

        Landing files are temporary and deleted after successful Bronze conversion.
        Original filenames from Content-Disposition header or URL are preserved.

        Returns:
            {data_dir_path}/landing/{dataset_name}/
        """
        return self.base_dir / "landing" / self.dataset_name

    # =========================================================================
    # Bronze Layer (Versioned History)
    # =========================================================================

    @property
    def _bronze_dir(self) -> Path:
        """Get bronze directory for this dataset.

        Returns:
            {data_dir_path}/bronze/{dataset_name}/
        """
        return self.base_dir / "bronze" / self.dataset_name

    def bronze_path(self, version: str) -> Path:
        """Get versioned bronze file path.

        Args:
            version: Specific version to retrieve

        Returns:
            {data_dir_path}/bronze/{dataset_name}/{version}.parquet
        """
        return self._bronze_dir / f"{version}.parquet"

    @property
    def bronze_latest_path(self) -> Path:
        """Get path to latest bronze version (symlink).

        This is a symbolic link pointing to the most recent bronze file.
        It should be updated after each successful bronze write.

        Returns:
            Path to latest.parquet symlink

        Example:
            bronze/ign_contours_iris/latest.parquet → 2025-01-17.parquet
        """
        return self._bronze_dir / "latest.parquet"

    def bronze_latest_version(self) -> str | None:
        """Get the version string that latest.parquet points to.

        Returns:
            Version string (e.g., "2025-01-17") or None if symlink doesn't exist
        """
        latest_link = self.bronze_latest_path

        if not latest_link.exists() or not latest_link.is_symlink():
            return None

        # Resolve symlink to get target filename
        # .stem: extract version from filename (remove .parquet)
        return latest_link.readlink().stem

    def list_bronze_versions(self) -> list[Path]:
        """Sorted by filename (lexicographical) which matches chronological for ISO dates.

        Returns:
            List of Path objects, newest first (excludes latest.parquet symlink)
        """
        if not self._bronze_dir.exists():
            return []

        versions = [
            path
            for path in self._bronze_dir.glob("*.parquet")
            # Exclude symlink and only include regular files
            # TODO: speed vs security (alternative: p.stem != "latest")
            if path.is_file() and not path.is_symlink()
        ]

        return sorted(versions, key=lambda p: p.name, reverse=True)

    # =========================================================================
    # Silver Layer (Current + Backup)
    # =========================================================================

    @property
    def _silver_dir(self) -> Path:
        """Get silver directory for this dataset.

        Returns:
            {data_dir_path}/silver/{dataset_name}/
        """
        return self.base_dir / "silver" / self.dataset_name

    @property
    def silver_current_path(self) -> Path:
        """Get current silver file path (always named 'current.parquet').

        This is the active version consumed by downstream processes.

        Returns:
            {data_dir_path}/silver/{dataset_name}/current.parquet
        """
        return self._silver_dir / "current.parquet"

    @property
    def silver_backup_path(self) -> Path:
        """Get backup silver file path (previous version for rollback).

        Before updating current.parquet, it should be copied to the backup.parquet.
        This allows fast rollback without reprocessing Bronze.

        Returns:
            {data_dir_path}/silver/{dataset_name}/backup.parquet
        """
        return self._silver_dir / "backup.parquet"

    # =========================================================================
    # Gold Layer (Analytical datasets from joined sources)
    # =========================================================================

    @property
    def _gold_dir(self) -> Path:
        """Get gold directory for this dataset.

        Returns:
            {data_dir_path}/gold/{dataset_name}/
        """
        return self.base_dir / "gold" / self.dataset_name

    @property
    def gold_current_path(self) -> Path:
        """Get current gold file path (always named 'current.parquet').

        Gold layer datasets are analytical tables built from joining
        multiple Silver sources. They follow the same current/backup
        strategy as Silver for fast rollback.

        Returns:
            {data_dir_path}/gold/{dataset_name}/current.parquet
        """
        return self._gold_dir / "current.parquet"

    @property
    def gold_backup_path(self) -> Path:
        """Get backup gold file path (previous version for rollback).

        Returns:
            {data_dir_path}/gold/{dataset_name}/backup.parquet
        """
        return self._gold_dir / "backup.parquet"


if __name__ == "__main__":
    import sys

    from de_projet_perso.core.data_catalog import DataCatalog
    from de_projet_perso.core.exceptions import InvalidCatalogError

    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        logger.exception(str(e), extra=e.validation_errors)
        sys.exit(-1)

    for _name, _dataset in _catalog.datasets.items():
        # Generate run_version from IngestionFrequency
        _run_version = _dataset.ingestion.frequency.format_datetime_as_version(datetime.now())

        _resolver = PathResolver(dataset_name=_dataset.name)

        logger.info(
            f"dataset → {_name}",
            extra={
                "dataset": _dataset.model_dump(),
                "run_version": _run_version,
                "landing_dir": _resolver.landing_dir,
                f"bronze_path ('{_run_version}')": _resolver.bronze_path(_run_version),
                "bronze_latest_path": _resolver.bronze_latest_path,
                "bronze_latest_version": _resolver.bronze_latest_version(),
                "silver_backup_path": _resolver.silver_backup_path,
                "silver_current_path": _resolver.silver_current_path,
                "gold_current_path": _resolver.gold_current_path,
                "gold_backup_path": _resolver.gold_backup_path,
            },
        )
