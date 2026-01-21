"""Centralized path resolution for medallion architecture.

This module provides pure path resolution logic, completely independent of Dataset.
It implements the medallion architecture (landing → bronze → silver → gold) with:
- Landing: Temporary storage with original filenames
- Bronze: Versioned history with automatic 'latest.parquet' symlink
- Silver: Current + backup files for fast rollback

Key Design Decisions:
- No dependency on Dataset class (uses primitives only)
- Supports both Airflow ({{ ds }}/{{ ts_nodash }}) and non-Airflow contexts
- Bronze versions retained for 1 year (cleanup via maintenance DAG)
- Silver uses current.parquet + backup.parquet strategy

Example:
    # Airflow context (template variables)
    resolver = PathResolver(
        dataset_name="ign_contours_iris",
        provider="IGN",
        run_version="{{ ds }}"
    )
    bronze = resolver.bronze_path()

    # Non-Airflow context (auto-generates version)
    resolver = PathResolver(
        dataset_name="ign_contours_iris",
        provider="IGN",
        frequency=IngestionFrequency.DAILY,
    )
    bronze = resolver.bronze_path()  # Auto-generates from datetime.now()
"""

import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings


@dataclass(frozen=True)
class PathResolver:
    """Pure path resolution logic for medallion architecture.

    This class is completely independent of Dataset configuration.
    It only needs minimal metadata to construct paths.

    Attributes:
        dataset_name: Dataset identifier (e.g., "ign_contours_iris")
        run_version: Airflow template ({{ ds }} or {{ ts_nodash }}) or None for auto-generation
        base_dir: Base directory for data storage (defaults to settings.data_dir_path)
    """

    dataset_name: str
    run_version: str | None = None
    base_dir: Path = settings.data_dir_path

    # =========================================================================
    # Landing Layer (Temporary)
    # =========================================================================

    def landing_dir(self) -> Path:
        """Get landing directory (preserves original filenames).

        Landing files are temporary and deleted after successful Bronze conversion.
        Original filenames from Content-Disposition header or URL are preserved.

        Returns:
            {data_dir_path}/landing/{dataset_name}/

        Example:
            data/landing/ign_contours_iris/
        """
        return self.base_dir / "landing" / self.dataset_name

    # =========================================================================
    # Bronze Layer (Versioned History)
    # =========================================================================

    def _bronze_dir(self) -> Path:
        """Get bronze directory for this dataset.

        Returns:
            {data_dir_path}/bronze/{dataset_name}/

        Example:
            data/bronze/ign_contours_iris/
        """
        return self.base_dir / "bronze" / self.dataset_name

    def bronze_path(self, version: str | None = None) -> Path:
        """Get versioned bronze file path.

        Args:
            version: Specific version to retrieve, or None for current run_version

        Returns:
            {data_dir_path}/bronze/{dataset_name}/{version}.parquet

        Examples:
            # Current run
            resolver.bronze_path()  # .../2025-01-15.parquet

            # Specific historical version
            resolver.bronze_path("2025-01-10")  # .../2025-01-10.parquet
        """
        target_version = version or self.run_version
        return self._bronze_dir() / f"{target_version}.parquet"

    def bronze_latest_path(self) -> Path:
        """Get path to latest bronze version (symlink).

        This is a symbolic link pointing to the most recent bronze file.
        It is automatically updated after each successful bronze write.

        Returns:
            Path to latest.parquet symlink

        Example:
            bronze/ign_contours_iris/latest.parquet → 2025-01-17.parquet

        Usage:
            # Read the latest bronze without knowing the version
            df = pl.read_parquet(resolver.bronze_latest_path())
        """
        return self._bronze_dir() / "latest.parquet"

    def list_bronze_versions(self) -> list[Path]:
        """List all bronze versions for this dataset, sorted by modification time.

        Returns:
            List of Path objects, newest first (excludes latest.parquet symlink)

        Example:
            >>> versions = resolver.list_bronze_versions()
            [Path('.../2025-01-17.parquet'), Path('.../2025-01-16.parquet')]
        """
        bronze_dir = self._bronze_dir()
        if not bronze_dir.exists():
            return []

        versions = []
        for path in bronze_dir.glob("*.parquet"):
            # Exclude symlink and only include regular files
            if path.is_file() and not path.is_symlink():
                versions.append(path)

        return sorted(versions, key=lambda p: p.stat().st_mtime, reverse=True)

    def update_bronze_latest_link(self, target_version: str | None = None) -> None:
        """Update 'latest.parquet' symlink to point to the newest bronze version.

        This should be called AFTER successfully writing a new bronze file.
        The operation is atomic (uses atomic rename).

        Args:
            target_version: Specific version to link to, or None for current run_version

        Raises:
            FileNotFoundError: If target bronze file doesn't exist

        Example:
            # After writing bronze
            bronze_path = resolver.bronze_path()
            df.write_parquet(bronze_path)
            resolver.update_bronze_latest_link()  # Update symlink
        """
        target_version = target_version or self.run_version
        target_file = self.bronze_path(version=target_version)
        latest_link = self.bronze_latest_path()

        if not target_file.exists():
            raise FileNotFoundError(
                f"Cannot create symlink: target bronze file doesn't exist: {target_file}"
            )

        # Create parent directory if needed
        latest_link.parent.mkdir(parents=True, exist_ok=True)

        # Atomic symlink update strategy:
        # 1. Create temporary symlink with unique name
        # 2. Atomically rename it to replace old symlink

        temp_link = latest_link.parent / f".latest.tmp.{os.getpid()}"

        try:
            # Create symlink pointing to relative path (more portable)
            # latest.parquet → 2025-01-17.parquet (not absolute path)
            relative_target = target_file.name
            temp_link.symlink_to(relative_target)

            # Atomic replace (works even if latest_link already exists)
            temp_link.replace(latest_link)

            logger.info(
                "Updated bronze latest symlink",
                extra={
                    "dataset_name": self.dataset_name,
                    "target_version": target_version,
                    "symlink": str(latest_link),
                    "target": relative_target,
                },
            )
        except Exception:
            # Cleanup temp file if error
            if temp_link.exists():
                temp_link.unlink()
            raise

    def bronze_latest_version(self) -> str | None:
        """Get the version string that latest.parquet points to.

        Returns:
            Version string (e.g., "2025-01-17") or None if symlink doesn't exist

        Example:
            >>> resolver.bronze_latest_version()
            '2025-01-17'
        """
        latest_link = self.bronze_latest_path()

        if not latest_link.exists():
            return None

        # Resolve symlink to get target filename
        if latest_link.is_symlink():
            target = latest_link.readlink()  # Gets relative target
            # Extract version from filename (remove .parquet)
            return target.stem

        return None

    def cleanup_old_bronze_versions(
        self, retention_days: int = settings.bronze_retention_days
    ) -> list[Path]:
        """Remove bronze versions older than retention period.

        Args:
            retention_days: Number of days to keep (default: 1 year)

        Returns:
            List of deleted file paths

        Note:
            This should be called by a separate Airflow maintenance DAG,
            not during pipeline execution.
        """
        cutoff_time = datetime.now() - timedelta(days=retention_days)
        deleted = []

        # TODO: find cutoff date and remove all files older than that instead of checking all files
        for version_path in self.list_bronze_versions():
            # Check file modification time
            file_mtime = datetime.fromtimestamp(version_path.stat().st_mtime)

            if file_mtime < cutoff_time:
                version_path.unlink()
                deleted.append(version_path)
                logger.debug(
                    "Deleted old bronze version",
                    extra={
                        "dataset": self.dataset_name,
                        "version": version_path.stem,
                        "age_days": (datetime.now() - file_mtime).days,
                    },
                )

        return deleted

    # =========================================================================
    # Silver Layer (Current + Backup)
    # =========================================================================

    def silver_dir(self) -> Path:
        """Get silver directory for this dataset.

        Returns:
            {data_dir_path}/silver/{dataset_name}/

        Example:
            data/silver/ign_contours_iris/
        """
        return self.base_dir / "silver" / self.dataset_name

    def silver_current_path(self) -> Path:
        """Get current silver file path (always named 'current.parquet').

        This is the active version consumed by downstream processes.

        Returns:
            {data_dir_path}/silver/{dataset_name}/current.parquet
        """
        return self.silver_dir() / "current.parquet"

    def silver_backup_path(self) -> Path:
        """Get backup silver file path (previous version for rollback).

        Before updating current.parquet, it is copied to the backup.parquet.
        This allows fast rollback without reprocessing Bronze.

        Returns:
            {data_dir_path}/silver/{dataset_name}/backup.parquet
        """
        return self.silver_dir() / "backup.parquet"

    def rotate_silver(self) -> None:
        """Rotate silver files: current → backup (before writing new current).

        This should be called BEFORE writing the new silver current file.
        If current exists, it becomes backup (old backup is overwritten).

        Example workflow:
            1. resolver.rotate_silver()  # current → backup
            2. df.write_parquet(resolver.silver_current_path())  # New current
        """
        current = self.silver_current_path()
        backup = self.silver_backup_path()

        if current.exists():
            # Copy current to back up (overwrite old backup)
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(current, backup)
            logger.debug(
                "Rotated silver files",
                extra={
                    "dataset_name": self.dataset_name,
                    "current": str(current),
                    "backup": str(backup),
                },
            )

    def rollback_silver(self) -> bool:
        """Rollback silver: restore backup → current.

        Returns:
            True if rollback succeeded, False if no backup exists

        Use case:
            If new silver transformation produces invalid data,
            quickly restore previous version without reprocessing.
        """
        backup = self.silver_backup_path()
        current = self.silver_current_path()

        if not backup.exists():
            logger.warning(
                "Cannot rollback silver: no backup exists",
                extra={"dataset_name": self.dataset_name},
            )
            return False

        shutil.copy2(backup, current)
        logger.info(
            "Rolled back silver to backup version",
            extra={
                "dataset_name": self.dataset_name,
                "backup": str(backup),
                "current": str(current),
            },
        )
        return True


if __name__ == "__main__":
    import sys

    from de_projet_perso.core.data_catalog import DataCatalog
    from de_projet_perso.core.exceptions import InvalidCatalogError

    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        logger.exception(message=str(e), extra=e.validation_errors)
        sys.exit(1)

    for _name, _dataset in _catalog.datasets.items():
        _resolver = PathResolver(dataset_name=_dataset.name)
        logger.info(
            message=f"dataset: {_name}",
            extra={
                "dataset": _dataset.model_dump(),
                "landing_dir": str(_resolver.landing_dir()),
                "bronze_path (default, no args)": str(_resolver.bronze_path()),
                "bronze_latest_path": _resolver.bronze_latest_path(),
                "bronze_latest_version": _resolver.bronze_latest_version(),
                "silver_current_path": str(_resolver.silver_current_path()),
            },
        )
