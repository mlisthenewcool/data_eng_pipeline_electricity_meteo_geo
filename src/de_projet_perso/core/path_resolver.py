"""Pure path resolution for medallion architecture (landing/bronze/silver/gold).

Independent of Dataset class. Bronze uses versioned files + latest symlink,
Silver and Gold use current + backup files for fast rollback.
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
    """Pure path construction for medallion layers. No file I/O."""

    dataset_name: str
    base_dir: Path = settings.data_dir_path

    def __post_init__(self):
        if not self.dataset_name or not self.dataset_name.strip():
            raise ValueError("dataset_name must be a non-empty string")

    # =========================================================================
    # Landing Layer (Temporary)
    # =========================================================================

    @property
    def landing_dir(self) -> Path:
        """Temporary storage dir, deleted after Bronze conversion."""
        return self.base_dir / "landing" / self.dataset_name

    # =========================================================================
    # Bronze Layer (Versioned History)
    # =========================================================================

    @property
    def _bronze_dir(self) -> Path:
        return self.base_dir / "bronze" / self.dataset_name

    def bronze_path(self, version: str) -> Path:
        """Return ``bronze/{dataset_name}/{version}.parquet``."""
        return self._bronze_dir / f"{version}.parquet"

    @property
    def bronze_latest_path(self) -> Path:
        """Symlink to most recent bronze version."""
        return self._bronze_dir / "latest.parquet"

    def bronze_latest_version(self) -> str | None:
        """Version string from latest.parquet symlink target, or None."""
        latest_link = self.bronze_latest_path

        if not latest_link.exists() or not latest_link.is_symlink():
            return None

        # Resolve symlink to get target filename
        # .stem: extract version from filename (remove .parquet)
        return latest_link.readlink().stem

    def list_bronze_versions(self) -> list[Path]:
        """All bronze version files sorted newest-first (excludes latest symlink)."""
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
        return self.base_dir / "silver" / self.dataset_name

    @property
    def silver_current_path(self) -> Path:
        """Active silver version consumed by downstream processes."""
        return self._silver_dir / "current.parquet"

    @property
    def silver_backup_path(self) -> Path:
        """Previous silver version (N-1) for fast rollback."""
        return self._silver_dir / "backup.parquet"

    # =========================================================================
    # Gold Layer (Current + Backup)
    # =========================================================================

    @property
    def _gold_dir(self) -> Path:
        return self.base_dir / "gold" / self.dataset_name

    @property
    def gold_current_path(self) -> Path:
        """Active gold version (analytical table from joined Silver sources)."""
        return self._gold_dir / "current.parquet"

    @property
    def gold_backup_path(self) -> Path:
        """Previous gold version (N-1) for fast rollback."""
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

    # Only display remote datasets (derived datasets don't have ingestion config)
    for _dataset in _catalog.get_remote_datasets():
        # Generate run_version from IngestionFrequency
        _run_version = _dataset.ingestion.frequency.format_datetime_as_version(datetime.now())

        _resolver = PathResolver(dataset_name=_dataset.name)

        logger.info(
            f"dataset → {_dataset.name}",
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
