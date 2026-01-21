"""Pipeline transformation logic.

This module handles bronze and silver layer transformations,
completely decoupled from Airflow orchestration.

Transformations:
- Bronze: Landing → Parquet with normalized columns (versioned by run date)
- Silver: Bronze latest → Business logic applied → current.parquet + backup.parquet

Path Resolution:
- Uses PathResolver for all path operations
- Bronze: Creates versioned files ({date}.parquet) + updates latest.parquet symlink
- Silver: Rotates current → backup before writing new current
"""

import shutil
from pathlib import Path

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.pipeline.results import BronzeResult, SilverResult
from de_projet_perso.pipeline.transformations import get_bronze_transform, get_silver_transform


class PipelineTransformer:
    """Transformation logic for bronze and silver layers."""

    @staticmethod
    def to_bronze(landing_path: Path, dataset: Dataset, run_version: str) -> BronzeResult:
        """Convert landing file to Parquet with normalized column names.

        Bronze layer transformations:
        1. Read raw file from landing
        2. Apply custom transformations (normalize columns, filter, etc.)
        3. Write to versioned bronze Parquet file
        4. Update latest.parquet symlink
        5. Delete landing file

        Args:
            landing_path: Path to landing file (with original filename)
            dataset: Dataset configuration
            run_version: Version for this run (e.g., "2025-01-21" or "20250121T143022")

        Returns:
            BronzeResult with row count and columns

        Raises:
            NotImplementedError: If no bronze transformation is registered
        """
        resolver = PathResolver(dataset_name=dataset.name, run_version=run_version)

        bronze_path = resolver.bronze_path()
        bronze_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Converting to bronze for {dataset.name}",
            extra={
                "landing_path": str(landing_path),
                "bronze_path": str(bronze_path),
                "run_version": run_version,
            },
        )

        # Apply dataset-specific bronze transformation
        transforms = get_bronze_transform(dataset.name)
        if transforms is None:
            raise NotImplementedError(
                f"No bronze transformation registered for dataset: {dataset.name}"
            )

        # Pass both dataset config and actual landing file path
        df = transforms(dataset, landing_path)
        df.write_parquet(bronze_path)

        # Update latest symlink to point to this version
        resolver.update_bronze_latest_link()

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Bronze conversion complete for {dataset.name}",
            extra={
                "rows": row_count,
                "columns": len(columns),
                "bronze_path": str(bronze_path),
                "latest_link": str(resolver.bronze_latest_path()),
            },
        )

        # Cleanup all landing files (inside the landing directories)
        landing_dir = resolver.landing_dir()
        if landing_dir.exists() and landing_dir.is_dir():
            shutil.rmtree(landing_dir)

        return BronzeResult(row_count=row_count, columns=columns)

    @staticmethod
    def to_silver(bronze_result: BronzeResult, dataset: Dataset, run_version: str) -> SilverResult:
        """Apply business transformations to create silver layer.

        Silver layer transformations:
        1. Rotate silver files (current → backup)
        2. Read from bronze latest.parquet
        3. Apply custom business logic
        4. Write to silver current.parquet

        Args:
            bronze_result: Result from bronze transformation (for context)
            dataset: Dataset configuration
            run_version: Version for this run

        Returns:
            SilverResult with row count and columns
        """
        resolver = PathResolver(dataset_name=dataset.name, run_version=run_version)

        # Rotate silver: current → backup
        resolver.rotate_silver()

        silver_current = resolver.silver_current_path()
        silver_current.parent.mkdir(parents=True, exist_ok=True)
        bronze_latest = resolver.bronze_latest_path()

        logger.info(
            f"Transforming to silver for {dataset.name}",
            extra={
                "bronze_source": str(bronze_latest),
                "silver_dest": str(silver_current),
            },
        )

        # Apply dataset-specific silver transformation
        transforms = get_silver_transform(dataset.name)
        if transforms is None:
            raise NotImplementedError(
                f"No silver transformation registered for dataset: {dataset.name}"
            )

        # Transformations now read from resolver.bronze_latest_path()
        df = transforms(dataset, resolver)
        df.write_parquet(silver_current)

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Silver transformation complete for {dataset.name}",
            extra={
                "rows": row_count,
                "columns": len(columns),
                "silver_current": str(silver_current),
                "silver_backup": str(resolver.silver_backup_path()),
            },
        )

        return SilverResult(row_count=row_count, columns=df.columns)
