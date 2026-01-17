"""Pipeline transformation logic.

This module handles bronze and silver layer transformations,
completely decoupled from Airflow orchestration.

Transformations:
- Bronze: Raw → Parquet with normalized columns
- Silver: Bronze → Business logic applied (custom transformations)

SHA256 Propagation:
- Landing SHA256 is propagated through all layers for traceability
- Archive SHA256 is also propagated to track original source
"""

import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.results import BronzeResult, LandingResult, SilverResult
from de_projet_perso.pipeline.transformations import get_bronze_transform, get_silver_transform


class PipelineTransformer:
    """Transformation logic for bronze and silver layers."""

    @staticmethod
    def to_bronze(
        landing_result: LandingResult, dataset_name: str, dataset: Dataset
    ) -> BronzeResult:
        """Convert landing file to Parquet with normalized column names.

        Bronze layer transformations:
        1. Read raw file (GeoPackage, Parquet, JSON, etc.)
        2. Normalize column names to snake_case
        3. Apply custom transformations if registered
        4. Write to versioned Parquet file

        Args:
            landing_result: Result from landing validation (contains file path and SHA256s)
            dataset_name: Dataset identifier
            dataset: Dataset configuration

        Returns:
            BronzeResult with bronze file info and propagated SHA256s

        Raises:
            ValueError: If source format is unsupported
        """
        landing_path = landing_result.path  # TODO: replace by get_storage_path
        # settings.data_dir_path / dataset.get_storage_path("landing")
        bronze_path = settings.data_dir_path / dataset.get_storage_path("bronze")
        bronze_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Converting to bronze for {dataset_name}",
            extra={"source": landing_path, "dest": bronze_path},
        )

        # # Normalize column names to snake_case (always applied)
        # df = df.rename(lambda col: col.lower().replace(" ", "_").replace("-", "_"))

        # Apply dataset-specific bronze transformation
        transforms = get_bronze_transform(dataset_name)
        if transforms is None:
            raise NotImplementedError(
                f"No bronze transformation registered for dataset: {dataset_name}"
            )

        logger.info(
            f"Applying bronze transformations for {dataset_name}",
            extra={"dataset": dataset},
        )
        df = transforms(dataset)
        df.write_parquet(bronze_path)

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Bronze conversion complete for {dataset_name}",
            extra={"rows": row_count, "columns": len(columns)},
        )

        return BronzeResult(
            path=bronze_path,
            row_count=row_count,
            columns=columns,
            sha256=landing_result.sha256,  # Propagate landing file SHA256
            archive_sha256=landing_result.archive_sha256,  # Propagate archive SHA256
        )

    @staticmethod
    def to_silver(
        bronze_result: BronzeResult,
        dataset_name: str,
        dataset: Dataset,
    ) -> SilverResult:
        """Apply business transformations to create silver layer.

        Silver layer transformations:
        1. Read bronze Parquet file
        2. Apply custom business logic (if registered)
        3. Write to versioned silver Parquet file

        Args:
            bronze_result: Result from bronze transformation (contains file path and SHA256s)
            dataset_name: Dataset identifier
            dataset: Dataset configuration

        Returns:
            SilverResult with silver file info and propagated SHA256s
        """
        silver_path = settings.data_dir_path / dataset.get_storage_path("silver")
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        bronze_path = bronze_result.path  # TODO: replace

        logger.info(
            f"Transforming to silver for {dataset_name}",
            extra={"source": bronze_path.name},
        )

        # Read bronze parquet file
        df = pl.read_parquet(bronze_path)

        # Apply dataset-specific silver transformation
        transforms = get_silver_transform(dataset_name)
        if transforms is None:
            raise NotImplementedError(
                f"No silver transformation registered for dataset: {dataset_name}"
            )

        logger.info(
            f"Applying custom silver transformation for {dataset_name}",
            extra={"dataset": dataset_name},
        )
        df = transforms(dataset)
        df.write_parquet(silver_path)

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Silver transformation complete for {dataset_name}",
            extra={"rows": row_count, "columns": len(columns)},
        )

        return SilverResult(
            path=silver_path,
            row_count=row_count,
            columns=df.columns,
            sha256=bronze_result.sha256,  # Propagate from bronze (= landing SHA256)
            archive_sha256=bronze_result.archive_sha256,  # Propagate archive SHA256
        )
