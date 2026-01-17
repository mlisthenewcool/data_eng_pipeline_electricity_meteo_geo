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

from pathlib import Path

import duckdb
import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.results import BronzeResult, LandingResult, SilverResult
from de_projet_perso.pipeline.transformations import get_bronze_transform, get_silver_transform


class PipelineTransformer:
    """Transformation logic for bronze and silver layers."""

    @staticmethod
    def to_bronze(
        landing_result: LandingResult,
        dataset_name: str,
        dataset: Dataset,
        bronze_dir: Path,
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
            bronze_dir: Bronze layer directory

        Returns:
            BronzeResult with bronze file info and propagated SHA256s

        Raises:
            ValueError: If source format is unsupported
        """
        bronze_dir.mkdir(parents=True, exist_ok=True)
        bronze_path = bronze_dir / f"{dataset.ingestion.version}.parquet"

        source_path = landing_result.path

        logger.info(
            f"Converting to bronze for {dataset_name}",
            extra={"source": source_path.name, "dest": bronze_path.name},
        )

        # Read based on format
        if source_path.suffix == ".gpkg":
            # GeoPackage needs special handling with DuckDB

            conn = duckdb.connect()
            conn.execute("INSTALL spatial; LOAD spatial;")
            # ST_AsGeoJSON(geometrie) AS geom_json
            # noinspection SqlResolve
            query = """
                SELECT
                    * EXCLUDE (geometrie),
                    ST_AsWKB(geometrie) AS geom_wkb
                FROM st_read(?, layer = 'contours_iris')
            """
            logger.info("duckdb spatial query started")
            df = conn.execute(query, parameters=[str(source_path)]).pl()
            logger.info("duckdb spatial query ended")
        elif source_path.suffix == ".parquet":
            df = pl.read_parquet(source_path)
        elif source_path.suffix == ".json":
            df = pl.read_json(source_path)
        else:
            raise ValueError(f"Unsupported format: {source_path.suffix}")

        # Normalize column names to snake_case (always applied)
        df = df.rename(lambda col: col.lower().replace(" ", "_").replace("-", "_"))

        # Apply dataset-specific bronze transformation if registered
        custom_transform = get_bronze_transform(dataset_name)
        if custom_transform:
            logger.info(
                f"Applying custom bronze transformation for {dataset_name}",
                extra={"dataset": dataset_name},
            )
            df = custom_transform(df, dataset_name)
        else:
            logger.debug(
                f"No custom bronze transformation for {dataset_name}",
                extra={"dataset": dataset_name},
            )

        # Write to Parquet
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
        silver_dir: Path,
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
            silver_dir: Silver layer directory

        Returns:
            SilverResult with silver file info and propagated SHA256s
        """
        silver_dir.mkdir(parents=True, exist_ok=True)
        silver_path = silver_dir / f"{dataset.ingestion.version}.parquet"

        bronze_path = bronze_result.path

        logger.info(
            f"Transforming to silver for {dataset_name}",
            extra={"source": bronze_path.name},
        )

        df = pl.read_parquet(bronze_path)

        custom_transform = get_silver_transform(dataset_name)
        if custom_transform:
            logger.info(
                f"Applying custom silver transformation for {dataset_name}",
                extra={"dataset": dataset_name},
            )
            df = custom_transform(df, dataset_name)
        else:
            logger.debug(
                f"No custom silver transformation for {dataset_name}, using default (no-op)",
                extra={"dataset": dataset_name},
            )
            # Default: no transformation, just pass through

        df.write_parquet(silver_path)

        logger.info(
            f"Silver transformation complete for {dataset_name}",
            extra={"path": str(silver_path), "rows": len(df)},
        )

        return SilverResult(
            path=silver_path,
            row_count=len(df),
            columns=df.columns,
            sha256=bronze_result.sha256,  # Propagate from bronze (= landing SHA256)
            archive_sha256=bronze_result.archive_sha256,  # Propagate archive SHA256
        )
