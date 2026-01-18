"""Transformations for IGN Contours IRIS dataset."""

from pathlib import Path

import duckdb
import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("ign_contours_iris")
def transform_bronze_ign_iris(dataset: Dataset, landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for IGN Contours IRIS.

    Reads GeoPackage from landing layer (with original filename preserved)
    and applies basic filtering and geometry conversion.

    Args:
        dataset: Dataset configuration from catalog
        landing_path: Actual path to landing file (e.g., data/landing/ign_contours_iris/iris.gpkg)

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    # GeoPackage needs special handling with DuckDB
    logger.info(
        "Reading GeoPackage from landing",
        extra={"landing_path": str(landing_path), "filename": landing_path.name},
    )

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
    logger.info("DuckDB spatial query started")
    df = conn.execute(query, parameters=[str(landing_path)]).pl()
    logger.info("DuckDB spatial query completed", extra={"rows": len(df)})

    return df.filter(pl.col("code_iris").is_not_null())


@register_silver("ign_contours_iris")
def transform_silver_ign_iris(dataset: Dataset) -> pl.DataFrame:
    """Silver transformation for IGN Contours IRIS.

    Reads from standardized bronze Parquet and applies business transformations.
    For now, pass-through transformation (no additional business logic).

    Future enhancements could include:
    - Data enrichment
    - Geocoding
    - Aggregations
    - Quality metrics

    Args:
        dataset: Dataset configuration (reads from bronze using get_bronze_path())

    Returns:
        Silver layer DataFrame
    """
    # Read from standardized bronze layer file
    bronze_path = dataset.get_bronze_path()
    logger.info("Reading from bronze", extra={"bronze_path": str(bronze_path)})
    df = pl.read_parquet(bronze_path)

    # For now, pass-through (no transformation needed)
    return df
