"""Transformations for IGN Contours IRIS dataset."""

import duckdb
import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("ign_contours_iris")
def transform_bronze_ign_iris(dataset: Dataset) -> pl.DataFrame:
    """Bronze transformation for IGN Contours IRIS.

    Filters out rows without valid IRIS codes.

    Args:
        dataset: Dataset object

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    # GeoPackage needs special handling with DuckDB

    landing_path = dataset.get_landing_path()

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
    df = conn.execute(query, parameters=[str(landing_path)]).pl()
    logger.info("duckdb spatial query ended")
    return df.filter(pl.col("code_iris").is_not_null())


@register_silver("ign_contours_iris")
def transform_silver_ign_iris(dataset: Dataset) -> pl.DataFrame:
    """Silver transformation for IGN Contours IRIS.

    For now, pass-through transformation (no additional business logic).
    Future enhancements could include:
    - Data enrichment
    - Geocoding
    - Aggregations
    - Quality metrics

    Args:
        dataset: Dataset object containing configuration

    Returns:
        Silver layer DataFrame
    """
    # Read bronze layer file
    bronze_path = dataset.get_bronze_path()
    df = pl.read_parquet(bronze_path)

    # For now, pass-through (no transformation needed)
    return df
