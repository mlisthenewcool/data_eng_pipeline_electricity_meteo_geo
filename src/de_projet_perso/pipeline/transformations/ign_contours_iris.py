"""Transformations for IGN Contours IRIS dataset."""

from pathlib import Path

import duckdb
import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("ign_contours_iris")
def transform_bronze(dataset: Dataset, landing_path: Path) -> pl.DataFrame:
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

    # TODO: DuckDB queries hang in Airflow forever but not on local execution ...
    #  ST_AsWKB(geometrie) AS geom_wkb
    #  ST_AsGeoJSON(geometrie) AS geom_json
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("SET memory_limit = '1GB'")
    conn.execute("SET threads = 2")

    # noinspection SqlResolve
    query = """
SELECT\
    cleabs, code_insee, nom_commune, iris, code_iris, nom_iris, type_iris,\
    ST_AsWKB(geometrie) AS geom_wkb \
FROM ST_read(?, layer = 'contours_iris')
"""
    logger.debug("Executing DuckDB spatial query ...", extra={"query": query})
    df = conn.execute(query, parameters=[str(landing_path)]).pl()
    logger.debug("DuckDB spatial query completed", extra={"n_rows": len(df), "columns": df.columns})
    conn.close()
    return df


@register_silver("ign_contours_iris")
def transform_silver(dataset: Dataset, resolver) -> pl.DataFrame:
    """Silver transformation for IGN Contours IRIS.

    Reads from latest bronze Parquet and applies business transformations.
    For now, pass-through transformation (no additional business logic).

    Future enhancements could include:
    - Data enrichment
    - Geocoding
    - Aggregations
    - Quality metrics

    Args:
        dataset: Dataset configuration
        resolver: PathResolver instance for path operations

    Returns:
        Silver layer DataFrame
    """
    # Read from latest bronze (via symlink)
    bronze_latest = resolver.bronze_latest_path()
    logger.info("Reading from bronze latest", extra={"bronze_path": str(bronze_latest)})
    df = pl.read_parquet(bronze_latest)

    # For now, pass-through (no transformation needed)
    return df
