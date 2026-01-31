"""Transformations for IGN Contours IRIS dataset."""

import shutil
import tempfile
from pathlib import Path

import duckdb
import polars as pl

from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.validators import validate_ign_contours_iris


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for IGN Contours IRIS.

    Reads GeoPackage from landing layer (with original filename preserved)
    and applies basic filtering and geometry conversion.

    The GeoPackage (.gpkg) format is SQLite-based. GDAL (used internally by
    DuckDB's ST_read) acquires file locks when reading, which can cause
    indefinite hangs in Airflow's LocalExecutor if another process holds a
    lock on the same file. To avoid this, the file is copied to a temporary
    location before reading.

    Args:
        landing_path: Actual path to landing file (e.g., data/landing/ign_contours_iris/iris.gpkg)

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    logger.info(
        "Reading GeoPackage from landing",
        extra={"landing_path": str(landing_path), "filename": landing_path.name},
    )

    # Copy .gpkg to a temp file to avoid SQLite/GDAL file lock contention
    # that causes intermittent hangs in Airflow (LocalExecutor).
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_gpkg = Path(tmp_dir) / landing_path.name
        shutil.copy2(landing_path, tmp_gpkg)
        logger.debug("Copied GeoPackage to temp file", extra={"tmp_path": str(tmp_gpkg)})

        conn = duckdb.connect(":memory:")
        try:
            conn.execute("LOAD spatial;")
            conn.execute("SET threads = 1")
            conn.execute("SET memory_limit = '1GB'")

            # noinspection SqlResolve
            query = """
        SELECT\
            cleabs, code_insee, nom_commune, iris, code_iris, nom_iris, type_iris,\
            ST_AsWKB(geometrie) AS geom_wkb \
        FROM ST_read(?, layer = 'contours_iris')
        """
            logger.info("Executing DuckDB spatial query ...", extra={"query": query})
            df = conn.execute(query, parameters=[str(tmp_gpkg)]).pl()
            logger.info(
                "DuckDB spatial query completed",
                extra={"n_rows": len(df), "columns": df.columns},
            )
        finally:
            conn.close()
    return df


def transform_silver(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for IGN Contours IRIS.

    Enriches IRIS contours with centroid coordinates in WGS84.
    The original geometry is in Lambert 93 (EPSG:2154), we transform
    the centroid to WGS84 (EPSG:4326) for compatibility with other datasets.

    Transformations applied:
    - Remove cleabs column (internal IGN identifier, not useful)
    - Compute centroid of each IRIS polygon
    - Transform centroid from Lambert 93 to WGS84
    - Extract latitude and longitude as separate columns

    Args:
        latest_bronze_path: Path to the latest bronze version

    Returns:
        Silver layer DataFrame with centroid_lat and centroid_lon columns
    """
    logger.info("Reading from bronze latest", extra={"bronze_path": str(latest_bronze_path)})

    # Use DuckDB for spatial operations on the WKB geometry
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("LOAD spatial;")

        # Register the parquet file as a table
        conn.execute(f"CREATE VIEW bronze AS SELECT * FROM read_parquet('{latest_bronze_path}')")

        # Compute centroids and transform to WGS84
        # geom_wkb is stored as WKB in Lambert 93 (EPSG:2154)
        # Note: DuckDB's ST_Transform from Lambert 93 to WGS84 returns coordinates
        # in (lat, lon) order instead of standard (lon, lat), so we use ST_X for lat
        # and ST_Y for lon (counterintuitive but verified empirically)
        # noinspection SqlResolve
        query = """
        SELECT
            code_iris,
            nom_iris,
            code_insee,
            nom_commune,
            type_iris,
            geom_wkb,
            ST_X(ST_Transform(
                ST_Centroid(ST_GeomFromWKB(geom_wkb)),
                'EPSG:2154',
                'EPSG:4326'
            )) AS centroid_lat,
            ST_Y(ST_Transform(
                ST_Centroid(ST_GeomFromWKB(geom_wkb)),
                'EPSG:2154',
                'EPSG:4326'
            )) AS centroid_lon
        FROM bronze
        """

        logger.info("Computing centroids with DuckDB spatial extension")
        df = conn.execute(query).pl()
    finally:
        conn.close()

    # Validate output before returning
    validate_ign_contours_iris(df)

    logger.info(
        "Silver transformation completed",
        extra={"n_rows": len(df), "columns": df.columns},
    )
    return df
