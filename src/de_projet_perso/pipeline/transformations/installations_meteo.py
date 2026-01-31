"""Gold transformation: Join installations with nearest weather stations.

This module creates the main analytical dataset linking renewable energy
installations (solar and wind) with their nearest compatible weather station.

The join uses spatial distance calculation (Haversine) to find the closest
station that measures the relevant parameters for each energy type.
"""

from pathlib import Path

import duckdb
import polars as pl

from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver

# Maximum distance in km to consider a weather station as relevant
MAX_DISTANCE_KM = 20.0


def transform_gold(
    odre_installations_silver_path: Path | None = None,
    ign_contours_iris_silver_path: Path | None = None,
    meteo_france_stations_silver_path: Path | None = None,
) -> pl.DataFrame:
    """Create analytical dataset linking installations to weather stations.

    This transformation:
    1. Joins ODRE installations with IGN IRIS to get centroid coordinates
    2. Filters to renewable energy (solar + wind) only
    3. Finds the nearest compatible weather station for each installation
       - For solar: station must have mesure_solaire = True
       - For wind: station must have mesure_eolien = True
    4. Filters to stations within MAX_DISTANCE_KM

    Args:
        odre_installations_silver_path: Path to ODRE installations Silver parquet
            (defaults to silver/odre_installations/current.parquet)
        ign_contours_iris_silver_path: Path to IGN contours IRIS Silver parquet
            (defaults to silver/ign_contours_iris/current.parquet)
        meteo_france_stations_silver_path: Path to Meteo France stations Silver parquet
            (defaults to silver/meteo_france_stations/current.parquet)

    Returns:
        DataFrame with installations enriched with nearest weather station info
    """
    # Use default paths if not provided
    if odre_installations_silver_path is None:
        odre_installations_silver_path = PathResolver("odre_installations").silver_current_path
    if ign_contours_iris_silver_path is None:
        ign_contours_iris_silver_path = PathResolver("ign_contours_iris").silver_current_path
    if meteo_france_stations_silver_path is None:
        meteo_france_stations_silver_path = PathResolver(
            "meteo_france_stations"
        ).silver_current_path

    logger.info(
        "Starting Gold transformation: installations_meteo",
        extra={
            "odre_path": str(odre_installations_silver_path),
            "ign_path": str(ign_contours_iris_silver_path),
            "meteo_path": str(meteo_france_stations_silver_path),
            "max_distance_km": MAX_DISTANCE_KM,
        },
    )

    # Load Silver datasets
    logger.debug("Loading Silver datasets")
    df_odre = pl.read_parquet(odre_installations_silver_path)
    df_ign = pl.read_parquet(ign_contours_iris_silver_path)
    df_meteo = pl.read_parquet(meteo_france_stations_silver_path)

    logger.debug(
        "Silver datasets loaded",
        extra={
            "odre_rows": len(df_odre),
            "ign_rows": len(df_ign),
            "meteo_rows": len(df_meteo),
        },
    )

    # Filter ODRE to solar and wind only
    df_odre_filtered = df_odre.filter(
        pl.col("type_energie").is_in(["solaire", "eolien"]) & pl.col("est_actif")
    )
    logger.debug(
        "Filtered to solar/wind installations",
        extra={"filtered_rows": len(df_odre_filtered)},
    )

    # Join ODRE with IGN to get centroid coordinates
    df_with_coords = df_odre_filtered.join(
        df_ign.select("code_iris", "centroid_lat", "centroid_lon"),
        on="code_iris",
        how="left",
    )

    # Count installations without coordinates (no matching IRIS)
    n_without_coords = df_with_coords.filter(pl.col("centroid_lat").is_null()).shape[0]
    if n_without_coords > 0:
        logger.warning(
            "Some installations have no matching IRIS code",
            extra={"n_without_coords": n_without_coords},
        )

    # Filter out installations without coordinates
    df_with_coords = df_with_coords.filter(pl.col("centroid_lat").is_not_null())

    logger.debug(
        "Joined with IGN coordinates",
        extra={"rows_with_coords": len(df_with_coords)},
    )

    # Use DuckDB for efficient spatial join
    result = _find_nearest_stations_duckdb(df_with_coords, df_meteo, MAX_DISTANCE_KM)

    logger.info(
        "Gold transformation completed",
        extra={
            "total_installations": len(result),
            "with_station": result["station_meteo_id"].is_not_null().sum(),
            "without_station": result["station_meteo_id"].is_null().sum(),
        },
    )

    return result


def _find_nearest_stations_duckdb(
    df_installations: pl.DataFrame,
    df_stations: pl.DataFrame,
    max_distance_km: float,
) -> pl.DataFrame:
    """Find nearest compatible weather station for each installation using DuckDB.

    Uses Haversine formula for distance calculation and filters by:
    - Maximum distance
    - Compatibility (mesure_solaire for solar, mesure_eolien for wind)

    Args:
        df_installations: Installations with centroid_lat, centroid_lon, type_energie
        df_stations: Stations with latitude, longitude, mesure_solaire, mesure_eolien
        max_distance_km: Maximum distance to consider

    Returns:
        Installations DataFrame enriched with nearest station info
    """
    logger.debug("Computing nearest stations with DuckDB")

    conn = duckdb.connect(":memory:")

    # Register DataFrames as DuckDB tables
    conn.register("installations", df_installations.to_arrow())
    conn.register("stations", df_stations.to_arrow())

    # Haversine distance formula in SQL
    # Returns distance in km between two lat/lon points
    haversine_sql = """
    -- Haversine formula for distance in km
    2 * 6371 * ASIN(SQRT(
        POWER(SIN(RADIANS(s.latitude - i.centroid_lat) / 2), 2) +
        COS(RADIANS(i.centroid_lat)) * COS(RADIANS(s.latitude)) *
        POWER(SIN(RADIANS(s.longitude - i.centroid_lon) / 2), 2)
    ))
    """

    # Query to find nearest compatible station for each installation
    # noinspection SqlResolve
    query = f"""
    WITH distances AS (
        SELECT
            i.*,
            s.id AS station_id,
            s.nom AS station_nom,
            s.latitude AS station_lat,
            s.longitude AS station_lon,
            s.mesure_solaire,
            s.mesure_eolien,
            {haversine_sql} AS distance_km
        FROM installations i
        CROSS JOIN stations s
        WHERE
            -- Filter by compatibility
            (i.type_energie = 'solaire' AND s.mesure_solaire = TRUE)
            OR (i.type_energie = 'eolien' AND s.mesure_eolien = TRUE)
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id_peps
                ORDER BY distance_km ASC
            ) AS rn
        FROM distances
        WHERE distance_km <= {max_distance_km}
    )
    SELECT
        id_peps,
        nom_installation,
        type_energie,
        code_filiere,
        filiere,
        technologie,
        puissance_max_installee,
        commune,
        departement,
        region,
        code_iris,
        centroid_lat,
        centroid_lon,
        date_mise_en_service,
        station_id AS station_meteo_id,
        station_nom AS station_meteo_nom,
        station_lat AS station_meteo_lat,
        station_lon AS station_meteo_lon,
        distance_km AS distance_station_km
    FROM ranked
    WHERE rn = 1
    """

    result_with_stations = conn.execute(query).pl()

    # Get installations without a nearby station (beyond max_distance_km)
    installations_with_stations = set(result_with_stations["id_peps"].to_list())
    all_installations = set(df_installations["id_peps"].to_list())
    installations_without = all_installations - installations_with_stations

    logger.debug(
        "Distance calculation completed",
        extra={
            "with_station": len(installations_with_stations),
            "without_station": len(installations_without),
        },
    )

    # Get installations without stations and add NULL station columns
    if installations_without:
        df_without = df_installations.filter(pl.col("id_peps").is_in(list(installations_without)))
        df_without = df_without.select(
            "id_peps",
            "nom_installation",
            "type_energie",
            "code_filiere",
            "filiere",
            "technologie",
            "puissance_max_installee",
            "commune",
            "departement",
            "region",
            "code_iris",
            "centroid_lat",
            "centroid_lon",
            "date_mise_en_service",
        ).with_columns(
            pl.lit(None).cast(pl.String).alias("station_meteo_id"),
            pl.lit(None).cast(pl.String).alias("station_meteo_nom"),
            pl.lit(None).cast(pl.Float64).alias("station_meteo_lat"),
            pl.lit(None).cast(pl.Float64).alias("station_meteo_lon"),
            pl.lit(None).cast(pl.Float64).alias("distance_station_km"),
        )
        result = pl.concat([result_with_stations, df_without])
    else:
        result = result_with_stations

    conn.close()

    # Sort by type_energie and puissance for readability
    return result.sort(["type_energie", "puissance_max_installee"], descending=[False, True])
