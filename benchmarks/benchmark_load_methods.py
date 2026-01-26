#!/usr/bin/env python3
"""Benchmark comparison: executemany vs COPY + staging table.

This script demonstrates the performance difference between:
1. executemany - row-by-row INSERT (slow)
2. COPY + staging - bulk COPY then UPSERT (fast)

Usage:
    uv run python scripts/benchmark_load_methods.py

Requirements:
    - PostgreSQL running (docker compose up -d postgres_service)
    - Database initialized (loader.initialize_all())
"""

import time
from datetime import datetime

import polars as pl
from psycopg import sql

from de_projet_perso.core.logger import logger
from de_projet_perso.database.connection import DatabaseConnection
from de_projet_perso.database.loader import PostgresLoader


def generate_test_data(n_rows: int) -> pl.DataFrame:
    """Generate synthetic test data for benchmarking.

    Args:
        n_rows: Number of rows to generate

    Returns:
        DataFrame with columns matching silver.dim_stations_meteo
    """
    return pl.DataFrame(
        {
            "id_station": [f"STATION_{i:06d}" for i in range(n_rows)],
            "nom_usuel": [f"Station Test {i}" for i in range(n_rows)],
            "latitude": [45.0 + (i * 0.001) for i in range(n_rows)],
            "longitude": [2.0 + (i * 0.001) for i in range(n_rows)],
            "altitude": [i % 2000 for i in range(n_rows)],
            "date_debut": [datetime(2020, 1, 1)] * n_rows,
            "date_fin": [None] * n_rows,
            "type_poste": ["SYNOP"] * n_rows,
            "region": ["Auvergne-Rhone-Alpes"] * n_rows,
            "departement": ["Puy-de-Dome"] * n_rows,
        }
    )


def benchmark_executemany(df: pl.DataFrame, conn: DatabaseConnection) -> float:
    """Benchmark executemany (row-by-row INSERT).

    Args:
        df: DataFrame to insert
        conn: Database connection

    Returns:
        Elapsed time in seconds
    """
    upsert_sql = b"""
    INSERT INTO silver.dim_stations_meteo (
        id_station, nom_usuel, latitude, longitude, altitude,
        date_debut, date_fin, type_poste, region, departement
    ) VALUES (
        %(id_station)s, %(nom_usuel)s, %(latitude)s, %(longitude)s, %(altitude)s,
        %(date_debut)s, %(date_fin)s, %(type_poste)s, %(region)s, %(departement)s
    )
    ON CONFLICT (id_station) DO UPDATE SET
        nom_usuel = EXCLUDED.nom_usuel,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        altitude = EXCLUDED.altitude,
        date_debut = EXCLUDED.date_debut,
        date_fin = EXCLUDED.date_fin,
        type_poste = EXCLUDED.type_poste,
        region = EXCLUDED.region,
        departement = EXCLUDED.departement,
        updated_at = NOW()
    """

    rows = df.to_dicts()

    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.executemany(upsert_sql, rows)
    elapsed = time.perf_counter() - start

    return elapsed


def benchmark_copy_staging(df: pl.DataFrame, conn: DatabaseConnection) -> float:
    """Benchmark COPY + staging table approach.

    Args:
        df: DataFrame to insert
        conn: Database connection

    Returns:
        Elapsed time in seconds
    """
    staging_table = "staging_benchmark"
    target_table = ("silver", "dim_stations_meteo")
    copy_columns = list(df.columns)

    upsert_sql = b"""
    INSERT INTO silver.dim_stations_meteo (
        id_station, nom_usuel, latitude, longitude, altitude,
        date_debut, date_fin, type_poste, region, departement
    )
    SELECT
        id_station, nom_usuel, latitude, longitude, altitude,
        date_debut, date_fin, type_poste, region, departement
    FROM staging_benchmark
    ON CONFLICT (id_station) DO UPDATE SET
        nom_usuel = EXCLUDED.nom_usuel,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        altitude = EXCLUDED.altitude,
        date_debut = EXCLUDED.date_debut,
        date_fin = EXCLUDED.date_fin,
        type_poste = EXCLUDED.type_poste,
        region = EXCLUDED.region,
        departement = EXCLUDED.departement,
        updated_at = NOW()
    """

    raw_conn = conn.connect()

    start = time.perf_counter()
    with raw_conn.cursor() as cur:
        # 1. Create staging table
        create_sql = sql.SQL(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP"
        ).format(sql.Identifier(staging_table), sql.Identifier(*target_table))
        cur.execute(create_sql)

        # 2. COPY data to staging
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
            sql.Identifier(staging_table),
            sql.SQL(", ").join(sql.Identifier(c) for c in copy_columns),
        )
        with cur.copy(copy_sql) as copy:
            for row in df.iter_rows():
                copy.write_row(row)

        # 3. UPSERT from staging
        cur.execute(upsert_sql)

    raw_conn.commit()
    elapsed = time.perf_counter() - start

    return elapsed


def cleanup_table(conn: DatabaseConnection) -> None:
    """Delete test data from table."""
    with conn.cursor() as cur:
        cur.execute(b"DELETE FROM silver.dim_stations_meteo WHERE id_station LIKE 'STATION_%'")


def main() -> None:
    """Run benchmarks and print results."""
    logger.info("Starting benchmark: executemany vs COPY + staging")

    # Initialize
    conn = DatabaseConnection()
    loader = PostgresLoader(conn)
    loader.initialize_all()

    # Test sizes
    test_sizes = [100, 1000, 5000, 10000]

    results = []
    for n_rows in test_sizes:
        logger.info(f"Testing with {n_rows:,} rows...")

        df = generate_test_data(n_rows)

        # Benchmark executemany
        cleanup_table(conn)
        time_executemany = benchmark_executemany(df, conn)

        # Benchmark COPY + staging
        cleanup_table(conn)
        time_copy = benchmark_copy_staging(df, conn)

        # Calculate speedup
        speedup = time_executemany / time_copy if time_copy > 0 else float("inf")

        results.append(
            {
                "rows": n_rows,
                "executemany_sec": time_executemany,
                "copy_staging_sec": time_copy,
                "speedup": speedup,
            }
        )

        logger.info(
            f"  executemany: {time_executemany:.3f}s | "
            f"COPY+staging: {time_copy:.3f}s | "
            f"Speedup: {speedup:.1f}x"
        )

    # Cleanup
    cleanup_table(conn)

    # Summary
    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS: executemany vs COPY + staging")
    print("=" * 70)
    print(f"{'Rows':>10} | {'executemany':>12} | {'COPY+staging':>12} | {'Speedup':>10}")
    print("-" * 70)
    for r in results:
        print(
            f"{r['rows']:>10,} | "
            f"{r['executemany_sec']:>10.3f}s | "
            f"{r['copy_staging_sec']:>10.3f}s | "
            f"{r['speedup']:>9.1f}x"
        )
    print("=" * 70)
    print("\nConclusion: COPY + staging is significantly faster for bulk loads.")
    print("Expected speedup: 10-100x depending on row count and table complexity.\n")


if __name__ == "__main__":
    main()
