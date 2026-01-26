#!/usr/bin/env python3
"""Benchmark comparison: UPSERT vs TRUNCATE+INSERT for fact tables.

This script demonstrates the potential performance gains from using
TRUNCATE + INSERT instead of UPSERT for large fact tables where:
- Full refresh is acceptable (no history preservation needed)
- No foreign key constraints prevent TRUNCATE
- Performance is critical

Usage:
    uv run python scripts/benchmark_truncate_vs_upsert.py

Requirements:
    - PostgreSQL running (docker compose up -d postgres_service)
    - Database initialized

WARNING: This script creates and drops test tables. It does NOT modify
production tables.
"""

import time
from datetime import datetime

import polars as pl
from psycopg import sql

from de_projet_perso.core.logger import logger
from de_projet_perso.database.connection import DatabaseConnection


def generate_fact_data(n_rows: int) -> pl.DataFrame:
    """Generate synthetic fact table data.

    Args:
        n_rows: Number of rows to generate

    Returns:
        DataFrame mimicking fact_eco2mix_tr structure
    """
    return pl.DataFrame(
        {
            "id": list(range(n_rows)),
            "date_heure": [datetime(2024, 1, 1, i % 24, 0, 0) for i in range(n_rows)],
            "code_insee_region": ["84"] * n_rows,  # Auvergne-Rhone-Alpes
            "region": ["Auvergne-Rhone-Alpes"] * n_rows,
            "consommation": [float(1000 + i % 500) for i in range(n_rows)],
            "thermique": [float(100 + i % 50) for i in range(n_rows)],
            "nucleaire": [float(500 + i % 200) for i in range(n_rows)],
            "eolien": [float(50 + i % 30) for i in range(n_rows)],
            "solaire": [float(20 + i % 15) for i in range(n_rows)],
            "hydraulique": [float(200 + i % 100) for i in range(n_rows)],
            "pompage": [float(10 + i % 5) for i in range(n_rows)],
            "bioenergies": [float(5 + i % 3) for i in range(n_rows)],
            "ech_physiques": [float(50 + i % 25) for i in range(n_rows)],
            "stockage_batterie": [float(1 + i % 2) for i in range(n_rows)],
            "destockage_batterie": [float(1 + i % 2) for i in range(n_rows)],
        }
    )


def setup_test_table(conn: DatabaseConnection) -> None:
    """Create test table for benchmarking."""
    create_sql = b"""
    DROP TABLE IF EXISTS benchmark_fact_test;
    CREATE TABLE benchmark_fact_test (
        id BIGINT PRIMARY KEY,
        date_heure TIMESTAMP NOT NULL,
        code_insee_region VARCHAR(10) NOT NULL,
        region VARCHAR(100) NOT NULL,
        consommation DOUBLE PRECISION,
        thermique DOUBLE PRECISION,
        nucleaire DOUBLE PRECISION,
        eolien DOUBLE PRECISION,
        solaire DOUBLE PRECISION,
        hydraulique DOUBLE PRECISION,
        pompage DOUBLE PRECISION,
        bioenergies DOUBLE PRECISION,
        ech_physiques DOUBLE PRECISION,
        stockage_batterie DOUBLE PRECISION,
        destockage_batterie DOUBLE PRECISION,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)


def benchmark_upsert(df: pl.DataFrame, conn: DatabaseConnection) -> float:
    """Benchmark COPY + staging + UPSERT approach.

    Args:
        df: DataFrame to insert
        conn: Database connection

    Returns:
        Elapsed time in seconds
    """
    staging_table = "staging_benchmark_fact"
    copy_columns = [c for c in df.columns if c != "updated_at"]

    upsert_sql = b"""
    INSERT INTO benchmark_fact_test (
        id, date_heure, code_insee_region, region, consommation,
        thermique, nucleaire, eolien, solaire, hydraulique,
        pompage, bioenergies, ech_physiques, stockage_batterie, destockage_batterie
    )
    SELECT
        id, date_heure, code_insee_region, region, consommation,
        thermique, nucleaire, eolien, solaire, hydraulique,
        pompage, bioenergies, ech_physiques, stockage_batterie, destockage_batterie
    FROM staging_benchmark_fact
    ON CONFLICT (id) DO UPDATE SET
        date_heure = EXCLUDED.date_heure,
        code_insee_region = EXCLUDED.code_insee_region,
        region = EXCLUDED.region,
        consommation = EXCLUDED.consommation,
        thermique = EXCLUDED.thermique,
        nucleaire = EXCLUDED.nucleaire,
        eolien = EXCLUDED.eolien,
        solaire = EXCLUDED.solaire,
        hydraulique = EXCLUDED.hydraulique,
        pompage = EXCLUDED.pompage,
        bioenergies = EXCLUDED.bioenergies,
        ech_physiques = EXCLUDED.ech_physiques,
        stockage_batterie = EXCLUDED.stockage_batterie,
        destockage_batterie = EXCLUDED.destockage_batterie,
        updated_at = NOW()
    """

    raw_conn = conn.connect()

    start = time.perf_counter()
    with raw_conn.cursor() as cur:
        # Create staging
        create_sql = sql.SQL(
            "CREATE TEMP TABLE {} (LIKE benchmark_fact_test INCLUDING DEFAULTS) ON COMMIT DROP"
        ).format(sql.Identifier(staging_table))
        cur.execute(create_sql)

        # COPY to staging
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
            sql.Identifier(staging_table),
            sql.SQL(", ").join(sql.Identifier(c) for c in copy_columns),
        )
        with cur.copy(copy_sql) as copy:
            for row in df.select(copy_columns).iter_rows():
                copy.write_row(row)

        # UPSERT
        cur.execute(upsert_sql)

    raw_conn.commit()
    elapsed = time.perf_counter() - start

    return elapsed


def benchmark_truncate_insert(df: pl.DataFrame, conn: DatabaseConnection) -> float:
    """Benchmark TRUNCATE + COPY (no staging, no conflict checking).

    Args:
        df: DataFrame to insert
        conn: Database connection

    Returns:
        Elapsed time in seconds
    """
    copy_columns = [c for c in df.columns if c != "updated_at"]

    raw_conn = conn.connect()

    start = time.perf_counter()
    with raw_conn.cursor() as cur:
        # TRUNCATE (instant, no row-by-row delete)
        cur.execute(b"TRUNCATE TABLE benchmark_fact_test")

        # COPY directly to target (no staging needed)
        copy_sql = sql.SQL("COPY benchmark_fact_test ({}) FROM STDIN").format(
            sql.SQL(", ").join(sql.Identifier(c) for c in copy_columns)
        )
        with cur.copy(copy_sql) as copy:
            for row in df.select(copy_columns).iter_rows():
                copy.write_row(row)

    raw_conn.commit()
    elapsed = time.perf_counter() - start

    return elapsed


def cleanup(conn: DatabaseConnection) -> None:
    """Drop test table."""
    with conn.cursor() as cur:
        cur.execute(b"DROP TABLE IF EXISTS benchmark_fact_test")


def main() -> None:
    """Run benchmarks and print results."""
    logger.info("Starting benchmark: UPSERT vs TRUNCATE+INSERT for fact tables")

    conn = DatabaseConnection()

    # Test sizes (simulating fact table loads)
    test_sizes = [10000, 50000, 100000, 500000]

    results = []
    for n_rows in test_sizes:
        logger.info(f"Testing with {n_rows:,} rows...")

        df = generate_fact_data(n_rows)

        # Setup fresh table
        setup_test_table(conn)

        # First run: UPSERT (table is empty, so INSERT path)
        time_upsert_empty = benchmark_upsert(df, conn)

        # Second run: UPSERT (table has data, so UPDATE path)
        time_upsert_update = benchmark_upsert(df, conn)

        # Setup fresh table again
        setup_test_table(conn)

        # TRUNCATE+INSERT (always same speed)
        time_truncate = benchmark_truncate_insert(df, conn)

        # Calculate speedups
        speedup_vs_insert = time_upsert_empty / time_truncate if time_truncate > 0 else 0
        speedup_vs_update = time_upsert_update / time_truncate if time_truncate > 0 else 0

        results.append(
            {
                "rows": n_rows,
                "upsert_insert_sec": time_upsert_empty,
                "upsert_update_sec": time_upsert_update,
                "truncate_sec": time_truncate,
                "speedup_vs_insert": speedup_vs_insert,
                "speedup_vs_update": speedup_vs_update,
            }
        )

        logger.info(
            f"  UPSERT (insert): {time_upsert_empty:.3f}s | "
            f"UPSERT (update): {time_upsert_update:.3f}s | "
            f"TRUNCATE+INSERT: {time_truncate:.3f}s"
        )

    # Cleanup
    cleanup(conn)

    # Summary
    print("\n" + "=" * 95)
    print("BENCHMARK RESULTS: UPSERT vs TRUNCATE+INSERT for Fact Tables")
    print("=" * 95)
    print(
        f"{'Rows':>10} | {'UPSERT':>12} | {'UPSERT':>12} | {'TRUNCATE':>12} | "
        f"{'Speedup':>10} | {'Speedup':>10}"
    )
    print(
        f"{'':>10} | {'(INSERT)':>12} | {'(UPDATE)':>12} | {'+INSERT':>12} | "
        f"{'vs INSERT':>10} | {'vs UPDATE':>10}"
    )
    print("-" * 95)
    for r in results:
        print(
            f"{r['rows']:>10,} | "
            f"{r['upsert_insert_sec']:>10.3f}s | "
            f"{r['upsert_update_sec']:>10.3f}s | "
            f"{r['truncate_sec']:>10.3f}s | "
            f"{r['speedup_vs_insert']:>9.1f}x | "
            f"{r['speedup_vs_update']:>9.1f}x"
        )
    print("=" * 95)

    print("\nConclusions:")
    print("1. TRUNCATE+INSERT is always faster than UPSERT (no conflict checking)")
    print("2. UPSERT UPDATE path is slower than INSERT path (more I/O)")
    print("3. For large fact tables with full refresh, TRUNCATE+INSERT is recommended")
    print("\nWhen to use each strategy:")
    print("- UPSERT: Dimension tables, incremental loads, history preservation")
    print("- TRUNCATE+INSERT: Fact tables, full refresh, no FK constraints")
    print("\nNote: Current implementation uses UPSERT for all tables (safer default).")
    print("TRUNCATE+INSERT can be added as an option for fact tables if needed.\n")


if __name__ == "__main__":
    main()
