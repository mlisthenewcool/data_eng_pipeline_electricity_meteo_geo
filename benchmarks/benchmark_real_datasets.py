#!/usr/bin/env python3
"""Benchmark loading real datasets to PostgreSQL.

This script measures the actual loading time for each Silver dataset
using the COPY + staging approach implemented in PostgresLoader.

Usage:
    uv run python scripts/benchmark_real_datasets.py

Requirements:
    - PostgreSQL running (docker compose up -d postgres_service)
    - Silver parquet files exist (run pipeline first)
"""

import time
from pathlib import Path

import polars as pl

from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.database.connection import DatabaseConnection
from de_projet_perso.database.loader import PostgresLoader

# Datasets with their expected characteristics
DATASETS = [
    {
        "name": "meteo_france_stations",
        "type": "dimension",
        "expected_rows": "~2.4k",
    },
    {
        "name": "ign_contours_iris",
        "type": "dimension",
        "expected_rows": "~48k",
    },
    {
        "name": "odre_installations",
        "type": "dimension",
        "expected_rows": "~124k",
    },
    {
        "name": "odre_eco2mix_tr",
        "type": "fact",
        "expected_rows": "~417k",
    },
    {
        "name": "odre_eco2mix_cons_def",
        "type": "fact",
        "expected_rows": "~2.5M",
    },
]


def get_parquet_stats(path: Path) -> dict:
    """Get stats for a parquet file.

    Args:
        path: Path to parquet file

    Returns:
        Dict with row count and file size
    """
    if not path.exists():
        return {"exists": False, "rows": 0, "size_mb": 0}

    df = pl.scan_parquet(path)
    row_count = df.select(pl.len()).collect().item()
    size_mb = path.stat().st_size / (1024 * 1024)

    return {"exists": True, "rows": row_count, "size_mb": size_mb}


def benchmark_dataset(loader: PostgresLoader, dataset_name: str) -> dict:
    """Benchmark loading a single dataset.

    Args:
        loader: PostgresLoader instance
        dataset_name: Dataset identifier

    Returns:
        Dict with timing and row count
    """
    resolver = PathResolver(dataset_name)
    parquet_path = resolver.silver_current_path

    # Get file stats
    stats = get_parquet_stats(parquet_path)
    if not stats["exists"]:
        return {
            "success": False,
            "error": f"File not found: {parquet_path}",
            "rows": 0,
            "elapsed_sec": 0,
            "rows_per_sec": 0,
        }

    # Check if SQL exists
    if not loader.has_silver_sql(dataset_name):
        return {
            "success": False,
            "error": "No SQL file configured",
            "rows": stats["rows"],
            "elapsed_sec": 0,
            "rows_per_sec": 0,
        }

    # Benchmark the load
    start = time.perf_counter()
    try:
        rows_affected = loader.load_silver(dataset_name, parquet_path)
        elapsed = time.perf_counter() - start
        rows_per_sec = stats["rows"] / elapsed if elapsed > 0 else 0

        return {
            "success": True,
            "error": None,
            "rows": rows_affected,
            "file_rows": stats["rows"],
            "file_size_mb": stats["size_mb"],
            "elapsed_sec": elapsed,
            "rows_per_sec": rows_per_sec,
        }
    except Exception as e:
        elapsed = time.perf_counter() - start
        return {
            "success": False,
            "error": str(e),
            "rows": 0,
            "elapsed_sec": elapsed,
            "rows_per_sec": 0,
        }


def main() -> None:
    """Run benchmarks for all real datasets."""
    logger.info("Starting benchmark: Real dataset loading times")

    # Initialize
    conn = DatabaseConnection()
    loader = PostgresLoader(conn)

    # Initialize schema (idempotent)
    logger.info("Initializing database schema...")
    loader.initialize_all()

    # Benchmark each dataset
    results = []
    for dataset_info in DATASETS:
        name = dataset_info["name"]
        logger.info(f"Benchmarking {name} ({dataset_info['type']})...")

        result = benchmark_dataset(loader, name)
        result["name"] = name
        result["type"] = dataset_info["type"]
        result["expected"] = dataset_info["expected_rows"]
        results.append(result)

        if result["success"]:
            logger.info(
                f"  Loaded {result['rows']:,} rows in {result['elapsed_sec']:.2f}s "
                f"({result['rows_per_sec']:,.0f} rows/sec)"
            )
        else:
            logger.warning(f"  Failed: {result['error']}")

    # Summary
    print("\n" + "=" * 90)
    print("BENCHMARK RESULTS: Real Dataset Loading (COPY + staging)")
    print("=" * 90)
    print(
        f"{'Dataset':<25} | {'Type':<10} | {'Rows':>10} | "
        f"{'Size MB':>8} | {'Time':>8} | {'rows/sec':>12}"
    )
    print("-" * 90)

    total_rows = 0
    total_time = 0

    for r in results:
        if r["success"]:
            total_rows += r["rows"]
            total_time += r["elapsed_sec"]
            print(
                f"{r['name']:<25} | {r['type']:<10} | {r['rows']:>10,} | "
                f"{r.get('file_size_mb', 0):>7.1f} | {r['elapsed_sec']:>7.2f}s | "
                f"{r['rows_per_sec']:>11,.0f}"
            )
        else:
            print(f"{r['name']:<25} | {r['type']:<10} | {'SKIPPED':>10} | {r['error']}")

    print("-" * 90)
    if total_time > 0:
        print(
            f"{'TOTAL':<25} | {'':<10} | {total_rows:>10,} | "
            f"{'':<8} | {total_time:>7.2f}s | "
            f"{total_rows / total_time:>11,.0f}"
        )
    print("=" * 90)

    print("\nNotes:")
    print("- COPY + staging is used (10-100x faster than executemany)")
    print("- Dimension tables use UPSERT for deduplication")
    print("- Fact tables could use TRUNCATE+INSERT for even better performance")
    print("- Performance depends on network, disk I/O, and PostgreSQL configuration\n")


if __name__ == "__main__":
    main()
