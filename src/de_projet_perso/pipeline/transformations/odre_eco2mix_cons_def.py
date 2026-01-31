"""Transformations for ODRE eco2mix_cons_def dataset."""

from pathlib import Path

import polars as pl


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE eco2mix_cons_def.

    Args:
        landing_path: Path to the parquet file from landing layer

    Returns:
        DataFrame ready for bronze layer
    """
    return pl.read_parquet(landing_path)


def transform_silver(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for ODRE eco2mix_cons_def.

    Args:
        latest_bronze_path: Path to the latest bronze parquet file

    Returns:
        Silver layer DataFrame
    """
    return pl.read_parquet(latest_bronze_path)
