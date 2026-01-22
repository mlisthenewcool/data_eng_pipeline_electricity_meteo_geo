"""Transformations for ODRE eco2mix_tr dataset."""

from pathlib import Path

import polars as pl

from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("odre_eco2mix_tr")
def transform_bronze_odre(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE eco2mix_tr.

    Args:
        landing_path: Actual path to landing file (e.g., data/landing/ign_contours_iris/iris.gpkg)

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    return pl.read_parquet(landing_path)


@register_silver("odre_eco2mix_tr")
def transform_silver_odre(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for ODRE eco2mix_tr.

    Args:
        latest_bronze_path: ...

    Returns:
        Silver layer DataFrame
    """
    return pl.read_parquet(latest_bronze_path)
