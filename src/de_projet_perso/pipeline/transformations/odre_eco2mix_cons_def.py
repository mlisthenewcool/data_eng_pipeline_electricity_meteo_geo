"""Transformations for ODRE eco2mix_cons_def dataset."""

from pathlib import Path

import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("odre_eco2mix_cons_def")
def transform_bronze_odre(dataset: Dataset, landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE eco2mix_cons_def.

    Args:
        dataset: Dataset configuration from catalog
        landing_path: Actual path to landing file (e.g., data/landing/ign_contours_iris/iris.gpkg)

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    return pl.read_parquet(landing_path)


@register_silver("odre_eco2mix_cons_def")
def transform_silver_odre(dataset: Dataset) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Args:
        dataset: Dataset configuration (reads from bronze)

    Returns:
        Silver layer DataFrame
    """
    return pl.read_parquet(dataset.get_bronze_path())
