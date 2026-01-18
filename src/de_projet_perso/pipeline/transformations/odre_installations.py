"""Transformations for ODRE installations dataset."""

from pathlib import Path

import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("odre_installations")
def transform_bronze_odre(dataset: Dataset, landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE installations.

    Args:
        dataset: Dataset configuration from catalog
        landing_path: Actual path to landing Parquet file (with original filename)

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    # TODO: Implement actual transformation
    # For now, read and return as-is
    df = pl.read_parquet(landing_path)
    return df


@register_silver("odre_installations")
def transform_silver_odre(dataset: Dataset) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Args:
        dataset: Dataset configuration (reads from bronze)

    Returns:
        Silver layer DataFrame
    """
    # TODO: Implement actual transformation
    bronze_path = dataset.get_bronze_path()
    df = pl.read_parquet(bronze_path)
    return df
