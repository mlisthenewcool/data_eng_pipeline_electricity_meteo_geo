"""Transformations for ODRE installations dataset."""

import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("odre_installations")
def transform_bronze_odre(dataset: Dataset) -> pl.DataFrame:
    """Bronze transformation for ODRE installations.

    Args:
        dataset: Dataset configuration from catalog

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    return pl.read_parquet(dataset.get_landing_path())


@register_silver("odre_installations")
def transform_silver_odre(dataset: Dataset) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Args:
        dataset: Dataset configuration (reads from bronze)

    Returns:
        Silver layer DataFrame
    """
    return pl.read_parquet(dataset.get_bronze_path())
