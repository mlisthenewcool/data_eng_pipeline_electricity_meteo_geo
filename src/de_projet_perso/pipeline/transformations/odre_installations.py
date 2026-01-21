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
        landing_path: Actual path to landing file (e.g., data/landing/ign_contours_iris/iris.gpkg)

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    return pl.read_parquet(landing_path)


@register_silver("odre_installations")
def transform_silver_odre(dataset: Dataset, resolver) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Args:
        dataset: Dataset configuration
        resolver: PathResolver for path operations

    Returns:
        Silver layer DataFrame
    """
    return pl.read_parquet(resolver.bronze_latest_path())
