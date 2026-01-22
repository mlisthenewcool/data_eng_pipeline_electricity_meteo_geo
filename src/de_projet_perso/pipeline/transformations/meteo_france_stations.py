"""Transformations for Météo France stations dataset."""

from pathlib import Path

import polars as pl

from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("meteo_france_stations")
def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """TODO."""
    return pl.read_json(landing_path)


@register_silver("meteo_france_stations")
def transform_silver(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for Météo France stations.

    Args:
        latest_bronze_path: ...

    Returns:
        Silver layer DataFrame
    """
    return pl.read_parquet(latest_bronze_path)
