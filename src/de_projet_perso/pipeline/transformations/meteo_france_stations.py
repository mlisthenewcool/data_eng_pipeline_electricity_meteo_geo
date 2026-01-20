"""Transformations for Météo France stations dataset."""

from pathlib import Path

import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("meteo_france_stations")
def transform_bronze(dataset: Dataset, landing_path: Path) -> pl.DataFrame:
    """TODO."""
    return pl.read_json(landing_path)


@register_silver("meteo_france_stations")
def transform_silver(dataset: Dataset) -> pl.DataFrame:
    """TODO."""
    return pl.read_parquet(dataset.get_bronze_path())
