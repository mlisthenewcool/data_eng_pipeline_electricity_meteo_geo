"""Transformations for ODRE installations dataset."""

import polars as pl

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("odre_installations")
def transform_bronze_odre(dataset: Dataset) -> pl.DataFrame:
    """TODO."""
    raise NotImplementedError()


@register_silver("odre_installations")
def transform_silver_odre(dataset: Dataset) -> pl.DataFrame:
    """TODO."""
    raise NotImplementedError()
