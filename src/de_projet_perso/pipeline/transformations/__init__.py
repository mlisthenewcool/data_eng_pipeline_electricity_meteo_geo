"""Transformation registry for dataset-specific processing.

Each dataset has a module at transformations/{dataset_name}.py exposing
one or more of these functions by convention:

    - transform_bronze(landing_path: Path) -> pl.DataFrame
    - transform_silver(latest_bronze_path: Path) -> pl.DataFrame
    - transform_gold(**kwargs) -> pl.DataFrame

The module filename must match the dataset_name from the catalog.
Transforms are loaded lazily via importlib when first requested.

Example:
    # transformations/my_dataset.py
    def transform_bronze(landing_path: Path) -> pl.DataFrame:
        return pl.read_parquet(landing_path)

    def transform_silver(latest_bronze_path: Path) -> pl.DataFrame:
        df = pl.read_parquet(latest_bronze_path)
        return df.filter(pl.col("status") == "active")
"""

import importlib
from collections.abc import Callable
from pathlib import Path

import polars as pl

# Type aliases for transform function signatures
BronzeTransformFunc = Callable[[Path], pl.DataFrame]
SilverTransformFunc = Callable[[Path], pl.DataFrame]
GoldTransformFunc = Callable[..., pl.DataFrame]

_BASE_MODULE = "de_projet_perso.pipeline.transformations"


def _load_transform(dataset_name: str, func_name: str) -> Callable[..., pl.DataFrame]:
    """Import a dataset module and return the named transform function.

    Args:
        dataset_name: Dataset identifier (must match a module filename).
        func_name: Function to retrieve (transform_bronze, transform_silver, transform_gold).

    Returns:
        The transform function from the dataset module.

    Raises:
        ModuleNotFoundError: If no module exists for dataset_name.
        AttributeError: If the module doesn't expose func_name.
    """
    module = importlib.import_module(f"{_BASE_MODULE}.{dataset_name}")
    return getattr(module, func_name)


def get_bronze_transform(dataset_name: str) -> BronzeTransformFunc:
    """Load the bronze transform for a dataset.

    Args:
        dataset_name: Dataset identifier (e.g., "ign_contours_iris").

    Returns:
        Bronze transformation function.

    Raises:
        ModuleNotFoundError: If no transformation module exists for this dataset.
        AttributeError: If the module doesn't expose transform_bronze.
    """
    return _load_transform(dataset_name, "transform_bronze")


def get_silver_transform(dataset_name: str) -> SilverTransformFunc:
    """Load the silver transform for a dataset.

    Args:
        dataset_name: Dataset identifier (e.g., "ign_contours_iris").

    Returns:
        Silver transformation function.

    Raises:
        ModuleNotFoundError: If no transformation module exists for this dataset.
        AttributeError: If the module doesn't expose transform_silver.
    """
    return _load_transform(dataset_name, "transform_silver")


def get_gold_transform(dataset_name: str) -> GoldTransformFunc:
    """Load the gold transform for a dataset.

    Args:
        dataset_name: Dataset identifier (e.g., "installations_meteo").

    Returns:
        Gold transformation function.

    Raises:
        ModuleNotFoundError: If no transformation module exists for this dataset.
        AttributeError: If the module doesn't expose transform_gold.
    """
    return _load_transform(dataset_name, "transform_gold")


__all__ = [
    "BronzeTransformFunc",
    "GoldTransformFunc",
    "SilverTransformFunc",
    "get_bronze_transform",
    "get_gold_transform",
    "get_silver_transform",
]
