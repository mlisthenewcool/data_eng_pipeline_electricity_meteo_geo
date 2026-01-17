"""Transformation registry for dataset-specific processing.

This module provides a registration system for bronze and silver transformations.
Each dataset can register custom transformation functions that will be applied
during pipeline execution.

Example:
    @register_bronze("my_dataset")
    def transform(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
        return df.filter(pl.col("status") == "active")
"""

from typing import Callable

import polars as pl

from de_projet_perso.core.data_catalog import Dataset

TransformFunction = Callable[[Dataset], pl.DataFrame]

# Private registries
_BRONZE_TRANSFORMS: dict[str, TransformFunction] = {}
_SILVER_TRANSFORMS: dict[str, TransformFunction] = {}


def register_bronze(dataset_name: str):
    """Decorator to register a bronze layer transformation.

    Args:
        dataset_name: Dataset identifier (must match catalog key)

    Example:
        @register_bronze("ign_contours_iris")
        def transform(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
            return df.filter(pl.col("code_iris").is_not_null())
    """

    def decorator(func: TransformFunction) -> TransformFunction:
        _BRONZE_TRANSFORMS[dataset_name] = func
        return func

    return decorator


def register_silver(dataset_name: str):
    """Decorator to register a silver layer transformation.

    Args:
        dataset_name: Dataset identifier (must match catalog key)

    Example:
        @register_silver("ign_contours_iris")
        def transform(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
            return df.with_columns([pl.col("area").cast(pl.Float64)])
    """

    def decorator(func: TransformFunction) -> TransformFunction:
        _SILVER_TRANSFORMS[dataset_name] = func
        return func

    return decorator


def get_bronze_transform(dataset_name: str) -> TransformFunction | None:
    """Get registered bronze transformation for a dataset.

    Args:
        dataset_name: Dataset identifier

    Returns:
        Transformation function or None if not registered
    """
    return _BRONZE_TRANSFORMS.get(dataset_name)


def get_silver_transform(dataset_name: str) -> TransformFunction | None:
    """Get registered silver transformation for a dataset.

    Args:
        dataset_name: Dataset identifier

    Returns:
        Transformation function or None if not registered
    """
    return _SILVER_TRANSFORMS.get(dataset_name)


# Import all transformation modules to trigger registration
# Add new transformation modules here as you create them
try:
    from de_projet_perso.pipeline.transformations import (  # noqa: F401
        ign_contours_iris,
        odre_installations,
    )
except ImportError as e:
    # Fail gracefully if transformation modules don't exist yet
    import warnings

    warnings.warn(f"Some transformation modules could not be loaded: {e}")


__all__ = [
    "register_bronze",
    "register_silver",
    "get_bronze_transform",
    "get_silver_transform",
    "TransformFunction",
]
