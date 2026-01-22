"""Transformation registry for dataset-specific processing.

This module provides a registration system for bronze and silver transformations.
Each dataset can register custom transformation functions that will be applied
during pipeline execution.

Example:
    from pathlib import Path

    @register_bronze("my_dataset")
    def transform(dataset: Dataset, landing_path: Path) -> pl.DataFrame:
        df = pl.read_parquet(landing_path)
        return df.filter(pl.col("status") == "active")
"""

from pathlib import Path
from typing import Callable

import polars as pl

# Bronze transforms receive the landing file path
BronzeTransformFunc = Callable[[Path], pl.DataFrame]

# Silver transforms receive latest bronze path
SilverTransformFunc = Callable[[Path], pl.DataFrame]

# Legacy type alias for backward compatibility
TransformFunction = BronzeTransformFunc

# Private registries
_BRONZE_TRANSFORMS: dict[str, BronzeTransformFunc] = {}
_SILVER_TRANSFORMS: dict[str, SilverTransformFunc] = {}


def register_bronze(dataset_name: str):
    """Decorator to register a bronze layer transformation.

    Bronze transformations receive:
    - dataset: Dataset configuration from catalog
    - landing_path: Actual path to the landing file (with original filename)

    Args:
        dataset_name: Dataset identifier (must match catalog key)

    Example:
        from pathlib import Path

        @register_bronze("ign_contours_iris")
        def transform(dataset: Dataset, landing_path: Path) -> pl.DataFrame:
            # Read from actual landing file (preserves original filename)
            df = pl.read_parquet(landing_path)
            return df.filter(pl.col("code_iris").is_not_null())
    """

    def decorator(func: BronzeTransformFunc) -> BronzeTransformFunc:
        _BRONZE_TRANSFORMS[dataset_name] = func
        return func

    return decorator


def register_silver(dataset_name: str):
    """Decorator to register a silver layer transformation.

    Silver transformations receive only the dataset configuration.
    They read from the standardized bronze path.

    Args:
        dataset_name: Dataset identifier (must match catalog key)

    Example:
        @register_silver("ign_contours_iris")
        def transform(dataset: Dataset) -> pl.DataFrame:
            # Read from bronze (standardized path)
            bronze_path = dataset.get_bronze_path()
            df = pl.read_parquet(bronze_path)
            return df.with_columns([pl.col("area").cast(pl.Float64)])
    """

    def decorator(func: SilverTransformFunc) -> SilverTransformFunc:
        _SILVER_TRANSFORMS[dataset_name] = func
        return func

    return decorator


def get_bronze_transform(dataset_name: str) -> BronzeTransformFunc | None:
    """Get registered bronze transformation for a dataset.

    Args:
        dataset_name: Dataset identifier

    Returns:
        Bronze transformation function or None if not registered
    """
    return _BRONZE_TRANSFORMS.get(dataset_name)


def get_silver_transform(dataset_name: str) -> SilverTransformFunc | None:
    """Get registered silver transformation for a dataset.

    Args:
        dataset_name: Dataset identifier

    Returns:
        Silver transformation function or None if not registered
    """
    return _SILVER_TRANSFORMS.get(dataset_name)


# TODO: fast-fail
# Import all transformation modules to trigger registration
# Add new transformation modules here as you create them
try:
    from de_projet_perso.pipeline.transformations import (  # noqa: F401
        ign_contours_iris,
        meteo_france_stations,
        odre_eco2mix_cons_def,
        odre_eco2mix_tr,
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
    "BronzeTransformFunc",
    "SilverTransformFunc",
]
