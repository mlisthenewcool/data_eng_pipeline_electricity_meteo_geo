"""Transformations for ODRE installations dataset."""

import polars as pl

from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("odre_installations")
def transform_bronze_odre(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Bronze transformation for ODRE installations.

    No specific transformation needed at bronze level for this dataset.
    The default normalization (snake_case columns) is sufficient.

    Args:
        df: Input DataFrame from landing layer
        dataset_name: Dataset identifier

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    return df  # Pass-through


@register_silver("odre_installations")
def transform_silver_odre(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Applies business transformations:
    - Parse date columns
    - Cast numeric columns
    - Filter to active installations only

    Args:
        df: Input DataFrame from bronze layer
        dataset_name: Dataset identifier

    Returns:
        Transformed DataFrame ready for silver layer
    """
    transformations = []

    # Parse dates if column exists
    if "date_mise_service" in df.columns:
        transformations.append(pl.col("date_mise_service").str.to_date().alias("date_mise_service"))

    # Cast power to numeric if exists
    if "puissance_mw" in df.columns:
        transformations.append(pl.col("puissance_mw").cast(pl.Float64))

    # Apply transformations if any
    if transformations:
        df = df.with_columns(transformations)

    # Filter to active installations if status column exists
    if "statut" in df.columns:
        df = df.filter(pl.col("statut") == "En service")

    return df
