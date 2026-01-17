"""Transformations for IGN Contours IRIS dataset."""

import polars as pl

from de_projet_perso.pipeline.transformations import register_bronze, register_silver


@register_bronze("ign_contours_iris")
def transform_bronze_ign_iris(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Bronze transformation for IGN Contours IRIS.

    Filters out rows without valid IRIS codes.

    Args:
        df: Input DataFrame from landing layer
        dataset_name: Dataset identifier

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    return df.filter(pl.col("code_iris").is_not_null())


@register_silver("ign_contours_iris")
def transform_silver_ign_iris(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Silver transformation for IGN Contours IRIS.

    Applies business transformations:
    - Casts area to Float64
    - Adds calculated columns (area in hectares)
    - Cleans text columns

    Args:
        df: Input DataFrame from bronze layer
        dataset_name: Dataset identifier

    Returns:
        Transformed DataFrame ready for silver layer
    """
    # Check if expected columns exist before transforming
    if "nom_iris" not in df.columns:
        # Fallback if schema is different
        return df

    return df.with_columns(
        [
            # Cast numeric columns if they exist
            pl.col("area_m2").cast(pl.Float64).alias("area_m2")
            if "area_m2" in df.columns
            else pl.lit(None).alias("area_m2"),
            # Add calculated columns
            (pl.col("area_m2") / 10000).alias("area_hectares")
            if "area_m2" in df.columns
            else pl.lit(None).alias("area_hectares"),
            # Clean text columns
            pl.col("nom_iris").str.to_uppercase().alias("nom_iris_upper"),
        ]
    )
