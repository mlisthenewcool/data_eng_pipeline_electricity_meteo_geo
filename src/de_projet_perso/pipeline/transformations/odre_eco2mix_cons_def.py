"""Transformations for ODRE eco2mix_cons_def dataset."""

from pathlib import Path

import polars as pl

from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.validators import (
    SCHEMA_ODRE_ECO2MIX_CONS_DEF,
    validate_odre_eco2mix_cons_def,
)


def transform_bronze(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE eco2mix_cons_def.

    Args:
        landing_path: Path to the parquet file from landing layer

    Returns:
        DataFrame ready for bronze layer
    """
    return pl.read_parquet(landing_path)


def transform_silver(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for ODRE eco2mix_cons_def.

    Selects only the columns defined in the schema, dropping any spurious
    columns from the source (e.g., column_30 from the ODRE API).

    Args:
        latest_bronze_path: Path to the latest bronze parquet file

    Returns:
        Silver layer DataFrame with exact schema
    """
    logger.info("Reading from bronze", extra={"path": str(latest_bronze_path)})
    df = pl.read_parquet(latest_bronze_path)

    # Select only expected columns in the correct order
    # This drops any extra columns (like column_30) from the source
    df = df.select(list(SCHEMA_ODRE_ECO2MIX_CONS_DEF.names()))

    # Deduplicate on primary key (region + datetime)
    # Keep last occurrence (most recent data from source)
    # This handles DST transitions where ODRE API may return duplicate timestamps
    df = df.unique(subset=["code_insee_region", "date_heure"], keep="last")

    # Validate the output
    validate_odre_eco2mix_cons_def(df)

    logger.info(
        "Silver transformation completed", extra={"rows": len(df), "columns": len(df.columns)}
    )
    return df
