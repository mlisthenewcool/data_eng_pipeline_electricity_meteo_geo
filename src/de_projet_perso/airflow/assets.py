"""Centralized Asset definitions for Airflow DAGs.

This module provides a single source of truth for all Asset definitions
used across Silver and Gold DAGs. This ensures consistency and prevents
the "Cannot activate asset" warning when the same Asset is defined
with different URIs in different DAGs.

Usage:
    from de_projet_perso.airflow.assets import (
        get_silver_asset,
        get_gold_asset,
        # Pre-defined assets
        ODRE_INSTALLATIONS_SILVER,
        IGN_CONTOURS_IRIS_SILVER,
        METEO_FRANCE_STATIONS_SILVER,
        INSTALLATIONS_METEO_GOLD
    )
"""

from typing import Any

from airflow.sdk import Asset

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.path_resolver import PathResolver


def get_silver_asset(dataset: Dataset | str) -> Asset:
    """Create a Silver layer Asset for a dataset.

    Args:
        dataset: Dataset object or dataset name string

    Returns:
        Asset configured for the Silver layer output

    Example:
        >>> asset = get_silver_asset("odre_installations")
        >>> asset.name
        'odre_installations_silver'
    """
    dataset_name = dataset.name if isinstance(dataset, Dataset) else dataset
    resolver = PathResolver(dataset_name=dataset_name)

    return Asset(
        name=f"{dataset_name}_silver",
        uri=f"file:///{resolver.silver_current_path}",
        group="data-pipeline",
        extra={
            "layer": "silver",
            "dataset_name": dataset_name,
        },
    )


def get_gold_asset(dataset_name: str, description: str | None = None) -> Asset:
    """Create a Gold layer Asset for an analytical dataset.

    Args:
        dataset_name: Name of the Gold dataset (e.g., "installations_meteo")
        description: Optional description for the Asset

    Returns:
        Asset configured for the Gold layer output

    Example:
        >>> asset = get_gold_asset("installations_meteo")
        >>> asset.name
        'installations_meteo_gold'
    """
    resolver = PathResolver(dataset_name=dataset_name)

    extra: dict[str, Any] = {
        "layer": "gold",
        "dataset_name": dataset_name,
    }
    if description:
        extra["description"] = description

    return Asset(
        name=f"{dataset_name}_gold",
        uri=f"file:///{resolver.gold_current_path}",
        group="gold-layer",
        extra=extra,  # type: ignore[arg-type]  # Airflow JsonValue typing issue
    )


# =============================================================================
# Pre-defined Silver Assets
# =============================================================================
# These are instantiated at module load time for convenience.
# Use these when referencing Silver assets in Gold DAGs or cross-DAG dependencies.

ODRE_INSTALLATIONS_SILVER = get_silver_asset("odre_installations")
IGN_CONTOURS_IRIS_SILVER = get_silver_asset("ign_contours_iris")
METEO_FRANCE_STATIONS_SILVER = get_silver_asset("meteo_france_stations")
ODRE_ECO2MIX_CONS_DEF_SILVER = get_silver_asset("odre_eco2mix_cons_def")
ODRE_ECO2MIX_TR_SILVER = get_silver_asset("odre_eco2mix_tr")

# =============================================================================
# Pre-defined Gold Assets
# =============================================================================

INSTALLATIONS_METEO_GOLD = get_gold_asset(
    "installations_meteo",
    description="Renewable installations linked to nearest weather stations",
)
