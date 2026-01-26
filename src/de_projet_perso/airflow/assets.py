"""Centralized Asset definitions for Airflow DAGs.

This module provides a single source of truth for all Asset definitions
used across Silver and Gold DAGs. This ensures consistency and prevents
the "Cannot activate asset" warning when the same Asset is defined
with different URIs in different DAGs.

TODO: improvements
    - add meaningful extras (description, ...)
"""

from airflow.sdk import Asset

from de_projet_perso.core.path_resolver import PathResolver


def get_silver_asset(dataset_name: str) -> Asset:
    """Create a Silver layer Asset for a dataset.

    Args:
        dataset_name: Name of the Silver dataset (e.g., "ign_contours_iris")

    Returns:
        Asset configured for the Silver layer output

    Example:
        >>> asset = get_silver_asset("ign_contours_iris")
        >>> asset.name
        'ign_contours_iris_silver'
    """
    resolver = PathResolver(dataset_name=dataset_name)

    return Asset(
        name=f"{dataset_name}_silver",
        uri=f"file:///{resolver.silver_current_path}",
        group="data-pipeline",
        extra={"dataset_name": dataset_name},
    )


def get_gold_asset(dataset_name: str) -> Asset:
    """Create a Gold layer Asset for an analytical dataset.

    Args:
        dataset_name: Name of the Gold dataset (e.g., "installations_meteo")

    Returns:
        Asset configured for the Gold layer output

    Example:
        >>> asset = get_gold_asset("installations_meteo")
        >>> asset.name
        'installations_meteo_gold'
    """
    resolver = PathResolver(dataset_name=dataset_name)

    return Asset(
        name=f"{dataset_name}_gold",
        uri=f"file:///{resolver.gold_current_path}",
        group="gold-layer",
        extra={"dataset_name": dataset_name},
    )


# =============================================================================
# Pre-defined Silver Assets
# =============================================================================
# These are instantiated at module load time for convenience.
# Use these when referencing Silver assets in Gold DAGs or cross-DAG dependencies.

ASSET_SILVER_IGN_CONTOURS_IRIS = get_silver_asset("ign_contours_iris")
ASSET_SILVER_METEO_FRANCE_STATIONS = get_silver_asset("meteo_france_stations")
ASSET_SILVER_ODRE_INSTALLATIONS = get_silver_asset("odre_installations")
ASSET_SILVER_ODRE_ECO2MIX_CONS_DEF = get_silver_asset("odre_eco2mix_cons_def")
ASSET_SILVER_ODRE_ECO2MIX_TR = get_silver_asset("odre_eco2mix_tr")

# =============================================================================
# Pre-defined Gold Assets
# =============================================================================

ASSET_GOLD_INSTALLATIONS_METEO = get_gold_asset("installations_meteo")
