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


def _get_silver_asset(dataset_name: str) -> Asset:
    """Create a Silver layer Asset for a dataset.

    Args:
        dataset_name: Name of the Silver dataset (e.g., "ign_contours_iris")

    Returns:
        Asset configured for the Silver layer output

    Example:
        >>> asset = _get_silver_asset("ign_contours_iris")
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


def _get_gold_asset(dataset_name: str) -> Asset:
    """Create a Gold layer Asset for an analytical dataset.

    Args:
        dataset_name: Name of the Gold dataset (e.g., "installations_meteo")

    Returns:
        Asset configured for the Gold layer output

    Example:
        >>> asset = _get_gold_asset("installations_meteo")
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

ASSETS_SILVER = {
    "ign_contours_iris": _get_silver_asset("ign_contours_iris"),
    "meteo_france_stations": _get_silver_asset("meteo_france_stations"),
    "odre_installations": _get_silver_asset("odre_installations"),
    "odre_eco2mix_cons_def": _get_silver_asset("odre_eco2mix_cons_def"),
    "odre_eco2mix_tr": _get_silver_asset("odre_eco2mix_tr"),
}

# =============================================================================
# Pre-defined Gold Assets
# =============================================================================

ASSETS_GOLD = {"installations_meteo": _get_gold_asset("installations_meteo")}
