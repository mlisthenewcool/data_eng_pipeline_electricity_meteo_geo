"""DAG generation entry point for Airflow.

This module dynamically generates DAGs from the data catalog, creating either:
- Simple DAGs for direct file downloads (Parquet, JSON, CSV)
- Archive DAGs for compressed sources (7z, zip)
"""

import traceback

from airflow.sdk import DAG

from de_projet_perso.airflow.dags.dataset_pipelines_factory import (
    _create_asset_for_dataset,
    _create_error_dag,
    create_archive_dag,
    create_simple_dag,
)
from de_projet_perso.core.data_catalog import DataCatalog, Dataset
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings


def _should_use_archive_dag(dataset: Dataset) -> bool:
    return dataset.source.format.is_archive


def _generate_all_dags() -> dict[str, DAG]:
    """Generate all DAGs from catalog with error handling.

    Returns:
        Dictionary mapping pipeline names to DAG objects.
        If catalog fails to load, returns a single error DAG.
    """
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        logger.exception(
            "Failed to load data catalog",
            extra={"path": str(settings.data_catalog_file_path), "errors": e.validation_errors},
        )
        # TODO: return {"catalog_error": _create_error_dag("catalog_load_error", str(e))}
        raise
    except Exception:
        logger.exception("Unexpected error loading catalog")
        # TODO : return {"catalog_error": _create_error_dag("catalog_load_error", str(e))}
        raise

    pipelines: dict[str, DAG] = {}

    for name, dataset in catalog.datasets.items():
        try:
            asset = _create_asset_for_dataset(dataset)

            # TODO: injecter directement la fonction si plus de types de DAGs
            if _should_use_archive_dag(dataset):
                dag_id = f"dag_archive_{name}"
                dag_obj = create_archive_dag(dataset, asset)
            else:
                dag_id = f"dag_simple_{name}"
                dag_obj = create_simple_dag(dataset, asset)

            pipelines[name] = dag_obj
            logger.info(f"Created dataset DAG: {dag_id}")
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            logger.exception(
                f"Failed to create DAG for {name}",
                extra={"error": error_msg, "traceback": traceback.format_exc()},
            )
            # TODO: Create individual error DAG with unique ID
            pipelines[f"{name}_error"] = _create_error_dag(
                dag_id=f"dataset_pipeline_{name}_error",
                error_message=error_msg,
            )
            raise

    return pipelines


# =============================================================================
# Module-Level DAG Exposure (Required by Airflow)
# =============================================================================
# Airflow scans module globals for DAG objects
# We must expose each DAG individually in the module namespace
_all_dags = _generate_all_dags()
