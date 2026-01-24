"""DAG generation entry point for Airflow.

This module dynamically generates DAGs from the data catalog, creating either:
- Simple DAGs for direct file downloads (Parquet, JSON, CSV)
- Archive DAGs for compressed sources (7z, zip)
"""

import traceback

from airflow.sdk import DAG

from de_projet_perso.airflow.assets import get_silver_asset
from de_projet_perso.airflow.dags.dataset_pipelines_factory import (
    _create_error_dag,
    create_archive_dag,
    create_simple_dag,
)
from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.manager import PipelineManager


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
            asset = get_silver_asset(dataset)
            manager = PipelineManager(dataset=dataset)

            # TODO: injecter directement la fonction si plus de types de DAGs
            if dataset.source.format.is_archive:
                dag_id = f"dag_archive_{name}"
                dag_obj = create_archive_dag(manager, asset)
            else:
                dag_id = f"dag_simple_{name}"
                dag_obj = create_simple_dag(manager, asset)

            pipelines[name] = dag_obj
            logger.info(f"Created dataset DAG: {dag_id}")
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            logger.exception(
                f"Failed to create DAG for {name}",
                extra={"error": error_msg, "traceback": traceback.format_exc()},
            )
            # Create individual error DAG with unique ID
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
