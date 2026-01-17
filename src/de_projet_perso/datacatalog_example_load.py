"""TODO documentation."""

import sys

from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.datacatalog import DataCatalog
from de_projet_perso.pipeline.state import PipelineStateManager

try:
    catalog = DataCatalog.load(settings.data_catalog_file_path)
except InvalidCatalogError as e:
    logger.exception(message=str(e), extra=e.validation_errors)
    sys.exit(1)

logger.info(message=f"Catalog loaded: {len(catalog.datasets)} dataset(s)")

for name, dataset in catalog.datasets.items():
    logger.info(
        message=f"dataset: {name}",
        extra={
            "provider": dataset.source.provider,
            "version": dataset.ingestion.version,
            "format": dataset.source.format.value,
            "storage (landing)": dataset.get_storage_path(layer="landing"),
        },
    )

    pipeline_state_manager = PipelineStateManager()
