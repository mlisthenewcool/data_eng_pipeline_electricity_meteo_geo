"""TODO."""

import sys

from de_projet_perso.core.catalog import DataCatalog
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import DATA_CATALOG_PATH

try:
    catalog = DataCatalog.load(DATA_CATALOG_PATH)
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
            "format": dataset.source.format.name,
            "storage": dataset.get_storage_path(),
        },
    )
