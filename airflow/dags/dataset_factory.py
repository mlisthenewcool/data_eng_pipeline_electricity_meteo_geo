"""TODO: documentation."""

from airflow.sdk import Metadata, asset

from de_projet_perso.core.catalog import DataCatalog
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import DATA_CATALOG_PATH

catalog = DataCatalog.load(path=DATA_CATALOG_PATH)

for name, ds in catalog.datasets.items():

    @asset(
        name=f"asset_{name}",
        uri=f"file:///{ds.get_storage_path()}",  # TODO: version stable (latest)
        description=ds.description,
        schedule=ds.ingestion.frequency.airflow_schedule,
    )
    def dynamic_asset(current_asset):  # noqa: D103
        logger.info(message=f"inside {name} asset func !")

        # TODO: mettre la logique…
        # 1. déterminer action (en fonction de l'état du dataset et de sa fréquence)
        # TODO: quelles métadonnées ajouter à celles d'Airflow ?
        # * sha256
        # last_successful_ingestion ? normalement Airflow
        # last_error ? normalement Airflow
        # updated_at ? normalement Airflow
        # version_in_storage ? "latest" doit pointer là-dessus
        # bronze/silver status ? se baser sur les fichiers
        # bronze/silver paths ? se baser sur un pattern
        # history (started/finished_at, duration_seconds, status, version, sha256, paths, error)

        # TODO: devrait permettre de maintenir un data_catalog_state ?
        yield Metadata(
            asset=current_asset,
            extra={
                "version": ds.ingestion.version,
                "provider": ds.source.provider,
                "row_count": 7,
                "frequency": ds.ingestion.frequency,
            },
        )
