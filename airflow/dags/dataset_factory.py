# noqa: D100
from airflow.sdk import dag

from de_projet_perso.core.catalog import DataCatalog
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import DATA_CATALOG_PATH

catalog = DataCatalog.load(path=DATA_CATALOG_PATH)

for name, ds in catalog.datasets.items():

    @dag(
        dag_id=f"ingest_{name}",
        schedule=ds.ingestion.frequency.airflow_schedule,
        catchup=False,
        tags=["ingestion", ds.source.provider],
    )
    def dynamic_dag():  # noqa: D103
        logger.info(message="dynamic dag started !")

    dynamic_dag()
