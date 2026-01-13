"""TODO."""

import sys

from de_projet_perso.core.catalog import DataCatalog
from de_projet_perso.core.settings import DATA_CATALOG_PATH

try:
    catalog = DataCatalog.load(DATA_CATALOG_PATH)
    print(f"Catalog loaded: {len(catalog.datasets)} dataset(s)\n")

    for name, dataset in catalog.datasets.items():
        print(f"--- {name} ---")
        print(f"  Provider: {dataset.source.provider}")
        print(f"  Format: {dataset.source.format}")
        print(f"  Version: {dataset.ingestion.version}")
        print(f"  Storage: {dataset.get_storage_path()}")
        print()

except FileNotFoundError as e:
    print(f"Catalog file not found: {e}")
    sys.exit(1)
except Exception as e:
    print(e)
    sys.exit(1)
