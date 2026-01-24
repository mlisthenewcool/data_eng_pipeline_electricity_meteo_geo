"""Gold layer transformations - analytical datasets from joined sources.

Gold transformations join multiple Silver sources to create analytical
datasets optimized for specific use cases.

Unlike Bronze/Silver transforms which work on single datasets, Gold transforms
receive a dictionary of Silver paths as input.
"""

from de_projet_perso.pipeline.transformations.gold.installations_meteo import (
    transform_gold_installations_meteo,
)

__all__ = ["transform_gold_installations_meteo"]
