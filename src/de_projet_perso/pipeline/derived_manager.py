"""Pipeline manager for derived (Gold) datasets.

This module handles datasets that are built from Silver sources rather than
downloaded from external servers. It provides:
- Dependency validation (ensure all Silver files exist)
- Gold transformation execution with registered functions
- Backup rotation (current → backup) before writing

Architecture:
    DerivedDatasetPipeline
        ├── validate_silver_dependencies() → dict[str, Path]
        └── to_gold() → GoldStageResult

Example:
    >>> from de_projet_perso.core.data_catalog import DataCatalog
    >>> from de_projet_perso.core.settings import settings
    >>> from de_projet_perso.pipeline.derived_manager import DerivedDatasetPipeline
    >>>
    >>> catalog = DataCatalog.load(settings.data_catalog_file_path)
    >>> dataset = catalog.get_dataset("installations_meteo")
    >>>
    >>> manager = DerivedDatasetPipeline(dataset)
    >>>
    >>> # Validate dependencies exist
    >>> silver_paths = manager.validate_silver_dependencies()
    >>>
    >>> # Execute transformation
    >>> result = manager.to_gold()
"""

from dataclasses import dataclass
from pathlib import Path

from de_projet_perso.core.data_catalog import DatasetDerivedConfig, DerivedSourceConfig
from de_projet_perso.core.exceptions import SilverDependencyNotFoundError
from de_projet_perso.core.file_manager import FileManager
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.pipeline.results import GoldStageResult
from de_projet_perso.pipeline.transformations import get_gold_transform


@dataclass(frozen=True)
class DerivedDatasetPipeline:
    """Pipeline manager for derived (Gold) datasets.

    Handles datasets built from multiple Silver sources. Unlike RemoteDatasetPipeline
    which handles download → bronze → silver, this manager handles:
    validate_dependencies → transform → gold

    Attributes:
        dataset: A derived Dataset from the catalog (DatasetDerived type)
    """

    dataset: DatasetDerivedConfig

    def __post_init__(self) -> None:
        """Validate that gold transform module exists for this dataset (fast-fail)."""
        get_gold_transform(self.dataset.name)

    @property
    def _resolver(self) -> PathResolver:
        """Get PathResolver for this dataset."""
        return PathResolver(self.dataset.name)

    @property
    def _file_manager(self) -> FileManager:
        """Get FileManager for this dataset."""
        return FileManager(self._resolver)

    @property
    def _source(self) -> DerivedSourceConfig:
        """Get typed DerivedSourceConfig (for type narrowing)."""
        return self.dataset.source

    def get_silver_dependency_paths(self) -> dict[str, Path]:
        """Get paths to all Silver dependencies.

        Returns:
            Dict mapping dependency dataset name to its silver_current_path.

        Example:
            >>> paths = manager.get_silver_dependency_paths()
            >>> paths
            {
                'odre_installations': Path('data/silver/odre_installations/current.parquet'),
                'ign_contours_iris': Path('data/silver/ign_contours_iris/current.parquet'),
                'meteo_france_stations': Path('data/silver/meteo_france_stations/current.parquet'),
            }
        """
        return {
            dep_name: PathResolver(dep_name).silver_current_path
            for dep_name in self._source.depends_on
        }

    def validate_silver_dependencies(self) -> dict[str, Path]:
        """Validate all Silver dependencies exist and return their paths.

        This should be called before to_gold() to ensure all required
        Silver files are available.

        Returns:
            Dict mapping dependency names to validated Silver paths.

        Raises:
            SilverDependencyNotFoundError: If any required Silver file is missing.
        """
        paths = self.get_silver_dependency_paths()
        missing: list[tuple[str, Path]] = []

        for name, path in paths.items():
            if not path.exists():
                missing.append((name, path))

        if missing:
            raise SilverDependencyNotFoundError(
                gold_dataset=self.dataset.name,
                missing_dependencies=missing,
            )

        logger.info(
            "Silver dependencies validated",
            extra={
                "gold_dataset": self.dataset.name,
                "dependencies": list(paths.keys()),
            },
        )
        return paths

    def to_gold(self) -> GoldStageResult:
        """Execute Gold transformation.

        Pipeline steps:
        1. Validate all Silver dependencies exist
        2. Retrieve registered transformation function
        3. Execute transformation
        4. Rotate backup (current → backup)
        5. Write new current.parquet

        Returns:
            GoldStageResult with output path, row count, and dependency info.

        Raises:
            SilverDependencyNotFoundError: If Silver dependencies are missing.
            KeyError: If no transformation is registered for this dataset.
        """
        # 1. Validate dependencies
        silver_paths = self.validate_silver_dependencies()

        # 2. Get registered transformation function
        transform_fn = get_gold_transform(self.dataset.name)

        logger.info(
            "Starting Gold transformation",
            extra={
                "gold_dataset": self.dataset.name,
                "dependencies": list(silver_paths.keys()),
            },
        )

        # 3. Execute transformation
        # Convention: transform functions accept named Path arguments
        kwargs = self._build_transform_kwargs(silver_paths)
        df = transform_fn(**kwargs)

        # 4. Ensure output directory exists
        self._resolver._gold_dir.mkdir(parents=True, exist_ok=True)

        # 5. Rotate backup (current → backup)
        self._file_manager.rotate_gold()

        # 6. Write new current
        output_path = self._resolver.gold_current_path
        df.write_parquet(output_path)

        # Compute statistics
        row_count = len(df)
        columns = df.columns
        file_size_mib = output_path.stat().st_size / (1024 * 1024)

        logger.info(
            "Gold transformation completed",
            extra={
                "gold_dataset": self.dataset.name,
                "output_path": str(output_path),
                "row_count": row_count,
                "columns_count": len(columns),
                "file_size_mib": round(file_size_mib, 2),
            },
        )

        return GoldStageResult(
            dataset_name=self.dataset.name,
            path=output_path,
            row_count=row_count,
            columns=columns,
            file_size_mib=file_size_mib,
            dependencies=list(silver_paths.keys()),
        )

    def _build_transform_kwargs(self, silver_paths: dict[str, Path]) -> dict[str, Path]:
        """Build keyword arguments for transform function.

        Converts dependency paths to the expected parameter names.
        Convention: {dataset_name}_silver_path

        Example:
            {"odre_installations": Path(...)}
            → {"odre_installations_silver_path": Path(...)}
        """
        return {
            f"{name.replace('-', '_')}_silver_path": path for name, path in silver_paths.items()
        }
