"""Pipeline manager for remote source datasets.

This module handles downloading source files and extracting archives,
completely decoupled from Airflow orchestration.

Key features:
- Download from URL with SHA256 integrity check
- Extract archives (7z) with automatic file detection
- Recalculate SHA256 after extraction (detects corruption)
- Propagate both archive and extracted file hashes for traceability

Architecture:
- Landing layer integration: download() and extract_archive() write directly
  to landing/ directory (no separate validate_landing step needed)
- Uses PathResolver for all path operations

Transformations:
- Bronze: Landing → Parquet with normalized columns (versioned by run date)
- Silver: Bronze latest → Business logic applied → current.parquet + backup.parquet

Path Resolution:
- Uses PathResolver for all path operations
- Bronze: Creates versioned files ({date}.parquet) + updates latest.parquet symlink
- Silver: Rotates current → backup before writing new current

Note:
    For derived datasets (Gold), use DerivedDatasetPipeline instead.
"""

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from de_projet_perso.core.data_catalog import DatasetRemoteConfig, RemoteSourceConfig
from de_projet_perso.core.file_manager import FileManager
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.pipeline.results import (
    BronzeStageResult,
    DownloadStageResult,
    ExtractionStageResult,
    IngestionDecision,
    PipelineRunSnapshot,
    SilverStageResult,
)
from de_projet_perso.pipeline.transformations import get_bronze_transform, get_silver_transform
from de_projet_perso.utils.archive_extractor import extract_7z
from de_projet_perso.utils.downloader import download_to_file
from de_projet_perso.utils.pydantic import format_pydantic_errors
from de_projet_perso.utils.remote_checker import (
    RemoteFileMetadata,
    get_remote_file_info,
    has_remote_file_changed,
)

# if TYPE_CHECKING:
#     from airflow.sdk.execution_time.context import AssetEventResult


@dataclass(frozen=True)
class RemoteDatasetPipeline:
    """Pipeline manager for remote source datasets.

    Handles the full pipeline for datasets downloaded from external servers:
    ingest → (extract) → to_bronze → to_silver

    For derived datasets (Gold), use DerivedDatasetPipeline instead.

    Attributes:
        dataset: A remote Dataset from the catalog (DatasetRemote type)
    """

    dataset: DatasetRemoteConfig

    def __post_init__(self) -> None:
        """Validate that transform modules exist for this dataset (fast-fail)."""
        get_bronze_transform(self.dataset.name)
        get_silver_transform(self.dataset.name)

    @property
    def _source(self) -> RemoteSourceConfig:
        """Get typed RemoteSourceConfig (for type narrowing)."""
        return self.dataset.source

    @staticmethod
    def _safely_load_metadata(metadata: dict[str, Any] | None) -> PipelineRunSnapshot | None:
        """Parses previous metadata. Returns None if data is missing or corrupted."""
        if not metadata:
            return None
        try:
            return PipelineRunSnapshot.model_validate(metadata)
        except ValidationError as e:
            logger.warning("Metadata corrupted.", extra=format_pydantic_errors(e))
            return None

    # ========================================
    # INGESTION
    # ========================================

    def _evaluate_ingestion_need(
        self,
        current_remote_file_info: RemoteFileMetadata,
        previous_remote_file_info: RemoteFileMetadata | None,
    ) -> IngestionDecision:
        """Determines if ingestion is required based on remote metadata and local state consistency.

        Returns:
            Tuple: (should_ingest, is_healing, remote_metadata)
        """
        # --- 1. Check upstream consistency ---
        silver_path = PathResolver(dataset_name=self.dataset.name).silver_current_path
        silver_file_exists = silver_path.exists()
        remote_metadata_exists = previous_remote_file_info is not None

        if remote_metadata_exists != silver_file_exists:
            logger.warning(
                "Inconsistent state. Forcing ingestion to heal.",
                extra={
                    "does_file_exist": silver_file_exists,
                    "does_metadata_exist": remote_metadata_exists,
                },
            )
            return IngestionDecision(
                should_ingest=True, is_healing=True, remote_metadata=current_remote_file_info
            )

        # --- Smart skip #1 : Remote metadata comparison ---
        if previous_remote_file_info and silver_file_exists:
            if not has_remote_file_changed(
                current=current_remote_file_info, previous=previous_remote_file_info
            ):
                logger.info("Skipping : hash (sha256) is identical to previous successful run.")
                return IngestionDecision(
                    should_ingest=False, is_healing=False, remote_metadata=current_remote_file_info
                )

        return IngestionDecision(
            should_ingest=True, is_healing=False, remote_metadata=current_remote_file_info
        )

    def _download(self, version: str, remote_metadata: RemoteFileMetadata) -> DownloadStageResult:
        """Download source file to landing layer."""
        # Get landing directory (not file path - preserves original filename)
        resolver = PathResolver(dataset_name=self.dataset.name)

        download = download_to_file(
            url=self._source.url_as_str,
            dest_dir=resolver.landing_dir,
            default_name=f"{version}.{self._source.format.value}",
        )

        return DownloadStageResult(
            version=version,
            remote_metadata=remote_metadata,
            path=download.path,
            sha256=download.sha256,
            size_mib=download.size_mib,
        )

    def ingest(
        self, version: str, previous_metadata: dict[str, Any] | None
    ) -> DownloadStageResult | bool:
        """Todo."""
        # --- 1. Load metadata from previous run ---
        safe_previous_metadata = self._safely_load_metadata(previous_metadata)

        # --- 2. Retrieve metadata from remote with HEAD request ---
        remote_file_info = get_remote_file_info(url=self._source.url_as_str)
        previous_remote_metadata = (
            safe_previous_metadata.remote_metadata if safe_previous_metadata else None
        )

        # --- 3. todo: merge with the other function ? explain ?
        need = self._evaluate_ingestion_need(
            current_remote_file_info=remote_file_info,
            previous_remote_file_info=previous_remote_metadata,
        )

        if not need.should_ingest:
            return False

        # --- 4. Download content ---
        ingest_result = self._download(version=version, remote_metadata=remote_file_info)

        # --- 5. Hash check (smart skip #2) but don't skip if we are in healing mode ---
        if not need.is_healing and safe_previous_metadata:
            if self._should_skip_on_hash(
                previous_hash=safe_previous_metadata.raw_file_sha256,
                current_hash=ingest_result.sha256,
            ):
                return False

        return ingest_result

    def should_skip_extraction(
        self, extract_result: ExtractionStageResult, previous_metadata: dict[str, Any] | None
    ) -> bool:
        """Independent logic to decide if extraction output is redundant."""
        safe_previous_metadata = self._safely_load_metadata(previous_metadata)
        if not safe_previous_metadata or not safe_previous_metadata.extracted_file_sha256:
            return False

        return self._should_skip_on_hash(
            safe_previous_metadata.extracted_file_sha256, extract_result.extracted_file_sha256
        )

    def extract_archive(self, ingest_result: DownloadStageResult) -> ExtractionStageResult:
        """Extract archive and recalculate SHA256.

        For archive formats (7z):
            1. Extract the target file from archive
            2. Recalculate SHA256 of extracted file (detects corruption during extraction)
            3. Return both archive SHA256 (for traceability) and extracted SHA256

        Args:
            ingest_result: todo

        Returns:
            ExtractionStageResult todo

        Raises:
            ValueError: If archive format requires inner_file but none specified
            FileNotFoundError: If extracted file doesn't exist after extraction
        """
        if not self._source.format.is_archive or not self._source.inner_file:
            logger.warning(f"Trying to extract a non-archive format: {self._source.format.value}")
            raise ValueError(
                "Dataset {self.dataset.name} is not an archive or missing inner_file"
            )  # TODO exception spécifique

        # Extract to same directory as archive (preserves directory structure)
        archive_path = ingest_result.path
        landing_dir = archive_path.parent

        extract_info = extract_7z(
            archive_path=archive_path,
            target_filename=self._source.inner_file,
            dest_dir=landing_dir,
            validate_sqlite=Path(self._source.inner_file).suffix == ".gpkg",
        )

        return ExtractionStageResult(
            **ingest_result.model_dump(),
            extracted_file_path=extract_info.path,
            extracted_file_sha256=extract_info.sha256,
            extracted_file_size_mib=extract_info.size_mib,
        )

    # =============================================================================
    # Smart skip #2
    # =============================================================================

    def _should_skip_on_hash(self, previous_hash: str | None, current_hash: str) -> bool:
        """Smart Skip #2: Compares content hashes to avoid redundant processing."""
        if previous_hash == current_hash:
            logger.info("Content hash identical to previous run. Cleaning up and skipping.")
            self._cleanup_landing()
            return True

        return False

    def _cleanup_landing(self) -> None:
        """Cleanup all landing files (inside the landing directory)."""
        path_resolver = PathResolver(dataset_name=self.dataset.name)
        if path_resolver.landing_dir.exists() and path_resolver.landing_dir.is_dir():
            shutil.rmtree(path_resolver.landing_dir)

    # =============================================================================
    # Transformations
    # =============================================================================

    def to_bronze(
        self, ingest_or_extract_result: DownloadStageResult | ExtractionStageResult
    ) -> BronzeStageResult:
        """Convert landing file to Parquet with normalized column names.

        Bronze layer transformations:
        1. Read raw file from landing
        2. Apply custom transformations (normalize columns, filter, etc.)
        3. Write to versioned bronze Parquet file
        4. Update latest.parquet symlink
        5. Delete landing file

        Args:
            ingest_or_extract_result: todo

        Returns:
            BronzeStageResult with row count and columns

        Raises:
            NotImplementedError: If no bronze transformation is registered
        """
        resolver = PathResolver(dataset_name=self.dataset.name)

        bronze_path = resolver.bronze_path(ingest_or_extract_result.version)
        bronze_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Converting to bronze for {self.dataset.name}",
            extra={
                "landing_path": ingest_or_extract_result.landing_path,
                "bronze_path": bronze_path,
                "version": ingest_or_extract_result.version,
            },
        )

        # Retrieve and apply dataset-specific bronze transformation
        transform = get_bronze_transform(self.dataset.name)
        df = transform(ingest_or_extract_result.landing_path)
        df.write_parquet(bronze_path)

        # Update latest symlink to point to this version
        file_manager = FileManager(resolver)
        file_manager.update_bronze_latest_link(ingest_or_extract_result.version)

        # Cleanup all landing files
        self._cleanup_landing()

        columns = df.columns
        row_count = len(df)
        parquet_size = bronze_path.stat().st_size / (1024 * 1024)

        logger.info(
            f"Bronze conversion complete for {self.dataset.name}",
            extra={
                "parquet_file_size": parquet_size,
                "row_count": row_count,
                "columns": columns,
                "bronze_path": bronze_path,
                "latest_link": resolver.bronze_latest_path,
            },
        )

        return BronzeStageResult(
            **ingest_or_extract_result.model_dump(),
            bronze_file_size_mib=parquet_size,
            bronze_row_count=row_count,
            bronze_columns=columns,
        )

    def to_silver(self, bronze_result: BronzeStageResult) -> SilverStageResult:
        """Apply business transformations to create silver layer.

        Silver layer transformations:
        1. Rotate silver files (current → backup)
        2. Read from bronze latest.parquet
        3. Apply custom business logic
        4. Write to silver current.parquet

        Returns:
            SilverStageResult with row count and columns
        """
        # TODO: do we need bronze_result ?
        resolver = PathResolver(dataset_name=self.dataset.name)

        logger.info(
            f"Transforming to silver for {self.dataset.name}",
            extra={
                "bronze_source": resolver.bronze_latest_path,
                "silver_dest": resolver.silver_current_path,
            },
        )

        # Retrieve and apply dataset-specific silver transformation
        transform = get_silver_transform(self.dataset.name)
        df = transform(resolver.bronze_latest_path)

        # Rotate silver BEFORE writing to disk: current → backup
        file_manager = FileManager(resolver)
        file_manager.rotate_silver()

        # TODO: at this point, is it necessary to create parent ?
        resolver.silver_current_path.parent.mkdir(parents=True, exist_ok=True)

        # Now that we rotated versions, write to disk
        df.write_parquet(resolver.silver_current_path)

        columns = df.columns
        row_count = len(df)
        parquet_size = resolver.silver_current_path.stat().st_size / (1024 * 1024)

        logger.info(
            f"Silver transformation complete for {self.dataset.name}",
            extra={
                "parquet_file_size_mib": parquet_size,
                "row_count": row_count,
                "len_columns": len(columns),
                "silver_current": resolver.silver_current_path,
                "silver_backup": resolver.silver_backup_path,
            },
        )

        return SilverStageResult(
            **bronze_result.model_dump(),
            silver_file_size_mib=parquet_size,
            silver_row_count=row_count,
            silver_columns=columns,
        )

    @staticmethod
    def create_metadata_emission(silver_result: SilverStageResult) -> PipelineRunSnapshot:
        """Todo."""
        return PipelineRunSnapshot(
            # ingest
            remote_metadata=silver_result.remote_metadata,
            version=silver_result.version,
            raw_file_sha256=silver_result.sha256,
            raw_file_size_mib=silver_result.size_mib,
            # (optional) extract
            extracted_file_sha256=silver_result.extracted_file_sha256,
            extracted_file_size_mib=silver_result.extracted_file_size_mib,
            # bronze
            bronze_file_size_mib=silver_result.bronze_file_size_mib,
            bronze_row_count=silver_result.bronze_row_count,
            bronze_columns=silver_result.bronze_columns,
            # silver
            silver_file_size_mib=silver_result.silver_file_size_mib,
            silver_row_count=silver_result.silver_row_count,
            silver_columns=silver_result.silver_columns,
        )


# Backward compatibility aliases
PipelineManager = RemoteDatasetPipeline  # Deprecated, use RemoteDatasetPipeline
SourcePipelineManager = RemoteDatasetPipeline  # Deprecated, use RemoteDatasetPipeline
