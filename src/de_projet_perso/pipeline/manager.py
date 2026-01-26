"""Pipeline logic.

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
"""

import shutil
from dataclasses import dataclass
from pathlib import Path

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.file_manager import FileManager
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.pipeline.constants import PipelineAction
from de_projet_perso.pipeline.results import (
    BronzeResult,
    CheckMetadataResult,
    ExtractResult,
    IngestResult,
    SilverResult,
)
from de_projet_perso.pipeline.state import PipelineStateManager
from de_projet_perso.pipeline.transformations import get_bronze_transform, get_silver_transform
from de_projet_perso.utils.archive_extractor import extract_7z
from de_projet_perso.utils.downloader import download_to_file
from de_projet_perso.utils.remote_checker import (
    RemoteFileInfo,
    get_remote_file_info,
    has_remote_file_changed,
)


@dataclass(frozen=True)
class PipelineManager:
    """All functions used for pipeline data sources."""

    dataset: Dataset

    def has_dataset_metadata_changed(self) -> CheckMetadataResult:
        """Check if remote file has changed using HTTP HEAD (Smart Skip #1).

        This performs HTTP HEAD request to fetch ETag, Last-Modified, and
        Content-Length headers. It compares these with the previous run's metadata
        to determine if the remote file has changed.

        Priority-based comparison:
        1. ETag (strongest validator) - if present, use it
        2. Last-Modified (datetime comparison) - fallback if no ETag
        3. Content-Length (size comparison) - weakest validator

        Returns:
            dict: Serialized PipelineContext if remote changed (FIRST_RUN or REFRESH)
            bool: False to short-circuit if remote unchanged (SKIP all downstream tasks)
        """
        # 1. Fetch remote metadata
        remote_info = get_remote_file_info(
            url=self.dataset.source.url_as_str,
            timeout=30,  # TODO: move to settings
        )

        # 2. Load previous state
        state = PipelineStateManager.load(self.dataset.name)

        # 3. FIRST_RUN: No previous state
        if state is None or state.last_successful_run is None:
            logger.info(f"No previous successful run found for {self.dataset.name}, will run")

            return CheckMetadataResult(
                action=PipelineAction.FIRST_RUN,
                remote_metadata=remote_info,
            )

        # 4. Extract previous remote metadata from download stage
        if not state.last_successful_run:
            # TODO: traiter comme une erreur ici ? ou considérer comme FIRST_RUN ?
            logger.error(f"No successful run found for {self.dataset.name}, treating as first run")
            raise Exception  # TODO: erreur spécifique à créer

        previous_remote = RemoteFileInfo(
            etag=state.last_successful_run.remote_metadata.etag,
            last_modified=state.last_successful_run.remote_metadata.last_modified,
            content_length=state.last_successful_run.remote_metadata.content_length,
        )

        # 5. Compare remote metadata
        changed_result = has_remote_file_changed(remote_info, previous_remote)

        # SKIP: Remote unchanged
        if not changed_result.has_changed:
            logger.info(
                f"Pipeline skipped for {self.dataset.name}: remote unchanged",
                extra={
                    "reason": changed_result.reason,
                    "etag": remote_info.etag,
                    "last_modified": (
                        remote_info.last_modified.isoformat() if remote_info.last_modified else None
                    ),
                    "content_length": remote_info.content_length,
                    "last_successful_run": state.last_successful_run.model_dump(),
                },
            )
            # Short-circuit: skip all downstream tasks
            return CheckMetadataResult(action=PipelineAction.SKIP, remote_metadata=remote_info)

        # REFRESH: Remote changed
        logger.info(
            f"Pipeline will execute for {self.dataset.name}: remote changed",
            extra={
                "reason": changed_result.reason,
                "action": PipelineAction.REFRESH.value,
                "etag": remote_info.etag,
                "last_modified": (
                    remote_info.last_modified.isoformat() if remote_info.last_modified else None
                ),
            },
        )
        return CheckMetadataResult(action=PipelineAction.REFRESH, remote_metadata=remote_info)

    def ingest(self, version: str, remote_metadata: RemoteFileInfo) -> IngestResult:
        """Download source file from URL.

        Downloads to landing directory preserving original filename from server.
        The filename is extracted from Content-Disposition header or URL path.

        Args:
            version: Version for path resolution (Airflow template or explicit)
            remote_metadata: todo

        Returns:
            IngestResult with remote_metadata, path, sha256, size
        """
        # Get landing directory (not file path - preserves original filename)
        resolver = PathResolver(dataset_name=self.dataset.name)

        download = download_to_file(
            url=self.dataset.source.url_as_str,
            dest_dir=resolver.landing_dir,
            default_name=f"{version}.{self.dataset.source.format.value}",
        )

        return IngestResult(
            version=version,
            remote_metadata=remote_metadata,
            path=download.path,
            sha256=download.sha256,
            size_mib=download.size_mib,
        )

    def extract_archive(self, ingest_result: IngestResult) -> ExtractResult:
        """Extract archive and recalculate SHA256.

        For archive formats (7z):
            1. Extract the target file from archive
            2. Recalculate SHA256 of extracted file (detects corruption during extraction)
            3. Return both archive SHA256 (for traceability) and extracted SHA256

        Args:
            ingest_result: todo

        Returns:
            ExtractResult todo

        Raises:
            ValueError: If archive format requires inner_file but none specified
            FileNotFoundError: If extracted file doesn't exist after extraction
        """
        if not self.dataset.source.format.is_archive or not self.dataset.source.inner_file:
            logger.warning(
                f"Trying to extract a non-archive format: {self.dataset.source.format.value}"
            )
            raise ValueError(
                "Dataset {self.dataset.name} is not an archive or missing inner_file"
            )  # TODO exception spécifique

        # Extract to same directory as archive (preserves directory structure)
        archive_path = ingest_result.path
        landing_dir = archive_path.parent

        extract_info = extract_7z(
            archive_path=archive_path,
            target_filename=self.dataset.source.inner_file,
            dest_dir=landing_dir,
            validate_sqlite=Path(self.dataset.source.inner_file).suffix == ".gpkg",
        )

        logger.info(
            "Extraction completed with integrity check",
            extra={
                "archive_path": archive_path,
                "extracted_file_path": extract_info.path,
                "extracted_file_sha256": extract_info.sha256,
                "extracted_file_size_mib": extract_info.size_mib,
            },
        )

        return ExtractResult(
            **ingest_result.model_dump(),
            extracted_file_path=extract_info.path,
            extracted_file_sha256=extract_info.sha256,
            extracted_file_size_mib=extract_info.size_mib,
        )

    def has_hash_changed(self, result: IngestResult | ExtractResult) -> bool:
        """Download file and check if SHA256 has changed (Smart Skip #2).

        This task downloads the file and compares its SHA256 with the previous
        run's SHA256. If the hash is identical, the remote file was re-uploaded
        but the content hasn't changed (false change). In this case, we skip
        all downstream processing and clean up the downloaded file.

        Args:
            result: todo

        Returns:
            dict: Updated PipelineContext if SHA256 changed (continue processing)
            bool: False to short-circuit if SHA256 unchanged (SKIP downstream)
        """
        state = PipelineStateManager.load(self.dataset.name)

        if not state or not state.last_successful_run:
            logger.info(f"Could not find a previous successful run for {self.dataset.name}")
            return True

        # find the correct hash to compare with
        # for an archive, compare the target file hash and not the archive
        # because other files could change in archive but not that specific target file
        current_hash = (
            result.extracted_file_sha256 if isinstance(result, ExtractResult) else result.sha256
        )

        if current_hash != state.last_successful_run.sha256:
            logger.info(f"Hash changed for {self.dataset.name}: processing new data")
            return True

        # hash unchanged - data is identical (false change)
        logger.info(f"Hash unchanged for {self.dataset.name}: false change detected")

        # Cleanup downloaded file (duplicate)
        try:
            result.path.unlink()
            if isinstance(result, ExtractResult):
                result.extracted_file_path.unlink()
        except Exception as e:
            logger.warning(
                "Failed to remove duplicate file (non-critical)", extra={"error": str(e)}
            )

        logger.info("Removed duplicate downloaded file")
        return False

    # =============================================================================
    # Transformations
    # =============================================================================

    def to_bronze(self, result: IngestResult | ExtractResult) -> BronzeResult:
        """Convert landing file to Parquet with normalized column names.

        Bronze layer transformations:
        1. Read raw file from landing
        2. Apply custom transformations (normalize columns, filter, etc.)
        3. Write to versioned bronze Parquet file
        4. Update latest.parquet symlink
        5. Delete landing file

        Args:
            result: todo

        Returns:
            BronzeResult with row count and columns

        Raises:
            NotImplementedError: If no bronze transformation is registered
        """
        resolver = PathResolver(dataset_name=self.dataset.name)

        bronze_path = resolver.bronze_path(result.version)
        bronze_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Converting to bronze for {self.dataset.name}",
            extra={
                "landing_path": result.landing_path,
                "bronze_path": bronze_path,
                "version": result.version,
            },
        )

        # Retrieve dataset-specific bronze transformations
        transforms = get_bronze_transform(self.dataset.name)
        if transforms is None:
            raise NotImplementedError(
                f"No bronze transformation registered for dataset: {self.dataset.name}"
            )

        # Apply the transformations
        df = transforms(result.landing_path)
        df.write_parquet(bronze_path)

        # Update latest symlink to point to this version
        file_manager = FileManager(resolver)
        file_manager.update_bronze_latest_link(result.version)

        # Cleanup all landing files (inside the landing directories)
        if resolver.landing_dir.exists() and resolver.landing_dir.is_dir():
            shutil.rmtree(resolver.landing_dir)

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

        return BronzeResult(
            **result.model_dump(),
            parquet_file_size_mib=parquet_size,
            row_count=row_count,
            columns=columns,
        )

    def to_silver(self, bronze_result: BronzeResult) -> SilverResult:
        """Apply business transformations to create silver layer.

        Silver layer transformations:
        1. Rotate silver files (current → backup)
        2. Read from bronze latest.parquet
        3. Apply custom business logic
        4. Write to silver current.parquet

        Returns:
            SilverResult with row count and columns
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

        # Retrieve dataset-specific silver transformations
        transforms = get_silver_transform(self.dataset.name)
        if transforms is None:
            raise NotImplementedError(
                f"No silver transformation registered for dataset: {self.dataset.name}"
            )

        # Apply the transformations
        df = transforms(resolver.bronze_latest_path)

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

        return SilverResult(
            **bronze_result.model_dump(),
            silver_parquet_file_size_mib=parquet_size,
            silver_row_count=row_count,
            silver_columns=columns,
        )
