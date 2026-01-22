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
from de_projet_perso.pipeline.results import (
    BronzeResult,
    CheckMetadataResult,
    DownloadResult,
    ExtractionResult,
    SilverResult,
)
from de_projet_perso.pipeline.state import PipelineAction, PipelineStateManager
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

    @classmethod
    def has_dataset_metadata_changed(cls) -> CheckMetadataResult:
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
            url=cls.dataset.source.url_as_str,
            timeout=30,  # TODO: mettre dans setting. 30 seconds for HTTP HEAD
        )

        # 2. Load previous state
        state = PipelineStateManager.load(cls.dataset.name)

        # 3. FIRST_RUN: No previous state
        if state is None or state.last_successful_run is None:
            logger.info(f"No previous successful run found for {cls.dataset.name}, will run")

            return CheckMetadataResult(
                action=PipelineAction.FIRST_RUN,
                remote_file_metadata=remote_info,
            )

        # 4. Extract previous remote metadata from download stage
        if not state.last_successful_run:
            # TODO: traiter comme une erreur ici ? ou considérer comme FIRST_RUN ?
            logger.error(f"No successful run found for {cls.dataset.name}, treating as first run")
            raise Exception  # TODO: erreur spécifique à créer

        previous_remote = RemoteFileInfo(
            etag=state.last_successful_run.etag,
            last_modified=state.last_successful_run.last_modified,
            content_length=state.last_successful_run.content_length,
        )

        # 5. Compare remote metadata
        changed_result = has_remote_file_changed(remote_info, previous_remote)

        # SKIP: Remote unchanged
        if not changed_result.has_changed:
            logger.info(
                f"Pipeline skipped for {cls.dataset.name}: remote unchanged",
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
            return CheckMetadataResult(action=PipelineAction.SKIP, remote_file_metadata=remote_info)

        # REFRESH: Remote changed
        logger.info(
            f"Pipeline will execute for {cls.dataset.name}: remote changed",
            extra={
                "reason": changed_result.reason,
                "action": PipelineAction.REFRESH.value,
                "etag": remote_info.etag,
                "last_modified": (
                    remote_info.last_modified.isoformat() if remote_info.last_modified else None
                ),
            },
        )
        return CheckMetadataResult(
            action=PipelineAction.REFRESH,
            remote_file_metadata=remote_info,
        )

    @classmethod
    def download(cls, version: str) -> DownloadResult:
        """Download source file from URL.

        Downloads to landing directory preserving original filename from server.
        The filename is extracted from Content-Disposition header or URL path.

        Args:
            version: Version for path resolution (Airflow template or explicit)

        Returns:
            DownloadResult with path, sha256, size, and original_filename
        """
        # Get landing directory (not file path - preserves original filename)
        resolver = PathResolver(dataset_name=cls.dataset.name)

        return download_to_file(
            url=cls.dataset.source.url_as_str,
            dest_dir=resolver.landing_dir,
            default_name=f"{version}.{cls.dataset.source.format.value}",
        )

    @classmethod
    def extract_archive(cls, archive_path: Path) -> ExtractionResult:
        """Extract archive and recalculate SHA256.

        For archive formats (7z):
            1. Extract the target file from archive
            2. Recalculate SHA256 of extracted file (detects corruption during extraction)
            3. Return both archive SHA256 (for traceability) and extracted SHA256

        Args:
            archive_path: Path to archive file (or direct file if not archive)

        Returns:
            ExtractionResult with extracted file info, dual SHA256 tracking, and original filename

        Raises:
            ValueError: If archive format requires inner_file but none specified
            FileNotFoundError: If extracted file doesn't exist after extraction
        """
        if not cls.dataset.source.format.is_archive:
            logger.warning(
                f"Trying to extract a non-archive format: {cls.dataset.source.format.value}"
            )
            raise Exception  # TODO exception spécifique

        if cls.dataset.source.inner_file is None:
            raise ValueError(f"inner_file required for archive format: {cls.dataset.source.format}")

        # Extract to same directory as archive (preserves directory structure)
        landing_dir = archive_path.parent

        file_info = extract_7z(
            archive_path=archive_path,
            target_filename=cls.dataset.source.inner_file,
            dest_dir=landing_dir,
            validate_sqlite=Path(cls.dataset.source.inner_file).suffix == ".gpkg",
        )

        logger.info(
            "Extraction completed with integrity check",
            extra={
                "archive_path": archive_path,
                "extracted_file_path": file_info.path,
                "extracted_file_sha256": file_info.sha256,
                "size_mib": file_info.size_mib,
            },
        )

        return ExtractionResult(
            archive_path=archive_path,
            extracted_file_path=file_info.path,
            extracted_file_sha256=file_info.sha256,
            size_mib=file_info.size_mib,
        )

    @classmethod
    def has_hash_changed(
        cls, download_or_extract_result: DownloadResult | ExtractionResult
    ) -> bool:
        """Download file and check if SHA256 has changed (Smart Skip #2).

        This task downloads the file and compares its SHA256 with the previous
        run's SHA256. If the hash is identical, the remote file was re-uploaded
        but the content hasn't changed (false change). In this case, we skip
        all downstream processing and clean up the downloaded file.

        Args:
            download_or_extract_result: ...

        Returns:
            dict: Updated PipelineContext if SHA256 changed (continue processing)
            bool: False to short-circuit if SHA256 unchanged (SKIP downstream)
        """
        # TODO: passer seulement le hash en paramètre et laisser la task Airflow nettoyer les
        #  fichiers car il serait mieux de séparer les deux DAGs
        state = PipelineStateManager.load(cls.dataset.name)

        if not state or not state.last_successful_run:
            logger.info(f"Could not find a previous successful run for {cls.dataset.name}")
            return True

        # find the correct hash to compare with
        # for an archive, compare the target file hash and not the archive
        # because other files could change in archive but not that specific target file
        if isinstance(download_or_extract_result, ExtractionResult):
            known_sha256 = download_or_extract_result.extracted_file_sha256
        else:
            known_sha256 = download_or_extract_result.sha256

        if known_sha256 != state.last_successful_run.sha256:
            logger.info(f"Hash changed for {cls.dataset.name}: processing new data")
            return True

        # hash unchanged - data is identical (false change)
        logger.info(f"Hash unchanged for {cls.dataset.name}: false change detected")

        # Cleanup downloaded file (duplicate)
        try:
            if isinstance(download_or_extract_result, ExtractionResult):
                download_or_extract_result.archive_path.unlink()
                download_or_extract_result.extracted_file_path.unlink()
            else:
                download_or_extract_result.path.unlink()
        except Exception as e:
            logger.warning(
                "Failed to remove duplicate file (non-critical)", extra={"error": str(e)}
            )

        logger.info("Removed duplicate downloaded file")
        return False

    # =============================================================================
    # Transformations
    # =============================================================================

    @classmethod
    def to_bronze(cls, landing_path: Path, version: str) -> BronzeResult:
        """Convert landing file to Parquet with normalized column names.

        Bronze layer transformations:
        1. Read raw file from landing
        2. Apply custom transformations (normalize columns, filter, etc.)
        3. Write to versioned bronze Parquet file
        4. Update latest.parquet symlink
        5. Delete landing file

        Args:
            landing_path: Path to landing file (with original filename)
            version: Version for this run (e.g., "2025-01-21" or "20250121T143022")

        Returns:
            BronzeResult with row count and columns

        Raises:
            NotImplementedError: If no bronze transformation is registered
        """
        resolver = PathResolver(dataset_name=cls.dataset.name)

        bronze_path = resolver.bronze_path(version)
        bronze_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Converting to bronze for {cls.dataset.name}",
            extra={
                "landing_path": str(landing_path),
                "bronze_path": str(bronze_path),
                "version": version,
            },
        )

        # Retrieve dataset-specific bronze transformations
        transforms = get_bronze_transform(cls.dataset.name)
        if transforms is None:
            raise NotImplementedError(
                f"No bronze transformation registered for dataset: {cls.dataset.name}"
            )

        # Apply the transformations
        df = transforms(cls.dataset, landing_path)
        df.write_parquet(bronze_path)

        # Update latest symlink to point to this version
        file_manager = FileManager(resolver)
        file_manager.update_bronze_latest_link(version)

        # Cleanup all landing files (inside the landing directories)
        if resolver.landing_dir.exists() and resolver.landing_dir.is_dir():
            shutil.rmtree(resolver.landing_dir)

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Bronze conversion complete for {cls.dataset.name}",
            extra={
                "n_rows": row_count,
                "n_columns": len(columns),
                "bronze_path": bronze_path,
                "latest_link": resolver.bronze_latest_path,
            },
        )

        return BronzeResult(row_count=row_count, columns=columns)

    @classmethod
    def to_silver(cls, bronze_result: BronzeResult) -> SilverResult:
        """Apply business transformations to create silver layer.

        Silver layer transformations:
        1. Rotate silver files (current → backup)
        2. Read from bronze latest.parquet
        3. Apply custom business logic
        4. Write to silver current.parquet

        Args:
            bronze_result: Result from bronze transformation (for context)

        Returns:
            SilverResult with row count and columns
        """
        # TODO: do we need bronze_result ?
        resolver = PathResolver(dataset_name=cls.dataset.name)

        logger.info(
            f"Transforming to silver for {cls.dataset.name}",
            extra={
                "bronze_source": resolver.bronze_latest_path,
                "silver_dest": resolver.silver_current_path,
            },
        )

        # Retrieve dataset-specific silver transformations
        transforms = get_silver_transform(cls.dataset.name)  # noqa: F821
        if transforms is None:
            raise NotImplementedError(
                f"No silver transformation registered for dataset: {cls.dataset.name}"
            )

        # Apply the transformations
        df = transforms(cls.dataset, resolver)

        # Rotate silver BEFORE writing to disk: current → backup
        file_manager = FileManager(resolver)
        file_manager.rotate_silver()

        # TODO: at this point, is it necessary to create parent ?
        resolver.silver_current_path.parent.mkdir(parents=True, exist_ok=True)

        # Now that we rotated versions, write to disk
        df.write_parquet(resolver.silver_current_path)

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Silver transformation complete for {cls.dataset.name}",
            extra={
                "n_rows": row_count,
                "n_columns": len(columns),
                "silver_current": resolver.silver_current_path,
                "silver_backup": resolver.silver_backup_path,
            },
        )

        return SilverResult(row_count=row_count, columns=df.columns)
