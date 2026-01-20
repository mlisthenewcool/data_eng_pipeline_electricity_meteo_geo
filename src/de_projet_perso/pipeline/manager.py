"""Pipeline download and extraction logic.

This module handles downloading source files and extracting archives,
completely decoupled from Airflow orchestration.

Key features:
- Download from URL with SHA256 integrity check
- Extract archives (7z) with automatic file detection
- Recalculate SHA256 after extraction (detects corruption)
- Propagate both archive and extracted file hashes for traceability

Architecture changes:
- Landing layer integration: download() and extract_archive() write directly
  to landing/ directory (no separate validate_landing step needed)
- LandingResult removed: ExtractionResult serves both purposes
"""

from pathlib import Path

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.results import CheckMetadataResult, DownloadResult, ExtractionResult
from de_projet_perso.pipeline.state import PipelineAction, PipelineStateManager
from de_projet_perso.utils.downloader import (
    download_to_file,
    extract_7z,
)
from de_projet_perso.utils.remote_checker import (
    RemoteFileInfo,
    get_remote_file_info,
    has_remote_file_changed,
)


class PipelineManager:
    """All functions used for pipeline data sources."""

    @staticmethod
    def has_dataset_metadata_changed(dataset: Dataset) -> CheckMetadataResult:
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
            url=dataset.source.url_as_str,
            timeout=30,  # TODO: mettre dans setting. 30 seconds for HTTP HEAD
        )

        # 2. Load previous state
        state = PipelineStateManager.load(dataset.name)

        # 3. FIRST_RUN: No previous state
        if state is None or state.last_successful_run is None:
            logger.debug(f"No previous successful run found for {dataset.name}, will run")

            return CheckMetadataResult(
                action=PipelineAction.FIRST_RUN,
                remote_file_metadata=remote_info,
            )

        # 4. Extract previous remote metadata from download stage
        if not state.last_successful_run:
            # TODO: traiter comme une erreur ici ? ou considérer comme FIRST_RUN ?
            logger.error(f"No successful run found for {dataset.name}, treating as first run")
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
            logger.debug(
                f"Pipeline skipped for {dataset.source}: remote unchanged",
                extra={
                    "reason": changed_result.reason,
                    "etag": remote_info.etag,
                    "last_modified": (
                        remote_info.last_modified.isoformat() if remote_info.last_modified else None
                    ),
                    "content_length": remote_info.content_length,
                },
            )
            # Short-circuit: skip all downstream tasks
            return CheckMetadataResult(action=PipelineAction.SKIP, remote_file_metadata=remote_info)

        # REFRESH: Remote changed
        logger.debug(
            f"Pipeline will execute for {dataset.name}: remote changed",
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

    @staticmethod
    def download(dataset: Dataset) -> DownloadResult:
        """Download source file from URL.

        Downloads to landing directory preserving original filename from server.
        The filename is extracted from Content-Disposition header or URL path.

        Args:
            dataset: Dataset configuration

        Returns:
            DownloadResult with path, sha256, size, and original_filename
        """
        # Get landing directory (not file path - preserves original filename)
        landing_dir = dataset.get_landing_dir()

        return download_to_file(
            url=dataset.source.url_as_str,
            dest_dir=landing_dir,
            default_name=f"{dataset.ingestion.version}.{dataset.source.format.value}",
        )

    @staticmethod
    def extract_archive(archive_path: Path, dataset: Dataset) -> ExtractionResult:
        """Extract archive and recalculate SHA256.

        For archive formats (7z):
            1. Extract the target file from archive
            2. Recalculate SHA256 of extracted file (detects corruption during extraction)
            3. Return both archive SHA256 (for traceability) and extracted SHA256

        Args:
            archive_path: Path to archive file (or direct file if not archive)
            dataset: Dataset configuration

        Returns:
            ExtractionResult with extracted file info, dual SHA256 tracking, and original filename

        Raises:
            ValueError: If archive format requires inner_file but none specified
            FileNotFoundError: If extracted file doesn't exist after extraction
        """
        if not dataset.source.format.is_archive:
            logger.warning(f"Trying to extract a non-archive format: {dataset.source.format.value}")
            raise Exception  # TODO exception spécifique

        if dataset.source.inner_file is None:
            raise ValueError(f"inner_file required for archive format: {dataset.source.format}")

        # Extract to same directory as archive (preserves directory structure)
        landing_dir = archive_path.parent

        file_info = extract_7z(
            archive_path=archive_path,
            target_filename=dataset.source.inner_file,
            dest_dir=landing_dir,
            validate_sqlite=Path(dataset.source.inner_file).suffix == ".gpkg",
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

    @staticmethod
    def has_hash_changed(
        dataset_name: str, download_or_extract_result: DownloadResult | ExtractionResult
    ) -> bool:
        """Download file and check if SHA256 has changed (Smart Skip #2).

        This task downloads the file and compares its SHA256 with the previous
        run's SHA256. If the hash is identical, the remote file was re-uploaded
        but the content hasn't changed (false change). In this case, we skip
        all downstream processing and clean up the downloaded file.

        Args:
            dataset_name: ...
            download_or_extract_result: ...

        Returns:
            dict: Updated PipelineContext if SHA256 changed (continue processing)
            bool: False to short-circuit if SHA256 unchanged (SKIP downstream)
        """
        # TODO: passer seulement le hash en paramètre et laisser la task Airflow nettoyer les
        #  fichiers car il vaudrait mieux séparer les deux DAGs
        state = PipelineStateManager.load(dataset_name)

        if not state or not state.last_successful_run:
            logger.debug(f"Could not find a previous successful run for {dataset_name}")
            return True

        # find the correct hash to compare with
        # for an archive, compare the target file hash and not the archive
        # because other files could change in archive but not that specific target file
        if isinstance(download_or_extract_result, ExtractionResult):
            known_sha256 = download_or_extract_result.extracted_file_sha256
        else:
            known_sha256 = download_or_extract_result.sha256

        if known_sha256 != state.last_successful_run.sha256:
            logger.debug(f"Hash changed for {dataset_name}: processing new data")
            return True

        # hash unchanged - data is identical (false change)
        logger.debug(f"Hash unchanged for {dataset_name}: false change detected")

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

        logger.debug("Removed duplicate downloaded file")
        return False
