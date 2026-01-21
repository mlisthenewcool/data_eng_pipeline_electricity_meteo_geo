"""Example script to test pipeline logic without Airflow.

This script demonstrates the complete data pipeline flow using PipelineContext:
- Create pipeline context (similar to check_should_run task)
- Download source data (updates context.download)
- Extract from archive if needed (updates context.extraction)
- Transform to bronze layer (updates context.bronze)
- Transform to silver layer (updates context.silver)

Run with: PYTHONPATH=src uv run python src/de_projet_perso/pipeline/example.py
"""

import sys
from datetime import datetime

import httpx

from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.exceptions import (
    ArchiveNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
)
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.manager import PipelineManager
from de_projet_perso.pipeline.state import PipelineAction, PipelineStateManager
from de_projet_perso.pipeline.transformer import PipelineTransformer

if __name__ == "__main__":
    # ===================================================================================
    # /!\ DO NOT USE sys.exit(-1) WHEN RUNNING ON AIRFLOW. Let Airflow handle exceptions.
    # ===================================================================================
    start_time = datetime.now()

    _catalog = DataCatalog.load(settings.data_catalog_file_path)

    # __dataset_name = "meteo_france_stations"
    # __dataset_name = "odre_installations"
    __dataset_name = "ign_contours_iris"
    # __dataset_name = "odre_eco2mix_cons_def"
    _dataset = _catalog.get_dataset(__dataset_name)

    # ==============================
    # Step 0: Prepare version & PathResolver
    # ==============================
    _run_version = _dataset.ingestion.frequency.format_datetime_as_version(start_time)
    _path_resolver = PathResolver(dataset_name=_dataset.name, run_version=_run_version)

    # ==============================
    # Step 1: Check if metadata changed
    # ==============================
    logger.info("=" * 80)
    logger.info("Checking metadata ...")
    logger.info("=" * 80)
    _metadata = PipelineManager.has_dataset_metadata_changed(_dataset)
    if _metadata.action == PipelineAction.SKIP:
        _state = PipelineStateManager.load(_dataset.name)
        sys.exit(0)

    logger.info(
        "Server metadata has changed or we found insuffisant metadata, will download data to check",
        extra=_metadata.to_serializable(),
    )

    # ==============================
    # Step 2: Download
    # ==============================
    logger.info("=" * 80)
    logger.info("Downloading dataset...")
    logger.info("=" * 80)
    try:
        _download = PipelineManager.download(_dataset, _run_version)
    except httpx.HTTPStatusError as e:
        logger.exception(
            f"Download failed. Server returned code: {e.response.status_code}",
            extra={
                "message": e.response.reason_phrase,
                "url": str(e.request.url),
            },
        )
        sys.exit(-1)
    except httpx.TimeoutException as e:
        logger.exception("Download failed. Connection timed out", extra={"more_infos": e})
        sys.exit(-1)
    except httpx.HTTPError as e:
        logger.exception("Download failed. Network or request error", extra={"more_infos": e})
        sys.exit(-1)
    except Exception as e:
        logger.critical("Download failed. Unexpected error", extra={"more_infos": str(e)})
        sys.exit(-1)

    logger.info("Download completed !", extra=_download.to_serializable())

    # ==============================
    # Step 3: Extract (if archive) + check if hash changed
    # ==============================
    if _dataset.source.format.is_archive:
        logger.info("=" * 80)
        logger.info("Extracting archive...")
        logger.info("=" * 80)
        try:
            _extraction = PipelineManager.extract_archive(
                archive_path=_download.path,
                dataset=_dataset,
            )
        except (
            ArchiveNotFoundError,
            FileNotFoundInArchiveError,
            FileIntegrityError,
        ) as e:
            logger.exception("Extraction failed", extra={"error": str(e)})
            sys.exit(-1)

        landing_path = _extraction.extracted_file_path
        logger.info("Extraction completed !", extra=_extraction.to_serializable())

        logger.info("=" * 80)
        logger.info("Checking hash...")
        logger.info("=" * 80)
        should_continue = PipelineManager.has_hash_changed(_dataset.name, _extraction)
    else:
        logger.info("=" * 80)
        logger.info("Checking hash...")
        logger.info("=" * 80)
        should_continue = PipelineManager.has_hash_changed(_dataset.name, _download)

        # For non-archive: get landing_path from download
        _extraction = None  # hacky
        landing_path = _download.path

    if not should_continue:
        logger.info("Stopping pipeline...")
        sys.exit(0)

    logger.info("Hash didn't match, keep going !")

    # ==============================
    # Step 4: Transform to bronze
    # ==============================
    logger.info("=" * 80)
    logger.info("Transforming to bronze layer...")
    logger.info("=" * 80)
    _bronze = PipelineTransformer.to_bronze(
        landing_path=landing_path, dataset=_dataset, run_version=_run_version
    )
    logger.info("Bronze transformation completed", extra=_bronze.to_serializable())

    # ==============================
    # Step 5: Transform to silver
    # ==============================
    logger.info("=" * 80)
    logger.info("Transforming to silver layer...")
    logger.info("=" * 80)
    _silver = PipelineTransformer.to_silver(
        bronze_result=_bronze, dataset=_dataset, run_version=_run_version
    )

    logger.info("Silver transformation completed !", extra=_silver.to_serializable())

    logger.info(
        "Pipeline completed successfully !",
        extra={"duration": (datetime.now() - start_time).total_seconds()},
    )

    # ==============================
    # Step 6: Save successful run metadata
    # ==============================
    # TODO, pas descendu entre les tâches actuellement donc Airflow ne pourra pas les utiliser
    if not _extraction:
        sha256 = _download.sha256
        file_size_mib = _download.size_mib
    else:
        sha256 = _extraction.extracted_file_sha256
        file_size_mib = _extraction.size_mib

    PipelineStateManager.update_success(
        dataset_name=_dataset.name,
        version=_run_version,
        etag=_metadata.remote_file_metadata.etag,
        last_modified=_metadata.remote_file_metadata.last_modified,
        content_length=_metadata.remote_file_metadata.content_length,
        sha256=sha256,
        file_size_mib=file_size_mib,
        row_count=_silver.row_count,
        columns=_silver.columns,
    )
