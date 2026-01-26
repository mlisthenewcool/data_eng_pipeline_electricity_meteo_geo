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
from de_projet_perso.core.exception_handler import (
    log_exception_with_extra,
)
from de_projet_perso.core.exceptions import (
    ArchiveNotFoundError,
    DatasetNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
    InvalidCatalogError,
)
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.constants import PipelineAction
from de_projet_perso.pipeline.manager import PipelineManager
from de_projet_perso.pipeline.state import PipelineStateManager

if __name__ == "__main__":
    # ===================================================================================
    # /!\ DO NOT USE sys.exit() WHEN RUNNING ON AIRFLOW. Let Airflow handle exceptions.
    # ===================================================================================
    _start_time = datetime.now()

    try:
        _catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        log_exception_with_extra(e)
        sys.exit(-1)

    # __dataset_name = "meteo_france_stations"
    # __dataset_name = "odre_installations"
    __dataset_name = "ign_contours_iris"
    # __dataset_name = "odre_eco2mix_cons_def"

    try:
        _dataset = _catalog.get_dataset(__dataset_name)
    except DatasetNotFoundError as ds_not_found_error:
        log_exception_with_extra(ds_not_found_error)
        sys.exit(-1)

    # ==============================
    # Step 0: Prepare version, PathResolver and PipelineManager
    # ==============================
    _version = _dataset.ingestion.frequency.format_datetime_as_version(_start_time)
    _path_resolver = PathResolver(dataset_name=_dataset.name)
    _manager = PipelineManager(_dataset)

    # ==============================
    # Step 1: Check if metadata changed
    # ==============================
    logger.info("=" * 80)
    logger.info("Checking metadata ...")
    logger.info("=" * 80)
    _metadata = _manager.has_dataset_metadata_changed()
    if _metadata.action == PipelineAction.SKIP:
        _state = PipelineStateManager.load(_dataset.name)
        sys.exit(0)

    logger.info(
        "Server metadata has changed or we found insuffisant metadata, will download data to check",
        extra=_metadata.model_dump(),
    )

    # ==============================
    # Step 2: Download
    # ==============================
    logger.info("=" * 80)
    logger.info("Downloading dataset...")
    logger.info("=" * 80)
    try:
        _ingest = _manager.ingest(_version, _metadata.remote_metadata)
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

    logger.info("Download completed !", extra=_ingest.model_dump())

    # ==============================
    # Step 3: Extract (if archive) + check if hash changed
    # ==============================
    if _dataset.source.format.is_archive:
        logger.info("=" * 80)
        logger.info("Extracting archive...")
        logger.info("=" * 80)
        try:
            _extract = _manager.extract_archive(_ingest)
        except (
            ArchiveNotFoundError,
            FileNotFoundInArchiveError,
            FileIntegrityError,
        ) as e:
            logger.exception("Extraction failed", extra={"error": str(e)})
            sys.exit(-1)

        logger.info("Extraction completed !", extra=_extract.model_dump())

        logger.info("=" * 80)
        logger.info("Checking hash...")
        logger.info("=" * 80)
        _should_continue = _manager.has_hash_changed(_extract)
    else:
        logger.info("=" * 80)
        logger.info("Checking hash...")
        logger.info("=" * 80)
        _should_continue = _manager.has_hash_changed(_ingest)

        # For non-archive: get landing_path from download
        _extract = None  # for analyzer: hacky

    if not _should_continue:
        logger.info("Stopping pipeline...")
        sys.exit(0)

    logger.info("Hash didn't match, keep going !")

    # ==============================
    # Step 4: Transform to bronze
    # ==============================
    logger.info("=" * 80)
    logger.info("Transforming to bronze layer...")
    logger.info("=" * 80)
    _bronze = _manager.to_bronze(_extract if _extract else _ingest)
    logger.info("Bronze transformation completed", extra=_bronze.model_dump())

    # ==============================
    # Step 5: Transform to silver
    # ==============================
    logger.info("=" * 80)
    logger.info("Transforming to silver layer...")
    logger.info("=" * 80)
    _silver = _manager.to_silver(_bronze)

    logger.info("Silver transformation completed !", extra=_silver.model_dump())

    logger.info(
        "Pipeline completed successfully !",
        extra={"duration": (datetime.now() - _start_time).total_seconds()},
    )

    # ==============================
    # Step 6: Save successful run metadata
    # ==============================
    PipelineStateManager.update_success(dataset_name=_dataset.name, data=_silver)
