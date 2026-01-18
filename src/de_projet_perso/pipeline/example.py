"""Example script to test pipeline logic without Airflow.

This script demonstrates the complete data pipeline flow:
- Download source data
- Extract from archive (if needed)
- Validate landing layer
- Transform to bronze layer (Parquet conversion)
- Transform to silver layer (business logic)

Run with: PYTHONPATH=src uv run python src/de_projet_perso/pipeline/example.py
"""

import sys

import httpx

from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.exceptions import (
    ArchiveNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
)
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.downloader import PipelineDownloader
from de_projet_perso.pipeline.results import ExtractionResult
from de_projet_perso.pipeline.transformer import PipelineTransformer

if __name__ == "__main__":
    # ===================================================================================
    # /!\ DO NOT USE sys.exit(-1) WHEN RUNNING ON AIRFLOW. Let Airflow handle exceptions.
    # ===================================================================================
    _catalog = DataCatalog.load(settings.data_catalog_file_path)

    _dataset_name = "ign_contours_iris"
    _dataset = _catalog.get_dataset(_dataset_name)

    # ==============================
    # download task
    # ==============================
    logger.info("=" * 80)
    try:
        _download_result = PipelineDownloader.download(_dataset)
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
        # TODO: simuler disque plein ?
        logger.critical("Download failed. Unexpected error", extra={"more_infos": str(e)})
        sys.exit(-1)

    logger.info("download task completed !", extra={"_download_result": _download_result})
    logger.info("=" * 80)

    # ==============================
    # extract task (optional)
    # ==============================
    if _dataset.source.inner_file is not None:
        try:
            _extract_result = PipelineDownloader.extract_archive(
                archive_path=_download_result.path,
                archive_sha256=_download_result.sha256,
                dataset=_dataset,
            )
        except (
            ArchiveNotFoundError,
            FileNotFoundInArchiveError,
            FileIntegrityError,
        ) as e:
            logger.exception("Extraction failed", extra={"error": str(e)})
            sys.exit(-1)

        logger.info("extract task completed !", extra={"_extract_result": _extract_result})
    else:
        # TODO: retirer ce besoin, on devrait pouvoir passer à la tâche bronze directement
        _extract_result = ExtractionResult(
            path=_download_result.path,
            size_mib=_download_result.size_mib,
            extracted_sha256=_download_result.sha256,
            archive_sha256=_download_result.sha256,
            original_filename=_download_result.original_filename,
        )
        logger.info("extract task skipped !", extra={"_extract_result": _extract_result})

    logger.info("=" * 80)

    # ==============================
    # merge results from either download or extract
    # ==============================
    # TODO: is that the correct way ?
    _landing_result = PipelineDownloader.validate_landing(_extract_result)
    logger.info("landing task completed !", extra={"_landing_result": _landing_result})
    logger.info("=" * 80)

    # ==============================
    # landing_to_bronze
    # ==============================
    _bronze_result = PipelineTransformer.to_bronze(
        landing_result=_landing_result, dataset_name=_dataset_name, dataset=_dataset
    )
    logger.info("landing_to_bronze task completed !", extra={"_bronze_result": _bronze_result})
    logger.info("=" * 80)

    # ==============================
    # bronze_to_silver
    # ==============================
    _silver_result = PipelineTransformer.to_silver(
        bronze_result=_bronze_result, dataset_name=_dataset_name, dataset=_dataset
    )
    logger.info("bronze_to_silver task completed !", extra={"_silver_result": _silver_result})
