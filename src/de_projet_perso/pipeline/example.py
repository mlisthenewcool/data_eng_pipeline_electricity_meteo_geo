"""Example script to test pipeline logic without Airflow.

This script demonstrates the complete data pipeline flow:
- Download source data
- Extract from archive (if needed)
- Validate landing layer
- Transform to bronze layer (Parquet conversion)
- Transform to silver layer (business logic)

Run with: PYTHONPATH=src uv run python src/de_projet_perso/pipeline/example.py
"""

from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.downloader import PipelineDownloader
from de_projet_perso.pipeline.results import ExtractionResult
from de_projet_perso.pipeline.transformer import PipelineTransformer

if __name__ == "__main__":
    _catalog = DataCatalog.load(settings.data_catalog_file_path)

    _dataset_name = "ign_contours_iris"
    _dataset = _catalog.get_dataset(_dataset_name)

    # ==============================
    # download
    # ==============================
    _download_result = PipelineDownloader.download(_dataset_name, _dataset)
    logger.info("download task completed !", extra={"_download_result": _download_result})
    logger.info("=" * 80)

    # ==============================
    # extract
    # ==============================
    if _dataset.source.inner_file is not None:
        # TODO: should pass DownloadResult directly
        _extract_result = PipelineDownloader.extract_archive(
            archive_path=_download_result.path,
            archive_sha256=_download_result.sha256,
            dataset=_dataset,
        )
        logger.info("extract task completed !", extra={"_extract_result": _extract_result})
    else:
        _extract_result = ExtractionResult(
            path=_download_result.path,
            size_mib=_download_result.size_mib,
            extracted_sha256=_download_result.sha256,
            archive_sha256=_download_result.sha256,  # TODO: should be none ?
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
    logger.info("=" * 80)
