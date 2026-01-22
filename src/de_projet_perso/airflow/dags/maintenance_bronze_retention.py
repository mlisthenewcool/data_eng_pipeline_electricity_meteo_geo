"""Bronze retention maintenance DAG.

This DAG runs weekly to clean up old bronze versions across all datasets,
maintaining the retention policy defined in settings (default: 365 days).

Architecture:
- Iterates all datasets from catalog
- Uses PathResolver.cleanup_old_bronze_versions() for each dataset
- Logs detailed summary of deleted files and freed space
- Non-critical: failures don't affect pipeline execution
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.file_manager import FileManager
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.core.settings import settings


@dag(
    dag_id="maintenance_bronze_retention",
    description=f"Clean up bronze versions older than {settings.bronze_retention_days} days",
    schedule="@weekly",  # Run every Sunday
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maintenance", "cleanup", "bronze"],
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=f"""
# Bronze Retention Maintenance

Automatically cleans up old bronze layer versions to manage storage space.

## Configuration

- **Retention Period**: {settings.bronze_retention_days} days
- **Schedule**: Weekly (every Sunday at midnight)
- **Scope**: All datasets in catalog

## How It Works

1. Load all datasets from `data_catalog.yaml`
2. For each dataset:
   - List all bronze versions (*.parquet files)
   - Check file modification time
   - Delete files older than retention period
   - Log deleted files and freed space
3. Generate summary report

## Storage Impact

Bronze files are typically the largest layer (raw data).
Regular cleanup prevents unbounded storage growth while maintaining
sufficient history for auditing and reconstruction.

## Override Retention

To temporarily keep more history, update `ENV_BRONZE_RETENTION_DAYS`
in `.env` or docker-compose environment variables.

Example: Keep 2 years instead of 1:
```bash
ENV_BRONZE_RETENTION_DAYS=730
```

## Manual Execution

You can trigger this DAG manually from the Airflow UI if you need
to free up space immediately or test the cleanup logic.
    """,
)
def bronze_retention_dag():
    """Cleanup old bronze versions across all datasets."""

    @task
    def cleanup_all_datasets():
        """Iterate all datasets and cleanup old bronze files.

        Returns:
            Summary dict with deleted file counts and freed space per dataset
        """
        catalog = DataCatalog.load(settings.data_catalog_file_path)

        total_deleted = 0
        total_freed_mb = 0.0
        summary = []

        for name, dataset in catalog.datasets.items():
            resolver = PathResolver(dataset.name)
            file_manager = FileManager(resolver)

            # Get file sizes before deletion
            versions_to_delete = resolver.list_bronze_versions()

            # Filter by age (mtime)

            cutoff_time = datetime.now() - timedelta(days=settings.bronze_retention_days)
            old_versions = [
                v
                for v in versions_to_delete
                if datetime.fromtimestamp(v.stat().st_mtime) < cutoff_time
            ]

            if not old_versions:
                logger.info(f"No old versions to delete for {name}")
                continue

            # Calculate freed space before deletion
            freed_bytes = sum(v.stat().st_size for v in old_versions if v.exists())

            # Delete old versions
            deleted_files = file_manager.cleanup_old_bronze_versions(settings.bronze_retention_days)

            if deleted_files:
                total_deleted += len(deleted_files)
                freed_mb = freed_bytes / (1024**2)
                total_freed_mb += freed_mb

                summary.append(
                    {
                        "dataset": name,
                        "deleted_count": len(deleted_files),
                        "freed_space_mb": round(freed_mb, 2),
                        "versions_deleted": [v.stem for v in deleted_files],
                    }
                )

                logger.info(
                    f"Cleaned up bronze for {name}",
                    extra={
                        "deleted_count": len(deleted_files),
                        "freed_mb": round(freed_mb, 2),
                    },
                )

        # Log final summary
        logger.info(
            "Bronze retention cleanup complete",
            extra={
                "total_files_deleted": total_deleted,
                "total_freed_mb": round(total_freed_mb, 2),
                "retention_days": settings.bronze_retention_days,
                "datasets_affected": len(summary),
                "details": summary,
            },
        )

        return {
            "total_files_deleted": total_deleted,
            "total_freed_mb": round(total_freed_mb, 2),
            "datasets_affected": len(summary),
            "summary": summary,
        }

    cleanup_all_datasets()


# Register DAG
bronze_retention = bronze_retention_dag()
