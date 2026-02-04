"""File operations for medallion architecture (symlinks, rotation, rollback, cleanup)."""

import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.core.settings import settings


@dataclass(frozen=True)
class FileManager:
    """File operations companion to PathResolver: symlinks, rotation, rollback, cleanup."""

    resolver: PathResolver

    def update_bronze_latest_link(self, target_version: str) -> None:
        """Atomically update latest.parquet symlink. Call AFTER writing a new bronze file."""
        target_file = self.resolver.bronze_path(version=target_version)
        latest_link = self.resolver.bronze_latest_path

        if not target_file.exists():
            raise FileNotFoundError(
                f"Cannot create symlink: target bronze file doesn't exist: {target_file}"
            )

        # Create parent directory if needed
        latest_link.parent.mkdir(parents=True, exist_ok=True)

        # Atomic symlink update strategy:
        # 1. Create temporary symlink with unique name
        # 2. Atomically rename it to replace old symlink
        temp_link = latest_link.parent / f".latest.tmp.{os.getpid()}"

        try:
            # Create symlink pointing to relative path (more portable)
            # latest.parquet → 2025-01-17.parquet (not absolute path)
            relative_target = target_file.name
            temp_link.symlink_to(relative_target)

            # Atomic replace (works even if latest_link already exists)
            temp_link.replace(latest_link)

            logger.debug(
                "Updated bronze latest symlink",
                extra={
                    "dataset_name": self.resolver.dataset_name,
                    "target_version": target_version,
                    "symlink": latest_link,
                    "target": relative_target,
                },
            )
        except Exception:
            # Cleanup temp file if error
            if temp_link.exists():
                temp_link.unlink()
            raise

    def cleanup_old_bronze_versions(
        self, retention_days: int = settings.bronze_retention_days
    ) -> list[Path]:
        """Remove bronze versions older than retention period. Called by maintenance DAG."""
        cutoff_time = datetime.now() - timedelta(days=retention_days)
        deleted = []

        # TODO: Optimize by calculating cutoff date (YYYYMMDD format) and using binary search
        #       on sorted filenames, instead of checking mtime of all files individually.
        #       This requires version format to be sortable (currently "YYYY-MM-DD" or
        #       "YYYYMMDDTHHMMSS").
        for version_path in self.resolver.list_bronze_versions():
            # Check file modification time
            file_mtime = datetime.fromtimestamp(version_path.stat().st_mtime)

            if file_mtime < cutoff_time:
                version_path.unlink()
                deleted.append(version_path)
                logger.debug(
                    "Deleted old bronze version",
                    extra={
                        "dataset": self.resolver.dataset_name,
                        "version": version_path.stem,
                        "age_days": (datetime.now() - file_mtime).days,
                    },
                )

        return deleted

    def rollback_silver(self) -> bool:
        """Restore backup → current. Returns False if no backup exists."""
        if not self.resolver.silver_backup_path.exists():
            logger.warning(
                "Cannot rollback silver: no backup exists",
                extra={"dataset_name": self.resolver.dataset_name},
            )
            return False

        shutil.copy2(self.resolver.silver_backup_path, self.resolver.silver_current_path)
        logger.debug(
            "Rolled back silver to backup version",
            extra={
                "dataset_name": self.resolver.dataset_name,
                "backup": self.resolver.silver_backup_path,
                "current": self.resolver.silver_current_path,
            },
        )
        return True

    def rotate_silver(self) -> None:
        """Copy current → backup. Call BEFORE writing new current."""
        if self.resolver.silver_current_path.exists():
            # Copy current to back up (overwrite old backup)
            self.resolver.silver_backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.resolver.silver_current_path, self.resolver.silver_backup_path)
            logger.debug(
                "Rotated silver files",
                extra={
                    "dataset_name": self.resolver.dataset_name,
                    "current": str(self.resolver.silver_current_path),
                    "backup": str(self.resolver.silver_backup_path),
                },
            )

    def rotate_gold(self) -> None:
        """Copy current → backup. Call BEFORE writing new current."""
        if self.resolver.gold_current_path.exists():
            # Copy current to backup (overwrite old backup)
            self.resolver.gold_backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.resolver.gold_current_path, self.resolver.gold_backup_path)
            logger.debug(
                "Rotated gold files",
                extra={
                    "dataset_name": self.resolver.dataset_name,
                    "current": str(self.resolver.gold_current_path),
                    "backup": str(self.resolver.gold_backup_path),
                },
            )

    def rollback_gold(self) -> bool:
        """Restore backup → current. Returns False if no backup exists."""
        if not self.resolver.gold_backup_path.exists():
            logger.warning(
                "Cannot rollback gold: no backup exists",
                extra={"dataset_name": self.resolver.dataset_name},
            )
            return False

        shutil.copy2(self.resolver.gold_backup_path, self.resolver.gold_current_path)
        logger.debug(
            "Rolled back gold to backup version",
            extra={
                "dataset_name": self.resolver.dataset_name,
                "backup": str(self.resolver.gold_backup_path),
                "current": str(self.resolver.gold_current_path),
            },
        )
        return True
