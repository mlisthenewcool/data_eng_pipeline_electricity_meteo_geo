"""Pipeline decision logic.

This module contains pure business logic for determining pipeline actions
based on state, completely decoupled from Airflow orchestration.
"""

from datetime import datetime, timedelta
from pathlib import Path

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.state import PipelineAction, PipelineState, PipelineStateManager


class PipelineDecisionEngine:
    """Decision logic for determining pipeline actions."""

    @staticmethod
    def decide_action(
        dataset_name: str,
        dataset: Dataset,
        data_dir: Path,
    ) -> PipelineAction:
        """Determine which action to take based on pipeline state.

        Priority order:
        1. FIRST_RUN - First run (no state file)
        2. HEAL - File missing from disk
        3. RETRY - Previous run failed
        4. REFRESH - Data is stale based on frequency
        5. SKIP - Everything is up to date

        Args:
            dataset_name: Dataset identifier
            dataset: Dataset configuration
            data_dir: Root data directory

        Returns:
            PipelineAction enum value
        """
        state = PipelineStateManager.load(dataset_name)

        # 1. FIRST_RUN - No state means first run
        if state is None:
            logger.info(
                "No state found, forcing execution",
                extra={"dataset": dataset_name, "action": PipelineAction.FIRST_RUN},
            )
            return PipelineAction.FIRST_RUN

        # 2. HEAL - Check if expected files exist
        expected_path = data_dir / dataset.get_storage_path("silver")
        if not expected_path.exists():
            logger.info(
                "Expected file missing, healing",
                extra={
                    "dataset": dataset_name,
                    "path": str(expected_path),
                    "action": PipelineAction.HEAL,
                },
            )
            return PipelineAction.HEAL

        # 3. RETRY - Last run failed
        if state.last_failed_run is not None:
            if state.last_successful_run is None or (
                state.last_failed_run.timestamp > state.last_successful_run.timestamp
            ):
                logger.info(
                    "Last run failed, retrying",
                    extra={
                        "dataset": dataset_name,
                        "failed_at": state.last_failed_run.timestamp.isoformat(),
                        "action": PipelineAction.RETRY,
                    },
                )
                return PipelineAction.RETRY

        # 4. REFRESH - Check if data is stale based on frequency
        if state.last_successful_run is not None:
            frequency_delta = PipelineDecisionEngine.frequency_to_timedelta(
                dataset.ingestion.frequency
            )
            if frequency_delta is not None:
                age = datetime.now() - state.last_successful_run.timestamp
                if age > frequency_delta:
                    logger.info(
                        "Data is stale, refreshing",
                        extra={
                            "dataset": dataset_name,
                            "age_hours": round(age.total_seconds() / 3600, 1),
                            "action": PipelineAction.REFRESH,
                        },
                    )
                    return PipelineAction.REFRESH

        # 5. SKIP - All good
        logger.info(
            "Data is up to date",
            extra={"dataset": dataset_name, "action": PipelineAction.SKIP},
        )
        return PipelineAction.SKIP

    @staticmethod
    def infer_action_from_state(
        state: PipelineState | None,
        dataset: Dataset,
        data_dir: Path,
    ) -> PipelineAction:
        """Infer which action was taken based on current state.

        This helper reconstructs the action taken by applying the same
        logic as decide_action(), useful for metadata enrichment.

        Args:
            state: Current pipeline state (PipelineState or None)
            dataset: Dataset configuration
            data_dir: Root data directory

        Returns:
            PipelineAction enum value
        """
        if state is None:
            return PipelineAction.FIRST_RUN

        expected_path = data_dir / dataset.get_storage_path("silver")
        if not expected_path.exists():
            return PipelineAction.HEAL

        if state.last_failed_run is not None:
            if state.last_successful_run is None or (
                state.last_failed_run.timestamp > state.last_successful_run.timestamp
            ):
                return PipelineAction.RETRY

        if state.last_successful_run is not None:
            frequency_delta = PipelineDecisionEngine.frequency_to_timedelta(
                dataset.ingestion.frequency
            )
            if frequency_delta is not None:
                age = datetime.now() - state.last_successful_run.timestamp
                if age > frequency_delta:
                    return PipelineAction.REFRESH

        return PipelineAction.SKIP

    @staticmethod
    def frequency_to_timedelta(frequency: str) -> timedelta | None:
        """Convert ingestion frequency to timedelta for staleness check.

        Args:
            frequency: Frequency string (hourly, daily, weekly, monthly, yearly)

        Returns:
            Corresponding timedelta or None if unknown frequency
        """
        mapping = {
            "hourly": timedelta(hours=1),
            "daily": timedelta(days=1),
            "weekly": timedelta(weeks=1),
            "monthly": timedelta(days=30),
            "yearly": timedelta(days=365),
        }
        return mapping.get(str(frequency))
