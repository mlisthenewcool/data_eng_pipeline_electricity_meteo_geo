"""Todo."""

from enum import StrEnum


class PipelineAction(StrEnum):
    """todo."""

    FIRST_RUN = "first_run"
    REFRESH = "refresh"
    SKIP = "skip"
