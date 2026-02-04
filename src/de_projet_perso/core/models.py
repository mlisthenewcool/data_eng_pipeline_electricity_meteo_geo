"""Base Pydantic model with strict schema enforcement."""

from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    """Base model that forbids extra fields to prevent typos and configuration drift."""

    model_config = ConfigDict(extra="forbid")
