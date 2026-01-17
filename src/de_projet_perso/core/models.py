"""TODO."""

from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    """Base Pydantic model that forbids extra fields.

    This model enforces strict validation by rejecting any fields not
    explicitly defined in the schema, preventing typos and configuration drift.
    """

    model_config = ConfigDict(extra="forbid")
