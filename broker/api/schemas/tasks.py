from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PublishTaskRequest(BaseModel):
    task_type: str = Field(min_length=1)
    payload: dict[str, Any]
    delay_seconds: int = Field(default=0, ge=0)
    max_retries: int | None = Field(default=None, ge=0)


class PublishTaskResponse(BaseModel):
    task_id: str


class TaskStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    status: str
    retries: int
    max_retries: int
    available_at: datetime
    created_at: datetime
