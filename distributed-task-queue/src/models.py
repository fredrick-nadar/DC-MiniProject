from dataclasses import dataclass
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from config import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_PRIORITY,
    STATUS_PENDING,
    VALID_TASK_TYPES,
)


class SubmitTaskRequest(BaseModel):
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=DEFAULT_PRIORITY, ge=1, le=10)
    max_retries: int = Field(default=DEFAULT_MAX_RETRIES, ge=0, le=20)

    @field_validator("task_type")
    @classmethod
    def validate_task_type(cls, value: str) -> str:
        if value not in VALID_TASK_TYPES:
            raise ValueError(f"Unsupported task_type '{value}'. Allowed: {sorted(VALID_TASK_TYPES)}")
        return value


class SubmitTaskResponse(BaseModel):
    task_id: str
    status: str = STATUS_PENDING
    message: str


class TaskResponse(BaseModel):
    task_id: str
    task_type: str
    payload: dict[str, Any]
    status: str
    priority: int
    retry_count: int
    max_retries: int
    created_at: float
    started_at: Optional[float]
    completed_at: Optional[float]
    worker_id: Optional[str]
    error_message: Optional[str]


class RetryTaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


@dataclass
class WorkerStatus:
    worker_id: str
    status: str
    last_heartbeat: Optional[float]
    tasks_done: int = 0
    tasks_failed: int = 0
