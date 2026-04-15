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
    # Idempotency: if provided, duplicate submissions with the same key
    # return the original task instead of creating a new one.
    idempotency_key: Optional[str] = Field(default=None, max_length=128)
    # Delay: schedule the task to run at least this many seconds from now.
    run_after_seconds: int = Field(default=0, ge=0, le=86400)
    # Timeout: how long the worker HTTP call is allowed to take (seconds).
    timeout_seconds: int = Field(default=8, ge=1, le=60)

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
    # True when an existing idempotent task was returned instead of created
    deduplicated: bool = False


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
    idempotency_key: Optional[str] = None
    timeout_seconds: int = 8


class RetryTaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


class ReplayBatchRequest(BaseModel):
    task_ids: list[str] = Field(..., min_length=1, max_length=100)
    patch_payload: Optional[dict[str, Any]] = None  # optional: overwrite payload on replay


@dataclass
class WorkerStatus:
    worker_id: str
    status: str
    last_heartbeat: Optional[float]
    tasks_done: int = 0
    tasks_failed: int = 0
