import logging
import time
import uuid
from typing import Any, Optional

from config import STATUS_PENDING
from database import Database
from broker import KafkaBroker

logger = logging.getLogger(__name__)


class TaskProducer:
    def __init__(self, db: Database, broker: KafkaBroker) -> None:
        self.db = db
        self.broker = broker

    def submit_task(
        self,
        task_type: str,
        payload: dict[str, Any],
        priority: int,
        max_retries: int,
        idempotency_key: Optional[str] = None,
        run_after_seconds: int = 0,
        timeout_seconds: int = 8,
    ) -> tuple[dict[str, Any], bool]:
        """
        Returns (task_dict, deduplicated).
        If idempotency_key matches an existing task, that task is returned
        with deduplicated=True and no new task is created.
        """
        # ── Idempotency check ──────────────────────────────────────────────
        if idempotency_key:
            existing = self.db.find_by_idempotency_key(idempotency_key)
            if existing:
                logger.info(
                    "Idempotent task found for key=%s → task_id=%s",
                    idempotency_key,
                    existing["task_id"],
                )
                return existing, True

        # ── Build task dict ────────────────────────────────────────────────
        task_id = str(uuid.uuid4())
        now = time.time()

        task: dict[str, Any] = {
            "task_id": task_id,
            "task_type": task_type,
            "payload": payload,
            "status": STATUS_PENDING,
            "priority": priority,
            "retry_count": 0,
            "max_retries": max_retries,
            "created_at": now,
            "started_at": None,
            "completed_at": None,
            "worker_id": None,
            "error_message": None,
            "idempotency_key": idempotency_key,
            "timeout_seconds": timeout_seconds,
        }

        self.db.create_task(task)

        # ── Schedule or enqueue immediately ───────────────────────────────
        if run_after_seconds > 0:
            execute_after = now + run_after_seconds
            self.broker.schedule_delayed_task_direct(task, execute_after)
            logger.info(
                "Scheduled task %s type=%s delay=%ss",
                task_id, task_type, run_after_seconds,
            )
        else:
            self.broker.enqueue_new_task(task)
            logger.info(
                "Submitted task %s type=%s priority=%s",
                task_id, task_type, priority,
            )

        return task, False
