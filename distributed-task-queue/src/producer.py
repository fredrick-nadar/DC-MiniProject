import logging
import time
import uuid
from typing import Any

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
    ) -> dict[str, Any]:
        task_id = str(uuid.uuid4())
        now = time.time()

        task = {
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
        }

        self.db.create_task(task)
        self.broker.enqueue_new_task(task)
        logger.info("Submitted task %s type=%s priority=%s", task_id, task_type, priority)
        return task
