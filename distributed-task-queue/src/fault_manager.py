import logging
import time

from broker import KafkaBroker
from config import FAULT_MANAGER_INTERVAL_SECONDS, LOG_LEVEL, STATUS_PENDING, WORKER_TIMEOUT_SECONDS
from database import Database

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [fault-manager] %(message)s",
)
logger = logging.getLogger(__name__)


class FaultToleranceManager:
    def __init__(self) -> None:
        self.db = Database()
        self.broker = KafkaBroker(self.db)

    def _is_worker_dead(self, last_heartbeat: float | None) -> bool:
        if last_heartbeat is None:
            return True
        return (time.time() - last_heartbeat) > WORKER_TIMEOUT_SECONDS

    def run(self) -> None:
        logger.info("Fault manager started")
        backoff = 1
        while True:
            try:
                heartbeats = self.broker.get_worker_heartbeats()
                in_progress = self.db.get_in_progress_tasks()
                now = time.time()

                for task in in_progress:
                    task_id = task["task_id"]
                    worker_id = task.get("worker_id")
                    if not worker_id:
                        continue

                    hb = heartbeats.get(worker_id)
                    if self._is_worker_dead(hb):
                        msg = (
                            f"Worker {worker_id} considered dead while running task {task_id}; "
                            "reassigning task"
                        )
                        logger.warning(msg)
                        self.db.log_event("worker_dead_detected", msg, task_id=task_id, worker_id=worker_id)

                        if not self.broker.acquire_task_lock(task_id, "fault-manager", ttl_seconds=60):
                            continue

                        try:
                            latest = self.db.get_task(task_id)
                            if not latest or latest["status"] != "IN_PROGRESS":
                                continue

                            latest["status"] = STATUS_PENDING
                            latest["worker_id"] = None
                            latest["started_at"] = None
                            latest["error_message"] = f"Recovered from dead worker {worker_id} at {now:.0f}"

                            self.db.update_task(
                                task_id,
                                status=STATUS_PENDING,
                                worker_id=None,
                                started_at=None,
                                error_message=latest["error_message"],
                            )
                            self.broker.requeue_immediate(latest, reason=msg)
                            self.db.log_event(
                                "task_recovered",
                                "Task was requeued after worker crash detection",
                                task_id=task_id,
                                worker_id=worker_id,
                            )
                        finally:
                            self.broker.release_task_lock(task_id)

                backoff = 1
                time.sleep(FAULT_MANAGER_INTERVAL_SECONDS)
            except Exception:
                logger.exception("Fault manager error. Retrying in %ss", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    FaultToleranceManager().run()
