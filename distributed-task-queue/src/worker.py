import json
import logging
import os
import random
import socket
import threading
import time
import uuid

from broker import KafkaBroker
from config import (
    HEARTBEAT_INTERVAL_SECONDS,
    LOG_LEVEL,
    MAX_BACKOFF_SECONDS,
    STATUS_COMPLETED,
    STATUS_DEAD,
    STATUS_IN_PROGRESS,
)
from database import Database

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [worker] %(message)s",
)
logger = logging.getLogger(__name__)

import requests

API_ENDPOINTS: dict[str, str] = {
    "fetch_joke":    "https://official-joke-api.appspot.com/random_joke",
    "fetch_dog":     "https://dog.ceo/api/breeds/image/random",
    "fetch_user":    "https://randomuser.me/api/",
    "fetch_fact":    "https://catfact.ninja/fact",
    "fetch_ip":      "https://api.ipify.org?format=json",
    "fetch_product": "https://dummyjson.com/products/1",
    "fetch_pokemon": "https://pokeapi.co/api/v2/pokemon/pikachu",
    "fetch_chuck":   "https://api.chucknorris.io/jokes/random",
    "fetch_country": "https://restcountries.com/v3.1/alpha/IN",
    "fetch_number":  "http://numbersapi.com/random/trivia?json",
}

# Failure rate per task type (5-15%) to demonstrate retry mechanics
FAILURE_RATES: dict[str, float] = {
    "fetch_joke": 0.05, "fetch_dog": 0.05, "fetch_user": 0.10,
    "fetch_fact": 0.05, "fetch_ip": 0.05, "fetch_product": 0.10,
    "fetch_pokemon": 0.15, "fetch_chuck": 0.05, "fetch_country": 0.10,
    "fetch_number": 0.10,
}


class WorkerNode:
    def __init__(self, worker_id: str | None = None) -> None:
        self.worker_id = worker_id or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self.db = Database()
        self.broker = KafkaBroker(self.db)
        self._stop_event = threading.Event()

    def start_heartbeat(self) -> threading.Thread:
        def _loop() -> None:
            while not self._stop_event.is_set():
                try:
                    self.broker.set_worker_heartbeat(self.worker_id)
                except Exception:
                    logger.exception("Failed heartbeat for worker %s", self.worker_id)
                time.sleep(HEARTBEAT_INTERVAL_SECONDS)

        thread = threading.Thread(target=_loop, daemon=True)
        thread.start()
        return thread

    def _simulate_task(self, task_type: str, task_id: str, timeout_seconds: int = 8) -> None:
        logger.info("Worker %s processing %s (task_id=%s, timeout=%ss)",
                    self.worker_id, task_type, task_id, timeout_seconds)
        if task_type not in API_ENDPOINTS:
            raise ValueError(f"Unknown task_type: {task_type}")
        try:
            res = requests.get(
                API_ENDPOINTS[task_type],
                timeout=timeout_seconds,
                headers={"User-Agent": "DistributedTaskQueueDemo/1.0"},
            )
            res.raise_for_status()
            logger.info("Task %s OK — %s bytes", task_id, len(res.content))
        except requests.RequestException as exc:
            raise RuntimeError(f"{task_type} API call failed: {exc}") from exc
        if random.random() < FAILURE_RATES.get(task_type, 0.10):
            raise RuntimeError(f"Simulated post-fetch failure for {task_type}")

    def run(self) -> None:
        logger.info("Starting worker_id=%s", self.worker_id)
        self.start_heartbeat()

        connection_backoff = 1
        while not self._stop_event.is_set():
            try:
                raw_task = self.broker.pop_active_task(timeout_seconds=5)
                connection_backoff = 1
                if not raw_task:
                    continue

                try:
                    task = json.loads(raw_task)
                except (json.JSONDecodeError, TypeError):
                    logger.warning("Dropping malformed task payload")
                    continue

                task_id = task.get("task_id")
                if not task_id:
                    logger.warning("Dropping task without task_id")
                    continue

                # Skip tasks already in a terminal state (e.g. cancelled)
                if self.broker.is_duplicate_or_terminal(task_id):
                    logger.info("Skipping task %s — already terminal", task_id)
                    continue

                if not self.broker.acquire_task_lock(task_id, self.worker_id, ttl_seconds=180):
                    logger.info("Task %s is already locked by another worker", task_id)
                    continue

                now = time.time()
                self.db.update_task(
                    task_id,
                    status=STATUS_IN_PROGRESS,
                    started_at=now,
                    worker_id=self.worker_id,
                    error_message=None,
                )

                try:
                    self._simulate_task(
                        task["task_type"],
                        task_id,
                        timeout_seconds=int(task.get("timeout_seconds", 8)),
                    )
                    done_at = time.time()
                    self.db.update_task(
                        task_id,
                        status=STATUS_COMPLETED,
                        completed_at=done_at,
                        error_message=None,
                    )
                    self.broker.increment_worker_stat(self.worker_id, "tasks_done", 1)
                    logger.info("Task %s completed by %s", task_id, self.worker_id)
                except Exception as exc:
                    retry_count = int(task.get("retry_count", 0)) + 1
                    max_retries = int(task.get("max_retries", 0))
                    err = str(exc)

                    self.db.update_task(task_id, retry_count=retry_count, error_message=err)
                    task["retry_count"] = retry_count
                    task["worker_id"] = self.worker_id

                    if retry_count < max_retries:
                        delay = min((2 ** retry_count), MAX_BACKOFF_SECONDS)
                        self.db.update_task(task_id, status="PENDING", started_at=None, worker_id=None)
                        self.broker.schedule_retry(task, delay_seconds=delay, reason=err)
                        self.broker.increment_worker_stat(self.worker_id, "tasks_retried", 1)
                        logger.warning(
                            "Task %s failed on %s; retry %s/%s in %ss",
                            task_id, self.worker_id, retry_count, max_retries, delay,
                        )
                    else:
                        self.db.update_task(
                            task_id,
                            status=STATUS_DEAD,
                            completed_at=time.time(),
                            worker_id=self.worker_id,
                        )
                        self.broker.increment_worker_stat(self.worker_id, "tasks_failed", 1)
                        logger.error(
                            "Task %s moved to DEAD after retries exhausted (%s/%s)",
                            task_id, retry_count, max_retries,
                        )
                finally:
                    self.broker.release_task_lock(task_id)

            except Exception:
                logger.exception(
                    "Worker %s connection/unexpected error. Reconnecting in %ss",
                    self.worker_id, connection_backoff,
                )
                time.sleep(connection_backoff)
                connection_backoff = min(connection_backoff * 2, 30)


if __name__ == "__main__":
    worker_id_from_env = os.getenv("WORKER_ID")
    WorkerNode(worker_id=worker_id_from_env).run()
