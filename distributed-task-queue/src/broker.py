"""
KafkaBroker — replaces RedisBroker.

Transport layer  : Apache Kafka (kafka-python)
State management : SQLite (via Database class)
  - Worker heartbeats   → worker_heartbeats table
  - Task locks          → task_locks table
  - Delayed retry queue → delayed_tasks table
"""

import json
import logging
import time
from typing import Any, Optional

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from config import (
    KAFKA_BOOTSTRAP,
    KAFKA_CONSUMER_GROUP,
    STATUS_PENDING,
    TERMINAL_STATUSES,
    TOPIC_TASKS,
)
from database import Database

logger = logging.getLogger(__name__)

# Number of partitions on TOPIC_TASKS.
# We map priority → partition so higher-priority tasks land on partition 0.
_NUM_PARTITIONS = 3


def _priority_to_partition(priority: int) -> int:
    """
    Map task priority (1-10) to Kafka partition index (0-2).
    Partition 0 = highest priority (8-10)
    Partition 1 = medium priority  (4-7)
    Partition 2 = lowest priority  (1-3)
    """
    if priority >= 8:
        return 0
    if priority >= 4:
        return 1
    return 2


def _ensure_topic(bootstrap: str, topic: str, num_partitions: int = _NUM_PARTITIONS) -> None:
    """Create the Kafka topic if it doesn't already exist."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="dtq-admin")
        try:
            admin.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)])
            logger.info("Created Kafka topic '%s' with %d partitions", topic, num_partitions)
        except TopicAlreadyExistsError:
            logger.debug("Kafka topic '%s' already exists", topic)
        finally:
            admin.close()
    except KafkaError:
        logger.exception("Could not ensure Kafka topic '%s' exists", topic)


class KafkaBroker:
    """
    Public interface matches the old RedisBroker so all callers
    (api.py, worker.py, producer.py, scheduler.py, fault_manager.py)
    need only change their import and constructor call.
    """

    def __init__(self, db: Database, bootstrap: str = KAFKA_BOOTSTRAP) -> None:
        self.db = db
        self.bootstrap = bootstrap
        _ensure_topic(bootstrap, TOPIC_TASKS)
        self._producer: Optional[KafkaProducer] = None
        self._consumer: Optional[KafkaConsumer] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
        return self._producer

    def get_consumer(self) -> KafkaConsumer:
        """Return (or lazily create) the shared KafkaConsumer for this broker instance."""
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                TOPIC_TASKS,
                bootstrap_servers=self.bootstrap,
                group_id=KAFKA_CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=5000,  # poll() returns after 5 s if no messages
            )
        return self._consumer

    def _publish(self, task: dict[str, Any]) -> None:
        partition = _priority_to_partition(task.get("priority", 5))
        self._get_producer().send(TOPIC_TASKS, value=task, partition=partition)
        self._get_producer().flush()

    # ------------------------------------------------------------------
    # Connectivity
    # ------------------------------------------------------------------

    def ping(self) -> bool:
        try:
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap, consumer_timeout_ms=2000)
            consumer.topics()
            consumer.close()
            return True
        except KafkaError:
            return False

    # ------------------------------------------------------------------
    # Task enqueueing / requeuing
    # ------------------------------------------------------------------

    def enqueue_new_task(self, task: dict[str, Any]) -> None:
        self._publish(task)
        logger.debug("Enqueued task %s (priority=%s)", task["task_id"], task.get("priority"))

    def requeue_immediate(self, task: dict[str, Any], reason: str = "") -> None:
        task["status"] = STATUS_PENDING
        self._publish(task)
        if reason:
            self.db.log_event("task_requeued", reason, task_id=task["task_id"], worker_id=task.get("worker_id"))
        logger.debug("Requeued task %s immediately: %s", task["task_id"], reason)

    def schedule_retry(self, task: dict[str, Any], delay_seconds: int, reason: str) -> None:
        execute_after = time.time() + delay_seconds
        task["status"] = STATUS_PENDING
        self.db.schedule_delayed_task(task, execute_after)
        self.db.append_retry_history(task["task_id"], task["retry_count"], execute_after, reason)
        logger.debug(
            "Scheduled retry for task %s in %ds (retry=%s)",
            task["task_id"],
            delay_seconds,
            task["retry_count"],
        )

    # ------------------------------------------------------------------
    # Consuming tasks (called by worker)
    # ------------------------------------------------------------------

    def pop_active_task(self, timeout_seconds: int = 5) -> Optional[str]:
        """
        Poll Kafka for the next task message.
        Returns the raw JSON string, or None if no message arrived within the timeout.
        """
        consumer = self.get_consumer()
        # consumer_timeout_ms is already set to 5000 ms; poll() iterates until timeout
        for msg in consumer:
            return json.dumps(msg.value)
        return None

    # ------------------------------------------------------------------
    # Delayed queue flush (called by scheduler)
    # ------------------------------------------------------------------

    def flush_due_delayed_tasks(self, max_batch: int = 200) -> int:
        """
        Move tasks from the SQLite delayed_tasks table back onto Kafka
        when their execute_after time has elapsed.
        Returns the number of tasks flushed.
        """
        due = self.db.get_due_delayed_tasks(max_batch)
        for row_id, task in due:
            try:
                self._publish(task)
                self.db.delete_delayed_task(row_id)
            except KafkaError:
                logger.exception("Failed to republish delayed task %s", task.get("task_id"))
        return len(due)

    # ------------------------------------------------------------------
    # Worker heartbeats (SQLite-backed)
    # ------------------------------------------------------------------

    def set_worker_heartbeat(self, worker_id: str) -> None:
        self.db.set_worker_heartbeat(worker_id)

    def get_worker_heartbeats(self) -> dict[str, float]:
        return self.db.get_worker_heartbeats()

    def increment_worker_stat(self, worker_id: str, field: str, amount: int = 1) -> None:
        self.db.increment_worker_stat(worker_id, field, amount)

    def get_worker_stats(self, worker_id: str) -> dict[str, int]:
        return self.db.get_worker_stats(worker_id)

    # ------------------------------------------------------------------
    # Task locks (SQLite-backed, replaces Redis SET NX)
    # ------------------------------------------------------------------

    def acquire_task_lock(self, task_id: str, owner: str, ttl_seconds: int = 120) -> bool:
        return self.db.acquire_task_lock(task_id, owner, ttl_seconds)

    def release_task_lock(self, task_id: str) -> None:
        self.db.release_task_lock(task_id)

    # ------------------------------------------------------------------
    # Duplicate / terminal guard
    # ------------------------------------------------------------------

    def is_duplicate_or_terminal(self, task_id: str) -> bool:
        task = self.db.get_task(task_id)
        if not task:
            return False
        return task["status"] in TERMINAL_STATUSES

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------

    def remove_task_from_queues(self, task_id: str) -> int:
        """
        Kafka messages cannot be deleted mid-stream.
        We mark cancellation in SQLite; the worker will detect the
        terminal status via is_duplicate_or_terminal() and skip processing.
        Returns 1 as a logical 'removed' indicator.
        """
        logger.info(
            "Task %s marked CANCELLED in SQLite; Kafka message will be skipped by workers", task_id
        )
        return 1

    def get_queue_depths(self) -> dict[str, int]:
        return self.db.get_queue_depths()

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._producer:
            self._producer.close()
        if self._consumer:
            self._consumer.close()
