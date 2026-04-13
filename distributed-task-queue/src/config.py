import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Kafka topic — 3 partitions; priority band determines partition (see broker.py)
TOPIC_TASKS = os.getenv("TOPIC_TASKS", "tasks.queue")

# Consumer group shared by all workers (Kafka load-balances partitions)
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "workers")

# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parents[1] / "data" / "tasks.db"))

# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# ---------------------------------------------------------------------------
# Worker / fault detection timings (seconds)
# ---------------------------------------------------------------------------
HEARTBEAT_INTERVAL_SECONDS = 5
WORKER_TIMEOUT_SECONDS = 15
FAULT_MANAGER_INTERVAL_SECONDS = 10
SCHEDULER_INTERVAL_SECONDS = 1

# ---------------------------------------------------------------------------
# Retry / backoff
# ---------------------------------------------------------------------------
MAX_BACKOFF_SECONDS = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_PRIORITY = 5

# ---------------------------------------------------------------------------
# Task statuses
# ---------------------------------------------------------------------------
STATUS_PENDING = "PENDING"
STATUS_IN_PROGRESS = "IN_PROGRESS"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
STATUS_DEAD = "DEAD"
STATUS_CANCELLED = "CANCELLED"

TERMINAL_STATUSES = {
    STATUS_COMPLETED,
    STATUS_DEAD,
    STATUS_FAILED,
    STATUS_CANCELLED,
}

VALID_TASK_TYPES = {
    "fetch_weather",
    "fetch_books",
    "fetch_wiki",
    "fetch_geocode",
    "fetch_cocktail",
}
