import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

from config import DB_PATH

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._ensure_parent()
        self._init_db()

    def _ensure_parent(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    completed_at REAL,
                    worker_id TEXT,
                    error_message TEXT,
                    last_updated REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_tasks_worker ON tasks(worker_id);

                CREATE TABLE IF NOT EXISTS retry_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    scheduled_at REAL NOT NULL,
                    reason TEXT,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS system_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    task_id TEXT,
                    worker_id TEXT,
                    message TEXT,
                    created_at REAL NOT NULL
                );

                -- Replaces Redis heartbeat keys (previously stored as Redis key with TTL)
                CREATE TABLE IF NOT EXISTS worker_heartbeats (
                    worker_id TEXT PRIMARY KEY,
                    last_seen REAL NOT NULL,
                    stats TEXT NOT NULL DEFAULT '{}'
                );

                -- Replaces Redis SET NX task locks
                CREATE TABLE IF NOT EXISTS task_locks (
                    task_id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    expires_at REAL NOT NULL
                );

                -- Replaces Redis ZSET delayed queue
                CREATE TABLE IF NOT EXISTS delayed_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    execute_after REAL NOT NULL
                );
                """
            )
            conn.commit()

    # -----------------------------------------------------------------------
    # Tasks
    # -----------------------------------------------------------------------

    def create_task(self, task: dict[str, Any]) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tasks (
                    task_id, task_type, payload, status, priority, retry_count,
                    max_retries, created_at, started_at, completed_at,
                    worker_id, error_message, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task["task_id"],
                    task["task_type"],
                    json.dumps(task.get("payload", {})),
                    task["status"],
                    task["priority"],
                    task["retry_count"],
                    task["max_retries"],
                    task["created_at"],
                    task.get("started_at"),
                    task.get("completed_at"),
                    task.get("worker_id"),
                    task.get("error_message"),
                    time.time(),
                ),
            )
            conn.commit()

    def get_task(self, task_id: str) -> Optional[dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            if not row:
                return None
            task = dict(row)
            task["payload"] = json.loads(task["payload"])
            return task

    def list_tasks(self, status: Optional[str] = None) -> list[dict[str, Any]]:
        with self._connect() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM tasks WHERE status = ? ORDER BY created_at DESC", (status,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC").fetchall()
        tasks: list[dict[str, Any]] = []
        for row in rows:
            task = dict(row)
            task["payload"] = json.loads(task["payload"])
            tasks.append(task)
        return tasks

    def update_task(self, task_id: str, **updates: Any) -> bool:
        if not updates:
            return False
        updates["last_updated"] = time.time()
        columns = ", ".join([f"{key} = ?" for key in updates.keys()])
        values = list(updates.values()) + [task_id]
        with self._lock, self._connect() as conn:
            cur = conn.execute(f"UPDATE tasks SET {columns} WHERE task_id = ?", values)
            conn.commit()
            return cur.rowcount > 0

    def append_retry_history(self, task_id: str, retry_count: int, scheduled_at: float, reason: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO retry_history (task_id, retry_count, scheduled_at, reason, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (task_id, retry_count, scheduled_at, reason, time.time()),
            )
            conn.commit()

    def get_retry_history(self, task_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM retry_history WHERE task_id = ? ORDER BY created_at ASC", (task_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def log_event(
        self,
        event_type: str,
        message: str,
        task_id: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO system_events (event_type, task_id, worker_id, message, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (event_type, task_id, worker_id, message, time.time()),
            )
            conn.commit()

    def get_status_counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) as c FROM tasks GROUP BY status").fetchall()
        return {row["status"]: row["c"] for row in rows}

    def get_in_progress_tasks(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status = 'IN_PROGRESS' ORDER BY started_at ASC"
            ).fetchall()
        tasks: list[dict[str, Any]] = []
        for row in rows:
            task = dict(row)
            task["payload"] = json.loads(task["payload"])
            tasks.append(task)
        return tasks

    def get_worker_completion_counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT worker_id, COUNT(*) AS c
                FROM tasks
                WHERE status = 'COMPLETED' AND worker_id IS NOT NULL
                GROUP BY worker_id
                """
            ).fetchall()
        return {row["worker_id"]: row["c"] for row in rows}

    def get_average_processing_time(self) -> float:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT AVG(completed_at - started_at) AS avg_seconds
                FROM tasks
                WHERE status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL
                """
            ).fetchone()
        if not row or row["avg_seconds"] is None:
            return 0.0
        return float(row["avg_seconds"])

    # -----------------------------------------------------------------------
    # Worker heartbeats (replaces Redis heartbeat keys)
    # -----------------------------------------------------------------------

    def set_worker_heartbeat(self, worker_id: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO worker_heartbeats (worker_id, last_seen, stats)
                VALUES (?, ?, '{}')
                ON CONFLICT(worker_id) DO UPDATE SET last_seen = excluded.last_seen
                """,
                (worker_id, time.time()),
            )
            conn.commit()

    def get_worker_heartbeats(self) -> dict[str, float]:
        with self._connect() as conn:
            rows = conn.execute("SELECT worker_id, last_seen FROM worker_heartbeats").fetchall()
        return {row["worker_id"]: row["last_seen"] for row in rows}

    def increment_worker_stat(self, worker_id: str, field: str, amount: int = 1) -> None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT stats FROM worker_heartbeats WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            stats: dict[str, int] = json.loads(row["stats"]) if row else {}
            stats[field] = stats.get(field, 0) + amount
            conn.execute(
                """
                INSERT INTO worker_heartbeats (worker_id, last_seen, stats)
                VALUES (?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET stats = excluded.stats
                """,
                (worker_id, time.time(), json.dumps(stats)),
            )
            conn.commit()

    def get_worker_stats(self, worker_id: str) -> dict[str, int]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT stats FROM worker_heartbeats WHERE worker_id = ?", (worker_id,)
            ).fetchone()
        if not row:
            return {}
        return json.loads(row["stats"])

    # -----------------------------------------------------------------------
    # Task locks (replaces Redis SET NX)
    # -----------------------------------------------------------------------

    def acquire_task_lock(self, task_id: str, owner: str, ttl_seconds: int = 120) -> bool:
        expires_at = time.time() + ttl_seconds
        with self._lock, self._connect() as conn:
            # Clean up any expired lock first
            conn.execute(
                "DELETE FROM task_locks WHERE task_id = ? AND expires_at < ?",
                (task_id, time.time()),
            )
            try:
                conn.execute(
                    "INSERT INTO task_locks (task_id, owner, expires_at) VALUES (?, ?, ?)",
                    (task_id, owner, expires_at),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def release_task_lock(self, task_id: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM task_locks WHERE task_id = ?", (task_id,))
            conn.commit()

    # -----------------------------------------------------------------------
    # Delayed task queue (replaces Redis ZSET queue:delayed)
    # -----------------------------------------------------------------------

    def schedule_delayed_task(self, task: dict[str, Any], execute_after: float) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO delayed_tasks (task_id, payload, execute_after) VALUES (?, ?, ?)",
                (task["task_id"], json.dumps(task), execute_after),
            )
            conn.commit()

    def get_due_delayed_tasks(self, max_batch: int = 200) -> list[tuple[int, dict[str, Any]]]:
        """Return (row_id, task_dict) pairs for tasks whose delay has elapsed."""
        now = time.time()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, payload FROM delayed_tasks WHERE execute_after <= ? LIMIT ?",
                (now, max_batch),
            ).fetchall()
        result = []
        for row in rows:
            try:
                task = json.loads(row["payload"])
                result.append((row["id"], task))
            except json.JSONDecodeError:
                pass
        return result

    def delete_delayed_task(self, row_id: int) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM delayed_tasks WHERE id = ?", (row_id,))
            conn.commit()

    def get_queue_depths(self) -> dict[str, int]:
        with self._connect() as conn:
            pending = conn.execute(
                "SELECT COUNT(*) AS c FROM tasks WHERE status = 'PENDING'"
            ).fetchone()["c"]
            in_progress = conn.execute(
                "SELECT COUNT(*) AS c FROM tasks WHERE status = 'IN_PROGRESS'"
            ).fetchone()["c"]
            delayed = conn.execute("SELECT COUNT(*) AS c FROM delayed_tasks").fetchone()["c"]
        return {
            "pending": pending,
            "in_progress": in_progress,
            "delayed": delayed,
        }
