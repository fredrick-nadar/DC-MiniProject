import logging
import os
import time
from typing import Any, Optional

import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from broker import KafkaBroker
from config import (
    API_HOST,
    API_PORT,
    LOG_LEVEL,
    STATUS_DEAD,
    STATUS_PENDING,
    WORKER_TIMEOUT_SECONDS,
)
from database import Database
from models import (
    ReplayBatchRequest,
    RetryTaskResponse,
    SubmitTaskRequest,
    SubmitTaskResponse,
    TelegramReportRequest,
    TelegramReportResponse,
)
from producer import TaskProducer

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [scheduler] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Distributed Task Queue API",
    description="Kafka-backed task queue with priority scheduling, idempotency, and DLQ replay.",
    version="2.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

db = Database()
broker = KafkaBroker(db)
producer = TaskProducer(db, broker)


def _send_telegram_message(message: str) -> bool:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        return False

    text = message.strip()
    if not text:
        return False
    if len(text) > 4096:
        text = text[:4080] + "\n\n...truncated..."

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    response = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=10,
    )
    response.raise_for_status()
    return True


@app.on_event("startup")
def startup() -> None:
    from scheduler import RetryScheduler
    scheduler = RetryScheduler(broker)
    scheduler.start()
    logger.info("Starting API and retry scheduler")


@app.on_event("shutdown")
def shutdown() -> None:
    broker.close()


# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["system"])
def health() -> dict[str, Any]:
    kafka_ok = broker.ping()
    return {
        "status": "ok" if kafka_ok else "degraded",
        "kafka": kafka_ok,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Task Submission
# ──────────────────────────────────────────────────────────────────────────────

@app.post("/tasks/submit", response_model=SubmitTaskResponse, tags=["tasks"])
def submit_task(req: SubmitTaskRequest) -> SubmitTaskResponse:
    try:
        task, deduplicated = producer.submit_task(
            task_type=req.task_type,
            payload=req.payload,
            priority=req.priority,
            max_retries=req.max_retries,
            idempotency_key=req.idempotency_key,
            run_after_seconds=req.run_after_seconds,
            timeout_seconds=req.timeout_seconds,
        )
        msg = "Existing task returned (idempotent)" if deduplicated else "Task submitted"
        return SubmitTaskResponse(
            task_id=task["task_id"],
            status=task["status"],
            message=msg,
            deduplicated=deduplicated,
        )
    except Exception as exc:
        logger.exception("Failed to submit task")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ──────────────────────────────────────────────────────────────────────────────
# Task Queries
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/tasks", tags=["tasks"])
def list_tasks(status: Optional[str] = Query(default=None)) -> list[dict[str, Any]]:
    try:
        return db.list_tasks(status=status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ──────────────────────────────────────────────────────────────────────────────
# DLQ — Dead Letter Queue
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/tasks/dead", tags=["dlq"])
# Keep this static route above `/tasks/{task_id}` so "dead" is not
# interpreted as a task ID by the router.
def list_dead_tasks() -> list[dict[str, Any]]:
    """Return all DEAD tasks with their full retry history."""
    tasks = db.list_tasks(status=STATUS_DEAD)
    for task in tasks:
        task["retry_history"] = db.get_retry_history(task["task_id"])
    return tasks


@app.get("/tasks/{task_id}", tags=["tasks"])
def get_task(task_id: str) -> dict[str, Any]:
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    task["retry_history"] = db.get_retry_history(task_id)
    return task


@app.post("/tasks/{task_id}/retry", response_model=RetryTaskResponse, tags=["dlq"])
def manual_retry_dead_task(
    task_id: str,
    patch_payload: Optional[dict[str, Any]] = None,
) -> RetryTaskResponse:
    """Replay a single DEAD task. Optionally patch its payload before re-enqueuing."""
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] != STATUS_DEAD:
        raise HTTPException(status_code=409, detail="Only DEAD tasks can be manually retried")

    try:
        if patch_payload is not None:
            task["payload"] = {**task["payload"], **patch_payload}

        db.update_task(
            task_id,
            retry_count=0,
            status=STATUS_PENDING,
            worker_id=None,
            started_at=None,
            completed_at=None,
            error_message=None,
        )
        task_to_enqueue = {**task, "retry_count": 0, "status": STATUS_PENDING,
                           "worker_id": None, "started_at": None}
        broker.requeue_immediate(task_to_enqueue, reason="Manual retry requested")
        db.log_event("manual_retry", "Dead task manually retried", task_id=task_id)
        return RetryTaskResponse(task_id=task_id, status=STATUS_PENDING, message="Task requeued")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/tasks/replay-batch", tags=["dlq"])
def replay_batch(req: ReplayBatchRequest) -> dict[str, Any]:
    """
    Bulk-replay multiple DEAD tasks.
    Optionally patch_payload is merged into every replayed task's payload.
    Returns counts of replayed / skipped tasks.
    """
    replayed, skipped = [], []
    for task_id in req.task_ids:
        task = db.get_task(task_id)
        if not task or task["status"] != STATUS_DEAD:
            skipped.append(task_id)
            continue
        try:
            if req.patch_payload:
                task["payload"] = {**task["payload"], **req.patch_payload}
            db.update_task(
                task_id,
                retry_count=0,
                status=STATUS_PENDING,
                worker_id=None,
                started_at=None,
                completed_at=None,
                error_message=None,
            )
            task_to_enqueue = {**task, "retry_count": 0, "status": STATUS_PENDING,
                               "worker_id": None, "started_at": None}
            broker.requeue_immediate(task_to_enqueue, reason="Batch replay")
            db.log_event("batch_replay", "Dead task replayed in batch", task_id=task_id)
            replayed.append(task_id)
        except Exception as exc:
            logger.exception("Failed to replay task %s", task_id)
            skipped.append(task_id)

    return {
        "replayed": len(replayed),
        "skipped": len(skipped),
        "replayed_ids": replayed,
        "skipped_ids": skipped,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Task Management
# ──────────────────────────────────────────────────────────────────────────────

@app.delete("/tasks/clear", tags=["tasks"])
def clear_all_tasks() -> dict[str, Any]:
    try:
        deleted = db.clear_all_tasks()
        return {"message": "All tasks cleared", "deleted": deleted}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.delete("/tasks/{task_id}", tags=["tasks"])
def cancel_task(task_id: str) -> dict[str, Any]:
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] != STATUS_PENDING:
        raise HTTPException(status_code=409, detail="Only pending tasks can be cancelled")

    try:
        broker.remove_task_from_queues(task_id)
        db.update_task(task_id, status="CANCELLED", completed_at=time.time(), error_message="Cancelled by user")
        db.log_event("task_cancelled", "Task cancelled by user", task_id=task_id)
        return {"task_id": task_id, "status": "CANCELLED", "message": "Task cancelled"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ──────────────────────────────────────────────────────────────────────────────
# Workers
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/workers", tags=["workers"])
def list_workers() -> list[dict[str, Any]]:
    now = time.time()
    heartbeats = broker.get_worker_heartbeats()
    completed_counts = db.get_worker_completion_counts()

    worker_ids = set(heartbeats.keys()) | set(completed_counts.keys())
    workers: list[dict[str, Any]] = []

    for worker_id in sorted(worker_ids):
        heartbeat = heartbeats.get(worker_id)
        is_alive = bool(heartbeat and (now - heartbeat) <= WORKER_TIMEOUT_SECONDS)
        stats = broker.get_worker_stats(worker_id)
        workers.append(
            {
                "worker_id": worker_id,
                "status": "alive" if is_alive else "dead",
                "last_heartbeat": heartbeat,
                "tasks_done": stats.get("tasks_done", completed_counts.get(worker_id, 0)),
                "tasks_failed": stats.get("tasks_failed", 0),
                "tasks_retried": stats.get("tasks_retried", 0),
            }
        )

    return workers


# ──────────────────────────────────────────────────────────────────────────────
# Queue Stats
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/queue/stats", tags=["system"])
def queue_stats() -> dict[str, Any]:
    counts = db.get_status_counts()
    queue_depths = broker.get_queue_depths()
    completed = counts.get("COMPLETED", 0)
    dead = counts.get("DEAD", 0)
    failed = counts.get("FAILED", 0)

    attempts = completed + dead + failed
    success_rate = (completed / attempts) if attempts > 0 else 0.0

    # Priority band distribution
    priority_dist = db.get_priority_distribution()

    return {
        "status_counts": {
            "PENDING": counts.get("PENDING", 0),
            "IN_PROGRESS": counts.get("IN_PROGRESS", 0),
            "COMPLETED": completed,
            "FAILED": failed,
            "DEAD": dead,
            "CANCELLED": counts.get("CANCELLED", 0),
        },
        "queue_depths": queue_depths,
        "success_rate": round(success_rate, 4),
        "average_processing_time_seconds": round(db.get_average_processing_time(), 4),
        "total_tasks": sum(counts.values()),
        "priority_distribution": priority_dist,
    }


@app.post("/reports/telegram/for-tasks", response_model=TelegramReportResponse, tags=["reports"])
def send_telegram_report_for_tasks(req: TelegramReportRequest) -> TelegramReportResponse:
    terminal_statuses = {"COMPLETED", "DEAD", "FAILED", "CANCELLED"}
    status_counts = {"COMPLETED": 0, "DEAD": 0, "FAILED": 0, "CANCELLED": 0}

    missing: list[str] = []
    not_terminal: list[str] = []

    for task_id in req.task_ids:
        task = db.get_task(task_id)
        if not task:
            missing.append(task_id)
            continue

        status = str(task.get("status", ""))
        if status not in terminal_statuses:
            not_terminal.append(task_id)
            continue
        status_counts[status] += 1

    if missing:
        raise HTTPException(status_code=404, detail=f"Task(s) not found: {missing[:5]}")
    if not_terminal:
        raise HTTPException(status_code=409, detail=f"Task(s) still in progress: {not_terminal[:5]}")

    total = len(req.task_ids)
    completed = status_counts["COMPLETED"]
    dead = status_counts["DEAD"]
    failed = status_counts["FAILED"]
    cancelled = status_counts["CANCELLED"]
    success_rate = (completed / total) if total else 0.0

    title = req.title or "Batch Completion Report"
    lines = [
        title,
        "=" * 26,
        f"Total tasks: {total}",
        f"Completed: {completed}",
        f"Dead: {dead}",
        f"Failed: {failed}",
        f"Cancelled: {cancelled}",
        f"Success rate: {success_rate * 100:.1f}%",
    ]
    message = "\n".join(lines)

    sent = _send_telegram_message(message)
    if not sent:
        return TelegramReportResponse(
            sent=False,
            message="Telegram credentials not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.",
            completed=completed,
            dead=dead,
            failed=failed,
            cancelled=cancelled,
            total=total,
        )

    return TelegramReportResponse(
        sent=True,
        message="Telegram report sent.",
        completed=completed,
        dead=dead,
        failed=failed,
        cancelled=cancelled,
        total=total,
    )


if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host=os.getenv("API_HOST", API_HOST),
        port=int(os.getenv("API_PORT", API_PORT)),
        reload=False,
    )
