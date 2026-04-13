import logging
import os
import time
from typing import Any, Optional

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
from models import RetryTaskResponse, SubmitTaskRequest, SubmitTaskResponse
from producer import TaskProducer
from scheduler import RetryScheduler

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [api] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Distributed Task Queue API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = Database()
broker = KafkaBroker(db)
producer = TaskProducer(db, broker)
scheduler = RetryScheduler(broker)


@app.on_event("startup")
def on_startup() -> None:
    logger.info("Starting API and retry scheduler")
    scheduler.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    logger.info("Stopping scheduler and closing Kafka connections")
    scheduler.stop()
    broker.close()


@app.get("/health")
def health() -> dict[str, Any]:
    kafka_ok = broker.ping()
    return {
        "status": "ok" if kafka_ok else "degraded",
        "kafka": kafka_ok,
    }


@app.post("/tasks/submit", response_model=SubmitTaskResponse)
def submit_task(req: SubmitTaskRequest) -> SubmitTaskResponse:
    try:
        task = producer.submit_task(
            task_type=req.task_type,
            payload=req.payload,
            priority=req.priority,
            max_retries=req.max_retries,
        )
        return SubmitTaskResponse(task_id=task["task_id"], status=task["status"], message="Task submitted")
    except Exception as exc:
        logger.exception("Failed to submit task")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/tasks/{task_id}")
def get_task(task_id: str) -> dict[str, Any]:
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    task["retry_history"] = db.get_retry_history(task_id)
    return task


@app.get("/tasks")
def list_tasks(status: Optional[str] = Query(default=None)) -> list[dict[str, Any]]:
    try:
        return db.list_tasks(status=status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.delete("/tasks/{task_id}")
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


@app.post("/tasks/{task_id}/retry", response_model=RetryTaskResponse)
def manual_retry_dead_task(task_id: str) -> RetryTaskResponse:
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task["status"] != STATUS_DEAD:
        raise HTTPException(status_code=409, detail="Only DEAD tasks can be manually retried")

    try:
        task["retry_count"] = 0
        task["status"] = STATUS_PENDING
        task["worker_id"] = None
        task["started_at"] = None
        task["completed_at"] = None
        task["error_message"] = None

        db.update_task(
            task_id,
            retry_count=0,
            status=STATUS_PENDING,
            worker_id=None,
            started_at=None,
            completed_at=None,
            error_message=None,
        )
        broker.requeue_immediate(task, reason="Manual retry requested")
        db.log_event("manual_retry", "Dead task manually retried", task_id=task_id)
        return RetryTaskResponse(task_id=task_id, status=STATUS_PENDING, message="Task requeued")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/workers")
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


@app.get("/queue/stats")
def queue_stats() -> dict[str, Any]:
    counts = db.get_status_counts()
    queue_depths = broker.get_queue_depths()
    completed = counts.get("COMPLETED", 0)
    dead = counts.get("DEAD", 0)
    failed = counts.get("FAILED", 0)

    attempts = completed + dead + failed
    success_rate = (completed / attempts) if attempts > 0 else 0.0

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
    }


if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host=os.getenv("API_HOST", API_HOST),
        port=int(os.getenv("API_PORT", API_PORT)),
        reload=False,
    )
