import base64
import binascii
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
from models import (
    FaultSimulationRequest,
    FaultSimulationResponse,
    ReplayBatchRequest,
    RetryTaskResponse,
    SubmitTaskRequest,
    SubmitTaskResponse,
    TelegramReportRequest,
    TelegramReportResponse,
    TelegramSessionReportRequest,
)
from producer import TaskProducer
from telegram_reporter import send_telegram_message, send_telegram_photo

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


def _format_report_time(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _collect_terminal_report(task_ids: list[str]) -> tuple[dict[str, int], list[str], list[str]]:
    terminal_statuses = {"COMPLETED", "DEAD", "FAILED", "CANCELLED"}
    status_counts = {"COMPLETED": 0, "DEAD": 0, "FAILED": 0, "CANCELLED": 0}

    missing: list[str] = []
    not_terminal: list[str] = []

    for task_id in task_ids:
        task = db.get_task(task_id)
        if not task:
            missing.append(task_id)
            continue

        status = str(task.get("status", ""))
        if status not in terminal_statuses:
            not_terminal.append(task_id)
            continue
        status_counts[status] += 1

    return status_counts, missing, not_terminal


def _build_telegram_report_message(
    task_ids: list[str],
    title: Optional[str],
    *,
    session_label: Optional[str] = None,
    session_started_at: Optional[float] = None,
    session_closed_at: Optional[float] = None,
) -> tuple[str, dict[str, int]]:
    status_counts, missing, not_terminal = _collect_terminal_report(task_ids)

    if missing:
        raise HTTPException(status_code=404, detail=f"Task(s) not found: {missing[:5]}")
    if not_terminal:
        raise HTTPException(status_code=409, detail=f"Task(s) still in progress: {not_terminal[:5]}")

    total = len(task_ids)
    completed = status_counts["COMPLETED"]
    dead = status_counts["DEAD"]
    failed = status_counts["FAILED"]
    cancelled = status_counts["CANCELLED"]
    success_rate = (completed / total) if total else 0.0

    started_text = _format_report_time(session_started_at)
    closed_text = _format_report_time(session_closed_at)

    lines = [
        title or "Batch Completion Report",
        "=" * 26,
    ]
    if session_label:
        lines.append(f"Session: {session_label}")
    if started_text:
        lines.append(f"Started: {started_text}")
    if closed_text:
        lines.append(f"Sealed: {closed_text}")
    if session_started_at is not None and session_closed_at is not None and session_closed_at >= session_started_at:
        lines.append(f"Grouping window: {session_closed_at - session_started_at:.1f}s")
    lines.extend(
        [
            f"Total tasks: {total}",
            f"Completed: {completed}",
            f"Dead: {dead}",
            f"Failed: {failed}",
            f"Cancelled: {cancelled}",
            f"Success rate: {success_rate * 100:.1f}%",
        ]
    )

    return "\n".join(lines), {
        "completed": completed,
        "dead": dead,
        "failed": failed,
        "cancelled": cancelled,
        "total": total,
    }


def _one_line(value: Any, max_length: int = 160) -> str:
    text = str(value or "").replace("\n", " ").replace("\r", " ").strip()
    if not text:
        return "-"
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def _build_dlq_details_message(max_tasks: int = 10) -> str:
    dead_tasks = db.list_tasks(status=STATUS_DEAD)
    lines = [
        "Dead Letter Queue",
        "=" * 17,
        f"Total dead tasks: {len(dead_tasks)}",
    ]

    if not dead_tasks:
        lines.append("No dead tasks currently in the DLQ.")
        return "\n".join(lines)

    for index, task in enumerate(dead_tasks[:max_tasks], start=1):
        task_id = task.get("task_id", "-")
        task_type = task.get("task_type", "-")
        priority = task.get("priority", "-")
        retry_count = task.get("retry_count", "-")
        max_retries = task.get("max_retries", "-")
        worker_id = task.get("worker_id") or "-"
        died_at = _format_report_time(task.get("completed_at")) or "-"
        last_error = _one_line(task.get("error_message"), max_length=180)

        lines.extend(
            [
                "",
                f"{index}. {task_type}",
                f"Task ID: {task_id}",
                f"Priority: {priority} | Retries: {retry_count}/{max_retries}",
                f"Worker: {worker_id}",
                f"Died at: {died_at}",
                f"Last error: {last_error}",
            ]
        )

    remaining = len(dead_tasks) - max_tasks
    if remaining > 0:
        lines.append("")
        lines.append(f"...and {remaining} more dead task(s) in the DLQ.")

    return "\n".join(lines)


def _combine_report_messages(message: str, followup_message: Optional[str]) -> str:
    if followup_message:
        return f"{message}\n\n{followup_message}"
    return message


def _decode_snapshot_data_url(data_url: str) -> tuple[bytes, str, str]:
    if not data_url.startswith("data:image/") or "," not in data_url:
        raise ValueError("snapshot_data_url must be an image data URL")

    header, encoded = data_url.split(",", 1)
    mime_type = header.split(";")[0].split(":", 1)[1]
    extension = mime_type.split("/", 1)[1] if "/" in mime_type else "png"
    try:
        image_bytes = base64.b64decode(encoded)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("snapshot_data_url is not valid base64 image data") from exc
    return image_bytes, mime_type, f"session-report.{extension}"


def _dispatch_telegram_report(
    message: str,
    *,
    snapshot_data_url: Optional[str] = None,
    followup_message: Optional[str] = None,
) -> tuple[bool, bool, str]:
    snapshot_sent = False

    if snapshot_data_url:
        try:
            image_bytes, mime_type, filename = _decode_snapshot_data_url(snapshot_data_url)
            snapshot_sent = send_telegram_photo(
                image_bytes,
                caption=message,
                filename=filename,
                mime_type=mime_type,
            )
            if snapshot_sent:
                if followup_message:
                    try:
                        followup_sent = send_telegram_message(followup_message)
                        if followup_sent:
                            return True, True, "Telegram snapshot report and DLQ details sent."
                        return True, True, "Telegram snapshot report sent. DLQ details were not sent."
                    except Exception:
                        logger.exception("Failed to send Telegram DLQ details after snapshot")
                        return True, True, "Telegram snapshot report sent. DLQ details failed."
                return True, True, "Telegram snapshot report sent."
        except Exception:
            logger.exception("Failed to send Telegram snapshot report; falling back to text")

    sent = send_telegram_message(_combine_report_messages(message, followup_message))
    if sent:
        fallback_message = "Telegram text report sent." if not snapshot_data_url else "Telegram text report sent (snapshot fallback used)."
        return True, snapshot_sent, fallback_message

    return False, snapshot_sent, "Telegram credentials not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."


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


@app.post("/faults/simulate-worker-crash", response_model=FaultSimulationResponse, tags=["faults"])
def simulate_worker_crash(req: FaultSimulationRequest) -> FaultSimulationResponse:
    now = time.time()
    heartbeats = broker.get_worker_heartbeats()
    alive_workers = {
        worker_id
        for worker_id, heartbeat in heartbeats.items()
        if heartbeat and (now - heartbeat) <= WORKER_TIMEOUT_SECONDS
    }
    if not alive_workers:
        raise HTTPException(status_code=409, detail="No alive workers available to simulate a crash")

    in_progress = db.get_in_progress_tasks()
    target_worker_id = req.worker_id
    target_task_id: Optional[str] = None
    worker_had_in_progress_task = False

    if target_worker_id:
        if target_worker_id not in heartbeats:
            raise HTTPException(status_code=404, detail="Worker not found")
        if target_worker_id not in alive_workers:
            raise HTTPException(status_code=409, detail="Selected worker is not currently alive")
        for task in in_progress:
            if task.get("worker_id") == target_worker_id:
                target_task_id = task["task_id"]
                worker_had_in_progress_task = True
                break
    else:
        for task in in_progress:
            worker_id = task.get("worker_id")
            if worker_id in alive_workers:
                target_worker_id = worker_id
                target_task_id = task["task_id"]
                worker_had_in_progress_task = True
                break
        if not target_worker_id:
            target_worker_id = sorted(alive_workers)[0]

    assert target_worker_id is not None
    reason = (
        f"Simulated crash requested for {target_worker_id}"
        + (f" while processing task {target_task_id}" if target_task_id else " while idle")
    )
    db.issue_worker_command(
        target_worker_id,
        "crash",
        payload={
            "reason": reason,
            "requested_at": now,
        },
    )
    db.log_event(
        "worker_fault_requested",
        reason,
        task_id=target_task_id,
        worker_id=target_worker_id,
    )

    return FaultSimulationResponse(
        worker_id=target_worker_id,
        message=reason,
        target_task_id=target_task_id,
        worker_had_in_progress_task=worker_had_in_progress_task,
    )


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
    message, stats = _build_telegram_report_message(req.task_ids, req.title)
    sent, snapshot_sent, response_message = _dispatch_telegram_report(message)
    return TelegramReportResponse(
        sent=sent,
        message=response_message,
        completed=stats["completed"],
        dead=stats["dead"],
        failed=stats["failed"],
        cancelled=stats["cancelled"],
        total=stats["total"],
        snapshot_sent=snapshot_sent,
    )


@app.post("/reports/telegram/session", response_model=TelegramReportResponse, tags=["reports"])
def send_telegram_session_report(req: TelegramSessionReportRequest) -> TelegramReportResponse:
    message, stats = _build_telegram_report_message(
        req.task_ids,
        req.title,
        session_label=req.session_label,
        session_started_at=req.session_started_at,
        session_closed_at=req.session_closed_at,
    )
    dlq_details = _build_dlq_details_message()
    sent, snapshot_sent, response_message = _dispatch_telegram_report(
        message,
        snapshot_data_url=req.snapshot_data_url,
        followup_message=dlq_details,
    )
    return TelegramReportResponse(
        sent=sent,
        message=response_message,
        completed=stats["completed"],
        dead=stats["dead"],
        failed=stats["failed"],
        cancelled=stats["cancelled"],
        total=stats["total"],
        snapshot_sent=snapshot_sent,
    )


if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host=os.getenv("API_HOST", API_HOST),
        port=int(os.getenv("API_PORT", API_PORT)),
        reload=False,
    )
