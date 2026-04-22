"""
load_test.py — Distributed Task Queue Load Tester
===================================================
Submits a configurable burst of tasks and tracks:
  - Tasks submitted per second (throughput)
  - Worker completed tasks per second
  - Consumer lag (PENDING + delayed depth over time)
  - Failure recovery time
  - Final summary table

Usage:
    python load_test.py [--tasks N] [--workers W] [--duration S]

Defaults: 100 tasks, poll for 60 seconds.
"""

import argparse
import random
import sys
import time
from collections import defaultdict
from typing import Any

import requests
from src.telegram_reporter import send_telegram_message

API_BASE = "http://localhost:8000"
TASK_TYPES = [
    "fetch_joke", "fetch_dog", "fetch_user", "fetch_fact", "fetch_ip",
    "fetch_product", "fetch_pokemon", "fetch_chuck", "fetch_country", "fetch_number",
    "fetch_large_photos", "fetch_slow_httpbin",
]

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get(path: str) -> Any:
    r = requests.get(f"{API_BASE}{path}", timeout=5)
    r.raise_for_status()
    return r.json()


def _post(path: str, body: dict) -> Any:
    r = requests.post(f"{API_BASE}{path}", json=body, timeout=5)
    r.raise_for_status()
    return r.json()


def check_api() -> bool:
    try:
        d = _get("/health")
        return d.get("kafka", False)
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Submission phase
# ──────────────────────────────────────────────────────────────────────────────

def submit_burst(n: int) -> tuple[list[str], float]:
    """Submit n tasks as fast as possible. Returns (task_ids, elapsed_seconds)."""
    print(f"\n📤  Submitting {n} tasks ...")
    ids: list[str] = []
    t0 = time.perf_counter()
    errors = 0
    for i in range(n):
        task_type = random.choice(TASK_TYPES)
        priority = random.randint(1, 10)
        try:
            resp = _post("/tasks/submit", {
                "task_type": task_type,
                "payload": {"load_test": True, "index": i},
                "priority": priority,
                "max_retries": 3,
                "timeout_seconds": 8,
            })
            ids.append(resp["task_id"])
            if (i + 1) % 20 == 0 or i == n - 1:
                elapsed = time.perf_counter() - t0
                tps = (i + 1) / elapsed
                print(f"   {i+1}/{n}  ({tps:.1f} tasks/s, {errors} errors)", end="\r")
        except Exception as e:
            errors += 1

    elapsed = time.perf_counter() - t0
    tps = n / elapsed
    print(f"\n✅  Submitted {n} tasks in {elapsed:.2f}s  →  {tps:.1f} tasks/s  ({errors} submit errors)")
    return ids, elapsed


# ──────────────────────────────────────────────────────────────────────────────
# Polling / monitoring phase
# ──────────────────────────────────────────────────────────────────────────────

def monitor(task_ids: set[str], poll_duration: int) -> dict:
    """
    Poll /queue/stats and /workers every second for poll_duration seconds.
    Returns summary metrics dict.
    """
    print(f"\n📊  Monitoring for {poll_duration}s ...")
    print(f"{'Time':>5}  {'Pending':>8}  {'Running':>8}  {'Done':>8}  {'Dead':>6}  {'Lag':>6}  {'Rate':>7}")
    print("-" * 62)

    samples: list[dict] = []
    first_dead_at: float | None = None
    all_done_at: float | None = None
    t0 = time.time()

    prev_completed = 0

    for tick in range(poll_duration):
        time.sleep(1)
        try:
            stats = _get("/queue/stats")
        except Exception:
            print(f"  [{tick+1}s] API unreachable")
            continue

        sc = stats["status_counts"]
        qd = stats["queue_depths"]
        pending = sc.get("PENDING", 0)
        running = sc.get("IN_PROGRESS", 0)
        completed = sc.get("COMPLETED", 0)
        dead = sc.get("DEAD", 0)
        lag = qd.get("pending", 0) + qd.get("delayed", 0)
        rate = f'{stats["success_rate"]*100:.1f}%'
        tps_now = completed - prev_completed
        prev_completed = completed

        samples.append({
            "tick": tick + 1,
            "pending": pending,
            "running": running,
            "completed": completed,
            "dead": dead,
            "lag": lag,
            "tps": tps_now,
        })

        print(f"{tick+1:>5}s  {pending:>8}  {running:>8}  {completed:>8}  {dead:>6}  {lag:>6}  {rate:>7}")

        if dead > 0 and first_dead_at is None:
            first_dead_at = time.time() - t0

        # Check if all terminal
        total_terminal = completed + dead + sc.get("CANCELLED", 0)
        if total_terminal >= len(task_ids) and all_done_at is None:
            all_done_at = time.time() - t0
            remaining = poll_duration - tick - 1
            if remaining > 5:
                print(f"\n🏁  All tasks terminal after {all_done_at:.1f}s — stopping early.")
                break

    return {
        "samples": samples,
        "first_dead_at": first_dead_at,
        "all_done_at": all_done_at,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────

def build_summary(task_count: int, submit_elapsed: float, metrics: dict) -> str:
    samples = metrics["samples"]
    if not samples:
        return "No samples collected."

    final = samples[-1]
    peak_tps = max(s["tps"] for s in samples)
    avg_tps = sum(s["tps"] for s in samples) / len(samples)
    peak_lag = max(s["lag"] for s in samples)

    lines = []
    lines.append("=" * 62)
    lines.append("  LOAD TEST SUMMARY")
    lines.append("=" * 62)
    lines.append(f"  Tasks submitted          : {task_count}")
    lines.append(f"  Submit throughput        : {task_count / submit_elapsed:.1f} tasks/s")
    lines.append(f"  Peak processing rate     : {peak_tps} tasks/s")
    lines.append(f"  Avg processing rate      : {avg_tps:.1f} tasks/s")
    lines.append(f"  Peak consumer lag        : {peak_lag} tasks")
    lines.append(f"  Final completed          : {final['completed']}")
    lines.append(f"  Final dead (DLQ)         : {final['dead']}")
    success = final['completed'] / max(final['completed'] + final['dead'], 1)
    lines.append(f"  Overall success rate     : {success*100:.1f}%")
    if metrics["first_dead_at"]:
        lines.append(f"  Time to first failure    : {metrics['first_dead_at']:.1f}s")
    if metrics["all_done_at"]:
        lines.append(f"  Time to all terminal     : {metrics['all_done_at']:.1f}s")
    lines.append("=" * 62)

    # Worker breakdown
    try:
        workers = _get("/workers")
        lines.append("")
        lines.append("  WORKER BREAKDOWN")
        lines.append(f"  {'Worker':<30} {'Done':>6} {'Retried':>8} {'Failed':>8} {'Status':>8}")
        lines.append("  " + "-" * 64)
        for w in workers:
            lines.append(
                f"  {w['worker_id']:<30} {w['tasks_done']:>6} "
                f"{w.get('tasks_retried', 0):>8} {w.get('tasks_failed', 0):>8} "
                f"{w['status']:>8}"
            )
    except Exception:
        lines.append("  (Could not fetch worker stats)")

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load test the distributed task queue.")
    parser.add_argument("--tasks", type=int, default=100, help="Number of tasks to submit (default: 100)")
    parser.add_argument("--duration", type=int, default=90, help="Seconds to monitor after submission (default: 90)")
    parser.add_argument("--telegram-token", type=str, default=None, help="Telegram bot token (or use TELEGRAM_BOT_TOKEN)")
    parser.add_argument("--telegram-chat-id", type=str, default=None, help="Telegram chat id (or use TELEGRAM_CHAT_ID)")
    args = parser.parse_args()

    print("Distributed Task Queue — Load Tester")
    print("=" * 62)

    if not check_api():
        print("❌  Cannot reach API at http://localhost:8000 or Kafka is down.")
        print("    Make sure 'python run_local.py --workers 3' is running first.")
        sys.exit(1)

    print("✅  API reachable and Kafka connected.")

    task_ids, submit_elapsed = submit_burst(args.tasks)
    metrics = monitor(set(task_ids), args.duration)
    summary = build_summary(args.tasks, submit_elapsed, metrics)
    print("\n" + summary + "\n")

    if args.telegram_token or args.telegram_chat_id:
        sent = send_telegram_message(summary, token=args.telegram_token, chat_id=args.telegram_chat_id)
        if sent:
            print("📨 Summary sent to Telegram.")
        else:
            print("ℹ️ Telegram credentials missing. Summary not sent.")
    else:
        # Also supports environment-driven integration for CI/local runs.
        sent = send_telegram_message(summary)
        if sent:
            print("📨 Summary sent to Telegram (env credentials).")
        else:
            print("ℹ️ Telegram credentials not found in current shell. Summary not sent.")


if __name__ == "__main__":
    main()
