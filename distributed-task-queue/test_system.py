import random
import sys
import time
from collections import Counter

import requests

API_BASE = "http://localhost:8000"
TASK_TYPES = [
    "fetch_joke", "fetch_dog", "fetch_user", "fetch_fact", "fetch_ip",
    "fetch_product", "fetch_pokemon", "fetch_chuck", "fetch_country",
]

def submit_tasks(n: int = 20) -> list[str]:
    task_ids: list[str] = []
    for i in range(n):
        task_type = random.choice(TASK_TYPES)
        payload = {"index": i, "email": f"user{i}@example.com", "filename": f"file_{i}.dat"}
        priority = random.randint(1, 10)
        retries = random.randint(2, 5)

        res = requests.post(
            f"{API_BASE}/tasks/submit",
            json={
                "task_type": task_type,
                "payload": payload,
                "priority": priority,
                "max_retries": retries,
            },
            timeout=10,
        )
        res.raise_for_status()
        data = res.json()
        task_id = data["task_id"]
        task_ids.append(task_id)
        print(f"Submitted {i+1:02d}/{n}: {task_id} ({task_type}, priority={priority}, retries={retries})")
    return task_ids


def poll_until_done(task_ids: list[str], timeout_seconds: int = 600) -> dict[str, dict]:
    start = time.time()
    terminal = {"COMPLETED", "DEAD", "FAILED", "CANCELLED"}

    while True:
        states: dict[str, dict] = {}
        status_counter = Counter()

        for task_id in task_ids:
            res = requests.get(f"{API_BASE}/tasks/{task_id}", timeout=10)
            res.raise_for_status()
            task = res.json()
            states[task_id] = task
            status_counter[task["status"]] += 1

        done = sum(v for k, v in status_counter.items() if k in terminal)
        print(f"Progress: {done}/{len(task_ids)} terminal | statuses={dict(status_counter)}")

        if done == len(task_ids):
            return states

        if (time.time() - start) > timeout_seconds:
            raise TimeoutError("Timed out waiting for tasks to reach terminal states")

        time.sleep(3)


def print_summary(states: dict[str, dict]) -> None:
    counter = Counter(task["status"] for task in states.values())
    print("\n=== FINAL SUMMARY ===")
    print(f"Total tasks: {len(states)}")
    for status, count in sorted(counter.items()):
        print(f"- {status}: {count}")

    dead_tasks = [t for t in states.values() if t["status"] == "DEAD"]
    if dead_tasks:
        print("\nDead Tasks:")
        for task in dead_tasks:
            print(
                f"  {task['task_id']} type={task['task_type']} retries={task['retry_count']}/{task['max_retries']} error={task.get('error_message')}"
            )


def main() -> int:
    try:
        print("Submitting 20 random tasks...")
        task_ids = submit_tasks(20)
        print("Polling task states until terminal...")
        states = poll_until_done(task_ids)
        print_summary(states)
        return 0
    except requests.RequestException as exc:
        print(f"HTTP/API error: {exc}")
        return 1
    except Exception as exc:
        print(f"Test run failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
