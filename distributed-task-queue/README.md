<div align="center">
  <h1>⚡ Distributed Task Queue</h1>
  <p>A robust, distributed task processing system built with <b>Python</b>, <b>Kafka</b>, and <b>FastAPI</b>, featuring a real-time analytics dashboard with DLQ replay, priority scheduling, and idempotency.</p>
</div>

<br />

## 🚀 Features

- **Distributed Architecture:** Producer-Consumer pattern using **Apache Kafka** (KRaft mode) for high-throughput, fault-tolerant message brokering.
- **Priority Scheduling:** Tasks carry a priority (1-10) which maps to a Kafka partition — high-priority jobs are consumed first.
- **Idempotency & Deduplication:** Submit tasks with an `idempotency_key` — duplicate submissions return the original task instead of creating a new one.
- **Scheduled / Delayed Tasks:** Submit tasks with `run_after_seconds` to defer execution — backed by the `delayed_tasks` queue.
- **Per-Task Timeout Policies:** Each task carries its own `timeout_seconds` (1-60s) controlling the HTTP call deadline.
- **Persistent State:** Uses **SQLite** for task lifecycle tracking, heartbeats, and queue statistics.
- **Real-Time Dashboard:** Premium dark-mode dashboard with **Chart.js** visualizations:
  - Task status doughnut, priority-band bar chart, worker performance bar chart
  - Dual-axis real-time line graph: **tasks/sec** + **consumer lag**
  - Consumer lag and scheduled task count cards
- **Fault Tolerance & Reliability:**
  - Worker heartbeat monitoring and automatic task reallocation on instance failure.
  - Configurable retry limits with exponential backoff and a Dead Letter Queue (DEAD status).
  - Manual and bulk DLQ replay with optional payload patching and full audit history.
- **Load Testing:** `load_test.py` — burst N tasks and see tasks/sec, worker throughput, consumer lag, failure recovery time, and a final summary table.

## 🛠️ Technology Stack

- **Backend / API**: FastAPI, Uvicorn
- **Message Broker**: Apache Kafka (kafka-python-ng)
- **Database**: SQLite
- **Frontend**: Vanilla HTML/CSS/JS, Chart.js

## 📦 Architecture Flow

1. **Producer (FastAPI)** accepts tasks, checks idempotency keys, and pushes to the `tasks.queue` Kafka topic (or schedules them via `delayed_tasks`).
2. **Workers** consume messages, acquire distributed locks, process tasks through real external API calls, and update their heartbeat.
3. **Fault Manager** monitors worker heartbeats. If a worker dies, it reclaims hanging tasks and re-queues them.
4. **Retry Scheduler** flushes due delayed tasks from SQLite back onto Kafka every second.
5. **Dashboard** continually polls the API for live metrics and provides controls (batch submit, DLQ replay, clear queue).

## ⚡ Getting Started

### Prerequisites

- Python 3.12+
- Apache Kafka (KRaft mode) running locally on port `9092`.

### 1. Start Apache Kafka
```bash
# Windows (Kafka binaries)
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```

### 2. Setup Python Environment
```bash
python -m venv venv
.\venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 3. Run the System
```bash
python run_local.py --workers 3
```

### 4. Open the Dashboard
Open `dashboard/index.html` in your browser. Click **"Submit Batch"** to enqueue tasks and watch all charts update live.

### 5. Run a Load Test
```bash
python load_test.py --tasks 100 --duration 90
```

### 6. Send Final Reports to Telegram
See `TELEGRAM_REPORT_README.md` for full bot setup and end-report delivery from:
- `load_test.py` (automatic if Telegram env vars are set)
- `test_system.py --telegram`

### 7. Realistic Stress Task Types
The queue includes heavier/slower APIs for more realistic failure scenarios:
- `fetch_large_photos` (large JSON payload parsing)
- `fetch_slow_httpbin` (slow endpoint, can timeout with lower task timeout values)

## 📐 Partitioning & Scaling Strategy

The Kafka topic `tasks.queue` has **3 partitions** mapped to priority bands:

| Partition | Priority Band | Description |
|-----------|--------------|-------------|
| 0 | High (8-10) | Consumed first — critical jobs |
| 1 | Medium (4-7) | Normal throughput |
| 2 | Low (1-3) | Background / deferred work |

**Scaling workers**: Run `python run_local.py --workers N`. All workers share the `workers` consumer group — Kafka automatically rebalances partitions across them. With 3 workers and 3 partitions, each worker gets exclusive ownership of one partition.

**Ordering guarantee**: Tasks within the same priority tier are processed in FIFO order within their partition. Cross-tier ordering is not guaranteed.

**Idempotency**: Tasks submitted with the same `idempotency_key` return the original task without creating duplicates — safe for retrying failed API calls from the client side.

## 📂 Project Structure

```text
├── dashboard/
│   └── index.html         # Real-time analytics and DLQ control UI
├── src/
│   ├── api.py             # FastAPI entrypoint (submit, DLQ replay, stats)
│   ├── broker.py          # Kafka abstraction layer
│   ├── config.py          # Global configurations & task type registry
│   ├── database.py        # SQLite state persistence + safe migrations
│   ├── fault_manager.py   # Heartbeat monitor & task recovery
│   ├── models.py          # Pydantic models (idempotency, timeout, delay)
│   ├── producer.py        # Task creation with deduplication & scheduling
│   └── worker.py          # Queue consumer & task executor
├── load_test.py           # Load testing: tasks/sec, lag, recovery time
├── test_system.py         # Batch testing script
└── run_local.py           # Multi-process local orchestration runner
```

## ⚠️ Notes
- **DB migration**: Column additions (`idempotency_key`, `timeout_seconds`) are handled automatically with `ALTER TABLE` on startup — no need to delete `tasks.db` between upgrades.
- If the database becomes corrupted, delete `data/tasks.db` and restart — the schema recreates automatically.
- The system connects to `localhost:9092` by default. Override via the `KAFKA_BOOTSTRAP` environment variable.
