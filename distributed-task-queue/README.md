# Distributed Task Queue System (Fault-Tolerant)

A production-ready **Distributed Computing mini-project** built with **FastAPI + Apache Kafka + Zookeeper + SQLite**.

> **v2.0** — Redis replaced with Apache Kafka + Zookeeper as the message transport layer.

---

## ✨ Features

- 📤 Task submission through a REST API
- 📨 **Kafka-backed** distributed queue with priority partitioning (3 partitions)
- 👷 Independent worker processes sharing a Kafka consumer group (auto load-balanced)
- 🔁 Automatic retries with **exponential backoff** (delays stored in SQLite)
- 🛡️ Fault manager recovers tasks from crashed workers via SQLite heartbeats
- 💀 Dead-letter handling when retries are exhausted
- 📊 Live dashboard (HTML + Tailwind + JavaScript polling)
- 💾 Full task state persistence in SQLite

---

## 🏗️ Architecture

```text
                 +-------------------------+
                 |  Dashboard (Browser)    |
                 |  dashboard/index.html   |
                 +------------+------------+
                              | HTTP polling
                              v
+-------------+      +----------------------------+
|   Clients   +----->+  FastAPI API + Producer    |
|  (submit)   | POST |  /tasks/submit etc.        |
+-------------+      +------------+---------------+
                                  |
                    +-------------+------------------+
                    |                                |
                    v                                v
           +--------------+             +-----------------------+
           |  SQLite DB   |             |  Kafka: tasks.queue   |
           |  task state  |             |  3 partitions         |
           |  heartbeats  |             |  (priority-mapped)    |
           |  locks       |             +----------+------------+
           |  delayed Q   |                        |
           +------+-------+                        | consumer group "workers"
                  ^                                v
                  |                 +-----------------------------+
                  +---- heartbeat --+ Worker 1..N (independent)  |
                  |                 | poll Kafka, process, retry  |
                  |                 +-----------------------------+
                  |
         +--------+---------+
         |  Fault Manager   |
         |  detects stale   |
         |  heartbeats,     |
         |  requeues tasks  |
         +------------------+

         +------------------+
         |  Retry Scheduler |
         |  flushes delayed |
         |  SQLite → Kafka  |
         +------------------+
```

**Priority → Kafka partition mapping:**

| Priority | Partition |
|----------|-----------|
| 8 – 10   | 0 (fastest) |
| 4 – 7    | 1 |
| 1 – 3    | 2 |

---

## 📁 Project Structure

```text
DC-MiniProject/
└── distributed-task-queue/
    ├── Dockerfile              ← (Docker build, kept for reference)
    ├── docker-compose.yml      ← (Docker-compose, kept for reference)
    ├── requirements.txt        ← Python deps: fastapi, uvicorn, kafka-python, pydantic, requests
    ├── README.md               ← You are here
    ├── HOW_TO_RUN.md           ← Step-by-step guide: install, run, test
    ├── run_local.py            ← Starts all Python services (no Docker)
    ├── test_system.py          ← End-to-end load test (20 tasks)
    ├── data/                   ← SQLite database (auto-created)
    ├── dashboard/
    │   └── index.html          ← Live web dashboard
    └── src/
        ├── config.py           ← All configuration / constants
        ├── models.py           ← Pydantic models
        ├── database.py         ← SQLite persistence (tasks, heartbeats, locks, delayed queue)
        ├── broker.py           ← KafkaBroker — Kafka producer/consumer + SQLite state
        ├── producer.py         ← Task submission logic
        ├── worker.py           ← Worker process (Kafka consumer)
        ├── fault_manager.py    ← Worker crash detection & task recovery
        ├── scheduler.py        ← Delayed retry flusher (SQLite → Kafka)
        └── api.py              ← FastAPI application (v2, Kafka-backed)
```

---

## 🛠️ Prerequisites

### System Requirements

| Tool | Version | Install |
|------|---------|---------|
| **Python** | ≥ 3.11 | https://www.python.org/downloads/ |
| **Java (JRE/JDK)** | ≥ 8 | https://www.java.com/en/download/ — required by Kafka/Zookeeper |
| **Apache Kafka** | ≥ 3.x | https://kafka.apache.org/downloads — includes Zookeeper |

> Kafka is distributed as a `.tgz` archive that includes Zookeeper. No separate install needed.

### Recommended Kafka Download

1. Go to: **https://kafka.apache.org/downloads**
2. Download the **Binary** release (e.g. `kafka_2.13-3.7.0.tgz`)
3. Extract to a short path, e.g. `C:\kafka`

---

## 📦 Python Packages

```
fastapi==0.115.2
uvicorn==0.31.1
kafka-python==2.0.2
pydantic==2.9.2
requests==2.32.3
```

Install:
```powershell
pip install -r requirements.txt
```

---

## 🚀 Quick Start

See **[HOW_TO_RUN.md](HOW_TO_RUN.md)** for the full step-by-step guide covering:
- Installing & starting Kafka + Zookeeper on Windows
- Creating the Kafka topic
- Running all Python services locally
- End-to-end testing with `test_system.py`
- Fault-tolerance scenarios

---

## 📡 API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/tasks/submit` | Submit a new task |
| `GET` | `/tasks/{task_id}` | Get task details + retry history |
| `GET` | `/tasks?status=...` | List tasks (optional status filter) |
| `GET` | `/workers` | Worker liveness + stats |
| `GET` | `/queue/stats` | Queue depth, success rate, timings |
| `DELETE` | `/tasks/{task_id}` | Cancel a pending task |
| `POST` | `/tasks/{task_id}/retry` | Manually requeue a DEAD task |
| `GET` | `/health` | Health check (Kafka connectivity) |

**Swagger UI:** http://localhost:8000/docs

### Sample Submit Body

```json
{
  "task_type": "email_send",
  "payload": { "to": "user@example.com" },
  "priority": 8,
  "max_retries": 3
}
```

Valid `task_type` values: `email_send`, `image_resize`, `data_export`, `report_gen`, `notification`

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `TOPIC_TASKS` | `tasks.queue` | Kafka topic for task messages |
| `KAFKA_CONSUMER_GROUP` | `workers` | Kafka consumer group for workers |
| `DB_PATH` | `<project>/data/tasks.db` | SQLite database path |
| `LOG_LEVEL` | `INFO` | Logging level |
| `API_HOST` | `0.0.0.0` | API bind host |
| `API_PORT` | `8000` | API bind port |
| `WORKER_ID` | auto-generated | Override worker identity |

---

## 🔑 Key Design Decisions

- **Kafka (not Redis)** used as the distributed message transport.
- **SQLite** handles all durable state: task records, retry history, worker heartbeats, task locks, delayed retry queue.
- **Priority queuing** via Kafka partition assignment (partition 0 = highest priority).
- **Workers share a single Kafka consumer group** — Kafka auto-balances partitions across workers.
- **Exponential backoff**: retry delay = `min(2^retry_count, 60)` seconds, stored in SQLite `delayed_tasks` table.
- **Cancelled tasks**: Kafka messages cannot be deleted; cancellation is recorded in SQLite and workers skip execution via terminal-status check.
