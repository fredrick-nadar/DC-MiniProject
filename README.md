<div align="center">
  <h1>Distributed Task Queue</h1>
  <p>A robust, distributed task processing system built with <b>Python</b>, <b>Kafka</b>, and <b>FastAPI</b>, featuring a real-time analytics dashboard.</p>
</div>

<br />

## 🚀 Features

- **Distributed Architecture:** Producer-Consumer pattern using **Apache Kafka** (KRaft mode) for high-throughput, fault-tolerant message brokering.
- **Persistent State:** Uses **SQLite** for maintaining task lifecycle tracking, heartbeats, and queue statistics.
- **Real-Time Dashboard:** A sleek, premium dashboard featuring **Chart.js** integration to visualize worker performance, task statuses, and real-time processing throughput.
- **Fault Tolerance & Reliability:** 
  - Worker heartbeat monitoring and automatic task reallocation on instance failure.
  - Configurable retry limits with a Dead Letter Queue (DEAD status) for exhausted tasks.
  - Manual retry capability for dead tasks.
- **Real API Integrations:** Workers actually execute HTTP calls to lightweight external APIs (e.g., to fetch facts, quotes, IPs) to simulate real-world, network-bound jobs.

## 🛠️ Technology Stack

- **Backend / API**: FastAPI, Uvicorn
- **Message Broker**: Apache Kafka (kafka-python-ng)
- **Database**: SQLite
- **Frontend**: Vanilla HTML/CSS/JS, Chart.js

## 📦 Architecture Flow

1. **Producer (FastAPI)** schedules tasks and pushes them to the `tasks.queue` Kafka topic.
2. **Workers** consume messages, acquire locks, process the tasks through real external API calls, and update their heartbeat.
3. **Fault Manager** monitors worker heartbeats. If a worker dies, it reclaims its hanging tasks and re-queues them.
4. **Dashboard** continually polls the API for live metrics and presents actionable controls (e.g., batch submissions, clear queue).

## ⚡ Getting Started

### Prerequisites

- Python 3.12+
- Apache Kafka (configured in KRaft mode) installed and running locally on port `9092`.

### 1. Start Apache Kafka
Ensure Kafka is running before starting the task queue.
```bash
# Example for Windows (if using Kafka binaries)
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```

### 2. Setup the Python Environment
```bash
python -m venv venv
.\venv\Scripts\activate  # On Windows
pip install -r requirements.txt
```

### 3. Run the System
Use the bundled runner script to launch the API, the Fault Manager, and multiple worker instances simultaneously.
```bash
python run_local.py --workers 3
```

### 4. Open the Dashboard
Navigate to `dashboard/index.html` in your browser. 
- Click **"Submit Batch"** to enqueue tasks.
- Watch the **Chart.js** analytics update in real time as workers process tasks.
- View live logs in the console to monitor system health and Kafka consumer group rebalances.

## 📂 Project Structure

```text
├── dashboard/
│   └── index.html         # Real-time analytics and control UI
├── src/
│   ├── api.py             # FastAPI entrypoint
│   ├── broker.py          # Kafka abstraction layer
│   ├── config.py          # Global configurations & API endpoints
│   ├── database.py        # SQLite state persistence operations
│   ├── fault_manager.py   # Heartbeat monitor & task recovery
│   ├── models.py          # Pydantic data models
│   └── worker.py          # Queue consumer & task executor
├── test_system.py         # Batch testing script
└── run_local.py           # Multi-process local orchestration runner
```

## ⚠️ Notes
- If the database (`tasks.db`) becomes corrupted due to manual edits, simply delete the `data/` directory and restart the system. It will be recreated automatically.
- The system is configured by default to connect to `localhost:9092`. Modify `src/config.py` if your topology changes.
