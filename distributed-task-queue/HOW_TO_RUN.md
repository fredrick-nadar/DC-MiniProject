# How to Run & Test — Distributed Task Queue (Kafka Edition)

This guide walks you through setting up, running, and end-to-end testing the system **without Docker** using **Apache Kafka KRaft Mode** (No Zookeeper required) natively via **WSL (Windows Subsystem for Linux)**.

---

## Prerequisites Checklist

- [ ] Python 3.11+ installed in WSL
- [ ] Java 8+ installed in WSL (e.g. `sudo apt install default-jre`)
- [ ] Apache Kafka downloaded and extracted (e.g. `/mnt/d/Programming/Tools/kafka_2.13-4.2.0`)

Verify:
```bash
python3 --version     # Python 3.11.x or higher
java -version         # Java 1.8.x / 11.x / 17.x / 21.x
```

---

## Part 1 — Install Apache Kafka (KRaft Mode)

### Step 1 — Download Kafka

1. Go to: **https://kafka.apache.org/downloads**
2. Under **Binary downloads**, click the latest release (e.g. Kafka 3.7+ or 4.0+)
3. Download and extract to your preferred directory, e.g. `/mnt/d/Programming/Tools/kafka_2.13-4.2.0` or `~/kafka`

Your directory should look like:
```text
<kafka_home>/
  bin/
    kafka-server-start.sh
    kafka-storage.sh
    kafka-topics.sh
    ...
  config/
    server.properties
```

### Step 2 — (Optional) Shorten log paths to avoid path-length issues

Open `<kafka_home>/config/server.properties` and set a short explicit WSL path (updating with your actual path):
```properties
log.dirs=/mnt/d/Programming/Tools/kafka_2.13-4.2.0/kafka-logs
```

---

## Part 2 — Start Kafka (KRaft Mode)

> Modern Kafka no longer requires Zookeeper. We will format the storage and start the KRaft broker directly.

Open a WSL terminal (keep it open throughout your session). Replace `/mnt/d/Programming/Tools/kafka_2.13-4.2.0` with your actual installation directory.

### Step 1 — Generate a Cluster UUID

```bash
/mnt/d/Programming/Tools/kafka_2.13-4.2.0/bin/kafka-storage.sh random-uuid
```
*(Copy the generated UUID from the terminal output)*

### Step 2 — Format Log Directories

Format the data directory using your copied UUID. The `--standalone` flag is required in Kafka 4.x for single-node setups:
```bash
/mnt/d/Programming/Tools/kafka_2.13-4.2.0/bin/kafka-storage.sh format -t <UUID_COPIED_ABOVE> -c /mnt/d/Programming/Tools/kafka_2.13-4.2.0/config/server.properties --standalone
```

### Step 3 — Start Kafka

```bash
/mnt/d/Programming/Tools/kafka_2.13-4.2.0/bin/kafka-server-start.sh /mnt/d/Programming/Tools/kafka_2.13-4.2.0/config/server.properties
```

Wait until you see: `[KafkaRaftServer nodeId=1] started` or `[KafkaServer id=1] started`.

### Step 4 — Create the Kafka topic

> Open a **second** WSL terminal for this:

```bash
/mnt/d/Programming/Tools/kafka_2.13-4.2.0/bin/kafka-topics.sh \
  --create \
  --topic tasks.queue \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Expected output: `Created topic tasks.queue.`

Verify the topic exists:
```bash
/mnt/d/Programming/Tools/kafka_2.13-4.2.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

> **Note:** The API auto-creates this topic on startup too, but creating it manually first ensures the correct partition count.

---

## Part 3 — Set Up and Run the Python Application

### Step 1 — Navigate to the project

```bash
cd /mnt/d/Programming/DC-MiniProject/distributed-task-queue
```

### Step 2 — Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3 — Install Python dependencies

```bash
pip install -r requirements.txt
```

Packages installed: `fastapi`, `uvicorn`, `kafka-python`, `pydantic`, `requests`

### Step 4 — Start all Python services

```bash
python run_local.py --workers 3
```

You should see:
```
[local-runner] -----------------------------------------------
[local-runner] Make sure Kafka is running first!
[local-runner]   Kafka    : port 9092
[local-runner] -----------------------------------------------
[local-runner] starting api: python api.py
[local-runner] starting fault_manager: python fault_manager.py
[local-runner] starting worker-1: python worker.py
[local-runner] starting worker-2: python worker.py
[local-runner] starting worker-3: python worker.py
[local-runner] all services started
[local-runner] API docs:  http://localhost:8000/docs
[local-runner] dashboard: open dashboard/index.html in browser
[local-runner] press Ctrl+C to stop all processes
```

### Step 5 — Verify the API is healthy

Open a **new WSL terminal** (keep run_local.py running):

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{"status": "ok", "kafka": true}
```

### Step 6 — Open the Dashboard

Open your web browser and navigate directly to the `dashboard/index.html` file path, or copy the path:
`file:///d:/Programming/DC-MiniProject/distributed-task-queue/dashboard/index.html`

---

## Part 4 — End-to-End Test 🧪

> ⚠️ Keep all services running before running the test.

In a new WSL terminal (with venv activated):

```bash
cd /mnt/d/Programming/DC-MiniProject/distributed-task-queue
python test_system.py
```

**What it does:**
1. Submits **20 random tasks** across 5 task types with random priorities (1-10) and retry counts (2-5)
2. Routes each task to the correct Kafka partition by priority
3. Polling task statuses every 3 seconds until all reach a terminal state
4. Prints a final summary

**Expected output:**
```
Submitting 20 random tasks...
Submitted 01/20: <uuid> (email_send, priority=7, retries=3)
...
Submitted 20/20: <uuid> (report_gen, priority=2, retries=4)
Polling task states until terminal...
Progress: 5/20 terminal | statuses={'IN_PROGRESS': 3, 'COMPLETED': 5, 'PENDING': 12}
...
Progress: 20/20 terminal | statuses={'COMPLETED': 17, 'DEAD': 3}

=== FINAL SUMMARY ===
Total tasks: 20
- COMPLETED: 17
- DEAD: 3
```

> ✅ A mix of COMPLETED and DEAD is **expected and correct** — DEAD = all retries exhausted (intentional fault simulation).

---

## Part 5 — Manual API Testing 🔧

### Submit a task

```bash
curl -X POST http://localhost:8000/tasks/submit \
  -H "Content-Type: application/json" \
  -d '{"task_type": "email_send", "payload": {"to": "test@example.com"}, "priority": 9, "max_retries": 3}'
```

### Check task status

```bash
curl http://localhost:8000/tasks/<task_id>
```

### List all tasks

```bash
curl http://localhost:8000/tasks
```

### Filter by status

```bash
curl "http://localhost:8000/tasks?status=COMPLETED"
curl "http://localhost:8000/tasks?status=DEAD"
```

### Worker status

```bash
curl http://localhost:8000/workers
```

### Queue statistics

```bash
curl http://localhost:8000/queue/stats
```

### Manually retry a DEAD task

```bash
curl -X POST http://localhost:8000/tasks/<task_id>/retry
```

### Cancel a PENDING task

```bash
curl -X DELETE http://localhost:8000/tasks/<task_id>
```

---

## Part 6 — Fault-Tolerance Scenarios 🛡️

### Scenario 1 — Worker crash mid-task

1. Start 3 workers and submit several long tasks (`data_export`, `report_gen`).
2. Kill one of the worker processes in the `run_local.py` terminal (Ctrl+C stops all; to kill one specifically, use `kill -9 <PID>` on a specific worker process).
3. **Observe:** The fault manager detects a stale SQLite heartbeat (>15 s) and requeues the in-progress task onto Kafka. A surviving worker picks it up.

### Scenario 2 — Kafka broker failure

1. Stop Kafka (Ctrl+C in the Kafka Terminal).
2. **Observe:** Workers and scheduler log connection errors with exponential backoff.
3. Restart Kafka (`kafka-server-start.sh ...`).
4. **Observe:** Services reconnect automatically.

### Scenario 3 — Exhausting retries → Dead Letter

Failed tasks are retried with delays:
- Retry 1: 2 s
- Retry 2: 4 s
- Retry 3: 8 s
- Max: 60 s

After all retries exhausted: status = `DEAD`.

```bash
# View dead tasks
curl "http://localhost:8000/tasks?status=DEAD"

# Manually revive a dead task
curl -X POST http://localhost:8000/tasks/<task_id>/retry
```

---

## Part 7 — Scaling Workers 📈

```bash
# Start with 5 workers instead of 3
python run_local.py --workers 5
```

Kafka automatically rebalances the 3 partitions across all workers in the `workers` consumer group.

---

## Troubleshooting 🔍

| Problem | Cause | Solution |
|---------|-------|----------|
| `NoBrokersAvailable` | Kafka not running | Start Kafka (Part 2) |
| `kafka` not found | Package not installed | `pip install -r requirements.txt` |
| `java` not found | Java not installed | `sudo apt install default-jre` |
| `ModuleNotFoundError` | venv not activated | `source .venv/bin/activate` then `pip install -r requirements.txt` |
| Dashboard shows no data | API not running/wrong port| Check `http://localhost:8000/health` |
| Tasks stuck `IN_PROGRESS` | Worker crashed | Fault manager requeues after 15 s |
| `TopicAlreadyExistsError` | Topic exists | Harmless — API handles this gracefully |

---

## Quick Reference Card 📋

```bash
# 1. Start Kafka (Terminal 1)
/mnt/d/Programming/Tools/kafka_2.13-4.2.0/bin/kafka-server-start.sh /mnt/d/Programming/Tools/kafka_2.13-4.2.0/config/server.properties

# 2. Create topic (Terminal 2, once)
/mnt/d/Programming/Tools/kafka_2.13-4.2.0/bin/kafka-topics.sh --create --topic tasks.queue --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

# 3. Set up Python (Terminal 3)
cd /mnt/d/Programming/DC-MiniProject/distributed-task-queue
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. Start all Python services
python run_local.py --workers 3

# 5. Test (new terminal, venv activated)
python test_system.py

# 6. Health check
curl http://localhost:8000/health
```
