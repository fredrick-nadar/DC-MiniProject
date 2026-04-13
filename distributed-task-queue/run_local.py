import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def start_process(name: str, cmd: list[str], cwd: Path, env: dict[str, str]) -> subprocess.Popen:
    print(f"[local-runner] starting {name}: {' '.join(cmd)}")
    return subprocess.Popen(cmd, cwd=str(cwd), env=env)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run distributed task queue locally — requires Kafka + Zookeeper already running"
    )
    parser.add_argument("--workers", type=int, default=3, help="Number of worker processes")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    src_dir = project_root / "src"

    if not src_dir.exists():
        print("[local-runner] Error: src directory not found.")
        return 1

    env = os.environ.copy()
    # Kafka bootstrap — override via KAFKA_BOOTSTRAP env var if needed
    env.setdefault("KAFKA_BOOTSTRAP", "localhost:9092")
    env.setdefault("DB_PATH", str(project_root / "data" / "tasks.db"))
    env.setdefault("LOG_LEVEL", "INFO")

    print("[local-runner] -----------------------------------------------")
    print("[local-runner] Make sure Kafka is running first!")
    print("[local-runner]   Kafka    : port 9092")
    print("[local-runner] -----------------------------------------------")

    python_exe = sys.executable
    processes: list[subprocess.Popen] = []

    try:
        api_proc = start_process("api", [python_exe, "api.py"], src_dir, env)
        processes.append(api_proc)

        time.sleep(2)  # Give API a moment to bind and create topics

        fault_proc = start_process("fault_manager", [python_exe, "fault_manager.py"], src_dir, env)
        processes.append(fault_proc)

        for i in range(1, args.workers + 1):
            worker_env = env.copy()
            worker_env["WORKER_ID"] = f"local-worker-{i}"
            worker_proc = start_process(
                f"worker-{i}",
                [python_exe, "worker.py"],
                src_dir,
                worker_env,
            )
            processes.append(worker_proc)

        print("[local-runner] all services started")
        print("[local-runner] API docs:  http://localhost:8000/docs")
        print("[local-runner] dashboard: open dashboard/index.html in browser")
        print("[local-runner] press Ctrl+C to stop all processes")

        while True:
            for proc in processes:
                code = proc.poll()
                if code is not None and code != 0:
                    print(f"[local-runner] process exited unexpectedly with code {code}")
                    raise RuntimeError("a service crashed")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[local-runner] stopping services...")
    except Exception as exc:
        print(f"[local-runner] error: {exc}")
        return_code = 1
    else:
        return_code = 0
    finally:
        for proc in processes:
            if proc.poll() is None:
                if os.name == "nt":
                    proc.send_signal(signal.CTRL_BREAK_EVENT)
                    time.sleep(0.2)
                proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
