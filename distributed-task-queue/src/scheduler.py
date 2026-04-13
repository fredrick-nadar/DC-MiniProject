import logging
import threading
import time

from broker import KafkaBroker
from config import LOG_LEVEL, SCHEDULER_INTERVAL_SECONDS

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [scheduler] %(message)s",
)
logger = logging.getLogger(__name__)


class RetryScheduler:
    def __init__(self, broker: KafkaBroker) -> None:
        self.broker = broker
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def _loop(self) -> None:
        backoff = 1
        while not self._stop_event.is_set():
            try:
                flushed = self.broker.flush_due_delayed_tasks(max_batch=200)
                if flushed:
                    logger.info("Scheduler flushed %d delayed task(s) to Kafka", flushed)
                backoff = 1
                self._stop_event.wait(SCHEDULER_INTERVAL_SECONDS)
            except Exception:
                logger.exception("Scheduler error. Retrying in %ss", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Retry scheduler started")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        logger.info("Retry scheduler stopped")
