"""
Async write queue for SQLite history operations.

Moves all DB writes off the hot path by routing them through
a background thread with a bounded queue. Reads remain synchronous
(they're infrequent and need immediate results).

Usage:
    from .async_db import db_queue

    # Non-blocking write (returns immediately):
    db_queue.add_trade({...})
    db_queue.add_event_log("BUY", {...})

    # Start the background writer (call once at startup):
    db_queue.start()

    # Graceful shutdown (flushes pending writes):
    db_queue.stop()
"""

import time
import queue
import threading
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class AsyncDBQueue:
    """Background writer thread for SQLite history operations.

    All writes are queued and executed in a dedicated thread,
    so the main asyncio event loop is never blocked by DB I/O.

    The queue is bounded (default 500) — if writes back up,
    oldest entries are dropped with a warning.
    """

    def __init__(self, max_queue: int = 500):
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue)
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._dropped = 0
        self._written = 0
        self._lock = threading.Lock()

    def start(self):
        """Start the background writer thread."""
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._writer_loop,
            name="db-writer",
            daemon=True,
        )
        self._thread.start()
        logger.info("AsyncDBQueue: background writer started")

    def stop(self, timeout: float = 5.0):
        """Stop the writer, flushing pending writes (up to timeout)."""
        self._running = False
        # Put a sentinel to unblock the queue.get()
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info(f"AsyncDBQueue: stopped (written={self._written}, dropped={self._dropped})")

    def _writer_loop(self):
        """Background thread: dequeue and execute writes sequentially."""
        # Import here to avoid circular imports
        from .history import history

        while self._running or not self._queue.empty():
            try:
                item = self._queue.get(timeout=1.0)
                if item is None:
                    continue  # sentinel for shutdown
                method, args, kwargs = item
                try:
                    func = getattr(history, method)
                    func(*args, **kwargs)
                    with self._lock:
                        self._written += 1
                except Exception as e:
                    logger.warning(f"AsyncDBQueue write error ({method}): {e}")
            except queue.Empty:
                continue
            except Exception as e:
                logger.warning(f"AsyncDBQueue loop error: {e}")

    def _enqueue(self, method: str, *args, **kwargs):
        """Add a write operation to the queue. Non-blocking."""
        try:
            self._queue.put_nowait((method, args, kwargs))
        except queue.Full:
            with self._lock:
                self._dropped += 1
            if self._dropped % 50 == 1:
                logger.warning(f"AsyncDBQueue: queue full, dropped {self._dropped} writes")

    # ── Public API (mirrors HistoryStore write methods) ───────

    def add_trade(self, data_or_market=None, **kwargs):
        """Queue a trade write."""
        self._enqueue("add_trade", data_or_market, **kwargs)

    def add_alert(self, channel: str, alert_type: str, message: str, **kwargs):
        """Queue an alert write."""
        self._enqueue("add_alert", channel, alert_type, message, **kwargs)

    def add_price_snapshot(self, market: str, up_ask: float, down_ask: float,
                           total_ask: float, btc_price: float = 0.0):
        """Queue a price snapshot write."""
        self._enqueue("add_price_snapshot", market, up_ask, down_ask, total_ask, btc_price)

    def add_event_log(self, event_type: str, data=None, source: str = "bot"):
        """Queue an event log write."""
        self._enqueue("add_event_log", event_type, data, source)

    @property
    def stats(self) -> dict:
        """Queue health stats for dashboard."""
        with self._lock:
            return {
                "pending": self._queue.qsize(),
                "written": self._written,
                "dropped": self._dropped,
                "alive": self._thread.is_alive() if self._thread else False,
            }


# Module-level singleton
db_queue = AsyncDBQueue()
