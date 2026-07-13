"""
In-process event bus for decoupling bot, dashboard, feeds, and alerts.
Thread-safe pub/sub with support for sync and async callbacks.
"""

import threading
import asyncio
import logging
from typing import Callable, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

# Event type constants
PRICE_UPDATE = "price_update"
OPPORTUNITY = "opportunity"
TRADE_EXECUTED = "trade_executed"
MARKET_CHANGED = "market_changed"
BOT_STATUS = "bot_status"
BTC_PRICE = "btc_price"
ALERT_SENT = "alert_sent"
NEWS_ITEM = "news_item"
POLYMARKET_EVENT = "polymarket_event"
SCANNER_UPDATE = "scanner_update"
AUTOTRADE_STATUS = "autotrade_status"  # Pushed when auto-trade state changes


class EventBus:
    """Thread-safe publish/subscribe event bus."""

    def __init__(self):
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)
        self._lock = threading.Lock()

    def subscribe(self, event_type: str, callback: Callable):
        """Register a callback for an event type."""
        with self._lock:
            self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable):
        """Remove a callback for an event type."""
        with self._lock:
            try:
                self._subscribers[event_type].remove(callback)
            except ValueError:
                pass

    def publish(self, event_type: str, data: Any = None):
        """
        Publish an event to all subscribers.
        Calls sync callbacks directly, schedules async callbacks if possible.
        """
        with self._lock:
            callbacks = list(self._subscribers.get(event_type, []))

        for cb in callbacks:
            try:
                if asyncio.iscoroutinefunction(cb):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(cb(data))
                    except RuntimeError:
                        # No running loop in this thread - skip async callback
                        pass
                else:
                    cb(data)
            except Exception as e:
                logger.error(f"Event callback error ({event_type}): {e}")


# Module-level singleton
bus = EventBus()
