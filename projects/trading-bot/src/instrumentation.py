"""
Lightweight latency + slippage instrumentation for the PolyBot trading loop.

Tracks three critical timing windows:
  1. recv→decision  : WS message received → trade decision made
  2. decision→send  : trade decision → order sent to CLOB
  3. send→ack       : order sent → fill acknowledgement

Also tracks slippage (expected vs actual fill price) and loop cycle time.

All metrics stored in-memory ring buffers (no DB, no blocking IO).
Exposes percentile stats (p50/p95/p99) via `.summary()` for dashboard.

Usage:
    from .instrumentation import perf

    # WS message received
    perf.stamp("ws_recv")

    # Trade decision made
    perf.stamp("decision")

    # After order sent
    perf.stamp("order_sent")

    # After fill received
    perf.stamp("order_acked")

    # Record slippage
    perf.record_slippage(expected=0.45, actual=0.47, side="BUY", shares=10)

    # Record loop cycle
    perf.record_cycle(elapsed_ms=12.5)

    # Get stats for dashboard
    stats = perf.summary()
"""

import time
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


# Ring buffer size — ~2000 samples ≈ 30 mins at 1 trade/sec
BUFFER_SIZE = 2000


@dataclass
class SlippageEvent:
    """Single slippage measurement."""
    ts: float
    expected: float
    actual: float
    side: str       # BUY or SELL
    shares: float
    slippage_pct: float  # (actual - expected) / expected × 100


class PerfTracker:
    """Thread-safe latency + slippage tracker with percentile reporting."""

    def __init__(self, buf_size: int = BUFFER_SIZE):
        self._buf_size = buf_size
        self._lock = threading.Lock()

        # Stamp dictionary: name → timestamp (for computing spans)
        self._stamps: dict[str, float] = {}

        # Latency ring buffers (milliseconds)
        self._recv_to_decision: deque[float] = deque(maxlen=buf_size)
        self._decision_to_send: deque[float] = deque(maxlen=buf_size)
        self._send_to_ack: deque[float] = deque(maxlen=buf_size)

        # Loop cycle times (milliseconds)
        self._cycle_times: deque[float] = deque(maxlen=buf_size)

        # Slippage events
        self._slippage: deque[SlippageEvent] = deque(maxlen=buf_size)

        # Blocking call durations (milliseconds): name → deque
        self._blocking: dict[str, deque] = {}

        # Order execution timing
        self._order_times: deque[float] = deque(maxlen=buf_size)

        # WS message processing times
        self._ws_process_times: deque[float] = deque(maxlen=buf_size)

        # Counters
        self._total_orders = 0
        self._failed_orders = 0
        self._total_ws_msgs = 0

    # ── Timestamps ──────────────────────────────────────────────

    def stamp(self, name: str) -> float:
        """Record a named timestamp. Returns the timestamp."""
        t = time.monotonic()
        with self._lock:
            self._stamps[name] = t
        return t

    def span_ms(self, start_name: str, end_name: str) -> Optional[float]:
        """Compute milliseconds between two named stamps."""
        with self._lock:
            s = self._stamps.get(start_name)
            e = self._stamps.get(end_name)
        if s is not None and e is not None:
            return (e - s) * 1000
        return None

    # ── Recording ───────────────────────────────────────────────

    def record_recv_to_decision(self, ms: float):
        """Record WS recv → trade decision latency (ms)."""
        with self._lock:
            self._recv_to_decision.append(ms)

    def record_decision_to_send(self, ms: float):
        """Record trade decision → order sent latency (ms)."""
        with self._lock:
            self._decision_to_send.append(ms)

    def record_send_to_ack(self, ms: float):
        """Record order sent → fill ack latency (ms)."""
        with self._lock:
            self._send_to_ack.append(ms)

    def record_cycle(self, elapsed_ms: float):
        """Record one main-loop cycle time (ms)."""
        with self._lock:
            self._cycle_times.append(elapsed_ms)

    def record_order_time(self, elapsed_ms: float, success: bool = True):
        """Record total order execution time (ms) and success/fail."""
        with self._lock:
            self._order_times.append(elapsed_ms)
            self._total_orders += 1
            if not success:
                self._failed_orders += 1

    def record_ws_processing(self, elapsed_ms: float):
        """Record WS message processing time (ms)."""
        with self._lock:
            self._ws_process_times.append(elapsed_ms)
            self._total_ws_msgs += 1

    def record_blocking_call(self, name: str, elapsed_ms: float):
        """Record a blocking sync call duration (ms).

        Used for: config.get_usdce_balance(), history.add_trade(), etc.
        """
        with self._lock:
            if name not in self._blocking:
                self._blocking[name] = deque(maxlen=self._buf_size)
            self._blocking[name].append(elapsed_ms)

    def record_slippage(self, expected: float, actual: float,
                        side: str, shares: float):
        """Record price slippage on a fill."""
        if expected <= 0:
            return
        slip_pct = (actual - expected) / expected * 100
        ev = SlippageEvent(
            ts=time.time(),
            expected=expected,
            actual=actual,
            side=side,
            shares=shares,
            slippage_pct=slip_pct,
        )
        with self._lock:
            self._slippage.append(ev)

    # ── Percentile helpers ──────────────────────────────────────

    @staticmethod
    def _percentiles(data: deque | list, pcts=(50, 95, 99)) -> dict:
        """Compute percentiles from a deque. Returns {p50, p95, p99, count, mean}."""
        if not data:
            return {"p50": 0, "p95": 0, "p99": 0, "count": 0, "mean": 0}
        arr = sorted(data)
        n = len(arr)
        result = {"count": n, "mean": round(sum(arr) / n, 2)}
        for p in pcts:
            idx = int(n * p / 100)
            idx = min(idx, n - 1)
            result[f"p{p}"] = round(arr[idx], 2)
        return result

    # ── Summary for dashboard ───────────────────────────────────

    def summary(self) -> dict:
        """Return full metrics summary for dashboard display.

        All times in milliseconds. Structure:
        {
            "latency": {
                "recv_to_decision": {p50, p95, p99, count, mean},
                "decision_to_send": {...},
                "send_to_ack": {...},
            },
            "cycle": {p50, p95, p99, count, mean},
            "orders": {
                "execution_time": {p50, p95, p99, count, mean},
                "total": int, "failed": int, "success_rate": float,
            },
            "ws": {
                "processing_time": {p50, p95, p99, count, mean},
                "total_msgs": int,
            },
            "slippage": {
                "buy": {mean_pct, count, worst_pct},
                "sell": {mean_pct, count, worst_pct},
            },
            "blocking": {
                "name": {p50, p95, p99, count, mean},
            },
        }
        """
        with self._lock:
            # Copy deques under lock
            r2d = list(self._recv_to_decision)
            d2s = list(self._decision_to_send)
            s2a = list(self._send_to_ack)
            cyc = list(self._cycle_times)
            ords = list(self._order_times)
            ws_p = list(self._ws_process_times)
            slips = list(self._slippage)
            blocking = {k: list(v) for k, v in self._blocking.items()}
            total_orders = self._total_orders
            failed_orders = self._failed_orders
            total_ws = self._total_ws_msgs

        # Slippage breakdown by side
        buy_slips = [s.slippage_pct for s in slips if s.side == "BUY"]
        sell_slips = [s.slippage_pct for s in slips if s.side == "SELL"]

        def _slip_stats(arr):
            if not arr:
                return {"mean_pct": 0, "count": 0, "worst_pct": 0}
            return {
                "mean_pct": round(sum(arr) / len(arr), 3),
                "count": len(arr),
                "worst_pct": round(max(arr, key=abs), 3),
            }

        success_rate = ((total_orders - failed_orders) / total_orders * 100
                        if total_orders > 0 else 100)

        return {
            "latency": {
                "recv_to_decision": self._percentiles(r2d),
                "decision_to_send": self._percentiles(d2s),
                "send_to_ack": self._percentiles(s2a),
            },
            "cycle": self._percentiles(cyc),
            "orders": {
                "execution_time": self._percentiles(ords),
                "total": total_orders,
                "failed": failed_orders,
                "success_rate": round(success_rate, 1),
            },
            "ws": {
                "processing_time": self._percentiles(ws_p),
                "total_msgs": total_ws,
            },
            "slippage": {
                "buy": _slip_stats(buy_slips),
                "sell": _slip_stats(sell_slips),
            },
            "blocking": {
                name: self._percentiles(vals)
                for name, vals in blocking.items()
            },
        }

    def one_liner(self) -> str:
        """Compact one-line summary for console logging."""
        s = self.summary()
        cyc = s["cycle"]
        ords = s["orders"]
        parts = [
            f"cycle={cyc['p50']}/{cyc['p95']}ms",
            f"orders={ords['total']}({ords['success_rate']:.0f}%)",
        ]
        if ords["execution_time"]["count"] > 0:
            parts.append(f"exec={ords['execution_time']['p50']}/{ords['execution_time']['p95']}ms")
        slip_buy = s["slippage"]["buy"]
        if slip_buy["count"] > 0:
            parts.append(f"slip_buy={slip_buy['mean_pct']:+.2f}%")
        slip_sell = s["slippage"]["sell"]
        if slip_sell["count"] > 0:
            parts.append(f"slip_sell={slip_sell['mean_pct']:+.2f}%")
        # Blocking calls summary
        for name, stats in s["blocking"].items():
            if stats["count"] > 0:
                parts.append(f"{name}={stats['p50']:.0f}/{stats['p95']:.0f}ms")
        return " · ".join(parts)


# ── Module-level singleton ──────────────────────────────────
perf = PerfTracker()
