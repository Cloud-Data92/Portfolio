"""
SQLite-backed history store for trades, alerts, and price snapshots.
Uses aiosqlite for async access from the FastAPI server.
"""

import time
import sqlite3
import json
import logging
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"

# Module-level cache: slug -> "UP" | "DOWN" | None (unresolved)
_resolution_cache: dict[str, str | None] = {}
# Track when None entries were last checked so we can retry
_resolution_last_check: dict[str, float] = {}

DB_PATH = Path(__file__).parent.parent / "data" / "history.db"


def _ensure_db():
    """Create database and tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            market TEXT NOT NULL,
            up_price REAL,
            down_price REAL,
            total_cost REAL,
            profit_pct REAL,
            shares INTEGER,
            investment REAL,
            expected_profit REAL,
            dry_run INTEGER DEFAULT 1,
            up_order_id TEXT,
            down_order_id TEXT,
            extra TEXT
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            channel TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            message TEXT,
            success INTEGER DEFAULT 1,
            extra TEXT
        );

        CREATE TABLE IF NOT EXISTS price_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            market TEXT,
            up_ask REAL,
            down_ask REAL,
            total_ask REAL,
            btc_price REAL
        );

        CREATE TABLE IF NOT EXISTS event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            event_type TEXT NOT NULL,
            data TEXT,
            source TEXT DEFAULT 'bot'
        );

        CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(timestamp);
        CREATE INDEX IF NOT EXISTS idx_prices_ts ON price_snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS idx_eventlog_ts ON event_log(timestamp);
        CREATE INDEX IF NOT EXISTS idx_eventlog_type ON event_log(event_type);
    """)
    conn.close()


def _migrate_add_asset_column():
    """Add 'asset' column to trades table if it doesn't exist (safe migration)."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        # Check if column exists
        cursor = conn.execute("PRAGMA table_info(trades)")
        columns = [row[1] for row in cursor.fetchall()]
        if "asset" not in columns:
            conn.execute("ALTER TABLE trades ADD COLUMN asset TEXT DEFAULT 'btc'")
            conn.commit()
            logger.info("Migrated trades table: added 'asset' column")
    except Exception as e:
        logger.debug(f"Migration check for asset column: {e}")
    finally:
        conn.close()


class HistoryStore:
    """Synchronous SQLite history store (safe to call from any thread)."""

    def __init__(self):
        _ensure_db()
        _migrate_add_asset_column()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        return conn

    def add_trade(self, data_or_market=None, up_price: float = 0.0, down_price: float = 0.0,
                  total_cost: float = 0.0, profit_pct: float = 0.0, shares: int = 0,
                  investment: float = 0.0, expected_profit: float = 0.0,
                  dry_run: bool = True, up_order_id: str = "",
                  down_order_id: str = "", extra: Optional[dict] = None,
                  asset: str = "btc"):
        """Record a trade. Accepts either keyword args or a dict with trade data.

        Dict mode (from feeds.py): add_trade({"market": ..., "shares": ..., ...})
        Keyword mode (legacy): add_trade("slug", up_price=..., ...)
        """
        if isinstance(data_or_market, dict):
            d = data_or_market
            market = d.get("market", "")
            up_price = d.get("up_price", 0.0)
            down_price = d.get("down_price", 0.0)
            total_cost = d.get("total_cost", 0.0)
            profit_pct = d.get("profit_pct", 0.0)
            shares = d.get("shares", 0)
            investment = d.get("investment", 0.0)
            expected_profit = d.get("expected_profit", 0.0)
            dry_run = d.get("dry_run", True)
            up_order_id = d.get("up_order_id", "")
            down_order_id = d.get("down_order_id", "")
            asset = d.get("asset", "btc")
            extra = d.get("extra")
        else:
            market = data_or_market or ""

        conn = self._conn()
        try:
            conn.execute(
                """INSERT INTO trades (timestamp, market, up_price, down_price,
                   total_cost, profit_pct, shares, investment, expected_profit,
                   dry_run, up_order_id, down_order_id, extra, asset)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (time.time(), market, up_price, down_price, total_cost,
                 profit_pct, shares, investment, expected_profit,
                 1 if dry_run else 0, up_order_id, down_order_id,
                 json.dumps(extra) if extra else None, asset)
            )
            conn.commit()
        finally:
            conn.close()

    def add_alert(self, channel: str, alert_type: str, message: str,
                  success: bool = True, extra: Optional[dict] = None):
        """Record an alert delivery."""
        conn = self._conn()
        try:
            conn.execute(
                """INSERT INTO alerts (timestamp, channel, alert_type, message,
                   success, extra) VALUES (?, ?, ?, ?, ?, ?)""",
                (time.time(), channel, alert_type, message,
                 1 if success else 0, json.dumps(extra) if extra else None)
            )
            conn.commit()
        finally:
            conn.close()

    def add_price_snapshot(self, market: str, up_ask: float, down_ask: float,
                           total_ask: float, btc_price: float = 0.0):
        """Record a price snapshot."""
        conn = self._conn()
        try:
            conn.execute(
                """INSERT INTO price_snapshots (timestamp, market, up_ask,
                   down_ask, total_ask, btc_price) VALUES (?, ?, ?, ?, ?, ?)""",
                (time.time(), market, up_ask, down_ask, total_ask, btc_price)
            )
            conn.commit()
        finally:
            conn.close()

    def get_trades(self, limit: int = 50, offset: int = 0) -> list[dict]:
        """Get recent trades."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_trades_with_pnl(self, limit: int = 50) -> list[dict]:
        """Get recent trades with realized P&L computed per row.

        For binary arb: you buy both sides for total_cost, payout is $1.00 per share.
        Realized P&L = (shares * $1.00) - investment - (shares * $0.02 fee)
        If investment is 0 but expected_profit exists, use expected_profit as P&L.
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                shares = d.get("shares", 0) or 0
                investment = d.get("investment", 0) or 0
                expected_profit = d.get("expected_profit", 0) or 0

                # Compute realized P&L
                if investment > 0 and shares > 0:
                    payout = shares * 1.0  # Binary markets pay $1 per share
                    fee = shares * 0.02    # 2% winner fee
                    pnl = payout - investment - fee
                elif expected_profit != 0:
                    pnl = expected_profit
                else:
                    pnl = 0

                pnl_pct = (pnl / investment * 100) if investment > 0 else 0
                d["pnl"] = round(pnl, 4)
                d["pnl_pct"] = round(pnl_pct, 2)
                d["asset"] = d.get("asset", "btc")
                result.append(d)
            return result
        finally:
            conn.close()

    def get_alerts(self, limit: int = 50, offset: int = 0) -> list[dict]:
        """Get recent alerts."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_price_history(self, market: str = "", hours: float = 1.0) -> list[dict]:
        """Get price snapshots for a time range."""
        conn = self._conn()
        try:
            cutoff = time.time() - (hours * 3600)
            if market:
                rows = conn.execute(
                    """SELECT * FROM price_snapshots
                       WHERE timestamp > ? AND market = ?
                       ORDER BY timestamp ASC""",
                    (cutoff, market)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM price_snapshots
                       WHERE timestamp > ? ORDER BY timestamp ASC""",
                    (cutoff,)
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def add_event_log(self, event_type: str, data: Optional[dict] = None,
                      source: str = "bot"):
        """Record an event to the log."""
        conn = self._conn()
        try:
            conn.execute(
                """INSERT INTO event_log (timestamp, event_type, data, source)
                   VALUES (?, ?, ?, ?)""",
                (time.time(), event_type, json.dumps(data) if data else None, source)
            )
            conn.commit()
        except Exception as e:
            logger.debug(f"Event log write error: {e}")
        finally:
            conn.close()

    def get_event_log(self, limit: int = 200, offset: int = 0,
                      event_type: str = "") -> list[dict]:
        """Get event log entries."""
        conn = self._conn()
        try:
            if event_type:
                rows = conn.execute(
                    """SELECT * FROM event_log WHERE event_type = ?
                       ORDER BY timestamp DESC LIMIT ? OFFSET ?""",
                    (event_type, limit, offset)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM event_log ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                    (limit, offset)
                ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                if d.get("data"):
                    try:
                        d["data"] = json.loads(d["data"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                result.append(d)
            return result
        finally:
            conn.close()

    def get_live_trades(self, limit: int = 30) -> list[dict]:
        """Get recent LIVE (non-dry-run) trades for dashboard display.

        Queries Gamma API for market resolutions and computes actual P&L.
        Results are cached so we only hit the API once per slug.
        Returns trades sorted by most recent first.
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                """SELECT * FROM trades WHERE dry_run = 0
                   ORDER BY timestamp DESC LIMIT ?""",
                (limit,)
            ).fetchall()

            # Collect unique slugs that need resolution
            slugs_needed = set()
            now = time.time()
            for r in rows:
                slug = r["market"]
                if not slug:
                    continue
                if slug not in _resolution_cache:
                    slugs_needed.add(slug)
                elif _resolution_cache[slug] is None:
                    # Retry unresolved slugs every 30 seconds
                    last_check = _resolution_last_check.get(slug, 0)
                    if now - last_check > 30:
                        slugs_needed.add(slug)

            # Batch-resolve unknown slugs (sync, fast)
            if slugs_needed:
                self._resolve_slugs(slugs_needed)

            result = []
            for r in rows:
                d = dict(r)
                extra = {}
                if d.get("extra"):
                    try:
                        extra = json.loads(d["extra"])
                    except (json.JSONDecodeError, TypeError):
                        pass

                side = extra.get("side", "?")
                action = extra.get("action", "BUY")
                shares = d.get("shares", 0) or 0
                investment = d.get("investment", 0) or 0
                slug = d["market"]

                # Compute P&L from resolution
                resolution = _resolution_cache.get(slug)
                if action == "SELL" and extra.get("pnl") is not None:
                    # Early exit — P&L already computed at sell time
                    pnl = extra["pnl"]
                    res_label = "EXIT_WIN" if pnl > 0 else "EXIT_LOSS"
                elif action == "SETTLE" and extra.get("result"):
                    # Settlement with stored result — normalize to WIN/LOSS
                    pnl = extra.get("pnl", 0)
                    raw_res = extra["result"]
                    # P&L sign is the source of truth for WIN/LOSS.
                    # Directional correctness (UP_WON/DOWN_WON) can disagree
                    # with P&L when fees turn a directional win into a net loss.
                    if pnl != 0:
                        res_label = "WIN" if pnl > 0 else "LOSS"
                    elif raw_res in ("WIN", "LOSS"):
                        res_label = raw_res
                    elif "WON" in raw_res:
                        won = (side == "UP" and "UP_WON" in raw_res) or \
                              (side == "DOWN" and "DOWN_WON" in raw_res)
                        res_label = "WIN" if won else "LOSS"
                    else:
                        res_label = "LOSS"
                elif resolution and side in ("UP", "DOWN"):
                    # Resolved market: compute P&L
                    won = (side == resolution)
                    if won:
                        payout = shares * 1.0
                        fee = shares * 0.02  # 2% winner fee
                        pnl = payout - investment - fee
                        res_label = "WIN"
                    else:
                        pnl = -investment
                        res_label = "LOSS"
                elif resolution is None and slug in _resolution_cache:
                    # Market exists in cache but unresolved (still open)
                    pnl = 0
                    res_label = "OPEN"
                else:
                    pnl = 0
                    res_label = "OPEN"

                per_share_price = round(investment / max(shares, 1), 4)
                result.append({
                    "id": slug,
                    "asset": (d.get("asset") or "btc").upper(),
                    "interval": self._interval_from_slug(slug),
                    "side": side,
                    "action": action,
                    "price": per_share_price,
                    "shares": shares,
                    "cost": round(investment, 4),
                    "result": res_label,
                    "pnl": round(pnl, 4),
                    "timestamp": d.get("timestamp", 0),
                    "confidence": extra.get("confidence", 0),
                    "copy": extra.get("copy", False),
                })
            return result
        finally:
            conn.close()

    @staticmethod
    def _resolve_slugs(slugs: set[str]):
        """Query Gamma API for market resolutions. Caches results."""
        now = time.time()
        try:
            client = httpx.Client(timeout=8.0, headers={"User-Agent": "Mozilla/5.0"})
            for slug in slugs:
                _resolution_last_check[slug] = now
                try:
                    resp = client.get(f"{GAMMA_API}/events", params={"slug": slug})
                    if resp.status_code != 200:
                        _resolution_cache[slug] = None
                        continue
                    data = resp.json()
                    if not data:
                        _resolution_cache[slug] = None
                        continue
                    ev = data[0] if isinstance(data, list) else data
                    markets = ev.get("markets", [])
                    if not markets:
                        _resolution_cache[slug] = None
                        continue
                    # Check all markets for resolution (some events have multiple markets)
                    m = None
                    for mkt in markets:
                        if mkt.get("closed", False) and mkt.get("outcomePrices"):
                            m = mkt
                            break
                    if m is None:
                        # No resolved market found yet
                        _resolution_cache[slug] = None
                        continue
                    # outcomePrices: ["1","0"] = UP won, ["0","1"] = DOWN won
                    prices = m.get("outcomePrices", "[]")
                    if isinstance(prices, str):
                        prices = json.loads(prices)
                    if len(prices) >= 2:
                        if float(prices[0]) > 0.5:
                            _resolution_cache[slug] = "UP"
                        elif float(prices[1]) > 0.5:
                            _resolution_cache[slug] = "DOWN"
                        else:
                            _resolution_cache[slug] = None
                    else:
                        _resolution_cache[slug] = None
                except Exception as e:
                    logger.debug(f"Resolution fetch error for {slug}: {e}")
                    _resolution_cache[slug] = None
            client.close()
        except Exception as e:
            logger.warning(f"Resolution batch error: {e}")

    @staticmethod
    def _interval_from_slug(slug: str) -> str:
        """Extract interval from slug like 'btc-updown-5m-1771080300'."""
        if "-5m-" in slug:
            return "5m"
        elif "-15m-" in slug:
            return "15m"
        return "?"

    def get_live_pnl_summary(self) -> dict:
        """Compute ground-truth P&L from DB (not in-memory counters).

        Returns dict with total_pnl, wins, losses, total_bets.
        Groups by market slug+side — one W/L per unique trade, not per event.
        This is the authoritative source — immune to restart drift.
        """
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT market, extra FROM trades WHERE dry_run = 0"
            ).fetchall()

            total_pnl = 0.0
            buy_count = 0
            # Group PNL by slug+side for accurate W/L counting
            slug_pnl: dict[str, float] = {}

            for r in rows:
                extra = json.loads(r["extra"] or "{}")
                action = extra.get("action", "BUY")
                pnl = extra.get("pnl", 0) or 0
                side = extra.get("side", "?")
                slug = r["market"]

                if action == "BUY":
                    buy_count += 1
                elif action in ("SELL", "SETTLE"):
                    total_pnl += pnl
                    key = f"{slug}_{side}"
                    slug_pnl[key] = slug_pnl.get(key, 0) + pnl

            wins = sum(1 for v in slug_pnl.values() if v > 0.001)
            losses = sum(1 for v in slug_pnl.values() if v < -0.001)

            return {
                "total_pnl": round(total_pnl, 4),
                "wins": wins,
                "losses": losses,
                "total_bets": len(slug_pnl),  # unique trades, not raw BUY count
            }
        finally:
            conn.close()

    def has_recent_buy(self, market_slug: str, max_age: float = 600) -> bool:
        """Check if there's a recent BUY on this market slug (within max_age seconds).

        Used to prevent double-buys after bot restart — in-memory state
        (_copy_sniped_slugs, bet_side) is lost on restart but DB persists.
        """
        conn = self._conn()
        try:
            cutoff = time.time() - max_age
            row = conn.execute(
                """SELECT COUNT(*) as cnt FROM trades
                   WHERE market = ? AND timestamp > ?
                   AND extra LIKE '%"action": "BUY"%'""",
                (market_slug, cutoff)
            ).fetchone()
            return (row["cnt"] or 0) > 0
        finally:
            conn.close()

    def get_recent_buys(self, max_age: float = 600) -> dict:
        """Get all recent BUY market slugs mapped to full bet data.

        Returns {slug: {"side", "ts", "shares", "price", "order_id", "cost",
                        "fill_cost", "fill_shares", "approach"}}
        for BUYs within the last max_age seconds. Used to restore full bet
        state on active windows after restart.
        """
        conn = self._conn()
        try:
            cutoff = time.time() - max_age
            rows = conn.execute(
                """SELECT market, extra, timestamp FROM trades
                   WHERE timestamp > ? AND extra LIKE '%"action": "BUY"%'
                   ORDER BY timestamp DESC""",
                (cutoff,)
            ).fetchall()
            result = {}
            for r in rows:
                slug = r["market"]
                if slug in result:
                    continue  # keep most recent
                try:
                    extra = json.loads(r["extra"]) if r["extra"] else {}
                    side = extra.get("side", "")
                    if side:
                        result[slug] = {
                            "side": side,
                            "ts": r["timestamp"],
                            "shares": extra.get("shares", 0),
                            "price": extra.get("price", 0),
                            "order_id": extra.get("order_id", ""),
                            "cost": extra.get("cost", 0),
                            "fill_cost": extra.get("fill_cost", 0),
                            "fill_shares": extra.get("fill_shares", 0),
                            "approach": extra.get("approach", ""),
                        }
                except Exception:
                    pass
            return result
        finally:
            conn.close()

    def get_trade_stats(self) -> dict:
        """Get aggregate trade statistics."""
        conn = self._conn()
        try:
            row = conn.execute(
                """SELECT COUNT(*) as total_trades,
                   SUM(investment) as total_invested,
                   SUM(expected_profit) as total_profit,
                   SUM(CASE WHEN dry_run = 0 THEN 1 ELSE 0 END) as live_trades
                   FROM trades"""
            ).fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()


# Module-level singleton
history = HistoryStore()
