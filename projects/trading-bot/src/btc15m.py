"""
Crypto 15-Minute Up/Down Market — Dedicated Fast Scanner.

Supports multiple assets: BTC, ETH, SOL, XRP.
Each asset has 15-min Up/Down binary markets on Polymarket rolling every 15 minutes.
Slug pattern: {asset}-updown-15m-{start_time_unix}

Discovery strategy:
  - Compute the exact slug from current timestamp using 900-second alignment
  - Query Gamma API with exact `slug={asset}-updown-15m-{computed_ts}`
  - This is the ONLY reliable method — slug_contains does NOT work for recurring markets

SPEED OPTIMIZATIONS:
  - HTTP/2 connection pooling with keep-alive (reuses TCP connections)
  - 5s timeout for API calls (fast fail, don't block hot loop)
  - Event discovery cached aggressively (only re-fetched near window boundaries)
  - Parallel CLOB reads via asyncio.gather (Up + Down books simultaneously)
  - Reduced candidate slugs from 4 to 2 (current + next window)
"""

import json
import math
import time
import asyncio
import logging
from typing import Optional
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
WINNER_FEE = 0.02  # 2% on winning payout
INTERVAL = 900  # 15 minutes in seconds

# Supported assets for 15-minute Up/Down markets
SUPPORTED_ASSETS = ["btc", "eth", "sol", "xrp"]


class Crypto15mScanner:
    """Dedicated scanner for crypto 15-min Up/Down markets.

    Supports multiple assets (BTC, ETH, SOL, XRP) with per-asset caching.

    SPEED: Uses persistent HTTP connection pool, aggressive caching,
    and parallel requests to minimize latency per tick.
    """

    def __init__(self, assets: list[str] = None):
        self._assets = assets or ["btc"]
        self._http: Optional[httpx.AsyncClient] = None
        # Per-asset caching to avoid re-fetching every tick
        self._current_events: dict[str, Optional[dict]] = {}
        self._event_expires: dict[str, float] = {}
        self._last_fetch: dict[str, float] = {}

    @property
    def assets(self) -> list[str]:
        return self._assets

    @assets.setter
    def assets(self, value: list[str]):
        self._assets = [a for a in value if a in SUPPORTED_ASSETS] or ["btc"]

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            # Try HTTP/2 for multiplexing, fall back to HTTP/1.1
            try:
                self._http = httpx.AsyncClient(
                    timeout=5.0,  # Tight timeout — fail fast, don't block hot loop
                    http2=True,   # HTTP/2 multiplexing for faster parallel requests
                    limits=httpx.Limits(
                        max_connections=20,
                        max_keepalive_connections=10,
                        keepalive_expiry=30,  # Keep connections warm
                    ),
                    headers={
                        "Accept-Encoding": "identity",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Connection": "keep-alive",
                    },
                )
            except Exception:
                # h2 not installed — fall back to HTTP/1.1
                self._http = httpx.AsyncClient(
                    timeout=5.0,
                    limits=httpx.Limits(
                        max_connections=20,
                        max_keepalive_connections=10,
                        keepalive_expiry=30,
                    ),
                    headers={
                        "Accept-Encoding": "identity",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Connection": "keep-alive",
                    },
                )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Compute candidate slugs from current timestamp
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_candidate_slugs(asset: str = "btc") -> list[str]:
        """Compute exact 15m slugs — ONLY current + previous window (2 candidates).

        Slug pattern: {asset}-updown-15m-{start_time_unix}
        NOTE: The slug timestamp is the MARKET START TIME (not end time).
        Start times are aligned to 900-second (15-minute) boundaries.

        SPEED: Reduced from 4 candidates to 2 — the current window and
        previous window cover all active markets. No need to check future.
        """
        now = time.time()
        current_start = math.floor(now / INTERVAL) * INTERVAL
        prev_start = current_start - INTERVAL

        return [
            f"{asset}-updown-15m-{int(current_start)}",
            f"{asset}-updown-15m-{int(prev_start)}",
        ]

    # ------------------------------------------------------------------
    # Find active events
    # ------------------------------------------------------------------

    async def find_active_event(self, asset: str = "btc") -> Optional[dict]:
        """Find the currently active 15-min Up/Down event for a specific asset.

        Uses computed slug approach — queries Gamma API with exact slug=
        parameter since slug_contains does NOT work for recurring markets.

        Returns dict with keys:
          id, slug, title, asset, market_id, condition_id,
          up_token, down_token, end_time_unix, end_time_str, time_remaining
        Or None if no active market found.
        """
        now = time.time()

        # AGGRESSIVE CACHE: Use cached event if still valid
        # Only re-fetch if expired OR within 60s of expiry (to find next market early)
        cached = self._current_events.get(asset)
        expires = self._event_expires.get(asset, 0)
        last_fetch = self._last_fetch.get(asset, 0)
        time_to_expiry = expires - now

        if cached and time_to_expiry > 60 and (now - last_fetch) < 45:
            # Far from expiry — just update countdown, skip API call entirely
            cached["time_remaining"] = max(0, int(time_to_expiry))
            return cached

        if cached and time_to_expiry > 0 and (now - last_fetch) < 10:
            # Near expiry but fetched recently — use cache
            cached["time_remaining"] = max(0, int(time_to_expiry))
            return cached

        client = await self._client()
        slugs = self._compute_candidate_slugs(asset)

        logger.debug(f"{asset.upper()} 15m: Trying slugs: {slugs[:2]}...")

        best = None
        best_remaining = float("inf")

        for slug in slugs:
            try:
                resp = await client.get(
                    f"{GAMMA_API}/events",
                    params={"slug": slug},
                )
                if resp.status_code != 200:
                    continue

                events = resp.json()
                if not events or not isinstance(events, list):
                    continue

                for ev in events:
                    ev_slug = ev.get("slug", "")
                    if f"{asset}-updown-15m" not in ev_slug.lower():
                        continue

                    markets = ev.get("markets", [])
                    if not markets:
                        continue

                    m = markets[0]
                    end_str = m.get("endDate", "") or ev.get("endDate", "")
                    end_ts = self._parse_end_time(end_str)

                    if not end_ts:
                        end_ts = self._extract_ts_from_slug(ev_slug)
                        if end_ts:
                            end_ts += INTERVAL  # Slug has start time, add 900s for end

                    remaining = end_ts - now if end_ts else -1

                    if 10 < remaining < best_remaining:
                        best_remaining = remaining

                        clob_raw = m.get("clobTokenIds", "[]")
                        tokens = json.loads(clob_raw) if isinstance(clob_raw, str) else (clob_raw or [])
                        outcomes_raw = m.get("outcomes", "[]")
                        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])

                        up_idx, down_idx = 0, 1
                        for i, o in enumerate(outcomes):
                            ol = o.lower() if isinstance(o, str) else ""
                            if ol == "up":
                                up_idx = i
                            elif ol == "down":
                                down_idx = i

                        best = {
                            "id": ev.get("id", ""),
                            "slug": ev_slug,
                            "title": ev.get("title", "") or m.get("question", ""),
                            "asset": asset,
                            "market_id": m.get("id", ""),
                            "condition_id": m.get("conditionId", ""),
                            "up_token": tokens[up_idx] if len(tokens) > up_idx else "",
                            "down_token": tokens[down_idx] if len(tokens) > down_idx else "",
                            "end_time_unix": end_ts,
                            "end_time_str": end_str,
                            "time_remaining": int(best_remaining),
                        }

            except httpx.HTTPError as e:
                logger.debug(f"{asset.upper()} 15m: HTTP error for slug {slug}: {e}")
            except Exception as e:
                logger.debug(f"{asset.upper()} 15m: Error querying slug {slug}: {e}")

        if best:
            self._current_events[asset] = best
            self._event_expires[asset] = best["end_time_unix"]
            self._last_fetch[asset] = now
            logger.info(
                f"{asset.upper()} 15m: Active market = {best['slug']} "
                f"({best['time_remaining']}s remaining)"
            )
        else:
            logger.debug(f"{asset.upper()} 15m: No active market found")

        return best

    async def find_all_active_events(self) -> dict[str, Optional[dict]]:
        """Find active events for ALL configured assets in parallel.

        Returns dict mapping asset -> event (or None).
        """
        tasks = [self.find_active_event(asset) for asset in self._assets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        events = {}
        for asset, result in zip(self._assets, results):
            if isinstance(result, Exception):
                logger.debug(f"{asset.upper()} 15m: Error finding event: {result}")
                events[asset] = None
            else:
                events[asset] = result

        return events

    # ------------------------------------------------------------------
    # Read CLOB order book for real-time prices
    # ------------------------------------------------------------------

    async def get_live_prices(self, event: dict) -> Optional[dict]:
        """Fetch real-time order book prices from the CLOB API.

        This is asset-agnostic — works on token IDs from any asset's event.

        Returns dict with: up_ask, down_ask, up_bid, down_bid,
                          up_depth, down_depth, total_cost, gap_pct,
                          net_profit_pct, tradeable
        """
        up_token = event.get("up_token", "")
        down_token = event.get("down_token", "")
        if not up_token or not down_token:
            return None

        client = await self._client()

        try:
            up_resp, down_resp = await asyncio.gather(
                client.get(f"{CLOB_API}/book", params={"token_id": up_token}),
                client.get(f"{CLOB_API}/book", params={"token_id": down_token}),
            )

            if up_resp.status_code != 200 or down_resp.status_code != 200:
                return None

            up_book = up_resp.json()
            down_book = down_resp.json()

            up_asks = up_book.get("asks", [])
            down_asks = down_book.get("asks", [])
            up_bids = up_book.get("bids", [])
            down_bids = down_book.get("bids", [])

            if not up_asks or not down_asks:
                return None

            up_ask = float(up_asks[0]["price"])
            down_ask = float(down_asks[0]["price"])
            up_bid = float(up_bids[0]["price"]) if up_bids else 0.0
            down_bid = float(down_bids[0]["price"]) if down_bids else 0.0
            up_depth = float(up_asks[0]["size"])
            down_depth = float(down_asks[0]["size"])

            total_cost = up_ask + down_ask
            gap_pct = (1.0 - total_cost) * 100
            net_profit_pct = gap_pct - (WINNER_FEE * 100)

            return {
                "up_ask": up_ask,
                "down_ask": down_ask,
                "up_bid": up_bid,
                "down_bid": down_bid,
                "up_depth": up_depth,
                "down_depth": down_depth,
                "total_cost": round(total_cost, 6),
                "gap_pct": round(gap_pct, 3),
                "net_profit_pct": round(net_profit_pct, 3),
                "tradeable": net_profit_pct > 0,
                "timestamp": time.time(),
            }

        except Exception as e:
            logger.warning(f"Order book error: {e}")
            return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_end_time(end_str: str) -> float:
        """Parse ISO date string to unix timestamp."""
        if not end_str:
            return 0
        try:
            end_str = end_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(end_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _extract_ts_from_slug(slug: str) -> float:
        """Extract unix timestamp from slug like btc-updown-15m-1770685200."""
        try:
            parts = slug.split("-")
            ts = int(parts[-1])
            if ts > 1700000000:
                return float(ts)
        except (ValueError, IndexError):
            pass
        return 0


# Backward compatibility alias
BTC15mScanner = Crypto15mScanner
