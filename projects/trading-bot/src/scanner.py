"""
Universal Polymarket Arbitrage Opportunity Scanner.

Scans ALL active events on Polymarket (not just BTC), detects:
  - Binary arbitrage: YES + NO < $1.00
  - NegRisk multi-outcome: sum of all YES prices across outcomes < $1.00

Ranks opportunities by a composite score factoring profitability,
liquidity, time urgency, and opportunity type.

Based on research: 73% of Polymarket arb profits ($29M of $40M) come
from NegRisk multi-outcome events. Polymarket charges a 2% winner fee,
so spreads must exceed ~2.5% to be profitable.
"""

import json
import math
import time
import logging
from typing import Optional
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
WINNER_FEE = 0.02  # 2% fee on winning payout
INTERVAL = 900  # 15 minutes in seconds


SUPPORTED_ASSETS = ["btc", "eth", "sol", "xrp"]


class OpportunityScanner:
    """Scans all Polymarket events for arbitrage opportunities."""

    def __init__(self, min_liquidity: float = 0.0, min_profit_pct: float = -10.0):
        self.min_liquidity = min_liquidity
        self.min_profit_pct = min_profit_pct
        self._http: Optional[httpx.AsyncClient] = None
        self._assets: list[str] = ["btc"]  # Which assets to scan

    def set_assets(self, assets: list[str]):
        """Configure which assets to scan for 15m Up/Down markets."""
        self._assets = [a for a in assets if a in SUPPORTED_ASSETS] or ["btc"]

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=20.0,
                headers={
                    # Prevent Content-Length mismatch errors from Gamma API
                    "Accept-Encoding": "identity",
                    # Gamma API returns 403 without a browser-like User-Agent
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                },
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def scan(self) -> list[dict]:
        """
        Full scan: fetch all active events, detect arb opportunities,
        score and rank them. Returns list sorted by score descending.
        """
        t0 = time.time()
        events = await self._fetch_all_events()
        logger.info(f"Fetched {len(events)} active events in {time.time()-t0:.1f}s")

        opportunities = []

        for event in events:
            try:
                opps = self._analyze_event(event)
                opportunities.extend(opps)
            except Exception as e:
                logger.debug(f"Error analyzing event {event.get('slug', '?')}: {e}")

        # Score and rank
        for opp in opportunities:
            opp["score"] = self._score(opp)

        # Sort by score descending
        opportunities.sort(key=lambda x: x["score"], reverse=True)

        elapsed = time.time() - t0
        profitable = sum(1 for o in opportunities if o["net_profit_pct"] > 0)
        logger.info(
            f"Scanner found {len(opportunities)} opportunities "
            f"({profitable} profitable) in {elapsed:.1f}s"
        )

        return opportunities

    # ------------------------------------------------------------------
    # Event fetching with pagination
    # ------------------------------------------------------------------

    async def _fetch_all_events(self) -> list[dict]:
        """Fetch crypto 15-min Up/Down events from Gamma API for all configured assets.

        Uses computed slug approach — slug_contains does NOT work for
        recurring crypto markets. We compute the exact slug from the
        current timestamp aligned to 900-second boundaries.

        Slug pattern: {asset}-updown-15m-{unix_timestamp}
        Supported assets: btc, eth, sol, xrp
        """
        client = await self._client()
        all_events = []
        seen_ids = set()

        # Compute candidate timestamps for current + nearby windows
        # NOTE: Slug timestamp = market START time (not end time)
        now = time.time()
        current_start = math.floor(now / INTERVAL) * INTERVAL
        candidate_timestamps = [
            current_start,                # Current window
            current_start - INTERVAL,     # Previous window (may still be active)
            current_start + INTERVAL,     # Next window
        ]

        for asset in self._assets:
            for ts in candidate_timestamps:
                slug = f"{asset}-updown-15m-{int(ts)}"
                try:
                    resp = await client.get(
                        f"{GAMMA_API}/events",
                        params={"slug": slug},
                    )
                    if resp.status_code != 200:
                        continue

                    batch = resp.json()
                    if not batch or not isinstance(batch, list):
                        continue

                    for ev in batch:
                        ev_id = ev.get("id", "")
                        ev_slug = ev.get("slug", "").lower()
                        if f"{asset}-updown-15m" in ev_slug and ev_id not in seen_ids:
                            seen_ids.add(ev_id)
                            all_events.append(ev)
                except Exception as e:
                    logger.debug(f"Scanner: Error fetching slug {slug}: {e}")

        return all_events

    # ------------------------------------------------------------------
    # Event analysis
    # ------------------------------------------------------------------

    def _analyze_event(self, event: dict) -> list[dict]:
        """Analyze a single event for arbitrage opportunities."""
        markets = event.get("markets", [])
        if not markets:
            return []

        opportunities = []

        is_neg_risk = event.get("negRisk", False)
        event_title = event.get("title", "")
        event_slug = event.get("slug", "")

        if is_neg_risk and len(markets) >= 2:
            # NegRisk multi-outcome: sum all YES prices
            opp = self._check_negrisk(event, markets)
            if opp:
                opportunities.append(opp)

        # Also check each individual market for binary arb
        for market in markets:
            opp = self._check_binary(market, event_title, event_slug)
            if opp:
                opportunities.append(opp)

        return opportunities

    def _check_binary(self, market: dict, event_title: str, event_slug: str) -> Optional[dict]:
        """Check a single binary market for YES + NO < $1.00 arb."""
        outcome_prices_raw = market.get("outcomePrices", "")
        clob_token_ids_raw = market.get("clobTokenIds", "")

        if not outcome_prices_raw:
            return None

        # Parse stringified JSON
        try:
            prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
            token_ids = json.loads(clob_token_ids_raw) if isinstance(clob_token_ids_raw, str) else (clob_token_ids_raw or [])
        except (json.JSONDecodeError, TypeError):
            return None

        if len(prices) != 2:
            return None  # Not a binary market

        yes_price = float(prices[0])
        no_price = float(prices[1])
        total_cost = yes_price + no_price

        # Always include BTC 15m markets in results (even without arb)
        # so the dashboard shows current prices. Non-arb markets will
        # have tradeable=False and negative net_profit_pct.
        gap_pct = (1 - total_cost) * 100
        net_profit_pct = gap_pct - (WINNER_FEE * 100)
        roi = (net_profit_pct / 100) / total_cost * 100 if total_cost > 0 else 0

        # Get liquidity and time info
        volume = self._safe_float(market.get("volume", 0))
        liquidity = self._safe_float(market.get("liquidityNum", 0))
        end_date = market.get("endDate", "") or market.get("end_date_iso", "")
        time_remaining = self._calc_time_remaining(end_date)

        question = market.get("question", "")
        condition_id = market.get("conditionId", "")

        return {
            "id": condition_id or market.get("id", ""),
            "type": "binary",
            "category": self._detect_category(event_title or question),
            "event_title": event_title or question,
            "event_slug": event_slug,
            "markets": [{
                "question": question,
                "token_yes": token_ids[0] if len(token_ids) > 0 else "",
                "token_no": token_ids[1] if len(token_ids) > 1 else "",
                "yes_price": yes_price,
                "no_price": no_price,
            }],
            "total_cost": round(total_cost, 6),
            "gap_pct": round(gap_pct, 3),
            "net_profit_pct": round(net_profit_pct, 3),
            "roi": round(roi, 3),
            "min_liquidity": round(liquidity, 2),
            "volume": round(volume, 2),
            "time_remaining_seconds": time_remaining,
            "tradeable": net_profit_pct > 0,
            "timestamp": time.time(),
        }

    def _check_negrisk(self, event: dict, markets: list[dict]) -> Optional[dict]:
        """Check NegRisk event: sum all YES outcome prices across markets."""
        total_yes = 0.0
        market_details = []
        min_liq = float("inf")
        total_volume = 0.0

        for m in markets:
            outcome_prices_raw = m.get("outcomePrices", "")
            clob_token_ids_raw = m.get("clobTokenIds", "")

            if not outcome_prices_raw:
                continue

            try:
                prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                token_ids = json.loads(clob_token_ids_raw) if isinstance(clob_token_ids_raw, str) else (clob_token_ids_raw or [])
            except (json.JSONDecodeError, TypeError):
                continue

            if not prices:
                continue

            yes_price = float(prices[0])
            no_price = float(prices[1]) if len(prices) > 1 else (1.0 - yes_price)
            total_yes += yes_price

            liq = self._safe_float(m.get("liquidityNum", 0))
            vol = self._safe_float(m.get("volume", 0))
            if liq > 0:
                min_liq = min(min_liq, liq)
            total_volume += vol

            market_details.append({
                "question": m.get("question", ""),
                "token_yes": token_ids[0] if len(token_ids) > 0 else "",
                "token_no": token_ids[1] if len(token_ids) > 1 else "",
                "yes_price": round(yes_price, 4),
                "no_price": round(no_price, 4),
            })

        if not market_details or len(market_details) < 2:
            return None

        total_cost = total_yes

        if total_cost >= 1.0:
            return None  # No arb

        gap_pct = (1 - total_cost) * 100
        net_profit_pct = gap_pct - (WINNER_FEE * 100)
        roi = (net_profit_pct / 100) / total_cost * 100 if total_cost > 0 else 0

        if min_liq == float("inf"):
            min_liq = 0.0

        # Use first market's end date as proxy
        end_date = markets[0].get("endDate", "") or markets[0].get("end_date_iso", "")
        time_remaining = self._calc_time_remaining(end_date)

        event_title = event.get("title", "")
        event_slug = event.get("slug", "")

        return {
            "id": event.get("id", event_slug),
            "type": "negrisk",
            "category": self._detect_category(event_title),
            "event_title": event_title,
            "event_slug": event_slug,
            "num_outcomes": len(market_details),
            "markets": market_details,
            "total_cost": round(total_cost, 6),
            "gap_pct": round(gap_pct, 3),
            "net_profit_pct": round(net_profit_pct, 3),
            "roi": round(roi, 3),
            "min_liquidity": round(min_liq, 2),
            "volume": round(total_volume, 2),
            "time_remaining_seconds": time_remaining,
            "tradeable": net_profit_pct > 0,
            "timestamp": time.time(),
        }

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _score(self, opp: dict) -> float:
        """
        Composite score for ranking opportunities.
        Higher = better opportunity.
        Prioritizes: BTC 15-min events → expiry urgency → profitability → liquidity.
        """
        score = 0.0
        time_remaining = opp.get("time_remaining_seconds", -1)
        title_lower = opp.get("event_title", "").lower()
        slug_lower = opp.get("event_slug", "").lower()
        net_profit = opp.get("net_profit_pct", 0)

        is_btc = any(kw in title_lower or kw in slug_lower for kw in
                      ["btc", "bitcoin", "btcusdt", "btc-usd"])
        is_crypto = any(kw in slug_lower for kw in
                        ["btc-updown", "eth-updown", "sol-updown", "xrp-updown"])
        is_15min = any(kw in title_lower or kw in slug_lower for kw in
                        ["15m", "15-min", "15 min", "15min"])

        # CRYPTO 15-MIN MEGA BOOST — primary targets
        if is_crypto and is_15min:
            score += 500
        elif is_btc and is_15min:
            score += 500
        elif is_btc:
            score += 100
        elif is_15min:
            score += 80

        # TIME URGENCY — near-expiry is most actionable
        if 0 < time_remaining <= 900:          # ≤15 minutes
            score += 200
        elif 0 < time_remaining <= 3600:       # ≤1 hour
            score += 150
        elif 0 < time_remaining <= 86400:      # ≤1 day
            score += 100
        elif 0 < time_remaining <= 604800:     # ≤1 week
            score += 50
        else:
            score += 10

        # PROFITABILITY — scale aggressively
        score += net_profit * 20

        # LIQUIDITY (capped at 15 points)
        score += min(opp.get("min_liquidity", 0) / 100, 15)

        # VOLUME bonus (active markets are safer)
        volume = opp.get("volume", 0)
        if volume > 10000:
            score += 8
        elif volume > 1000:
            score += 3

        # Multi-outcome bonus (historically higher EV)
        if opp.get("type") == "negrisk":
            score += 10

        # Penalty for unprofitable after fees
        if net_profit < 0:
            score -= 25

        return round(score, 2)

    # ------------------------------------------------------------------
    # Category detection
    # ------------------------------------------------------------------

    CATEGORY_PATTERNS = {
        "crypto": ["btc", "bitcoin", "ethereum", "eth", "crypto", "solana", "sol ", "defi", "token", "coin", "updown", "up-down", "15m"],
        "politics": ["election", "president", "senate", "congress", "governor", "political", "vote", "democrat", "republican", "trump", "biden", "party"],
        "sports": ["nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball", "baseball", "hockey", "ufc", "boxing", "tennis", "golf", "race", "champion", "league", "match", "super bowl"],
        "finance": ["stock", "s&p", "nasdaq", "fed ", "interest rate", "gdp", "inflation", "recession", "market cap", "earnings"],
        "entertainment": ["oscar", "emmy", "grammy", "movie", "film", "album", "song", "celebrity", "award"],
        "world": ["war", "conflict", "treaty", "sanction", "un ", "nato", "summit", "diplomacy", "country"],
        "tech": ["ai ", "artificial intelligence", "openai", "google", "apple", "microsoft", "spacex", "launch"],
        "weather": ["hurricane", "earthquake", "weather", "temperature", "climate"],
    }

    @staticmethod
    def _detect_category(title: str) -> str:
        """Detect category from event title using keyword matching."""
        title_lower = title.lower()
        for category, keywords in OpportunityScanner.CATEGORY_PATTERNS.items():
            for kw in keywords:
                if kw in title_lower:
                    return category
        return "other"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_float(val) -> float:
        try:
            return float(val) if val else 0.0
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _calc_time_remaining(end_date_str: str) -> int:
        """Parse ISO date and return seconds until expiry. Returns -1 if unparseable."""
        if not end_date_str:
            return -1
        try:
            # Handle multiple ISO format variations
            end_date_str = end_date_str.replace("Z", "+00:00")
            end_dt = datetime.fromisoformat(end_date_str)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            remaining = (end_dt - now).total_seconds()
            return max(0, int(remaining))
        except (ValueError, TypeError):
            return -1
