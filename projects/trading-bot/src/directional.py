"""
Directional betting engine for Polymarket crypto Up/Down binary markets.

EDGE THESIS (from oracle latency arbitrage concept):
  Polymarket 5m/15m BTC markets resolve based on whether BTC price goes
  up or down during the window. The order book reprices with LAG relative
  to actual BTC spot movement. If we detect BTC momentum faster than the
  market maker reprices the Up/Down tokens, we have a latency edge.

SIGNALS (multi-timeframe):
  1. Micro momentum   — last 30s of BTC ticks (fastest, noisiest)
  2. Short momentum   — last 2-3 min (primary signal)
  3. Medium momentum  — last 5-10 min (trend confirmation)
  4. Book imbalance   — bid depth asymmetry on Polymarket CLOB
  5. Book reprice lag  — delta between BTC move and book mid shift
  6. Market price skew — implied probability from Up/Down mid prices

POSITION SIZING: Kelly Criterion
  f* = (p * b - q) / b
  Capped at fractional Kelly for safety. Adaptive based on track record.

RISK MANAGEMENT:
  - Session stop-loss: halt all betting if drawdown exceeds threshold
  - Per-bet max: never more than X% of bankroll
  - Cool-down after consecutive losses
  - Time gate: don't bet in first/last N seconds of window
"""

import math
import time
import logging
from dataclasses import dataclass, field
from typing import Optional
import collections
from collections import deque

logger = logging.getLogger(__name__)

# Polymarket Fee Structure (as of Feb 2026):
# - Settlement/winning payout: NO FEE ($1.00 per winning share, zero cut)
# - Taker fee: dynamic = C × p × 0.25 × (p × (1-p))^2
#   At p=0.50: ~1.56%, at p=0.30: ~0.66%, at p=0.90: ~0.20%, at p=0.99: ~0.002%
# - Maker fee: 0% (makers get 20% rebate from taker fees)
# - No deposit/withdrawal fees
WINNER_FEE = 0.0    # NO fee on settlement/winning payouts!
MAKER_FEE = 0.0     # Maker orders pay no fees on Polymarket (treat rebate as 0 until verified)
SELL_FRICTION = 0.005  # ~0.5% actual friction (taker fee at typical sell prices + slippage)

# ── v2 Fee Model ──
# Polymarket taker fee: feeRate * (p * (1-p))^exponent
# For crypto 5m/15m: feeRate=0.25 (25 bps), exponent=2
# Peak fee at p=0.50: 0.25 * 0.25^2 = 1.5625%
# BUY fees charged in shares: cost_per_share = ask / (1 - fr)
# SELL fees charged in USDC: net_proceeds = bid - fee_usdc
FEE_MIN_USDC = 0.0001  # Minimum fee per Polymarket docs

# Fee params for EV math.
# Polymarket fee formula (from docs):
#   fee_per_share = feeRate × (price × (1 - price))^exponent
#
# Market-type parameters (from Polymarket docs 2026-02):
#   Crypto 5m/15m:  feeRate=0.25,   exponent=2  → max 1.56% at p=0.50
#   Sports:         feeRate=0.0175, exponent=1  → max 0.44% at p=0.50
#
# NOTE: The /fee-rate API endpoint now returns {"base_fee": 1000} which is the
# signed-order fee consent cap (used by py_clob_client for order signing).
# This is NOT the feeRate used in the fee formula. The old format returned
# {"feeRateBps": 25, "exponent": 2} but that's been replaced.
# We hardcode the documented crypto market params.
_fee_params = {"fee_rate": 0.25, "exponent": 2}


def _fetch_fee_params(poly_client, token_id: str) -> None:
    """Fetch base_fee from Polymarket fee-rate endpoint for informational logging.

    The actual fee formula params (feeRate, exponent) are market-type-specific
    and documented by Polymarket. We hardcode them above. This function
    only logs the base_fee (order signing consent cap) for diagnostics.
    """
    try:
        resp = poly_client._session.get(
            "https://clob.polymarket.com/fee-rate",
            params={"token_id": token_id},
            timeout=5,
        )
        if resp.ok:
            data = resp.json()
            base_fee = data.get("base_fee", 0)
            # Log for diagnostics — base_fee is the order signing cap, not fee rate
            if base_fee:
                logger.info(f"Fee endpoint: base_fee={base_fee} (order cap) | "
                            f"Using fee model: rate={_fee_params['fee_rate']} "
                            f"exp={_fee_params['exponent']}")
    except Exception as e:
        logger.debug(f"Fee param fetch failed (using hardcoded): {e}")


def _fee_rate(price: float) -> float:
    """Taker fee as fraction of trade value per share.

    Formula: feeRate × (price × (1 - price))^exponent
    Crypto 5m: 0.25 × (p × (1-p))^2
    Max at p=0.50: 0.25 × 0.0625 = 1.5625%
    """
    fr = _fee_params["fee_rate"]  # 0.25 for crypto markets
    exp = _fee_params["exponent"]  # 2 for crypto markets
    return fr * (price * (1.0 - price)) ** exp


def _fee_usdc(trade_value: float, price: float) -> float:
    """Actual fee in USDC with rounding + minimum rule."""
    raw = trade_value * _fee_rate(price)
    rounded = round(raw, 4)
    return max(FEE_MIN_USDC, rounded) if rounded > 0 else 0.0


def _buy_cost_per_share(ask: float) -> float:
    """Effective cost per received share (BUY fees charged in shares).

    You pay ask*C USDC, receive C*(1-fr) shares → cost = ask/(1-fr).
    Applies Polymarket's rounding (4 decimals) + minimum fee rule.
    """
    fr = _fee_rate(ask)
    fee_per_unit = round(ask * fr, 4)
    fee_per_unit = max(FEE_MIN_USDC, fee_per_unit) if fee_per_unit > 0 else 0.0
    effective_fr = fee_per_unit / max(1e-6, ask)
    return ask / max(1e-6, 1.0 - effective_fr)


def _sell_proceeds_per_share(bid: float) -> float:
    """Net USDC received per share sold (SELL fees charged in USDC)."""
    fr = _fee_rate(bid)
    fee_per_unit = round(bid * fr, 4)
    fee_per_unit = max(FEE_MIN_USDC, fee_per_unit) if fee_per_unit > 0 else 0.0
    return bid - fee_per_unit


def _norm_cdf(x: float) -> float:
    """Standard normal CDF: Phi(x) = 0.5 * (1 + erf(x / sqrt(2)))"""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@dataclass
class DirectionalSignal:
    """A directional bet recommendation."""
    side: str               # "UP" or "DOWN"
    confidence: float       # 0.0 - 1.0 (estimated win probability)
    kelly_fraction: float   # Fraction of bankroll to bet (0.0 - 1.0)
    bet_size: float         # Dollar amount to bet
    expected_value: float   # Expected profit/loss
    signals: dict           # Individual signal components
    reason: str             # Human-readable explanation
    edge_type: str = ""     # "latency", "momentum", "book", "composite"
    # v2 conviction engine fields
    conviction: float = 0.0       # 0.0-1.0 multi-channel conviction score
    approach: str = ""            # AGGRESSIVE / HEDGED / CONSERVATIVE / SKIP
    max_adds: int = 0             # max add-on trades allowed this window
    dominant_pct: float = 1.0     # fraction of capital on dominant side
    edge: float = 0.0             # model edge carried into runtime gates
    regime: str = ""              # TREND / CHOP classification for runtime gates
    p_smooth: float = 0.5         # model win probability for the chosen side
    q_mid: float = 0.5            # market-implied probability midpoint


@dataclass
class PricePoint:
    """A single BTC price observation."""
    price: float
    timestamp: float


@dataclass
class BookSnapshot:
    """A snapshot of Polymarket book state for lag detection."""
    up_mid: float
    down_mid: float
    timestamp: float


class DirectionalEngine:
    """Picks a side and sizes bets using Kelly Criterion.

    Implements oracle latency arbitrage: detects BTC spot movement
    before Polymarket order books fully reprice.
    """

    def __init__(
        self,
        bankroll: float = 10.0,
        mode: str = "composite",
        kelly_fraction_cap: float = 0.25,
        min_confidence: float = 0.52,
        max_bet_pct: float = 0.25,
        max_bet_dollars: float = 2.00,          # HARD CAP per window
        lookback_minutes: float = 5.0,
        # Risk management
        session_stop_loss_pct: float = 0.40,   # Stop if bankroll drops 40% from peak
        max_consecutive_losses: int = 5,        # Cool-down after 5 losses
        cool_down_seconds: float = 120.0,       # 2 min cool-down
        min_time_to_bet: int = 15,              # Don't bet in last 15s (tighter for speed)
        max_time_to_wait: int = 20,             # Wait 20s into window for book to form (was 60)
    ):
        self.bankroll = bankroll
        self.initial_bankroll = bankroll
        self.mode = mode
        self.kelly_fraction_cap = kelly_fraction_cap
        self.min_confidence = min_confidence
        self.max_bet_pct = max_bet_pct
        self.max_bet_dollars = max_bet_dollars
        self.lookback_minutes = lookback_minutes

        # Risk management
        self.session_stop_loss_pct = session_stop_loss_pct
        self.max_consecutive_losses = max_consecutive_losses
        self.cool_down_seconds = cool_down_seconds
        self.min_time_to_bet = min_time_to_bet
        self.max_time_to_wait = max_time_to_wait

        # BTC price history (high-frequency from WebSocket)
        self._btc_prices: deque[PricePoint] = deque(maxlen=2000)

        # Book snapshots for lag detection
        self._book_history: deque[BookSnapshot] = deque(maxlen=200)

        # Track results for adaptive confidence
        self._results: deque[dict] = deque(maxlen=200)
        self._win_count: int = 0
        self._loss_count: int = 0
        self._total_pnl: float = 0.0

        # Risk state
        self._consecutive_losses: int = 0
        self._last_loss_time: float = 0
        self._halted: bool = False
        self._halt_reason: str = ""
        self._peak_bankroll: float = bankroll  # track peak for drawdown calc

        # EV exit evaluation (last result for logging/dashboard)
        self._last_exit_eval: dict = {}

        # Dashboard state (updated on each get_signal call)
        self._last_signals: dict = {}
        self._last_edge_type: str = ""
        self._last_pipeline: dict = {}  # Full execution pipeline for dashboard (legacy get_signal)
        self._last_conviction_pipeline: dict = {}  # Conviction pipeline (get_conviction_signal) — not overwritten by legacy

        # UP-bias schedule state
        self._up_bias_active: bool = False         # True when inside ±90s of bias target
        self._up_bias_mode: str = ""               # "soft", "hard", or "flip"
        self._up_bias_forced_flip: bool = False    # True when DOWN→UP flip was applied
        self._up_bias_auto_flip: bool = False      # Forced flips stay off unless explicitly re-enabled
        self._up_bias_hard_enabled: bool = False   # Hard bias is opt-in; soft bias is the safe default
        self._up_bias_last_target_secs: int = -1   # Dedup: target_secs for current bias window

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_btc_price(self, price: float, timestamp: float = None):
        """Record a BTC price tick. Call this as fast as possible."""
        self._btc_prices.append(PricePoint(
            price=price,
            timestamp=timestamp or time.time(),
        ))

    def record_book_snapshot(self, up_mid: float, down_mid: float):
        """Record Polymarket book midpoint for lag detection."""
        self._book_history.append(BookSnapshot(
            up_mid=up_mid,
            down_mid=down_mid,
            timestamp=time.time(),
        ))

    def record_result(self, won: bool, pnl: float):
        """Record a trade result."""
        self._results.append({"won": won, "pnl": pnl, "ts": time.time()})
        if won:
            self._win_count += 1
            self._consecutive_losses = 0
        else:
            self._loss_count += 1
            self._consecutive_losses += 1
            self._last_loss_time = time.time()
        self._total_pnl += pnl

        # Track peak bankroll
        if self.bankroll > self._peak_bankroll:
            self._peak_bankroll = self.bankroll
        # Minimum bankroll halt: stop trading if below $0.50
        if self.bankroll < 0.50:
            self._halted = True
            self._halt_reason = f"Bankroll ${self.bankroll:.2f} below $0.50 minimum"

    def get_signal(
        self,
        up_price: float,
        down_price: float,
        up_depth: float = 0,
        down_depth: float = 0,
        time_remaining: int = 0,
        interval_secs: int = 900,
    ) -> Optional[DirectionalSignal]:
        """Generate a directional bet signal.

        Returns DirectionalSignal or None if no bet meets criteria.
        """
        # Risk gates
        if self._halted:
            return None

        if self._consecutive_losses >= self.max_consecutive_losses:
            elapsed = time.time() - self._last_loss_time
            if elapsed < self.cool_down_seconds:
                return None
            self._consecutive_losses = 0  # Reset after cool-down

        if time_remaining < self.min_time_to_bet:
            return None

        time_into_window = interval_secs - time_remaining
        if time_into_window < self.max_time_to_wait:
            return None

        if self.bankroll <= 0.01:
            return None

        # Compute signals
        signals = {}

        # 1. Multi-timeframe momentum
        micro_mom = self._calc_momentum(lookback_secs=30)
        short_mom = self._calc_momentum(lookback_secs=150)
        medium_mom = self._calc_momentum(lookback_secs=self.lookback_minutes * 60)
        signals["micro_momentum"] = micro_mom
        signals["short_momentum"] = short_mom
        signals["medium_momentum"] = medium_mom

        # 2. Acceleration (is momentum increasing?)
        accel = self._calc_acceleration()
        signals["acceleration"] = accel

        # 3. Book imbalance
        book_imb = self._calc_book_imbalance(up_depth, down_depth)
        signals["book_imbalance"] = book_imb

        # 4. Book reprice lag — the latency edge
        reprice_lag = self._calc_reprice_lag(up_price, down_price)
        signals["reprice_lag"] = reprice_lag

        # 5. Market price skew
        market_skew = 0.5 - up_price if up_price > 0 else 0
        signals["market_skew"] = market_skew

        # 6. Time pressure — closer to expiry, short signals dominate
        time_frac = time_remaining / interval_secs if interval_secs > 0 else 0.5
        # In last 25% of window, micro momentum is king
        # In first 50%, medium momentum matters more
        if time_frac < 0.25:
            time_regime = "late"
        elif time_frac < 0.50:
            time_regime = "mid"
        else:
            time_regime = "early"
        signals["time_regime"] = time_regime

        # Composite score based on mode and time regime
        raw_score = self._compute_composite(
            micro_mom, short_mom, medium_mom, accel,
            book_imb, reprice_lag, market_skew, time_regime,
        )

        signals["raw_score"] = round(raw_score, 4)

        # Store for dashboard display
        self._last_signals = {k: v for k, v in signals.items() if isinstance(v, (int, float))}

        # === Probability-based estimation (GBM model) ===
        prob_side, p_estimated, pipeline = self._estimate_probability(
            up_price, down_price, time_remaining, interval_secs
        )

        # Primary: use probability model when it has a clear signal
        if prob_side != "NONE" and abs(p_estimated - 0.5) > 0.02:
            side = prob_side
            confidence = p_estimated
            edge_type = "probability"

            # Secondary: small momentum overlay to incorporate microstructure
            momentum_boost = raw_score * 0.03
            if (side == "UP" and momentum_boost > 0) or (side == "DOWN" and momentum_boost < 0):
                confidence = min(0.95, confidence + abs(momentum_boost))

            pipeline["mom_boost"] = round(momentum_boost, 4)
            pipeline["conf_final"] = round(confidence, 4)
        else:
            # Fallback: momentum composite (when model is uncertain)
            side = "UP" if raw_score > 0 else "DOWN"
            confidence = 0.5 + 0.2 * math.tanh(raw_score * 3)
            edge_type = "composite"
            pipeline["fallback"] = True
            pipeline["conf_final"] = round(confidence, 4)

            # Sub-classify composite edge type
            if abs(reprice_lag) > 0.3:
                edge_type = "latency"
            elif abs(micro_mom) > 0.5 and time_regime == "late":
                edge_type = "momentum"
            elif abs(book_imb) > 0.3:
                edge_type = "book"

        self._last_edge_type = edge_type

        # Adaptive: blend in track record (applies to both modes)
        if len(self._results) >= 10:
            recent = list(self._results)[-30:]
            recent_wr = sum(1 for r in recent if r["won"]) / len(recent)
            blend = min(0.3, len(self._results) / 250)
            confidence = confidence * (1 - blend) + recent_wr * blend

        # Latency edge bonus (applies to both modes)
        if abs(reprice_lag) > 0.4:
            confidence = min(0.90, confidence + 0.04)

        signals["confidence"] = round(confidence, 4)

        # Store pipeline for dashboard
        pipeline["edge_type"] = edge_type
        self._last_pipeline = pipeline

        if confidence < self.min_confidence:
            return None

        # Minimum edge threshold for probability mode
        if edge_type == "probability":
            edge = pipeline.get("edge", 0)
            if edge < 0.03:  # 3% minimum edge (was 1.5% — too loose, lost money)
                return None

        # MOMENTUM AGREEMENT GATE: model side must agree with short-term momentum
        # This prevents betting against the trend (e.g. betting DOWN when BTC is rallying)
        if short_mom != 0:
            mom_agrees = (side == "UP" and short_mom > 0) or (side == "DOWN" and short_mom < 0)
            if not mom_agrees:
                # Model says one thing, momentum says the opposite — skip
                # Exception: very strong edge (>8%) can override momentum disagreement
                edge_val = pipeline.get("edge", 0)
                if edge_val < 0.08:
                    return None

        # Kelly sizing
        buy_price = up_price if side == "UP" else down_price
        if buy_price <= 0 or buy_price >= 1.0:
            return None

        kelly = self._kelly_fraction(confidence, buy_price)
        if kelly <= 0:
            return None

        capped_kelly = min(kelly, self.kelly_fraction_cap)
        max_bet = self.bankroll * self.max_bet_pct
        bet_size = min(self.bankroll * capped_kelly, max_bet, self.max_bet_dollars)

        # Polymarket CLOB minimum order is $1.00.
        # If Kelly sizing lands below $1 but we have enough bankroll,
        # bump up to $1.01 to meet the minimum. Capped by max_bet_dollars.
        CLOB_MIN = 1.01  # Just above $1 to cover rounding
        if bet_size < CLOB_MIN and self.max_bet_dollars >= CLOB_MIN and self.bankroll >= CLOB_MIN:
            bet_size = CLOB_MIN

        if bet_size < 1.0:
            return None  # Can't meet Polymarket $1 minimum

        shares = int(bet_size / buy_price)
        actual_bet = shares * buy_price
        payout_if_win = shares * (1.0 - WINNER_FEE)
        ev = (confidence * payout_if_win) - actual_bet

        mom_dir = "UP" if short_mom > 0 else "DOWN" if short_mom < 0 else "FLAT"
        reason = (
            f"{edge_type.upper()}: mom[30s={micro_mom:+.3f} 2.5m={short_mom:+.3f} "
            f"5m={medium_mom:+.3f}] accel={accel:+.3f} "
            f"book={book_imb:+.3f} lag={reprice_lag:+.3f} "
            f"skew={market_skew:+.3f} regime={time_regime} → "
            f"{side} @ ${buy_price:.3f} x{shares}sh "
            f"(Kelly={kelly:.3f}→{capped_kelly:.3f}) EV=${ev:+.4f}"
        )

        return DirectionalSignal(
            side=side,
            confidence=round(confidence, 4),
            kelly_fraction=round(capped_kelly, 4),
            bet_size=round(actual_bet, 4),
            expected_value=round(ev, 4),
            signals=signals,
            reason=reason,
            edge_type=edge_type,
        )

    def get_stats(self) -> dict:
        """Get performance statistics."""
        total = self._win_count + self._loss_count
        peak = getattr(self, '_peak_bankroll', self.initial_bankroll)
        drawdown = (peak - self.bankroll) / peak if peak > 0 else 0
        return {
            "total_trades": total,
            "wins": self._win_count,
            "losses": self._loss_count,
            "win_rate": round(self._win_count / total, 4) if total > 0 else 0,
            "total_pnl": round(self._total_pnl, 4),
            "bankroll": round(self.bankroll, 4),
            "initial_bankroll": round(self.initial_bankroll, 4),
            "drawdown_pct": round(drawdown * 100, 2),
            "consecutive_losses": self._consecutive_losses,
            "halted": self._halted,
            "halt_reason": self._halt_reason,
            "mode": self.mode,
            "kelly_cap": self.kelly_fraction_cap,
            "min_confidence": self.min_confidence,
            "max_bet_dollars": self.max_bet_dollars,
            "btc_ticks": len(self._btc_prices),
            "last_edge_type": self._last_edge_type,
            "pipeline": self._last_pipeline,
            "conviction_pipeline": self._last_conviction_pipeline,
        }

    def should_exit_position(
        self,
        bet_side: str,
        entry_price: float,
        current_price: float,
        up_price: float,
        down_price: float,
        time_remaining: int,
        interval_secs: int,
        peak_profit_pct: float = 0.0,
        price_is_direct: bool = False,
        best_bid: float = 0.0,
    ) -> tuple[bool, str]:
        """HFT SCALPING EXIT — zero fees, fast in/out, model-driven.

        PHILOSOPHY: Like the top trader — scrape small profits FAST and repeat.
        - Model drives direction every second
        - Take 3% profit IMMEDIATELY (zero fees = every penny real)
        - Cut losses at -8% (free capital for next window)
        - Never hold through a model reversal
        - ALL profit calcs use best_bid (what we ACTUALLY get)
        - No bid = can't sell = hold to settlement
        """
        if current_price <= 0 or entry_price <= 0:
            return False, ""

        has_bid = best_bid >= 0.01
        # Profit is ONLY real if there's a bid — that's what we sell at
        if has_bid:
            profit_pct = (best_bid - entry_price) / entry_price
        else:
            # No bid — can't sell. Use mid for logging only.
            profit_pct = (current_price - entry_price) / entry_price

        # ── Compute EV (for logging) ──
        hold_ev, sell_ev, p_win_adj, detail = self._hold_vs_sell_ev(
            bet_side, entry_price, current_price,
            up_price, down_price, time_remaining, interval_secs,
            best_bid=best_bid,
        )
        detail["profit_pct"] = round(profit_pct, 3)
        detail["peak_profit_pct"] = round(peak_profit_pct, 3)
        detail["price_is_direct"] = price_is_direct
        detail["best_bid"] = best_bid
        self._last_exit_eval = detail

        if not price_is_direct:
            return False, ""

        t_elapsed = 1.0 - (time_remaining / max(interval_secs, 1))  # 0→1

        # No bid = can't sell on CLOB. Hold to settlement.
        if not has_bid:
            return False, ""

        # ══════════════════════════════════════════════════════════════
        # 1. TAKE PROFIT — scalp 3%+ and GET OUT. Zero fees = real money.
        # ══════════════════════════════════════════════════════════════
        if profit_pct >= 0.03:
            return True, (f"📈 take-profit: +{profit_pct:.0%} "
                          f"bid=${best_bid:.2f} — scalping gain")

        # ══════════════════════════════════════════════════════════════
        # 2. CUT LOSS — tight stop at -8%, free capital for next window
        # ══════════════════════════════════════════════════════════════
        if profit_pct <= -0.08:
            return True, (f"✂️ cut-loss: {profit_pct:+.0%} bid=${best_bid:.2f} "
                          f"— cutting at -8%")

        # ══════════════════════════════════════════════════════════════
        # 3. PEAK FADE — peaked 8%+ and gave back most gains
        # ══════════════════════════════════════════════════════════════
        if peak_profit_pct >= 0.08 and profit_pct < peak_profit_pct * 0.30:
            return True, (f"📉 peak-fade: peaked +{peak_profit_pct:.0%} now {profit_pct:+.0%} "
                          f"bid=${best_bid:.2f} — locking remainder")

        # ══════════════════════════════════════════════════════════════
        # 4. MODEL FLIP — model reversed, exit NOW
        # ══════════════════════════════════════════════════════════════
        if p_win_adj < 0.40:
            if profit_pct < 0:
                return True, (f"🔄 flip: p_win={p_win_adj:.0%} at {profit_pct:+.0%} "
                              f"— model reversed, cutting")
            elif profit_pct > 0.005:
                return True, (f"🔄 flip+profit: p_win={p_win_adj:.0%} at +{profit_pct:.0%} "
                              f"— model reversed, taking profit")

        # ══════════════════════════════════════════════════════════════
        # 5. NEAR EXPIRY — take ANY profit in final 60s
        # ══════════════════════════════════════════════════════════════
        if time_remaining < 60 and profit_pct > 0.005:
            return True, (f"⏰ expiry: +{profit_pct:.0%} bid=${best_bid:.2f} "
                          f"{time_remaining}s left")

        # ══════════════════════════════════════════════════════════════
        # 6. MEGA — near max payout
        # ══════════════════════════════════════════════════════════════
        if best_bid >= 0.85:
            return True, (f"💰 mega: bid=${best_bid:.2f} "
                          f"(+{profit_pct:.0%}) — near max")

        # ══════════════════════════════════════════════════════════════
        # 7. HOLD — model agrees, building position
        # ══════════════════════════════════════════════════════════════
        return False, ""

    def _hold_vs_sell_ev(
        self,
        bet_side: str,
        entry_price: float,
        current_price: float,
        up_price: float,
        down_price: float,
        time_remaining: int,
        interval_secs: int,
        best_bid: float = 0.0,
    ) -> tuple[float, float, float, dict]:
        """Compare expected value of HOLDING to settlement vs SELLING on CLOB.

        Returns (hold_ev, sell_ev, p_win_adj, detail_dict) per share.

        sell_ev  = best_bid (what you ACTUALLY get on CLOB — not theoretical mid)
        hold_ev  = p_win × $1.00 (settlement has ZERO fees on Polymarket)

        KEY CHANGE: sell_ev uses REAL best_bid, not mid price.
        This prevents trying to sell at mid=$0.85 when best bid=$0.01.

        When hold_ev > sell_ev → HOLD (settlement pays more than CLOB sell)
        When sell_ev > hold_ev → SELL (rare: CLOB bid exceeds settlement EV)
        """
        detail: dict = {}

        # ── Sell EV: what we ACTUALLY receive on CLOB ──
        # Use real best_bid — this is what a market sell would fill at
        # If no bid available, sell_ev = 0 (can't sell)
        if best_bid > 0.05:
            sell_ev = best_bid  # actual fill price (already accounts for spread)
        else:
            sell_ev = 0.0  # no liquidity — can't sell
        detail["sell_ev"] = round(sell_ev, 4)
        detail["best_bid"] = round(best_bid, 4)

        # ── Model-based probability of winning at expiry ──
        p_win_model = 0.5  # default fallback
        sigma = 0.0
        T = float(max(time_remaining, 1))

        if len(self._btc_prices) >= 10:
            prob_side, p_est, pipeline = self._estimate_probability(
                up_price, down_price, time_remaining, interval_secs,
            )
            sigma = pipeline.get("cex_sigma", 0.0)
            # Map model probability to our bet side
            if bet_side == "UP":
                p_win_model = pipeline.get("p_up_model", p_est if prob_side == "UP" else 1 - p_est)
            else:
                p_win_model = pipeline.get("p_down_model", p_est if prob_side == "DOWN" else 1 - p_est)
            detail["sigma"] = sigma
            detail["p_win_model"] = round(p_win_model, 4)

        # ── Confidence shrinkage — blend model with market-implied ──
        # For EXIT decisions, the market price is the dominant signal.
        tick_count = len(self._btc_prices)
        confidence = min(0.10, tick_count / 300.0)
        if time_remaining < 60:
            confidence *= 0.5
        detail["confidence"] = round(confidence, 3)

        # Market-implied probability = current token price
        p_market = current_price
        p_win_adj = confidence * p_win_model + (1.0 - confidence) * p_market
        p_win_adj = max(0.01, min(0.99, p_win_adj))
        detail["p_win_adj"] = round(p_win_adj, 4)

        # ── Hold EV: expected settlement payout ──
        # Win: $1.00 per share (NO FEE on Polymarket settlement!)
        # Lose: $0.00 per share
        # Expected: p_win × $1.00
        hold_ev = p_win_adj * (1.0 - WINNER_FEE)  # WINNER_FEE = 0.0
        detail["hold_ev_base"] = round(hold_ev, 4)

        # ── Time optionality premium ──
        # More time = more chance of recovery → slight hold bonus
        time_bonus = 0.0
        if time_remaining > 120 and sigma > 0:
            time_bonus = min(sigma * math.sqrt(T) * 0.10, 0.05)
        elif time_remaining > 45 and sigma > 0:
            time_bonus = min(sigma * math.sqrt(T) * 0.05, 0.02)
        hold_ev += time_bonus
        detail["time_bonus"] = round(time_bonus, 5)
        detail["hold_ev"] = round(hold_ev, 4)

        return hold_ev, sell_ev, p_win_adj, detail

    def reset_halt(self):
        """Manually reset halt state (e.g., after adding funds)."""
        self._halted = False
        self._halt_reason = ""
        self._peak_bankroll = self.bankroll  # reset peak to current
        self._halt_reason = ""
        self._consecutive_losses = 0

    # ------------------------------------------------------------------
    # Signal calculations
    # ------------------------------------------------------------------

    def _calc_momentum(self, lookback_secs: float = None) -> float:
        """BTC price momentum over a specific lookback.

        Returns normalized value roughly in [-1, 1].
        """
        if len(self._btc_prices) < 2:
            return 0.0

        if lookback_secs is None:
            lookback_secs = self.lookback_minutes * 60

        now = time.time()
        cutoff = now - lookback_secs

        recent = [p for p in self._btc_prices if p.timestamp >= cutoff]
        if len(recent) < 2:
            recent = [self._btc_prices[-2], self._btc_prices[-1]]

        first_price = recent[0].price
        last_price = recent[-1].price

        if first_price <= 0:
            return 0.0

        pct_change = (last_price - first_price) / first_price

        # Trend consistency
        ups = sum(1 for i in range(1, len(recent)) if recent[i].price > recent[i-1].price)
        consistency = (ups / (len(recent) - 1)) - 0.5 if len(recent) > 1 else 0

        # Blend magnitude and consistency
        magnitude = math.tanh(pct_change * 500)
        signal = magnitude * 0.65 + consistency * 0.35

        return round(signal, 4)

    def _calc_acceleration(self) -> float:
        """Is momentum increasing or decreasing?

        Compares momentum in last 30s vs prior 30s.
        Positive = momentum accelerating, negative = decelerating.
        """
        if len(self._btc_prices) < 10:
            return 0.0

        now = time.time()

        # Recent 30s momentum
        recent = [p for p in self._btc_prices if p.timestamp >= now - 30]
        # Prior 30s momentum
        prior = [p for p in self._btc_prices if now - 60 <= p.timestamp < now - 30]

        if len(recent) < 2 or len(prior) < 2:
            return 0.0

        recent_change = (recent[-1].price - recent[0].price) / recent[0].price if recent[0].price > 0 else 0
        prior_change = (prior[-1].price - prior[0].price) / prior[0].price if prior[0].price > 0 else 0

        accel = math.tanh((recent_change - prior_change) * 1000)
        return round(accel, 4)

    def _calc_book_imbalance(self, up_depth: float, down_depth: float) -> float:
        """Order book depth imbalance. Positive = bullish pressure."""
        total = up_depth + down_depth
        if total <= 0:
            return 0.0
        return round((up_depth - down_depth) / total, 4)

    def _calc_reprice_lag(self, current_up_mid: float, current_down_mid: float) -> float:
        """Detect lag between BTC spot move and Polymarket book reprice.

        This is the LATENCY EDGE. If BTC just moved up but the Polymarket
        book still shows equal up/down pricing, the book hasn't repriced yet.

        Returns:
          positive = BTC moved up but book hasn't repriced up yet (buy UP)
          negative = BTC moved down but book hasn't repriced down yet (buy DOWN)
        """
        if len(self._btc_prices) < 5:
            return 0.0

        # What BTC did in last 60 seconds
        btc_mom = self._calc_momentum(lookback_secs=60)

        # What the book implies (up_mid > 0.5 means market expects UP)
        book_bias = current_up_mid - 0.5 if current_up_mid > 0 else 0

        # Lag = BTC moved but book didn't follow
        # If btc_mom is positive (BTC up) but book_bias is near zero or negative,
        # the book hasn't repriced yet → we should buy UP
        lag = btc_mom - book_bias * 2  # Scale book bias to comparable range

        return round(lag, 4)

    # ------------------------------------------------------------------
    # Probability Engine (v2) — market-prior logit model
    # ------------------------------------------------------------------

    def _calc_probability(
        self,
        time_into: float,
        time_remaining: float,
        up_price: float,
        down_price: float,
        up_depth: float,
        down_depth: float,
        up_spread: float = 0.0,
        down_spread: float = 0.0,
        up_best_bid: float = 0.0,
        down_best_bid: float = 0.0,
        up_best_ask: float = 1.0,
        down_best_ask: float = 1.0,
    ) -> dict:
        """Market-prior logit model for P(UP wins).

        logit(p) = logit(q_mid) + BETA*z + micro_delta
        where q_mid = (bid+ask)/2 is market's implied probability.

        Conviction = edge vs market (not distance from 0.50).
        """
        interval = time_into + time_remaining

        # --- MARKET PRIOR from bid/ask mid ---
        if up_best_bid > 0 and up_best_ask < 1.0:
            q_mid = (up_best_bid + up_best_ask) / 2.0
        else:
            q_mid = up_price
        q_mid = max(0.02, min(0.98, q_mid))

        # --- LAYER A: BTC momentum → z-score ---
        ret_5 = self._calc_momentum(lookback_secs=5)
        ret_15 = self._calc_momentum(lookback_secs=15)
        ret_30 = self._calc_momentum(lookback_secs=30)
        ret_60 = self._calc_momentum(lookback_secs=60)
        vol = self._calc_realized_volatility()

        t_frac = time_into / interval if interval > 0 else 0
        if t_frac < 0.33:
            w5, w15, w30, w60 = 0.10, 0.20, 0.30, 0.40
        elif t_frac < 0.66:
            w5, w15, w30, w60 = 0.20, 0.30, 0.30, 0.20
        else:
            w5, w15, w30, w60 = 0.35, 0.30, 0.25, 0.10

        blended_ret = ret_5 * w5 + ret_15 * w15 + ret_30 * w30 + ret_60 * w60
        vol_scale = 1.0 / max(vol, 0.0005) * 0.001
        z = blended_ret * vol_scale

        # --- LAYER B: Microstructure → small logit shift ---
        book_imb = self._calc_book_imbalance(up_depth, down_depth)
        lag = self._calc_reprice_lag(up_price, down_price)
        mom_aligned = (ret_15 > 0 and book_imb > 0) or (ret_15 < 0 and book_imb < 0)
        pressure = abs(book_imb) * (1.5 if mom_aligned else 0.5)

        # Dampen microstructure in choppy conditions (~25% reduction)
        # CHOP proxy: low z-score + low momentum alignment → noisy book signal
        _chop_damp = 0.75 if (abs(z) < 0.05 and not mom_aligned) else 1.0
        micro_logit_delta = (
            book_imb * 0.10 * _chop_damp +
            pressure * 0.06 * _chop_damp +
            lag * 0.06
        )

        # --- COMBINE: logit(p) = logit(q_mid) + BETA*z + micro ---
        BETA = 2.5  # calibrate via scripts/calibrate_beta.py
        logit_q = math.log(q_mid / (1.0 - q_mid))
        total_shift = BETA * z + micro_logit_delta

        # --- DIVERGENCE CLAMPS ---
        max_shift = 0.35 + 0.25 * t_frac  # 0.35 early → 0.60 late
        total_shift = max(-max_shift, min(max_shift, total_shift))

        logit_p = logit_q + total_shift
        p_raw = 1.0 / (1.0 + math.exp(-logit_p))
        p_raw = max(0.01, min(0.99, p_raw))

        # Cap probability divergence from market
        # Widened from 0.06-0.10 → 0.08-0.14 to let the model express
        # signal vs market. Old clamp was too tight — p_smooth (after EMA)
        # couldn't exceed ask + fees, blocking 97% of entries at EV gate.
        max_div = 0.08 + 0.06 * t_frac  # 0.08 early → 0.14 late
        if abs(p_raw - q_mid) > max_div:
            p_raw = q_mid + (1.0 if p_raw > q_mid else -1.0) * max_div

        # Track history
        if not hasattr(self, '_p_raw_history'):
            self._p_raw_history = collections.deque(maxlen=60)
        self._p_raw_history.append(p_raw)

        # --- CONFIDENCE WEIGHT ---
        spread_penalty = min(1.0, max(up_spread, down_spread) / 0.10)
        depth_penalty = 1.0 - min(1.0, (up_depth + down_depth) / 10000)
        flip_penalty = self._calc_flip_rate()
        w = max(0.05, 1.0 - 0.30 * spread_penalty - 0.25 * depth_penalty - 0.30 * flip_penalty)

        # P2: Complement check — up_mid + down_mid should ≈ 1.0 for binary market
        # Halve confidence weight when books are stale/corrupt
        if down_best_bid > 0 and down_best_ask < 1.0:
            actual_dn_mid = (down_best_bid + down_best_ask) / 2
        else:
            actual_dn_mid = down_price
        complement_dev = abs(q_mid + actual_dn_mid - 1.0)
        if complement_dev > 0.12:
            w *= 0.50

        # --- STRENGTH FROM EDGE vs MARKET (not distance from 0.50) ---
        edge = abs(p_raw - q_mid)
        strength = min(1.0, edge / max_div) if max_div > 0 else 0.0
        conviction = strength * w

        # --- SIGNAL CONSISTENCY BOOST ---
        # When z-score and book imbalance agree (both pointing same direction),
        # boost conviction up to 25% — signals are confirming each other.
        if (z > 0.05 and book_imb > 0.05) or (z < -0.05 and book_imb < -0.05):
            consistency_boost = min(0.25, abs(z) * abs(book_imb) * 5.0)
            conviction = min(1.0, conviction * (1.0 + consistency_boost))
        # Penalty when signals contradict (momentum up but books lean down)
        elif (z > 0.08 and book_imb < -0.05) or (z < -0.08 and book_imb > 0.05):
            conviction *= 0.85

        # --- EMA SMOOTHING (before side selection so both use same p) ---
        # Base alpha ramps with time; final 60s gets aggressive ramp toward market.
        # Raised from 0.15+0.20t → 0.20+0.20t so p_smooth responds faster
        # to momentum. Old alpha was so low that p_smooth lagged behind ask,
        # causing systematic negative EV at the runner's entry gate.
        if t_frac > 0.80:
            # Last ~60s: lean hard into market tape (alpha up to 0.70)
            alpha = 0.35 + 0.35 * ((t_frac - 0.80) / 0.20)
        else:
            alpha = 0.20 + 0.20 * t_frac
        prev = getattr(self, '_p_smooth', 0.50)
        p_smooth = alpha * p_raw + (1 - alpha) * prev
        self._p_smooth = p_smooth

        # Side from p_smooth (same probability used for EV in runner)
        side = "UP" if p_smooth > 0.505 else "DOWN" if p_smooth < 0.495 else "NONE"

        regime = self._detect_regime()

        return {
            "p_raw": round(p_raw, 4),
            "p_smooth": round(p_smooth, 4),
            "conviction": round(conviction, 4),
            "side": side,
            "strength": round(strength, 4),
            "confidence_w": round(w, 4),
            "regime": regime,
            "q_mid": round(q_mid, 4),
            "edge": round(edge, 4),
            "signals": {
                "ret_5": round(ret_5, 6),
                "ret_15": round(ret_15, 6),
                "ret_30": round(ret_30, 6),
                "ret_60": round(ret_60, 6),
                "vol": round(vol, 6),
                "book_imb": round(book_imb, 4),
                "lag": round(lag, 4),
                "pressure": round(pressure, 4),
                "z": round(z, 4),
                "logit_shift": round(total_shift, 4),
            },
        }

    def _calc_flip_rate(self) -> float:
        """Fraction of p_raw crossings of 0.50 in last 30 ticks. High = choppy."""
        hist = getattr(self, '_p_raw_history', None)
        if not hist or len(hist) < 5:
            return 0.0
        recent = list(hist)[-30:]
        crossings = sum(
            1 for i in range(1, len(recent))
            if (recent[i] - 0.50) * (recent[i - 1] - 0.50) < 0
        )
        return min(1.0, crossings / max(1, len(recent) - 1))

    def _detect_regime(self) -> str:
        """Detect market regime from lag-1 autocorrelation of p_raw history.

        TREND: positive autocorr (momentum carries)
        MEAN_REVERT: negative autocorr (prices bounce)
        CHOP: near-zero autocorr (random walk)
        """
        hist = getattr(self, '_p_raw_history', None)
        if not hist or len(hist) < 10:
            return "CHOP"
        recent = list(hist)[-30:]
        if len(recent) < 5:
            return "CHOP"
        diffs = [recent[i] - recent[i - 1] for i in range(1, len(recent))]
        if len(diffs) < 4:
            return "CHOP"
        # Lag-1 autocorrelation
        mean_d = sum(diffs) / len(diffs)
        var_d = sum((d - mean_d) ** 2 for d in diffs) / len(diffs)
        if var_d < 1e-12:
            return "CHOP"
        cov = sum((diffs[i] - mean_d) * (diffs[i - 1] - mean_d)
                  for i in range(1, len(diffs))) / (len(diffs) - 1)
        autocorr = cov / var_d
        if autocorr > 0.15:
            return "TREND"
        elif autocorr < -0.15:
            return "MEAN_REVERT"
        return "CHOP"

    def _compute_minority_target(self, conviction: float, time_remaining: float,
                                  max_spread: float, regime: str) -> float:
        """Dynamic minority ratio target = f(conviction, time, spread, regime).
        Returns target in [0.14, 0.45].

        Wider-hedge calibration: keep meaningful hedge to survive wrong-side
        windows. Only go directional at very high conviction in TREND."""
        # Base: moderate hedge (lightened from 0.32)
        target = 0.30

        # Conviction → lower minority, but more gradual
        # at conv=0.50 → full -0.12, at conv=0.70+ → additional -0.04
        target -= 0.12 * min(1.0, conviction / 0.50)
        if conviction > 0.50:
            target -= 0.04 * min(1.0, (conviction - 0.50) / 0.30)

        # Wide spread → increase hedge (more uncertain)
        target += 0.06 * min(1.0, max_spread / 0.10)

        # Regime adjustment — moderate TREND reduction (was -0.07, too aggressive)
        if regime == "CHOP":
            target += 0.04  # more hedging in choppy regime
        elif regime == "TREND":
            target -= 0.04  # moderate reduction in trending regime
        elif regime == "MEAN_REVERT":
            target += 0.02  # slight increase in mean-reverting

        # Time decay: as window nears end, nudge toward neutral (less certain)
        t_frac = 1.0 - (time_remaining / 300.0)  # 0=start, 1=end
        if t_frac > 0.60:
            target += 0.05 * (t_frac - 0.60) / 0.40  # gradual increase near end

        # Dynamic floor: allow more directional in strong TREND + high conviction
        if regime == "TREND" and conviction >= 0.40:
            floor = 0.14
        elif regime == "TREND" and conviction >= 0.25:
            floor = 0.15
        else:
            floor = 0.16
        return max(floor, min(0.45, round(target, 3)))

    def _spread_quality_score(self, up_spread, down_spread, up_depth, down_depth,
                               up_mid, down_mid, flip_rate) -> float:
        """Composite quote quality score. 0=terrible, 1=excellent."""
        sum_spread = up_spread + down_spread
        complement_dev = abs(up_mid + down_mid - 1.0) if up_mid > 0 and down_mid > 0 else 0.10
        depth_score = min(1.0, (up_depth + down_depth) / 5000)
        stability = max(0, 1.0 - flip_rate)

        q = (0.40 * max(0, 1.0 - sum_spread / 0.15)
           + 0.25 * max(0, 1.0 - complement_dev / 0.10)
           + 0.20 * depth_score
           + 0.15 * stability)
        return round(max(0, min(1.0, q)), 3)

    def _should_enter(self, conviction_result: dict, time_into: float,
                      up_spread: float = 0.0, down_spread: float = 0.0,
                      up_depth: int = 0, down_depth: int = 0,
                      entry_ready_count: int = 0,
                      edge_history: Optional[list[float]] = None,
                      kelly_entry_time: int = 8,
                      kelly_min_edge: float = 0.02,
                      conv_floor_up: float = 0.08,
                      conv_floor_down: float = 0.10,
                      chop_conv: float = 0.06,
                      chop_edge: float = 0.03,
                      chop_div: float = 0.08,
                      price_div_early: float = 0.03,
                      price_div_mid: float = 0.02,
                      price_div_late: float = 0.01) -> tuple[bool, str, int]:
        """Gate: only enter when conditions are right.

        Uses v2 probability engine signals: edge, z-score, confidence_w.
        Conviction floors and gate thresholds are passed from RuntimeConfig.

        Returns: (should_enter, reason, updated_entry_ready_count)
        """
        c = conviction_result

        # GATE 1: Minimum conviction threshold (asymmetric by side)
        _side = c.get("side", "NONE")
        _conv_floor = conv_floor_down if _side == "DOWN" else conv_floor_up
        if c.get("conviction", 0) < _conv_floor:
            return False, f"conviction too low ({_side} floor={_conv_floor:.2f})", 0

        # GATE 2: No direction
        if c.get("side") == "NONE":
            return False, "no directional signal", 0

        # Extract early — needed by multiple gates below
        conviction = c.get("conviction", 0)
        edge = c.get("edge", 0)

        # GATE 2B: CHOP regime — soft gate.
        if c.get("regime") == "CHOP":
            _chop_edge = edge
            _chop_div = abs(c.get("q_mid", 0.50) - 0.50)
            if conviction < chop_conv and _chop_edge < chop_edge and _chop_div < chop_div:
                return False, "choppy regime, need higher conviction", 0

        # GATE 2C: Price divergence — wait for market to pick a direction.
        q_mid = c.get("q_mid", 0.50)
        price_div = abs(q_mid - 0.50)
        if time_into < 45:
            min_div = price_div_early
        elif time_into < 120:
            min_div = price_div_mid
        else:
            min_div = price_div_late
        if price_div < min_div and conviction < 0.15 and edge < 0.04:
            return False, f"price div {price_div:.3f} < {min_div:.2f} (q={q_mid:.3f})", 0

        # GATE 3: Edge must be meaningful (edge = abs(p_raw - q_mid))
        # Uses kelly_min_edge from aggression level (default 0.02 @ aggr=3)
        # Early window: require half of min_edge; later: full min_edge
        _edge_gate = kelly_min_edge * (0.5 if time_into < 60 else 1.0)
        if edge < _edge_gate and conviction < 0.04:
            return False, f"edge {edge:.4f} < {_edge_gate:.4f}", 0

        # GATE 4: Z-score or confidence_w must confirm
        # Relaxed: was z<0.08/conf<0.15/conv<0.30 — too strict in thin books
        sigs = c.get("signals", {})
        z_abs = abs(sigs.get("z", 0))
        conf_w = c.get("confidence_w", 0)
        if z_abs < 0.05 and conf_w < 0.10 and conviction < 0.20:
            return False, "insufficient signal strength", 0

        # GATE 4B: Quote quality score (depth passed explicitly, not from signals dict)
        quality = self._spread_quality_score(
            up_spread, down_spread, up_depth, down_depth,
            c.get("q_mid", 0.50), 1.0 - c.get("q_mid", 0.50),
            self._calc_flip_rate())
        self._last_quality_score = quality  # expose to runner for budget scaling

        if quality < 0.20:
            return False, f"quote quality {quality:.2f} < 0.20", 0
        if quality < 0.35 and conviction < 0.15:
            return False, f"quote quality {quality:.2f} < 0.35 low conv", 0

        # GATE 4B-hyst: Hysteresis — require conditions to hold for N consecutive ticks
        # Uses per-window counter (passed in, stored on MarketWindow, NOT engine-global)
        if conviction < 0.40:  # strong signals skip hysteresis
            entry_ready_count += 1
            if entry_ready_count < 2:
                return False, f"hysteresis {entry_ready_count}/2", entry_ready_count
        else:
            entry_ready_count = 3  # bypass for strong conviction

        # GATE 4C: Edge slope — prefer improving edge over flat/declining
        # Track edge history per market window to avoid cross-window contamination.
        if edge_history is None:
            edge_history = []
        edge_history.append(edge)
        if len(edge_history) > 30:
            del edge_history[:-30]
        if time_into >= 20 and len(edge_history) >= 10 and time_into < 60:
            recent_avg = sum(edge_history[-5:]) / 5
            older_avg = sum(edge_history[-10:-5]) / 5
            edge_slope = recent_avg - older_avg
            if edge_slope < -0.003 and conviction < 0.25:
                return False, f"declining edge slope={edge_slope:.4f}", 0

        # GATE 5: Time-based entry zones (using kelly_entry_time as early gate)
        # Data (244 resolved windows, in-gate scope):
        #   early <=60s:  n=114, WR=67.5%, avg=+0.0842
        #   mid 60-120s:  n=19,  WR=47.4%, avg=-0.1914  ← dead zone
        #   late >=120s:  n=10,  WR=90%,   avg=+0.2377
        #   skip 60-120:  n=124, WR=69.4%, avg=+0.0966  (P>0 = 0.992)
        # Warm-up: quote-aware — good books trade earlier, weak books wait longer
        #   Good book (quality >= 0.45, tight spread): use kelly_entry_time (5-8s)
        #   Weak book (quality < 0.30 or wide spread): extend to 12-15s
        # Prime (entry_time–60s): best zone — momentum forming
        # Dead zone (60-120s): block unless strong signal
        # Late (120-270s): small sample but high WR — allow with low bar
        # Final 30s (270+): skip — not worth it
        effective_entry_time = kelly_entry_time
        if quality < 0.30 or (up_spread + down_spread) > 0.06:
            effective_entry_time = max(kelly_entry_time, 12)
        if time_into < effective_entry_time:
            # Early: basic conviction check
            if conviction < 0.10 or edge < 0.01:
                return False, "early window, need conviction", 0
        elif time_into < 60:
            # Prime window
            if conviction < 0.05:
                return False, "prime window, need confirmation", 0
        elif time_into < 270:
            # Mid-to-late: allow with basic bar
            if conviction < 0.05:
                return False, "mid-late window, low conviction", 0
        else:
            return False, "too late in window (final 30s)", 0

        return True, "confirmed", entry_ready_count

    def _decide_approach(
        self,
        conviction: float,
        side: str,
        bankroll: float,
        buy_price: float,
        up_ask: float = 0.50,
        down_ask: float = 0.50,
        p_smooth: float = 0.50,
    ) -> tuple[str, float, float, int]:
        """Maps conviction to trading approach with ask-based drag gate.

        Returns: (approach, dominant_pct, budget, max_adds)

        DIRECTIONAL mode: buy ONE side only, hold to settlement.
        Kelly sizing uses model's actual win probability (p_smooth) rather
        than a synthetic mapping from conviction.  This prevents over-sizing
        when conviction is "strong" but directional probability is marginal.
        """
        # Actual win probability for chosen side from the probability model.
        # Old: p_win = 0.50 + conviction * 0.25 (synthetic, could overstate edge)
        # New: use p_smooth directly — that's the model's P(UP).
        p_win = p_smooth if side == "UP" else (1.0 - p_smooth)
        # Safety floor/cap: don't let extreme p_smooth produce absurd Kelly
        p_win = max(0.50, min(0.80, p_win))

        kelly_f = self._kelly_fraction(p_win, buy_price, use_maker=False, hold_to_settle=True)
        budget = max(1.01, bankroll * kelly_f * 0.25)  # quarter Kelly, min $1.01
        budget = min(budget, self.max_bet_dollars, bankroll * 0.25)

        if conviction < 0.03 or side == "NONE":
            return ("SKIP", 0, 0, 0)

        # Ask-based quality gate: still useful for filtering stale books
        # even without hedging. High drag = bad quote quality.
        entry_drag = up_ask + down_ask - 1.0
        if entry_drag > 0.05:
            return ("SKIP", 0, 0, 0)
        if entry_drag > 0.015:
            budget *= max(0.35, 1.0 - (entry_drag - 0.015) / 0.035)

        # Dead-zone: size damping instead of hard SKIP.
        # Near-even pricing = uncertain, but position size reduction beats
        # zero throughput. Weak conviction → half size, not full reject.
        if 0.50 <= buy_price <= 0.58 and conviction < 0.15:
            budget = round(budget * 0.50, 2)
        if 0.58 < buy_price <= 0.65 and conviction < 0.25:
            budget = round(budget * 0.60, 2)

        # DIRECTIONAL: one full favored-side entry, no add-ons.
        # This keeps the live path compact and reduces execution churn.
        return ("DIRECTIONAL", 1.0, round(budget, 2), 0)

    def should_act(
        self,
        bet_side: str,
        entry_price: float,
        current_price: float,
        up_price: float,
        down_price: float,
        time_remaining: int,
        interval_secs: int,
        conviction_result: dict,
        trade_count: int = 0,
        max_adds: int = 12,
        up_shares: float = 0.0,
        down_shares: float = 0.0,
        up_vwap: float = 0.0,
        down_vwap: float = 0.0,
        best_bid: float = 0.0,
        peak_profit_pct: float = 0.0,
        approach: str = "",
        up_best_bid: float = 0.0,
        down_best_bid: float = 0.0,
        # v2 HEDGED params
        budget: float = 0.0,
        total_spent: float = 0.0,
        p_smooth: float = 0.50,
        up_best_ask: float = 1.0,
        down_best_ask: float = 1.0,
        up_spread: float = 0.0,
        down_spread: float = 0.0,
        hold_secs: float = 999.0,
        up_cost: float = 0.0,
        down_cost: float = 0.0,
        # P5: advisor parameter adjustments
        advisor_trim_mult: float = 1.0,
        advisor_hedge_adj: float = 0.0,
        # Rehedge hysteresis: consecutive ticks above soft threshold
        rh_consec: int = 0,
        # DIRECTIONAL reversal persistence: wall-clock seconds p_smooth opposes bet
        reversal_secs: float = 0.0,
        # Wallet signal (Abrak) — share balances for wallet-flip cut
        wallet_up_shares: float = 0.0,
        wallet_down_shares: float = 0.0,
        wallet_prev_direction: str = "",  # direction at entry ("UP"/"DOWN"/"")
        wallet_entry_against_pct: float = 0.0,  # opposition % at entry (for delta gating)
        # Abrak position delta (share changes between polls)
        wallet_delta_up: float = 0.0,
        wallet_delta_down: float = 0.0,
        wallet_delta_age: float = 999.0,  # seconds since last delta computation
        # Deterioration speed: mark_ratio from ~15s ago (runner tracks history)
        mark_ratio_15s_ago: float = -1.0,  # -1.0 = not available
    ) -> dict:
        """Directional EV-gated action logic for HEDGED mode.

        Returns: {action, side, reason, conviction, fav_spend, hedge_spend, ...}

        Actions: ADD, TAKE_PROFIT, CUT_LOSS, LOCK, HOLD, REHEDGE
        No more HEDGE/REBALANCE/REDUCE — replaced by conditional hedge within ADD.
        """
        result = {
            "action": "HOLD",
            "side": bet_side,
            "fraction": 0.0,
            "reason": "",
            "conviction": conviction_result.get("conviction", 0),
            "sell_side": bet_side,
            "hedge_side": "",
            "hedge_fraction": 0.0,
            "fav_spend": 0.0,
            "hedge_spend": 0.0,
        }

        conviction = conviction_result.get("conviction", 0)
        time_into = interval_secs - time_remaining
        interval_secs_f = float(max(1, interval_secs))
        slip = 0.002
        buffer = 0.003
        min_take_profit_hold = 12.0
        min_cut_loss_hold = 35.0  # was 15s — give hedged positions more time

        # --- BUY EV PER SHARE (fee-in-shares model with rounding) ---
        ask_up = up_best_ask if 0 < up_best_ask < 1.0 else up_price + up_spread / 2
        ask_dn = down_best_ask if 0 < down_best_ask < 1.0 else down_price + down_spread / 2

        cost_up = _buy_cost_per_share(ask_up)   # includes rounding + min fee
        cost_dn = _buy_cost_per_share(ask_dn)
        # Fee rounding buffer: on small clips ($0.50-$1), rounding to 4dp + min
        # can add ~0.0002/share extra cost vs the continuous formula
        fee_round_buf = max(FEE_MIN_USDC, 0.0002)
        ev_up = p_smooth - cost_up - slip - buffer - fee_round_buf
        ev_dn = (1.0 - p_smooth) - cost_dn - slip - buffer - fee_round_buf

        # Deadband: don't flip fav_side on tiny p_smooth moves around 0.50.
        # Stick with current bet_side unless p_smooth crosses the opposite threshold.
        NEUTRAL_LO, NEUTRAL_HI = 0.495, 0.505
        if NEUTRAL_LO < p_smooth < NEUTRAL_HI:
            fav_side = bet_side  # stay with existing side in neutral zone
        elif p_smooth >= NEUTRAL_HI:
            fav_side = "UP"
        else:
            fav_side = "DOWN"
        fav_ev = ev_up if fav_side == "UP" else ev_dn
        hedge_ev = ev_dn if fav_side == "UP" else ev_up
        has_fav_edge = fav_ev > 0

        # Combined EV for HEDGED: settlement pays $1 to winning side.
        # Total cost = cost_up + cost_dn per share pair.
        # Combined profit per pair = 1.0 - cost_up - cost_dn - 2*(slip+buffer+fee_round_buf)
        combined_ev = 1.0 - cost_up - cost_dn - 2 * (slip + buffer + fee_round_buf)
        # Combined EV is typically -0.02 to -0.05 due to spread+fees.
        # Allow hedging when expected loss per pair < 5 cents.
        HEDGE_EV_TOLERANCE = -0.05

        # --- A(q) aggression multiplier ---
        q_mid_local = up_best_ask if 0 < up_best_ask < 1.0 else up_price
        if up_best_bid > 0:
            q_mid_local = (up_best_bid + q_mid_local) / 2
        s = max(0.02, 0.06 - 0.02 * (time_into / interval_secs_f))
        A_q = 1.0 + 0.6 * math.exp(-((q_mid_local - 0.30) / s) ** 2) + 0.6 * math.exp(-((q_mid_local - 0.60) / s) ** 2)

        # ── 1. LOCK — final 30s ──
        if time_remaining < 30:
            # Annotate EV telemetry even on LOCK (metrics completeness)
            if approach == "DIRECTIONAL" and bet_side:
                _lock_sh = up_shares if bet_side == "UP" else down_shares
                _lock_bid = up_best_bid if bet_side == "UP" else down_best_bid
                _lock_p = p_smooth if bet_side == "UP" else (1.0 - p_smooth)
                result["_ev_hold"] = round(_lock_p * _lock_sh, 4)
                result["_ev_exit"] = round(
                    _lock_sh * _sell_proceeds_per_share(_lock_bid)
                    if _lock_bid > 0.01 and _lock_sh >= 1.0 else 0.0, 4)
            result["action"] = "LOCK"
            result["reason"] = f"final {time_remaining}s — locking position"
            return result

        # ── 2. SMART EV EXIT for HEDGED ──
        # DIRECTIONAL: skip entirely — hold to settlement for $1 payout.
        # HEDGED: early exit only when selling BOTH sides now locks in MORE
        # profit than the best settlement outcome.
        if approach == "DIRECTIONAL":
            _dir_signal = conviction_result.get("side", "NONE")
            _dir_shares = up_shares if bet_side == "UP" else down_shares
            _dir_bid = up_best_bid if bet_side == "UP" else down_best_bid
            _dir_cost = total_spent

            # ══════════════════════════════════════════════════════════════
            # DIRECTIONAL: HOLD-TO-SETTLE
            #
            # Hold everything to settlement. Only two exceptions:
            #   1. PREMIUM TP — bid >= 0.95 with >2 min left (lock in near-max)
            #   2. REGIME SHIFT EXIT — model flipped against us near settlement
            # No re-entry after TP (slug stays sniped).
            # ══════════════════════════════════════════════════════════════

            _dir_mark_ratio = 0.0
            if _dir_cost > 0 and _dir_bid > 0:
                _dir_mark_ratio = (_dir_shares * _sell_proceeds_per_share(_dir_bid)) / _dir_cost

            _dir_flipped = (_dir_signal != "NONE" and _dir_signal != bet_side)
            _p_ours = p_smooth if bet_side == "UP" else (1.0 - p_smooth)

            _dir_proceeds = (_dir_shares * _sell_proceeds_per_share(_dir_bid)
                             if _dir_bid > 0.01 and _dir_shares >= 1.0 else 0.0)
            _dir_profit = _dir_proceeds - _dir_cost if _dir_cost > 0 else 0.0
            _dir_profit_pct = _dir_profit / max(_dir_cost, 0.01) if _dir_cost > 0 else 0.0

            _hold_ev = _p_ours * _dir_shares
            _exit_ev = _dir_proceeds
            result["_ev_hold"] = round(_hold_ev, 4)
            result["_ev_exit"] = round(_exit_ev, 4)

            _regime = conviction_result.get("regime", "CHOP")

            # ── 1. PREMIUM TP — bid >= 0.95, >2 min left, no re-enter ──
            if (_dir_bid >= 0.95 and time_remaining > 120
                    and _dir_shares >= 1.0 and _dir_cost > 0
                    and _dir_profit > 0):
                result["action"] = "TAKE_PROFIT"
                result["reason"] = (
                    f"PREMIUM_TP bid={_dir_bid:.2f} profit=${_dir_profit:.2f} "
                    f"({_dir_profit_pct:.0%}) t_left={time_remaining}s — no re-enter")
                # No _flip_exit/_high_bid_tp → slug stays sniped (no re-entry)
                return result

            # ── 2. REGIME SHIFT EXIT — last 90s, model flipped against us ──
            # Near settlement, if model conviction has flipped to the other
            # side and we're in profit, take it rather than risk reversal.
            if (time_remaining <= 90 and _dir_flipped
                    and conviction >= 0.15 and _dir_profit > 0
                    and _dir_shares >= 1.0 and _dir_cost > 0):
                result["action"] = "TAKE_PROFIT"
                result["reason"] = (
                    f"REGIME_SHIFT bid={_dir_bid:.2f} profit=${_dir_profit:.2f} "
                    f"({_dir_profit_pct:.0%}) t_left={time_remaining}s "
                    f"flipped={_dir_signal} conv={conviction:.2f} "
                    f"regime={_regime} — no re-enter")
                # No _flip_exit → slug stays sniped (no re-entry)
                return result

            # Everything else: HOLD to settlement

        elif hold_secs >= 20.0 and up_best_bid > 0 and down_best_bid > 0 and total_spent > 0:
            V_exit = (up_shares * _sell_proceeds_per_share(up_best_bid)
                      + down_shares * _sell_proceeds_per_share(down_best_bid)
                      - slip * (up_shares + down_shares))
            # Settlement value: probability-weighted expected payout.
            # For asymmetric hedges, max(up,dn) is too optimistic — the minority
            # side may win and settlement < max(up,dn). Use expected value instead.
            V_settle_prob = p_smooth * up_shares + (1.0 - p_smooth) * down_shares
            # Floor at the WORSE outcome (minority side wins) for conservative estimate
            V_settle_floor = min(up_shares, down_shares) if min(up_shares, down_shares) > 0 else 0
            V_settlement = max(V_settle_prob, V_settle_floor)
            profit_vs_cost = V_exit - total_spent
            profit_pct = profit_vs_cost / max(total_spent, 0.01)

            # Time-scaled exit thresholds — easier to exit as settlement approaches
            if time_remaining > 180:
                # Early: need V_exit to beat settlement by $0.12 AND 8%+ profit
                exit_buffer = 0.12
                min_profit_pct = 0.08
            elif time_remaining > 60:
                # Mid: $0.08 buffer, 5%+ profit
                exit_buffer = 0.08
                min_profit_pct = 0.05
            else:
                # Late (30-60s): $0.04 buffer, 3%+ profit (near settlement, low risk)
                exit_buffer = 0.04
                min_profit_pct = 0.03

            if V_exit > V_settlement + exit_buffer and profit_pct > min_profit_pct:
                result["action"] = "TAKE_PROFIT"
                result["reason"] = (f"smart exit V_exit={V_exit:.3f} > V_settle={V_settlement:.3f}+{exit_buffer:.2f} "
                                    f"profit=${profit_vs_cost:.3f} ({profit_pct:.1%}) t_left={time_remaining}s")
                return result

        # ── 2B. SMART TRIM — hold favored side, trim losing side to recover value ──
        #
        # Philosophy: In HEDGED, the favored (winning) side goes to $1 at settlement.
        # Never sell the winning side — let it ride to max payout.
        # Instead, sell the LOSING side while it still has bid value (before it goes to $0).
        # This recovers capital and proceeds get reinvested into the favored side.
        #
        # Exception: trim favored side ONLY if direction flipped AND already in profit
        # (protective exit when signals reverse).
        #
        # Always evaluate trim — log reason when skipped (Fix 4: continuous trim eval)
        current_signal_side = conviction_result.get("side", "NONE")
        budget_pct = total_spent / max(budget, 1e-9)
        trim_skip_reason = ""

        if hold_secs < 30.0:
            trim_skip_reason = f"hold_secs={hold_secs:.0f}<30"
        elif up_best_bid <= 0 or down_best_bid <= 0:
            trim_skip_reason = f"stale quotes ub={up_best_bid:.3f} db={down_best_bid:.3f}"
        elif total_spent <= 0:
            trim_skip_reason = "no spend"
        elif budget_pct < 0.50:
            trim_skip_reason = f"budget_pct={budget_pct:.2f}<0.50"

        # Store skip reason in result for logging upstream
        result["trim_skip_reason"] = trim_skip_reason

        if not trim_skip_reason:
            total_shares = up_shares + down_shares
            # DIRECTIONAL: skip losing-side trim entirely — there IS no losing side.
            # Only flip-based trims (direction reversal) apply to DIRECTIONAL.
            if approach == "DIRECTIONAL":
                trim_skip_reason = "DIRECTIONAL — no losing-side trim"
                result["trim_skip_reason"] = trim_skip_reason
            # Only apply losing-side trim when BOTH sides have shares (true HEDGED)
            both_sides = up_shares > 0.5 and down_shares > 0.5
            if not trim_skip_reason and total_shares <= 1.5:
                trim_skip_reason = f"total_shares={total_shares:.1f}<1.5"
                result["trim_skip_reason"] = trim_skip_reason
            elif not trim_skip_reason and not both_sides:
                trim_skip_reason = f"single side only (↑{up_shares:.1f} ↓{down_shares:.1f})"
                result["trim_skip_reason"] = trim_skip_reason

        if not trim_skip_reason and total_shares > 1.5 and both_sides:
                # Determine winning vs losing side from market bids
                up_is_winning = up_best_bid > down_best_bid
                winning_side = "UP" if up_is_winning else "DOWN"
                losing_side = "DOWN" if up_is_winning else "UP"
                losing_bid = down_best_bid if up_is_winning else up_best_bid
                losing_shares = down_shares if up_is_winning else up_shares

                trim_side = ""
                trim_shares = 0.0
                trim_reason = ""

                # ─ PRIMARY: Trim LOSING side to recover value before $0 ─
                # Fee-aware: only trim when net proceeds after fees > holding to settle.
                # For hedge side, settlement pays $0 if this side loses, but we
                # don't KNOW it will lose yet. Require stronger evidence.
                losing_cost = (down_cost if up_is_winning else up_cost)
                losing_vwap = losing_cost / losing_shares if losing_shares > 0 else 0
                # Tightened bid floor from $0.10 → $0.15 (don't sell for nearly nothing)
                # Tightened ceiling from $0.40 → $0.35 (only trim clearly losing side)
                # Raised VWAP gate from 25% → 35% (less eager to sell at deep loss)
                # Added: only trim when net proceeds > expected settlement value
                _trim_proceeds_per_sh = _sell_proceeds_per_share(losing_bid) if losing_bid > 0 else 0
                _trim_settle_value = 0  # if this side loses, it settles at $0
                # But we don't know if it will lose — use 1-p_smooth as probability
                _lose_prob = p_smooth if not up_is_winning else (1 - p_smooth)
                _expected_settle = (1.0 - _lose_prob) * 1.0  # prob(this side wins) * $1
                if losing_bid <= 0.15:
                    trim_skip_reason = f"losing bid ${losing_bid:.3f} too low (<$0.15)"
                elif losing_bid >= 0.35:
                    trim_skip_reason = f"losing bid ${losing_bid:.3f} too high (>$0.35, not clearly losing)"
                elif losing_shares <= 1.5:
                    trim_skip_reason = f"losing shares {losing_shares:.1f} too few (<1.5)"
                elif losing_vwap > 0 and losing_bid < losing_vwap * 0.35:
                    trim_skip_reason = f"bid ${losing_bid:.3f} < 35% of VWAP ${losing_vwap:.3f}"
                elif _trim_proceeds_per_sh > 0 and _trim_proceeds_per_sh < _expected_settle * 0.80:
                    # Net proceeds worse than 80% of expected settlement — hold
                    trim_skip_reason = (f"proceeds ${_trim_proceeds_per_sh:.3f} < "
                                        f"80% settle ${_expected_settle:.3f}")
                result["trim_skip_reason"] = trim_skip_reason
                if (losing_bid > 0.15 and losing_bid < 0.35
                        and losing_shares > 1.5
                        and (losing_vwap <= 0 or losing_bid >= losing_vwap * 0.35)
                        and (_trim_proceeds_per_sh <= 0 or _trim_proceeds_per_sh >= _expected_settle * 0.80)):
                    min_keep_losing = 0.5
                    trimmable = losing_shares - min_keep_losing
                    if trimmable >= 1.0:
                        # Trim 20% per cycle (was 30% — less eager)
                        trim_shares = int(min(trimmable * 0.20, losing_shares * 0.25))
                        if trim_shares < 1:
                            trim_shares = 0
                        trim_side = losing_side if trim_shares >= 1 else ""
                        recover = _sell_proceeds_per_share(losing_bid)
                        trim_reason = (f"recover {losing_side} {trim_shares:.1f}sh "
                                       f"bid=${losing_bid:.3f} vwap=${losing_vwap:.3f} "
                                       f"save=${recover:.3f}/sh "
                                       f"(vs settle=${_expected_settle:.3f})")

                # ─ SECONDARY: 2-stage direction flip (soft → hard) ─
                # Stage 1 (soft flip): signal changes direction → trim 20% of old favored
                # Stage 2 (hard flip): flip persists 30s+ AND EV supports → trim 40%
                if not trim_side and current_signal_side != "NONE":
                    fav_flipped = (bet_side != current_signal_side)
                    if fav_flipped and hold_secs >= 40.0:
                        # Track flip persistence via _flip_start_ts attribute
                        if not hasattr(self, '_flip_start_ts') or getattr(self, '_flip_to', '') != current_signal_side:
                            self._flip_start_ts = time.time() if hasattr(self, '_flip_start_ts') else 0
                            self._flip_to = current_signal_side
                            # First detection — set timer
                            if self._flip_start_ts == 0:
                                self._flip_start_ts = time.time()
                        flip_duration = time.time() - getattr(self, '_flip_start_ts', time.time())
                        is_hard_flip = flip_duration >= 30.0  # persistent for 30s+

                        mark = (up_shares * _sell_proceeds_per_share(up_best_bid)
                                + down_shares * _sell_proceeds_per_share(down_best_bid))
                        combined_pnl = mark - total_spent

                        old_fav_shares = up_shares if bet_side == "UP" else down_shares
                        old_fav_bid = up_best_bid if bet_side == "UP" else down_best_bid
                        old_fav_cost = up_cost if bet_side == "UP" else down_cost

                        # Soft flip: trim 20% (any profitable flip)
                        # Hard flip: FLIP_ROLL — sell old favored + buy new favored
                        if old_fav_shares > 1.5 and old_fav_bid > 0.10:
                            min_keep = 1.0
                            trimmable = old_fav_shares - min_keep
                            if trimmable >= 1.0:
                                if is_hard_flip and conviction >= 0.15:
                                    # Hard flip: FLIP_ROLL — sell old fav, buy new fav
                                    # More aggressive than trim: sell 50% old fav
                                    roll_pct = 0.50
                                    roll_shares = int(min(trimmable * roll_pct, old_fav_shares * 0.45))
                                    if roll_shares >= 1:
                                        new_fav_ask = down_best_ask if current_signal_side == "DOWN" else up_best_ask
                                        roll_proceeds_est = roll_shares * _sell_proceeds_per_share(old_fav_bid)
                                        # Allocate 60% of proceeds to buy new favored side
                                        roll_buy_spend = round(roll_proceeds_est * 0.60, 2)
                                        if roll_buy_spend >= 1.00 and new_fav_ask > 0.01 and new_fav_ask < 0.95:
                                            result["action"] = "FLIP_ROLL"
                                            result["sell_side"] = bet_side
                                            result["sell_shares"] = roll_shares
                                            result["buy_side"] = current_signal_side
                                            result["buy_spend"] = roll_buy_spend
                                            result["buy_ask"] = new_fav_ask
                                            result["reason"] = (
                                                f"flip_roll {bet_side}→{current_signal_side} "
                                                f"sell {roll_shares:.0f}sh @${old_fav_bid:.3f} "
                                                f"buy ${roll_buy_spend:.2f} @${new_fav_ask:.3f} "
                                                f"conv={conviction:.2f} dur={flip_duration:.0f}s")
                                            result["_flip_label"] = "roll"
                                            result["_flip_duration"] = flip_duration
                                            return result
                                    # Fall through to soft flip if roll_shares < 1
                                    flip_label = "hard"
                                    trim_pct = 0.40
                                elif combined_pnl > 0.10:
                                    # Soft flip: moderate trim (lower profit threshold)
                                    trim_pct = 0.20
                                    flip_label = "soft"
                                else:
                                    trim_pct = 0
                                    flip_label = "skip"

                                if trim_pct > 0:
                                    trim_shares = int(min(trimmable * trim_pct, old_fav_shares * 0.35))
                                    if trim_shares < 1:
                                        trim_shares = 0
                                    trim_side = bet_side if trim_shares >= 1 else ""
                                    trim_reason = (f"{flip_label} flip {bet_side}→{current_signal_side} "
                                                   f"trim {trim_side} {trim_shares:.1f}sh "
                                                   f"@${old_fav_bid:.3f} pnl=${combined_pnl:+.3f} "
                                                   f"dur={flip_duration:.0f}s")
                                    result["_flip_label"] = flip_label
                                    result["_flip_duration"] = flip_duration
                    else:
                        # Signal agrees with bet_side — reset flip tracker
                        self._flip_start_ts = 0
                        self._flip_to = ""

                if trim_side and trim_shares >= 1:
                    result["action"] = "TRIM"
                    result["sell_side"] = trim_side
                    result["fraction"] = trim_shares
                    result["reason"] = trim_reason
                    return result

        # ── 3. CUT_LOSS — emergency (net sell proceeds) ──
        # For HEDGED: use expected settlement value (probability-weighted),
        # not max(up,dn) which is too optimistic for asymmetric hedges.

        # POSITION INVARIANT: block CUT/TP when inventory is near-zero
        # Reconcile or chain lag can transiently zero out shares while total_spent
        # remains positive, producing settle/spent=0.00 → phantom CUT.
        total_shares = up_shares + down_shares
        if total_shares < 0.5 and total_spent > 0:
            result["action"] = "HOLD"
            result["reason"] = f"near-zero inventory ({total_shares:.1f}sh) with spent=${total_spent:.2f} — likely stale"
            result["_zero_inventory"] = True  # flag for runner metrics
            return result

        if up_best_bid > 0 and down_best_bid > 0 and total_spent > 0:
            mark = (up_shares * _sell_proceeds_per_share(up_best_bid)
                    + down_shares * _sell_proceeds_per_share(down_best_bid))
            mark_ratio = mark / total_spent
            # Expected settlement value (probability-weighted)
            V_settle_expected = p_smooth * up_shares + (1.0 - p_smooth) * down_shares
            # Conservative floor: minority side wins
            V_settle_floor = min(up_shares, down_shares) if min(up_shares, down_shares) > 0 else 0
            V_settle_ref = max(V_settle_expected, V_settle_floor)

            # Determine position shape: both-sides vs single-side
            _has_both_sides = up_shares >= 0.5 and down_shares >= 0.5

            if approach == "HEDGED" and _has_both_sides:
                # TRUE HEDGED: both sides have meaningful inventory
                # Use settle-ratio logic (settlement pays $1/share on winner)
                settle_ratio = V_settle_ref / total_spent
                if hold_secs >= min_cut_loss_hold and settle_ratio < 0.66 and mark_ratio < 0.62:
                    result["action"] = "CUT_LOSS"
                    result["reason"] = (f"cut-loss settle/spent={settle_ratio:.2f} "
                                        f"mark/spent={mark_ratio:.2f} conv={conviction:.2f}")
                    result["_cut_hysteresis"] = True  # runner applies hysteresis
                    return result
                # Catastrophic: even expected settlement can't save us AND selling NOW is better
                if hold_secs >= 15.0 and settle_ratio < 0.45 and mark > V_settle_ref:
                    result["action"] = "CUT_LOSS"
                    result["reason"] = (
                        f"catastrophic hedge: settle/spent={settle_ratio:.2f} "
                        f"mark={mark:.2f} > settle_exp={V_settle_ref:.2f}"
                    )
                    result["_cut_catastrophic"] = True  # bypasses hysteresis
                    return result
            elif approach == "HEDGED" and not _has_both_sides:
                # SINGLE-SIDE HEDGED: entered as HEDGED but hedge failed/trimmed/skipped.
                # Don't use settle-ratio (it's unstable with one side empty).
                # Use simpler mark-based logic with more generous thresholds
                # because the winning side still pays $1/share at settlement.
                settle_ratio = V_settle_ref / total_spent
                # Only cut if mark is really bad AND we've held long enough
                if hold_secs >= min_cut_loss_hold and mark_ratio < 0.50 and conviction < 0.08:
                    result["action"] = "CUT_LOSS"
                    result["reason"] = (f"single-side cut mark/spent={mark_ratio:.2f} "
                                        f"conv={conviction:.2f} hold={hold_secs:.0f}s")
                    result["_cut_hysteresis"] = True
                    return result
                # Catastrophic single-side: mark below 40% AND very long hold
                if hold_secs >= 45.0 and mark_ratio < 0.40:
                    result["action"] = "CUT_LOSS"
                    result["reason"] = (f"catastrophic single-side mark={mark_ratio:.2f} "
                                        f"hold={hold_secs:.0f}s")
                    result["_cut_catastrophic"] = True
                    return result
            elif approach == "DIRECTIONAL":
                # DIRECTIONAL: hold-to-settle ONLY — no cuts.
                # Every attempt at cut logic costs more than holding:
                #   - Original cuts: net -$128.44
                #   - Smart cuts (EV+reversal gated): still missed winners
                # 88% win rate means dips recover. Hold everything.
                pass
            else:
                # Non-HEDGED: original mark-based logic
                if hold_secs >= min_cut_loss_hold and mark_ratio < 0.85 and conviction < 0.10:
                    result["action"] = "CUT_LOSS"
                    result["reason"] = f"cut-loss mark/spent={mark_ratio:.2f} conv={conviction:.2f}"
                    result["_cut_hysteresis"] = True
                    return result
                if hold_secs >= 6.0 and mark_ratio < 0.55 and conviction < 0.05:
                    result["action"] = "CUT_LOSS"
                    result["reason"] = (
                        f"early catastrophic cut mark/spent={mark_ratio:.2f} hold={hold_secs:.1f}s"
                    )
                    result["_cut_catastrophic"] = True
                    return result

        # ── 3b. REHEDGE — ratio maintenance (HEDGED only) ──
        # DIRECTIONAL: no hedge side → skip entirely
        if approach == "DIRECTIONAL":
            result["action"] = "HOLD"
            result["reason"] = "directional hold-to-settle"
            return result
        regime = conviction_result.get("regime", "CHOP")
        minority_shares = down_shares if fav_side == "UP" else up_shares
        total_shares = up_shares + down_shares
        actual_minority = minority_shares / total_shares if total_shares > 1 else 0.50
        minority_target = self._compute_minority_target(
            conviction, time_remaining, max(up_spread, down_spread), regime)
        # P5: Apply advisor hedge adjustment (bounded ±0.10)
        minority_target = max(0.18, min(0.45, minority_target + advisor_hedge_adj))

        ratio_dev = actual_minority - minority_target

        # Stash rehedge telemetry for runner logging
        result["_rh_actual"] = round(actual_minority, 3)
        result["_rh_target"] = round(minority_target, 3)
        result["_rh_dev"] = round(ratio_dev, 3)
        result["_rh_consec"] = 0  # default: reset hysteresis

        # Time-adaptive hedge bands (aggressive-lite: wider to kill churn):
        #   Early  (t<0.40): soft ±0.16, Mid (0.40-0.70): soft ±0.13, Late (>0.70): soft ±0.10
        #   Hard trigger ±0.20 always acts (no hysteresis needed)
        t_frac_rh = (interval_secs - time_remaining) / max(1, interval_secs)
        HARD_THRESHOLD = 0.20
        if t_frac_rh > 0.70:
            rh_threshold = 0.10   # late: tighter soft band
        elif t_frac_rh > 0.40:
            rh_threshold = 0.13   # mid: moderate soft band
        else:
            rh_threshold = 0.16   # early: wide soft band

        # Rehedge quality gates: meaningful ask range + time remaining
        # Don't rehedge into near-zero (worthless) or near-one (no hedge value) asks
        _hedge_ask_rh = down_best_ask if fav_side == "UP" else up_best_ask
        _ask_in_range = 0.05 < _hedge_ask_rh < 0.90  # meaningful hedge price range
        can_rehedge = total_shares > 2 and time_remaining > 30 and _ask_in_range

        PM_MIN_REHEDGE = 1.00  # Polymarket minimum marketable order

        if abs(ratio_dev) > HARD_THRESHOLD and can_rehedge:
            # ── HARD TRIGGER: extreme deviation, act immediately ──
            result["_rh_consec"] = 0  # reset after action
            if ratio_dev < -HARD_THRESHOLD:
                deficit_shares = (minority_target - actual_minority) * total_shares
                hedge_ask = down_best_ask if fav_side == "UP" else up_best_ask
                cps = _buy_cost_per_share(hedge_ask) if hedge_ask > 0 else hedge_ask
                buy_cost = deficit_shares * cps if cps > 0 else 0
                # Skip tiny deficits — don't round up noise to PM minimum
                # Only rehedge if deficit naturally >= $1.25
                if buy_cost >= 1.25:
                    result["action"] = "REHEDGE"
                    result["side"] = "DOWN" if fav_side == "UP" else "UP"
                    result["rehedge_shares"] = round(deficit_shares, 2)
                    result["rehedge_spend"] = round(buy_cost, 2)
                    result["reason"] = (f"HARD rehedge {result['side']} "
                        f"actual={actual_minority:.2f} target={minority_target:.2f} "
                        f"dev={ratio_dev:+.3f}")
                    result["minority_target"] = minority_target
                    return result
            elif ratio_dev > HARD_THRESHOLD:
                excess_shares = (actual_minority - minority_target) * total_shares
                sell_shares = min(excess_shares, minority_shares * 0.60)
                sell_shares = round(sell_shares * advisor_trim_mult, 2)
                if sell_shares >= 1.0:
                    result["action"] = "TRIM"
                    result["sell_side"] = "DOWN" if fav_side == "UP" else "UP"
                    result["fraction"] = round(sell_shares, 2)
                    result["reason"] = (f"HARD overhedge trim "
                        f"actual={actual_minority:.2f} target={minority_target:.2f} "
                        f"dev={ratio_dev:+.3f}")
                    result["minority_target"] = minority_target
                    return result

        elif abs(ratio_dev) > rh_threshold and can_rehedge:
            # ── SOFT TRIGGER: require 2+ consecutive ticks (hysteresis) ──
            new_consec = rh_consec + 1
            result["_rh_consec"] = new_consec

            if new_consec >= 2:
                # Hysteresis met — act
                result["_rh_consec"] = 0  # reset after action
                if ratio_dev < -rh_threshold:
                    deficit_shares = (minority_target - actual_minority) * total_shares
                    hedge_ask = down_best_ask if fav_side == "UP" else up_best_ask
                    cps = _buy_cost_per_share(hedge_ask) if hedge_ask > 0 else hedge_ask
                    buy_cost = deficit_shares * cps if cps > 0 else 0
                    # Skip tiny deficits — only rehedge if naturally >= $1.25
                    if buy_cost >= 1.25:
                        result["action"] = "REHEDGE"
                        result["side"] = "DOWN" if fav_side == "UP" else "UP"
                        result["rehedge_shares"] = round(deficit_shares, 2)
                        result["rehedge_spend"] = round(buy_cost, 2)
                        result["reason"] = (f"rehedge {result['side']} "
                            f"actual={actual_minority:.2f} target={minority_target:.2f} "
                            f"dev={ratio_dev:+.3f} thresh={rh_threshold:.2f} hyst={new_consec}")
                        result["minority_target"] = minority_target
                        return result
                elif ratio_dev > rh_threshold:
                    excess_shares = (actual_minority - minority_target) * total_shares
                    sell_shares = min(excess_shares, minority_shares * 0.60)
                    sell_shares = round(sell_shares * advisor_trim_mult, 2)
                    if sell_shares >= 1.0:
                        result["action"] = "TRIM"
                        result["sell_side"] = "DOWN" if fav_side == "UP" else "UP"
                        result["fraction"] = round(sell_shares, 2)
                        result["reason"] = (f"overhedged trim "
                            f"actual={actual_minority:.2f} target={minority_target:.2f} "
                            f"dev={ratio_dev:+.3f} hyst={new_consec}")
                        result["minority_target"] = minority_target
                        return result
            else:
                result["_rh_block"] = f"hysteresis {new_consec}/2 dev={ratio_dev:+.3f}"
        else:
            # Deviation below soft threshold — reset hysteresis
            result["_rh_consec"] = 0
            if abs(ratio_dev) > 0.02:
                result["_rh_block"] = f"|dev|={abs(ratio_dev):.3f}<soft={rh_threshold:.2f}"

        # ── 4. ADD — EV-gated, momentum-aware, conditional hedge ──
        PM_MIN_ORDER = 1.00  # Polymarket minimum marketable order
        if time_remaining > 30 and budget > total_spent and has_fav_edge:
            reserve_pct = 0.10 + 0.30 * (time_into / interval_secs_f) ** 2
            available = (budget - total_spent) * (1 - reserve_pct)

            # Momentum-scaled ADD: increase when market agrees, decrease when it disagrees
            q_mid_add = conviction_result.get("q_mid", 0.50)
            our_q = q_mid_add if fav_side == "UP" else (1.0 - q_mid_add)
            # Moderate ADD scaling — wider hedge means cautious adds
            if our_q > 0.65:
                momentum_mult = 1.50  # market strongly confirms
            elif our_q > 0.58:
                momentum_mult = 1.20  # strong agreement
            elif our_q > 0.52:
                momentum_mult = 1.05  # moderate agreement
            elif our_q < 0.40:
                momentum_mult = 0.50  # market disagrees — scale way back
            elif our_q < 0.47:
                momentum_mult = 0.70  # slight disagreement
            else:
                momentum_mult = 1.0   # neutral zone

            # Edge-scaled: boost when fav_ev is strong (1.0-1.4x)
            edge_mult = 1.0 + min(0.4, max(0, fav_ev * 6))

            cycle_cap = min(available, 0.25 * A_q * momentum_mult * edge_mult)

            # Ensure cycle_cap >= $1.00 (Polymarket minimum), else skip
            effective_cycle_cap = max(cycle_cap, PM_MIN_ORDER)
            effective_cycle_cap = min(effective_cycle_cap, available)

            if effective_cycle_cap >= PM_MIN_ORDER:
                if approach == "DIRECTIONAL":
                    # DIRECTIONAL: all spend on favored side, no hedge
                    fav_spend = round(effective_cycle_cap, 4)
                    fav_spend = max(fav_spend, PM_MIN_ORDER)
                    result["action"] = "ADD"
                    result["side"] = fav_side
                    result["fav_spend"] = fav_spend
                    result["hedge_spend"] = 0.0
                    result["reason"] = (
                        f"add {fav_side} ${fav_spend:.2f}"
                        f" ev_f={fav_ev:+.4f} cev={combined_ev:+.4f}"
                    )
                    return result

                # HEDGED: check minority ratio and conditionally add hedge
                total_shares = up_shares + down_shares
                minority_shares = (down_shares if fav_side == "UP" else up_shares) if total_shares > 0 else 0
                minority_ratio = minority_shares / total_shares if total_shares > 0 else 0.0
                # Adaptive ADD hedge floor: tighter in TREND + high conviction
                _add_regime = conviction_result.get("regime", "CHOP")
                if _add_regime == "TREND" and conviction >= 0.40:
                    effective_floor = 0.14
                elif _add_regime == "TREND":
                    effective_floor = 0.16
                elif time_remaining <= 45 and conviction >= 0.80:
                    effective_floor = 0.14
                else:
                    effective_floor = 0.18
                needs_hedge = minority_ratio < effective_floor

                # Delta allocation: tilt strongly to favored side
                # High conviction → 90% fav / 10% hedge
                # Low conviction → 65% fav / 35% hedge
                fav_pct = min(0.90, 0.65 + 0.30 * conviction) if needs_hedge else 1.0
                fav_spend = round(effective_cycle_cap * fav_pct, 4)
                fav_spend = max(fav_spend, PM_MIN_ORDER)  # enforce $1.00 minimum
                hedge_spend = 0.0
                # Use combined_ev for hedge decision: HEDGED strategy profits
                # from settlement ($1 per winning share), so even if single-side
                # hedge_ev is negative, the pair can still be profitable.
                if needs_hedge and combined_ev > HEDGE_EV_TOLERANCE:
                    hedge_spend = round(effective_cycle_cap * (1.0 - fav_pct), 4)
                    if hedge_spend < PM_MIN_ORDER:
                        hedge_spend = 0  # below Polymarket minimum, skip hedge
                        fav_spend = round(effective_cycle_cap, 4)
                        fav_spend = max(fav_spend, PM_MIN_ORDER)
                    else:
                        # Share-ratio safety: check if this hedge ADD would push
                        # minority ratio FURTHER above target. When hedge_ask is
                        # cheap, $1 buys many more hedge shares than fav shares,
                        # easily creating severe over-hedge.
                        fav_ask_add = up_best_ask if fav_side == "UP" else down_best_ask
                        hedge_ask_add = down_best_ask if fav_side == "UP" else up_best_ask
                        if fav_ask_add > 0 and hedge_ask_add > 0:
                            _new_fav_sh = fav_spend / _buy_cost_per_share(fav_ask_add)
                            _new_h_sh = hedge_spend / _buy_cost_per_share(hedge_ask_add)
                            _post_fav = (up_shares if fav_side == "UP" else down_shares) + _new_fav_sh
                            _post_h = (down_shares if fav_side == "UP" else up_shares) + _new_h_sh
                            _post_total = _post_fav + _post_h
                            _post_minority = _post_h / _post_total if _post_total > 0 else 0
                            if _post_minority > minority_target + 0.15:
                                # Hedge would worsen over-hedge — skip it, fav-only
                                hedge_spend = 0
                                fav_spend = round(effective_cycle_cap, 4)
                                fav_spend = max(fav_spend, PM_MIN_ORDER)
                elif needs_hedge:
                    fav_spend = round(effective_cycle_cap * 0.50, 4)
                    fav_spend = max(fav_spend, PM_MIN_ORDER)

                result["action"] = "ADD"
                result["side"] = fav_side
                result["fav_spend"] = fav_spend
                result["hedge_spend"] = hedge_spend
                result["reason"] = (
                    f"add {fav_side} ${fav_spend:.2f}"
                    + (f"+h${hedge_spend:.2f}" if hedge_spend > 0 else "")
                    + f" ev_f={fav_ev:+.4f} cev={combined_ev:+.4f}"
                )
                return result

        # ── 5. HOLD ──
        result["reason"] = f"hold p={p_smooth:.3f} ev_up={ev_up:+.4f} ev_dn={ev_dn:+.4f}"
        return result

    def get_conviction_signal(
        self,
        up_price: float,
        down_price: float,
        up_depth: float,
        down_depth: float,
        time_remaining: int,
        interval_secs: int,
        bankroll: float,
        up_spread: float = 0.0,
        down_spread: float = 0.0,
        up_best_bid: float = 0.0,
        down_best_bid: float = 0.0,
        up_best_ask: float = 1.0,
        down_best_ask: float = 1.0,
        entry_ready_count: int = 0,
        edge_history: Optional[list[float]] = None,
        # Kelly-derived params (from runner _kelly_params)
        kelly_entry_time: int = 8,
        kelly_min_edge: float = 0.02,
        # RuntimeConfig gate params (passed from runner)
        rcfg_gates: dict | None = None,
    ) -> tuple[Optional[DirectionalSignal], int]:
        """v2 signal generation using probability engine + GBM confirmation.

        This wraps _calc_probability + _should_enter + _decide_approach + GBM.
        Called by the runner's scalp loop instead of the old get_signal().
        """
        if self._halted or self.bankroll <= 0.01:
            return None, entry_ready_count

        time_into = interval_secs - time_remaining

        # 1. Compute probability (new market-prior logit model)
        conv = self._calc_probability(
            time_into, time_remaining, up_price, down_price,
            up_depth, down_depth, up_spread, down_spread,
            up_best_bid, down_best_bid, up_best_ask, down_best_ask,
        )

        # 2. Also compute GBM probability (existing model, secondary confirmation)
        gbm_side, gbm_prob, pipeline = self._estimate_probability(
            up_price, down_price, time_remaining, interval_secs
        )

        # 3. Blend: probability engine is primary, GBM is secondary confirmation
        if gbm_side == conv["side"] and gbm_prob > 0.55:
            conv["conviction"] = min(1.0, conv["conviction"] * 1.15)  # 15% boost
        elif gbm_side != conv["side"] and gbm_side != "NONE":
            conv["conviction"] *= 0.70  # 30% penalty when GBM disagrees

        # ── UP-BIAS SCHEDULE ──────────────────────────────────────────────
        # Boost UP probability at specific ET times where BTC structurally
        # tends to tick up (market open, session transitions).
        # Active window: ±90s around each target timestamp.
        self._up_bias_active = False
        self._up_bias_forced_flip = False
        self._up_bias_mode = ""
        try:
            import zoneinfo as _zi
            _et_tz = _zi.ZoneInfo("America/New_York")
        except ImportError:
            from dateutil import tz as _dtz
            _et_tz = _dtz.gettz("America/New_York")
        import datetime as _dt
        _now_et = _dt.datetime.now(_et_tz)
        _now_secs = _now_et.hour * 3600 + _now_et.minute * 60 + _now_et.second

        _UP_BIAS_SCHEDULE = [
            (9, 35), (12, 5), (17, 55), (20, 0), (23, 55),
        ]
        _BIAS_WINDOW = 90  # ±90 seconds

        _in_bias = False
        _bias_target = -1
        for _bh, _bm in _UP_BIAS_SCHEDULE:
            _target = _bh * 3600 + _bm * 60
            _diff = abs(_now_secs - _target)
            if _diff > 43200:
                _diff = 86400 - _diff  # midnight wrap
            if _diff <= _BIAS_WINDOW:
                _in_bias = True
                _bias_target = _target
                break

        if _in_bias:
            self._up_bias_active = True
            self._up_bias_last_target_secs = _bias_target

            # Safety: validate asks before drag calc (require 0.02 < ask < 1.0)
            _up_ask_b = up_best_ask if 0.02 < up_best_ask < 1.0 else up_price + up_spread / 2
            _dn_ask_b = down_best_ask if 0.02 < down_best_ask < 1.0 else down_price + down_spread / 2
            _drag_b = _up_ask_b + _dn_ask_b - 1.0
            _soft_only = (not self._up_bias_hard_enabled
                          or _up_ask_b > 0.72
                          or _drag_b > 0.02)

            if _soft_only:
                # Soft bias: mild p_smooth nudge, no conviction mod, no flip
                self._up_bias_mode = "soft"
                conv["p_smooth"] = min(0.80, conv["p_smooth"] + 0.03)
                self._p_smooth = conv["p_smooth"]
                # Recompute side from biased p_smooth
                if conv["p_smooth"] > 0.505:
                    conv["side"] = "UP"
                elif conv["p_smooth"] < 0.495:
                    conv["side"] = "DOWN"
                else:
                    conv["side"] = "NONE"
            else:
                # Full bias: strong p_smooth nudge + conviction adjustment
                conv["p_smooth"] = min(0.80, conv["p_smooth"] + 0.06)
                self._p_smooth = conv["p_smooth"]

                if conv["side"] == "UP":
                    self._up_bias_mode = "hard"
                    conv["conviction"] = min(1.0, conv["conviction"] * 1.20)
                elif conv["side"] == "DOWN":
                    conv["conviction"] *= 0.75
                    # Force flip: weak DOWN → UP if auto_flip enabled
                    if conv["conviction"] < 0.35 and self._up_bias_auto_flip:
                        conv["side"] = "UP"
                        self._up_bias_forced_flip = True
                        self._up_bias_mode = "flip"
                        # After flip, conviction represents UP confidence.
                        # Use biased p_smooth as basis: p_smooth already nudged UP.
                        _p_up = conv["p_smooth"]
                        _q_mid = conv.get("q_mid", 0.50)
                        conv["conviction"] = min(1.0, abs(_p_up - _q_mid) / 0.15)
                    else:
                        self._up_bias_mode = "hard"

                # Recompute side from biased p_smooth (unless force-flipped)
                if not self._up_bias_forced_flip:
                    if conv["p_smooth"] > 0.505:
                        conv["side"] = "UP"
                    elif conv["p_smooth"] < 0.495:
                        conv["side"] = "DOWN"
                    else:
                        conv["side"] = "NONE"
        else:
            self._up_bias_last_target_secs = -1
        # ── END UP-BIAS ───────────────────────────────────────────────────

        # Store for dashboard (separate from legacy pipeline to avoid overwrite)
        pipeline["conviction"] = conv["conviction"]
        pipeline["conv_signals"] = conv["signals"]
        pipeline["p_smooth"] = conv.get("p_smooth", 0.50)
        pipeline["q_mid"] = conv.get("q_mid", 0.50)
        pipeline["regime"] = conv.get("regime", "CHOP")
        pipeline["up_bias"] = self._up_bias_active
        pipeline["up_bias_mode"] = self._up_bias_mode
        pipeline["up_bias_flip"] = self._up_bias_forced_flip
        self._last_conviction_pipeline = pipeline
        # Clear stale UI text each cycle; set explicit reason only when blocked.
        self._last_conviction_pipeline["gate_reason"] = ""

        # 4. Entry timing gate (with hysteresis counter)
        _gates = rcfg_gates or {}
        should_enter, reason, entry_ready_count = self._should_enter(
            conv, time_into, up_spread, down_spread,
            up_depth, down_depth, entry_ready_count,
            edge_history=edge_history,
            kelly_entry_time=kelly_entry_time,
            kelly_min_edge=kelly_min_edge,
            conv_floor_up=_gates.get("conv_floor_up", 0.08),
            conv_floor_down=_gates.get("conv_floor_down", 0.10),
            chop_conv=_gates.get("chop_conv", 0.06),
            chop_edge=_gates.get("chop_edge", 0.03),
            chop_div=_gates.get("chop_div", 0.08),
            price_div_early=_gates.get("price_div_early", 0.03),
            price_div_mid=_gates.get("price_div_mid", 0.02),
            price_div_late=_gates.get("price_div_late", 0.01))
        if not should_enter:
            self._last_conviction_pipeline["gate_reason"] = reason
            return None, entry_ready_count

        # 5. Approach + sizing (now with ask-based drag gate)
        buy_price = up_best_ask if conv["side"] == "UP" and up_best_ask < 1.0 else (
            down_best_ask if conv["side"] == "DOWN" and down_best_ask < 1.0 else (
                up_price if conv["side"] == "UP" else down_price
            )
        )
        if buy_price <= 0 or buy_price >= 1.0:
            return None, entry_ready_count

        approach, dom_pct, bet_size, max_adds = self._decide_approach(
            conv["conviction"], conv["side"], bankroll, buy_price,
            up_ask=up_best_ask if up_best_ask < 1.0 else up_price + up_spread / 2,
            down_ask=down_best_ask if down_best_ask < 1.0 else down_price + down_spread / 2,
            p_smooth=conv.get("p_smooth", 0.50),
        )

        if approach == "SKIP":
            ask_up = up_best_ask if up_best_ask < 1.0 else up_price + up_spread / 2
            ask_dn = down_best_ask if down_best_ask < 1.0 else down_price + down_spread / 2
            entry_drag = ask_up + ask_dn - 1.0
            self._last_conviction_pipeline["approach"] = "SKIP"
            if entry_drag > 0.04:
                self._last_conviction_pipeline["gate_reason"] = (
                    f"entry drag too high ({entry_drag:.3f})"
                )
            else:
                self._last_conviction_pipeline["gate_reason"] = "approach skip"
            return None, entry_ready_count

        # Kelly/EV telemetry should reflect the same probability model that sized
        # the trade, not an older synthetic conviction→probability mapping.
        p_model = conv.get("p_smooth", 0.50) if conv["side"] == "UP" else (1.0 - conv.get("p_smooth", 0.50))
        p_win = max(0.50, min(0.80, p_model))
        kelly = self._kelly_fraction(p_win, buy_price, use_maker=False, hold_to_settle=True)

        # EV using fee-in-shares model
        cost_per_share = _buy_cost_per_share(buy_price)
        shares = int(bet_size / cost_per_share) if cost_per_share > 0 else 0
        ev = (p_model * shares * 1.0) - (shares * cost_per_share)

        self._last_edge_type = "conviction"
        self._last_conviction_pipeline["approach"] = approach
        self._last_conviction_pipeline["bet_size"] = bet_size

        reason_str = (
            f"p={conv['p_smooth']:.3f} q={conv['q_mid']:.3f} edge={conv['edge']:.3f} "
            f"conv={conv['conviction']:.2f} regime={conv['regime']} "
            f"z={conv['signals']['z']:.2f} "
            f"GBM={gbm_side}@{gbm_prob:.2f} → {approach} {conv['side']} "
            f"${bet_size:.2f} @{buy_price:.3f}"
        )

        return DirectionalSignal(
            side=conv["side"],
            confidence=round(conv.get("p_smooth", 0.50), 4),
            kelly_fraction=round(min(kelly, self.kelly_fraction_cap), 4),
            bet_size=round(bet_size, 2),
            expected_value=round(ev, 4),
            signals=conv["signals"],
            reason=reason_str,
            edge_type="conviction",
            conviction=conv["conviction"],
            approach=approach,
            max_adds=max_adds,
            dominant_pct=dom_pct,
            edge=round(conv.get("edge", 0.0), 4),
            regime=conv.get("regime", "CHOP"),
            p_smooth=round(conv.get("p_smooth", 0.50), 4),
            q_mid=round(conv.get("q_mid", 0.50), 4),
        ), entry_ready_count

    def _calc_realized_volatility(self, lookback_secs: float = 300.0) -> float:
        """Calculate realized volatility from recent BTC ticks.

        Uses log returns from BTC price ticks over the lookback period.
        Returns sigma per sqrt(second), suitable for short-horizon GBM.
        """
        if len(self._btc_prices) < 10:
            return 0.0

        now = time.time()
        cutoff = now - lookback_secs
        recent = [p for p in self._btc_prices if p.timestamp >= cutoff]

        if len(recent) < 5:
            return 0.0

        # Compute log returns with time deltas
        log_returns = []
        for i in range(1, len(recent)):
            if recent[i - 1].price > 0 and recent[i].price > 0:
                dt = recent[i].timestamp - recent[i - 1].timestamp
                if dt > 0:
                    lr = math.log(recent[i].price / recent[i - 1].price)
                    log_returns.append((lr, dt))

        if len(log_returns) < 3:
            return 0.0

        # Variance of log returns per second
        total_dt = sum(dt for _, dt in log_returns)
        if total_dt <= 0:
            return 0.0
        mean_lr_per_sec = sum(lr for lr, _ in log_returns) / total_dt
        var_per_sec = sum(
            (lr / dt - mean_lr_per_sec) ** 2 * dt for lr, dt in log_returns
        ) / total_dt

        return math.sqrt(max(var_per_sec, 1e-20))  # sigma per sqrt(second)

    def _estimate_probability(
        self,
        up_price: float,
        down_price: float,
        time_remaining: int,
        interval_secs: int,
    ) -> tuple[str, float, dict]:
        """Estimate P(BTC goes up) using drift + volatility model.

        Model: BTC follows geometric Brownian motion over remaining window.
        P(up) = Phi((mu * T) / (sigma * sqrt(T)))

        Where:
          mu    = estimated drift per second (from recent price ticks)
          sigma = realized volatility per sqrt(second)
          T     = time remaining in seconds
          Phi   = standard normal CDF

        Returns: (side, p_estimated, pipeline_data)
        """
        pipeline: dict = {}

        # --- CEX FEEDS stage ---
        if len(self._btc_prices) < 10:
            pipeline["status"] = "insufficient_ticks"
            return "NONE", 0.5, pipeline

        now = time.time()
        lookback = min(300.0, interval_secs * 0.8)  # Up to 5 min of data
        recent = [p for p in self._btc_prices if p.timestamp >= now - lookback]

        if len(recent) < 5:
            pipeline["status"] = "insufficient_recent"
            return "NONE", 0.5, pipeline

        total_time = recent[-1].timestamp - recent[0].timestamp
        if total_time <= 0 or recent[0].price <= 0:
            pipeline["status"] = "no_time_span"
            return "NONE", 0.5, pipeline

        # Drift per second (log return rate)
        mu = math.log(recent[-1].price / recent[0].price) / total_time

        # Realized volatility per sqrt(second)
        sigma = self._calc_realized_volatility(lookback)

        pipeline["cex_btc"] = round(recent[-1].price, 2)
        pipeline["cex_mu"] = mu
        pipeline["cex_sigma"] = sigma
        pipeline["cex_ticks"] = len(recent)
        pipeline["cex_lookback"] = round(total_time, 1)

        T = float(time_remaining)
        if T <= 0 or sigma <= 0:
            pipeline["status"] = "no_vol"
            return "NONE", 0.5, pipeline

        # P(up) = Phi(mu * T / (sigma * sqrt(T))) = Phi(mu * sqrt(T) / sigma)
        z_score = (mu * T) / (sigma * math.sqrt(T))
        p_up = _norm_cdf(z_score)

        # Clamp to [0.01, 0.99] for numerical safety
        p_up = max(0.01, min(0.99, p_up))

        pipeline["z_score"] = round(z_score, 4)
        pipeline["p_up_model"] = round(p_up, 4)
        pipeline["p_down_model"] = round(1.0 - p_up, 4)

        # --- PM ODDS stage ---
        p_market_up = up_price
        p_market_down = down_price
        pipeline["pm_up"] = round(p_market_up, 4)
        pipeline["pm_dn"] = round(p_market_down, 4)
        pipeline["pm_implied"] = round(p_market_up * 100, 1)  # implied % for UP

        # --- EDGE stage ---
        edge_up = p_up - p_market_up
        edge_down = (1.0 - p_up) - p_market_down

        pipeline["edge_up"] = round(edge_up, 4)
        pipeline["edge_dn"] = round(edge_down, 4)

        if edge_up > edge_down and edge_up > 0:
            side = "UP"
            p_estimated = p_up
            edge = edge_up
            buy_price = up_price
        elif edge_down > 0:
            side = "DOWN"
            p_estimated = 1.0 - p_up
            edge = edge_down
            buy_price = down_price
        else:
            # No positive edge on either side
            pipeline["edge"] = 0.0
            pipeline["side"] = "NONE"
            pipeline["status"] = "no_edge"
            return "NONE", 0.5, pipeline

        pipeline["side"] = side
        pipeline["p_est"] = round(p_estimated, 4)
        pipeline["edge"] = round(edge, 4)
        pipeline["buy_price"] = round(buy_price, 4)
        pipeline["status"] = "signal"

        return side, p_estimated, pipeline

    def _compute_composite(
        self,
        micro_mom: float, short_mom: float, medium_mom: float,
        accel: float, book_imb: float, reprice_lag: float,
        market_skew: float, time_regime: str,
    ) -> float:
        """Compute composite score based on mode and market regime."""

        if self.mode == "momentum":
            if time_regime == "late":
                return micro_mom * 0.6 + short_mom * 0.3 + accel * 0.1
            elif time_regime == "mid":
                return short_mom * 0.5 + micro_mom * 0.3 + medium_mom * 0.2
            else:
                return medium_mom * 0.5 + short_mom * 0.3 + micro_mom * 0.2

        elif self.mode == "contrarian":
            return -market_skew * 2.0 + short_mom * 0.2

        elif self.mode == "book_imbalance":
            return book_imb * 0.6 + reprice_lag * 0.3 + short_mom * 0.1

        else:  # composite — full signal blend
            if time_regime == "late":
                # Near expiry: micro momentum + latency lag dominate
                return (
                    micro_mom * 0.30 +
                    reprice_lag * 0.25 +
                    short_mom * 0.20 +
                    accel * 0.10 +
                    book_imb * 0.10 +
                    market_skew * 0.05
                )
            elif time_regime == "mid":
                # Middle: balanced signals
                return (
                    short_mom * 0.25 +
                    reprice_lag * 0.20 +
                    micro_mom * 0.15 +
                    medium_mom * 0.15 +
                    book_imb * 0.15 +
                    market_skew * 0.10
                )
            else:
                # Early: medium-term trend + book structure
                return (
                    medium_mom * 0.30 +
                    short_mom * 0.20 +
                    book_imb * 0.20 +
                    reprice_lag * 0.15 +
                    market_skew * 0.10 +
                    micro_mom * 0.05
                )

    # ------------------------------------------------------------------
    # Kelly Criterion
    # ------------------------------------------------------------------

    def _kelly_fraction(self, p: float, buy_price: float, use_maker: bool = False,
                        spread: float = -1.0, hold_to_settle: bool = False) -> float:
        """Friction-aware Kelly fraction for a Polymarket binary bet.

        buy_price is the ASK (executable price), so no half_spread addition.
        Uses fee-in-shares model: cost = ask / (1 - fr).
        Settlement payout = $1/share (no fee on winning).

        Kelly: f* = (p*b - q) / b where b = net_win/cost
        """
        q = 1.0 - p
        # buy_price is the ask (executable price), so no half_spread addition
        effective_cost = _buy_cost_per_share(buy_price)
        if hold_to_settle:
            # HEDGED mode: hold to settlement, payout is exactly $1, no sell friction
            net_payout = 1.0
        else:
            # If spread not explicitly passed, estimate from typical Polymarket BTC 5m
            if spread < 0:
                spread = max(0.02, buy_price * 0.04)  # ~2-4% of price, floor 2¢
            # Minimal early-exit friction (we mostly hold to settlement)
            sell_friction = max(0.0015, spread * 0.15)
            net_payout = 1.0 - sell_friction  # settlement $1/share, no fee on winning
        net_win = net_payout - effective_cost

        if net_win <= 0 or effective_cost <= 0:
            return 0.0

        b = net_win / effective_cost
        kelly = (p * b - q) / b

        return max(0.0, min(1.0, kelly))

    def empirical_kelly(
        self,
        p_est: float,
        buy_price: float,
        trade_history: list[dict],
        bankroll: float,
        use_maker: bool = True,
        kelly_mult: float = 0.67,
        spread: float = 0.0,
    ) -> dict:
        """Empirical Kelly with uncertainty haircut — from Roan's hedge fund article.

        Instead of trusting point-estimate edge blindly, we:
        1. Compute raw Kelly fraction f* from our model's p_est
        2. Measure uncertainty in our edge from actual trade history (CV of returns)
        3. Apply haircut: f_empirical = f_kelly × (1 - CV_edge) × safety_factor
        4. Monte Carlo resampling of past returns → 95th percentile drawdown guard

        For small bankrolls ($10-20), this is critical: one overbet = ruin.

        Args:
            p_est: Model's estimated probability of winning
            buy_price: Current market price for the side we want to buy
            trade_history: List of past trade dicts with 'pnl' and 'investment' keys
            bankroll: Current bankroll in dollars
            use_maker: Whether we're using maker orders (0 fee) vs taker

        Returns: dict with kelly_raw, kelly_empirical, cv_edge, haircut,
                 drawdown_95, alloc_dollars, and reasoning
        """
        import random

        result = {
            "kelly_raw": 0.0,
            "kelly_empirical": 0.0,
            "cv_edge": 0.0,
            "haircut": 0.0,
            "drawdown_95": 0.0,
            "alloc_dollars": 0.0,
            "reasoning": "",
        }

        # Step 1: Raw Kelly (friction-aware with live spread data)
        kelly_raw = self._kelly_fraction(p_est, buy_price, use_maker, spread=spread)
        result["kelly_raw"] = round(kelly_raw, 4)

        if kelly_raw <= 0:
            result["reasoning"] = "negative EV"
            return result

        # Step 2: Measure uncertainty from trade history
        # Need at least 10 trades to have meaningful statistics
        returns = []
        for t in trade_history:
            inv = t.get("investment", 0) or t.get("cost", 0)
            pnl = t.get("pnl", 0)
            if inv and inv > 0:
                returns.append(pnl / inv)  # return as fraction of investment

        if len(returns) < 5:
            # Not enough history — use maximum conservatism (quarter-Kelly)
            safety = 0.25
            kelly_emp = kelly_raw * safety
            result["kelly_empirical"] = round(kelly_emp, 4)
            result["haircut"] = round(1.0 - safety, 4)
            result["reasoning"] = f"<5 trades, quarter-Kelly safety"
            result["alloc_dollars"] = round(
                max(1.01, min(bankroll * kelly_emp, bankroll * 0.15)), 2
            )
            return result

        # CV of returns = std(returns) / |mean(returns)|
        mean_ret = sum(returns) / len(returns)
        var_ret = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        std_ret = math.sqrt(var_ret) if var_ret > 0 else 0

        if abs(mean_ret) < 0.001:
            # Mean return ~0, edge is noise — be very conservative
            cv_edge = 2.0
        else:
            cv_edge = std_ret / abs(mean_ret)

        result["cv_edge"] = round(cv_edge, 3)

        # Step 3: Haircut formula from article
        # f_empirical = f_kelly × (1 - CV_edge)
        # Kelly base: controlled by aggression level (0.50 conservative → 0.85 aggressive)
        uncertainty_factor = max(0.15, 1.0 - cv_edge)  # floor at 15%
        base_kelly = kelly_raw * kelly_mult  # from aggression setting
        kelly_emp = base_kelly * uncertainty_factor

        # Small bankroll guard: scale down further if bankroll < $20
        if bankroll < 20:
            bankroll_factor = max(0.6, bankroll / 20.0)
            kelly_emp *= bankroll_factor

        result["kelly_empirical"] = round(kelly_emp, 4)
        result["haircut"] = round(1.0 - (kelly_emp / kelly_raw) if kelly_raw > 0 else 0, 3)

        # Step 4: Monte Carlo drawdown estimate (lightweight — 500 paths)
        # Resample our actual returns to estimate 95th percentile max drawdown
        n_sims = 500
        n_trades = min(30, len(returns))  # simulate next 30 trades
        max_drawdowns = []

        for _ in range(n_sims):
            equity = 1.0
            peak = 1.0
            max_dd = 0.0
            for _ in range(n_trades):
                ret = random.choice(returns)
                equity *= (1.0 + ret * kelly_emp)  # apply sized return
                if equity > peak:
                    peak = equity
                dd = (peak - equity) / peak if peak > 0 else 0
                if dd > max_dd:
                    max_dd = dd
            max_drawdowns.append(max_dd)

        max_drawdowns.sort()
        dd_95 = max_drawdowns[int(len(max_drawdowns) * 0.95)] if max_drawdowns else 0
        result["drawdown_95"] = round(dd_95, 3)

        # If 95th percentile drawdown > 50%, scale down further
        # Mildly aggressive: allow up to 50% DD before scaling (was 40%)
        if dd_95 > 0.50:
            dd_scale = 0.50 / dd_95
            kelly_emp *= dd_scale
            result["kelly_empirical"] = round(kelly_emp, 4)
            result["haircut"] = round(1.0 - (kelly_emp / kelly_raw) if kelly_raw > 0 else 0, 3)

        # Final allocation in dollars
        alloc = bankroll * kelly_emp
        # Floor at CLOB minimum, cap at 20% of bankroll (mildly aggressive)
        max_alloc_pct = 0.18 if bankroll < 20 else 0.22
        alloc = max(1.01, min(alloc, bankroll * max_alloc_pct))
        result["alloc_dollars"] = round(alloc, 2)

        win_count = sum(1 for r in returns if r > 0)
        result["reasoning"] = (
            f"{len(returns)} trades, WR={win_count}/{len(returns)}, "
            f"CV={cv_edge:.2f}, DD95={dd_95:.0%}, "
            f"raw={kelly_raw:.3f}→emp={kelly_emp:.3f}"
        )

        return result

    @staticmethod
    def calibration_edge(buy_price: float) -> dict:
        """Calibration surface bias from Becker/Roan research on 72M+ trades.

        Key findings:
        - Longshots (< $0.15): takers lose 57%, massive overpricing
        - Mid-range ($0.30-$0.55): only 2-3% mispricing, market efficient
        - Value zone ($0.15-$0.30): sweet spot — enough bias to exploit
        - High-prob ($0.55-$0.75): moderate taker disadvantage, tradeable

        Returns calibration metadata for the Kelly mode decision.
        """
        if buy_price < 0.10:
            # Extreme longshot — takers lose huge here, but we're buying
            # so we're the taker. Actually bad for us unless we're making market.
            return {
                "zone": "extreme_longshot",
                "taker_bias": -0.57,  # -57% mispricing
                "quality": "avoid",
                "size_mult": 0.0,  # don't trade here as taker
                "reason": "extreme longshot, takers lose 57%",
            }
        elif buy_price < 0.20:
            # Cheap longshot — if our model says it's underpriced, edge is real
            return {
                "zone": "cheap_value",
                "taker_bias": -0.20,
                "quality": "good",
                "size_mult": 1.2,  # slight boost — value zone
                "reason": "cheap value zone, good if model agrees",
            }
        elif buy_price < 0.30:
            # Value zone — historically where copy+model combos work best
            return {
                "zone": "value",
                "taker_bias": -0.10,
                "quality": "best",
                "size_mult": 1.3,  # best zone for our strategy
                "reason": "sweet spot value zone $0.20-0.30",
            }
        elif buy_price < 0.55:
            # Efficient middle — market is well-calibrated here
            return {
                "zone": "efficient_mid",
                "taker_bias": -0.025,
                "quality": "poor",
                "size_mult": 0.6,  # reduce size, thin edge
                "reason": "efficient mid-range, only 2-3% taker bias",
            }
        elif buy_price < 0.75:
            # High-probability zone — moderate taker disadvantage
            return {
                "zone": "high_prob",
                "taker_bias": -0.05,
                "quality": "decent",
                "size_mult": 1.0,  # standard size
                "reason": "high-prob zone, moderate edge",
            }
        else:
            # Near-certainty — very expensive, low upside
            return {
                "zone": "expensive",
                "taker_bias": -0.02,
                "quality": "avoid",
                "size_mult": 0.0,
                "reason": "too expensive, minimal upside",
            }
