"""
Continuous 24/7 runner for BTC 5m and 15m Polymarket Up/Down markets.

Combines:
  - Market discovery (5m + 15m windows)
  - Live price feeds from CLOB order books
  - Directional betting engine (momentum/Kelly)
  - Auto-execution (dry-run or live)
  - Console dashboard with live updates

Usage:
    python -m src.runner                 # Dry run, composite mode
    python -m src.runner --live          # Live trading (careful!)
    python -m src.runner --mode momentum # Momentum-only signals
"""

import asyncio
import copy as _copy
import json
import math
import os
import re
import sys
import time
import logging
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

import httpx
from aiohttp import web

from .config import config
from .directional import (
    DirectionalEngine, DirectionalSignal,
    _fetch_fee_params, _buy_cost_per_share, _sell_proceeds_per_share,
    FEE_MIN_USDC,
)
from .history import history
from .polymarket import create_client as create_poly_client, _redeem_position_value
from .instrumentation import perf
from .async_db import db_queue
from .runtime_config import RuntimeConfig, should_enter as policy_should_enter

logger = logging.getLogger(__name__)

# Handle broken pipe gracefully — prevents crash when stdout pipe closes
# (e.g. when started via `python -m src.runner | head -5`)
# Redirect stdout to devnull if the pipe is broken, so print() calls
# silently no-op instead of raising BrokenPipeError every loop iteration.
def _install_pipe_guard():
    """Replace stdout with a pipe-safe wrapper that silences BrokenPipeError."""
    _real_write = sys.stdout.write
    _real_flush = sys.stdout.flush
    def _safe_write(s):
        try:
            return _real_write(s)
        except BrokenPipeError:
            # Pipe closed — redirect all future output to devnull
            sys.stdout = open(os.devnull, "w")
            return 0
    def _safe_flush():
        try:
            return _real_flush()
        except BrokenPipeError:
            sys.stdout = open(os.devnull, "w")
    sys.stdout.write = _safe_write
    sys.stdout.flush = _safe_flush
_install_pipe_guard()


# ───── Discord webhook helper ─────
def _discord_post(webhook_url: str, content: str = "", embeds: list | None = None) -> bool:
    """Fire-and-forget Discord webhook post. Returns True on success."""
    if not webhook_url:
        return False
    try:
        payload: dict = {}
        if content:
            payload["content"] = content
        if embeds:
            payload["embeds"] = embeds
        r = httpx.post(webhook_url, json=payload, timeout=5.0)
        return r.status_code in (200, 204)
    except Exception as e:
        logger.debug(f"Discord webhook error: {e}")
        return False

logger = logging.getLogger(__name__)

DASHBOARD_DIR = Path(__file__).parent.parent / "dashboard"
WEB_PORT = 8420

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

# WebSocket feeds — real-time, no polling
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

# Chainlink BTC/USD on Polygon — this is the RESOLUTION SOURCE for Polymarket
# latestRoundData() returns (roundId, answer, startedAt, updatedAt, answeredInRound)
CHAINLINK_BTC_USD = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
CHAINLINK_LATEST_ROUND = "0xfeaf968c"  # latestRoundData() selector
POLYGON_RPC_URL = "https://polygon-bor-rpc.publicnode.com"

INTERVALS = {
    "5m": 300,
    "15m": 900,
}

DATA_API = "https://data-api.polymarket.com"

def _ts_to_str(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")


def _clear_line():
    print("\033[2K\r", end="", flush=True)


def _metric_slug(text: str, max_len: int = 30) -> str:
    """Convert free-text reason strings into stable metric-safe suffixes."""
    slug = re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")
    if not slug:
        return "unknown"
    return slug[:max_len].rstrip("_")


def _gate_metric_key(reason: str) -> str:
    """Map gate reasons to stable metric keys."""
    reason_l = (reason or "").lower()
    if not reason_l:
        return ""
    if "conviction too low" in reason_l:
        return "gate_conviction_low"
    if "no directional" in reason_l:
        return "gate_no_direction"
    if "choppy regime" in reason_l:
        return "gate_chop_regime"
    if "price div" in reason_l:
        return "gate_price_div"
    if "edge" in reason_l and "slope" in reason_l:
        return "gate_edge_slope"
    if "edge" in reason_l or "waiting for edge" in reason_l:
        return "gate_edge_low"
    if "signal strength" in reason_l:
        return "gate_signal_strength"
    if "quality" in reason_l:
        return "gate_quote_quality"
    if "hysteresis" in reason_l:
        return "gate_hysteresis"
    if "early window" in reason_l:
        return "gate_time_early"
    if "prime window" in reason_l:
        return "gate_time_prime"
    if "mid-late window" in reason_l:
        return "gate_time_mid"
    if "too late" in reason_l:
        return "gate_time_late"
    if "drag" in reason_l:
        return "gate_drag_high"
    if "approach skip" in reason_l:
        return "gate_approach_skip"
    return f"gate_other_{_metric_slug(reason_l)}"


class MarketWindow:
    """Tracks a single 5m or 15m market window."""
    __slots__ = [
        "asset", "interval_label", "interval_secs", "slug", "title",
        "start_ts", "end_ts", "up_token", "down_token",
        "condition_id",       # conditionId for CTF split/merge operations
        "up_price", "down_price", "up_depth", "down_depth",
        "up_last", "down_last", "up_spread", "down_spread",
        "up_best_bid", "down_best_bid",  # actual best bid for sell pricing
        "status", "result", "bet_side", "bet_shares", "bet_price",
        "bet_order_id", "pnl", "price_history", "btc_at_open", "btc_at_close",
        "total_volume", "neg_risk", "peak_profit_pct",
        "exit_price", "exit_reason",
        "bet_ts",  # timestamp when position was entered (for min hold time)
        "up_price_direct", "down_price_direct",  # set ONLY by direct WS token match (not complement)
        # v2 scalp lifecycle fields
        "scalp_mode",         # WAIT / ENTER / SCALP / LOCK / SETTLE
        "approach",           # AGGRESSIVE / HEDGED / CONSERVATIVE / SKIP
        "trade_count",        # orders placed this window
        "max_adds",           # max add-on trades allowed
        "up_shares", "down_shares",  # inventory tracking
        "up_cost", "down_cost",      # total USDC spent per side (for VWAP)
        "open_orders",        # dict: order_id -> {side, price, size, placed_at}
        "realized_pnl",       # profit from intra-window sells
        "total_cost",         # total USDC deployed this window
        "conviction_history", # list of recent conviction scores
        "_last_add_ts",       # timestamp of last ADD order (throttle)
        "_last_trim_ts",      # timestamp of last TRIM order (throttle)
        "_last_lock_sell_ts", # timestamp of last LOCK sell attempt (throttle)
        "_needs_initial_hedge",  # hedge failed at entry, needs retry via ADDs
        "_last_act_log",      # timestamp of last should_act() log (visibility)
        # v2 HEDGED fields
        "up_best_ask", "down_best_ask",  # actual best ask for buy pricing
        "budget",             # total USDC allocated this window (from Kelly)
        "total_spent",        # actual USDC spent (from fills, not submits)
        "_fees_fetched",      # whether fee params have been fetched for this token
        "_last_ws_recv_mono", # monotonic timestamp of last WS price update (latency instrument.)
        "_last_feature_log",  # timestamp of last feature log write (throttle)
        # P1: periodic share reconciliation throttle + two-read confirmation
        "_last_reconcile_ts",
        "_reconcile_pending_up",   # pending low balance for UP (0=none)
        "_reconcile_pending_dn",   # pending low balance for DN (0=none)
        # P3: intra-window rebalancing
        "reserve_budget",
        "_last_rehedge_ts",
        # P4: per-window hysteresis counter
        "_entry_ready_count",
        # P5: advisor parameter adjustments
        "_advisor_params",
        "_advisor_params_ts",
        # CUT cooldown: prevent whipsaw re-entry
        "_last_cut_ts",
        # Rehedge hysteresis: consecutive ticks above soft threshold
        "_rh_consec",
        # Entry timestamp for overhedge hold gate (prevent instant overhedge trim)
        "_entry_fill_ts",
        # Post-reinvest CUT cooldown (prevent TRIM→REINVEST→CUT chain)
        "_last_reinvest_ts",
        # CUT hysteresis: require N consecutive cut-worthy cycles before firing
        "_cut_consec",
        # DIRECTIONAL reversal persistence: wall-clock seconds p_smooth opposes bet
        "_reversal_start_ts",
        # Staged cut tracking: first cut trims partial, second cut completes
        "_partial_cut_done",
        "_partial_cut_ts",
        # Wallet signal: Abrak's direction at entry (for wallet-flip detection)
        "_wallet_entry_direction",
        # Wallet opposition % at entry (for delta-opposition gating)
        "_wallet_entry_against_pct",
        # CUT hysteresis: wall-clock timestamp of first CUT signal
        "_cut_first_ts",
        # Stage-1 cut snapshots for stage-2 deterioration check
        "_stage1_mark_ratio",
        "_stage1_p_smooth",
        "_recovery_consec",  # consecutive non-CUT cycles after stage-1 (persistence check)
        "_stage1_count",     # number of stage-1 partial cuts on this position (escalate after 2)
        # Early flip TP: partial sell + re-check delay (#1, #5)
        "_flip_tp_first_ts",
        "_flip_tp_partial_done",
        "_entry_side",         # side at entry (for exit tracking)
        # S2 deterioration: mark_ratio history [(ts, ratio), ...]
        "_mark_ratio_history",
        # UP-bias entry tag
        "_up_bias_entry",      # True if entered during UP-bias window
        "_up_bias_mode",       # "soft", "hard", or "flip" at entry time
        "_edge_history",       # per-window edge history for early-slope gating
    ]

    def __init__(self, asset, interval_label, interval_secs, slug, title,
                 start_ts, end_ts, up_token, down_token, neg_risk=False,
                 condition_id=""):
        self.asset = asset
        self.interval_label = interval_label
        self.interval_secs = interval_secs
        self.slug = slug
        self.title = title
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.up_token = up_token
        self.down_token = down_token
        self.condition_id = condition_id
        self.up_price = 0.0
        self.down_price = 0.0
        self.up_depth = 0
        self.down_depth = 0
        self.up_last = 0.0
        self.down_last = 0.0
        self.up_spread = 0.0
        self.down_spread = 0.0
        self.up_best_bid = 0.0     # actual highest bid on CLOB for sell pricing
        self.down_best_bid = 0.0   # actual highest bid on CLOB for sell pricing
        self.status = "active"  # active, closed, bet_placed
        self.result = ""        # UP_WON, DOWN_WON, pending
        self.bet_side = ""      # UP or DOWN
        self.bet_shares = 0
        self.bet_price = 0.0
        self.bet_order_id = ""  # On-chain order ID (empty = dry-run bet)
        self.pnl = 0.0
        # Per-event tracking
        self.price_history = []  # list of {t, up, dn, upD, dnD, btc}
        self.btc_at_open = 0.0
        self.btc_at_close = 0.0
        self.total_volume = 0
        self.neg_risk = neg_risk
        self.peak_profit_pct = 0.0  # High water mark for trailing profit lock
        self.exit_price = 0.0       # Price at exit (for display/logging)
        self.exit_reason = ""       # Reason for exit (for display/logging)
        self.bet_ts = 0.0           # Timestamp when position was entered
        self.up_price_direct = 0.0   # Set ONLY by direct WS book match (not complement)
        self.down_price_direct = 0.0 # Set ONLY by direct WS book match (not complement)
        # v2 scalp lifecycle
        self.scalp_mode = "WAIT"
        self.approach = ""
        self.trade_count = 0
        self.max_adds = 5
        self.up_shares = 0.0
        self.down_shares = 0.0
        self.up_cost = 0.0
        self.down_cost = 0.0
        self.open_orders = {}
        self.realized_pnl = 0.0
        self.total_cost = 0.0
        self.conviction_history = []
        self._last_add_ts = 0.0
        self._last_trim_ts = 0.0
        self._last_lock_sell_ts = 0.0
        self._needs_initial_hedge = False
        self._last_act_log = 0.0
        # v2 HEDGED fields
        self.up_best_ask = 1.0   # actual best ask for buy pricing
        self.down_best_ask = 1.0
        self.budget = 0.0        # total USDC allocated this window (from Kelly)
        self.total_spent = 0.0   # actual USDC spent (tracked from fills)
        self._fees_fetched = False
        self._last_feature_log = 0.0
        self._last_ws_recv_mono = time.monotonic()
        # P1: periodic share reconciliation throttle
        # Init to current time so first reconcile waits 30s (warmup period)
        self._last_reconcile_ts = time.time()
        # Two-read confirmation before ratcheting shares down
        self._reconcile_pending_up = 0.0   # pending low balance for UP (0=none)
        self._reconcile_pending_dn = 0.0   # pending low balance for DN (0=none)
        # P3: intra-window rebalancing
        self.reserve_budget = 0.0
        self._last_rehedge_ts = 0.0
        # P4: per-window hysteresis counter
        self._entry_ready_count = 0
        # P5: advisor parameter adjustments
        self._advisor_params = {}
        self._advisor_params_ts = 0.0
        # CUT cooldown: prevent whipsaw re-entry
        self._last_cut_ts = 0.0
        # Rehedge hysteresis: consecutive ticks above soft threshold
        self._rh_consec = 0
        # Entry timestamp for overhedge hold gate
        self._entry_fill_ts = 0.0
        # Post-reinvest CUT cooldown
        self._last_reinvest_ts = 0.0
        # CUT hysteresis: consecutive cycles where cut condition was true
        self._cut_consec = 0
        # DIRECTIONAL reversal persistence (wall-clock)
        self._reversal_start_ts = 0.0
        # Staged cut tracking
        self._partial_cut_done = False
        self._partial_cut_ts = 0.0
        self._stage1_count = 0
        self._wallet_entry_direction = ""
        self._wallet_entry_against_pct = 0.0
        self._cut_first_ts = 0.0
        self._stage1_mark_ratio = 0.0
        self._stage1_p_smooth = 0.50
        self._recovery_consec = 0
        self._flip_tp_first_ts = 0.0
        self._flip_tp_partial_done = False
        self._entry_side = ""
        self._mark_ratio_history = []  # [(ts, mark_ratio), ...] for deterioration speed
        self._up_bias_entry = False
        self._up_bias_mode = ""
        self._edge_history = []

    @property
    def time_remaining(self) -> int:
        return max(0, int(self.end_ts - time.time()))

    @property
    def total_ask(self) -> float:
        """Combined cost to buy both sides (sum of prices)."""
        return self.up_price + self.down_price


class ContinuousRunner:
    """Main 24/7 runner for 5m + 15m BTC markets."""

    def __init__(
        self,
        dry_run: bool = True,
        mode: str = "composite",
        bankroll: float = 10.0,
        kelly_cap: float = 0.25,
        min_confidence: float = 0.52,
        live_min_conviction: float = 0.05,
        max_bet_pct: float = 0.25,
        max_bet_dollars: float = 2.00,
        assets: list[str] = None,
        intervals: list[str] = None,
        max_bet_explicit: bool = False,
    ):
        self.dry_run = dry_run
        self.assets = assets or ["btc"]
        self.interval_labels = intervals or ["5m"]
        self._max_bet_explicit = max_bet_explicit  # True = CLI value wins over persisted

        # Directional engine
        self.engine = DirectionalEngine(
            bankroll=bankroll,
            mode=mode,
            kelly_fraction_cap=kelly_cap,
            min_confidence=min_confidence,
            max_bet_pct=max_bet_pct,
            max_bet_dollars=max_bet_dollars,
        )

        # State
        self._active_windows: dict[str, MarketWindow] = {}  # slug -> window
        self._upcoming_windows: dict[str, MarketWindow] = {}  # slug -> upcoming
        self._closed_windows: list[MarketWindow] = []        # Recent closed
        self._http: httpx.AsyncClient | None = None
        self._running = False
        self._cycle_count = 0
        self._btc_price = 0.0

        self._trade_mode = "kelly"
        self._kelly_sniped_slugs: set[str] = set()   # kelly mode: one-shot per window
        self._kelly_decisions: list[dict] = []          # recent kelly decisions for dashboard (max 20)
        self._kelly_live: dict = {}                      # real-time kelly eval for current window
        # Kelly mode: single "aggressiveness" control (1-5)
        # 1=conservative, 3=balanced, 5=aggressive
        # Maps to: entry timing, min edge, kelly multiplier, zone tolerance
        self._kelly_aggression: int = 3       # default: balanced
        self._kelly_min_kelly: float = 0.0    # minimum Kelly fraction (>0 means positive EV)
        # Derived from aggression level (computed dynamically):
        #   aggr=1: entry@60s, edge>0.08, kelly×0.50, skip mid
        #   aggr=2: entry@75s, edge>0.05, kelly×0.60, skip mid
        #   aggr=3: entry@90s, edge>0.03, kelly×0.67, trade mid@0.6x
        #   aggr=4: entry@105s, edge>0.02, kelly×0.75, trade mid@0.8x
        #   aggr=5: entry@120s, edge>0.01, kelly×0.85, trade mid@1.0x
        self._bot_paused: bool = False                # remote pause/resume flag
        self._time_gate_paused: bool = False          # gate forced us into dry mode

        # ── Centralized runtime config ──
        self.rcfg = RuntimeConfig()
        # Legacy aliases (will be removed once all references migrate)
        self._time_gate_enabled = self.rcfg.time_gate_enabled
        self._time_gate_hours = self.rcfg.time_gate_hours
        self._live_min_conviction = self.rcfg.live_min_conviction

        # Wallet confirmation signal (Abrak wallet)
        from src.wallet_signal import WalletSignal
        self.wallet_signal = WalletSignal(enabled=self.rcfg.wallet_confirm_enabled)
        # Copy snipe tuneables — adjustable from dashboard
        self._settle_cooldown_until = 0.0  # Don't trade until this time (wait for token redemption)
        self._discord_webhook = ""  # Will be loaded from state file or env
        self._state_file = Path(__file__).parent.parent / ".bot_state.json"
        self._load_persisted_state()

        # Telegram command bot
        self._tg_token = config.telegram_token or ""
        self._tg_chat_id = config.telegram_chat_id or ""
        self._tg_offset = 0     # getUpdates offset (last processed update_id + 1)
        self._tg_last_poll = 0.0
        self._tg_poll_interval = 3.0  # seconds between polls
        self._tg_enabled = bool(self._tg_token and self._tg_chat_id)


        # Feature logging for offline calibration
        import collections as _collections
        self._feature_log_q = _collections.deque()  # buffered feature rows
        self._feature_log_last_flush = 0.0

        # ── Recent-result ring buffer for risk scaling ──
        # Stores last N window outcomes: (side, pnl) tuples.
        # Used by time-of-day risk scaling and streak guard.
        self._result_ring: list = []   # [(side, pnl), ...] most recent last
        self._result_ring_max = 8      # keep last 8 outcomes

        # ── One-way streak guard ──
        # Tracks consecutive same-side entries with net-negative P&L.
        self._streak_side: str = ""
        self._streak_count: int = 0
        self._streak_pnl: float = 0.0

        # ── Reliability metrics (Fix 8) ──
        self._metrics = {
            "trim_attempts": 0, "trim_successes": 0, "trim_blocked": 0,
            "trim_pnl_total": 0.0,
            "flip_detected": 0, "flip_soft": 0, "flip_hard": 0,
            "flip_latency_sum": 0.0,  # cumulative flip detection → action latency
            "sell_attempts": 0, "sell_successes": 0, "sell_failures": 0,
            "sell_bal_mismatch": 0, "sell_deferred": 0,
            "lock_attempts": 0, "lock_successes": 0,
            "entry_skipped_drag": 0, "entry_skipped_ev": 0,
            "entry_skipped_divergence": 0, "entry_skipped_edge_slope": 0,
            # ── Gate funnel counters (reason attribution) ──
            "gate_conviction_low": 0, "gate_no_direction": 0,
            "gate_chop_regime": 0, "gate_price_div": 0,
            "gate_edge_low": 0, "gate_signal_strength": 0,
            "gate_quote_quality": 0, "gate_hysteresis": 0,
            "gate_edge_slope": 0, "gate_time_early": 0,
            "gate_time_prime": 0, "gate_time_late": 0, "gate_time_mid": 0,
            "gate_approach_skip": 0, "gate_drag_high": 0,
            "gate_pathological": 0, "gate_ev_block": 0,
            "gate_passed": 0,  # successfully passed all gates
            "add_attempts": 0, "add_successes": 0,
            "hedge_attempts": 0, "hedge_successes": 0,
            "windows_total": 0, "windows_profitable": 0,
            "window_pnl_total": 0.0,
            "quote_stale_at_decision": 0,  # times bid was 0 when we needed it
            "advisor_calls": 0, "advisor_applied": 0, "advisor_ignored": 0,
            "advisor_timeouts": 0, "advisor_parse_fails": 0,
            # P2+: new metric keys
            "quote_pathological": 0,      # P2: impossible quote detection
            "rehedge_attempts": 0,        # P3: rehedge buy attempts
            "rehedge_successes": 0,       # P3: rehedge buy fills
            "advisor_param_applied": 0,   # P5: param adjustments applied
            "advisor_param_rejected": 0,  # P5: param adjustments out-of-bounds
            # Diagnostic: reconcile + cut stability
            "reconcile_pending": 0,       # reconcile needed 2nd confirmation
            "reconcile_zero_hit": 0,      # reconcile would have zeroed shares (blocked)
            "zero_inventory_with_spent": 0,  # engine detected phantom zero-share state
            "cut_hysteresis_block": 0,    # CUT blocked by hysteresis requirement
            "cut_zero_ratio": 0,          # CUT with settle/spent ≈ 0 (likely stale)
            "flip_roll_attempts": 0,      # FLIP_ROLL sell+buy rotation attempts
            "flip_roll_successes": 0,     # FLIP_ROLL successful buy leg fills
            # ── Staged-cut counters (explicit init) ──
            "cut_staged_partial": 0,      # Stage-1 partial trims executed
            "cut_staged_full": 0,         # Stage-2 full liquidations
            "cut_staged_recovered": 0,    # Positions recovered after stage-1
            "flip_tp_recovered": 0,       # Early flip-TP positions recovered
            "dir_flip_exits": 0,          # DIRECTIONAL flip-exit count
            # ── Risk scaling + streak ──
            "risk_scale_half": 0,         # 01-03 ET half-size entries
            "gate_streak_block": 0,       # Entries blocked by streak guard
            "gate_wallet_disagree": 0,    # Entries blocked by wallet disagreement
            "wallet_confirm": 0,          # Entries confirmed by wallet
            "wallet_no_position": 0,      # Wallet had no position
            "wallet_sizing_applied": 0,   # Wallet sizing multiplier applied
            "entry_high_price_damped": 0, # High-price dampener applied
            "buy_fok_downsize_retries": 0, # BUY FOK retries with reduced notional
            "buy_fok_downsize_success": 0, # BUY fills salvaged by downsizing retry
            # ── Gate counters ──
            "gate_abrak_inactive": 0,    # Entries blocked — Abrak below share threshold
            "gate_time_of_day": 0,       # Entries blocked — outside time-of-day window (fallback)
            # UP-bias tracking (split by mode for attribution)
            "up_bias_windows_seen": 0,
            "up_bias_soft_entries": 0,
            "up_bias_hard_entries": 0,
            "up_bias_force_flips": 0,
            "up_bias_soft_pnl": 0.0,
            "up_bias_hard_pnl": 0.0,
        }

        # UP-bias window dedup and auto-disable tracking
        self._up_bias_last_target_key = (-1, -1)  # (YYYYMMDD, target_secs) dedup
        self._up_bias_entry_windows = 0            # count bias-entry windows for auto-disable

        # Cut-quality audit: metadata for evaluating CUT decisions at settlement
        self._cut_audit: list = []

        # Polymarket CLOB client — always init for balance reads + claiming
        # (even in dry-run mode, we need to read CLOB balance + claim winnings)
        self._poly_client = None
        try:
            self._poly_client = create_poly_client(config)
        except Exception as e:
            print(f"  WARNING: Could not init Polymarket client: {e}")
            if not dry_run:
                print(f"  Falling back to dry-run mode")
                self.dry_run = True

        # Resolved wallet identity: who signs, who owns, who can redeem
        self._identity = config.resolve_identity()
        if not self._identity.can_self_redeem:
            print(f"  ⚠ WARNING: signer ({self._identity.eoa[:12]}...) ≠ funder ({self._identity.funder[:12]}...)")
            print(f"    Auto-redeem will be SKIPPED. Redeem manually via Polymarket UI.")

        # Deferred live mode restore (after client init)
        # SINGLE SOURCE OF TRUTH: .bot_state.json dry_run flag only.
        # The .live_mode sentinel is deprecated — state file is authoritative.
        _live_sentinel = Path(__file__).parent.parent / ".live_mode"
        if getattr(self, "_restore_live", False) and self._poly_client:
            self.dry_run = False
            print(f"  Restored LIVE mode (from state file)")
        elif _live_sentinel.exists():
            # Legacy sentinel — sync state but warn
            print(f"  ⚠ Found .live_mode sentinel — ignoring (state file is authoritative)")
            _live_sentinel.unlink(missing_ok=True)
        self._restore_live = False

        # Restore max-bet from previous session — but CLI explicit value wins
        if hasattr(self, '_restore_max_bet'):
            if self._max_bet_explicit:
                # User explicitly passed --max-bet-dollars, that takes priority
                print(f"  Max bet: ${self.engine.max_bet_dollars:.2f} (CLI explicit, ignoring saved ${self._restore_max_bet:.2f})")
            else:
                # No explicit CLI value — restore from saved state
                cli_val = self.engine.max_bet_dollars
                self.engine.max_bet_dollars = self._restore_max_bet
                if abs(cli_val - self._restore_max_bet) > 0.01:
                    print(f"  Restored max bet: ${self._restore_max_bet:.2f} (default was ${cli_val:.2f})")
                else:
                    print(f"  Max bet: ${self._restore_max_bet:.2f}")
            del self._restore_max_bet

        # Cached balances (refreshed on demand)
        self._pol_balance = 0.0
        self._usdce_balance = 0.0
        self._native_usdc_balance = 0.0  # Native USDC (0x3c49...) — NOT used by CLOB
        self._clob_balance = 0.0  # USDC.e deposited on Polymarket exchange
        self._proxy_balance = 0.0  # Polymarket proxy wallet USDC.e
        self._last_balance_refresh = 0.0
        self._last_redeem_check = 0.0  # Auto-redeem winning positions
        self._redeem_burst_until = 0.0  # Post-settlement burst: 10s checks for 2 min
        self._redeemed_conditions: set[str] = set()  # condition IDs already redeemed — skip on stale API
        self._redeemable_value: float = 0.0  # estimated value of redeemable positions

        # Balance change tracking — distinguish deposits from prize payouts
        self._balance_events: list[dict] = []  # [{type, amount, balance, ts}]
        self._prev_usdce_balance = 0.0  # For detecting changes

        # Entry rejection cooldown — back off 5s after min-size or 400 error
        self._entry_retry_after: float = 0.0

        # Real-time event log — terminal-style feed for dashboard
        self._event_log: list[dict] = []  # [{ts, level, msg}]  max 200

        # Dashboard state cache — avoid expensive DB queries every cycle
        self._live_trades_cache: list[dict] = []
        self._live_trades_cache_ts: float = 0

        # Discord alerts — webhook URL stored in .bot_state.json or env
        # NOTE: _load_persisted_state() may have already set this from saved state
        if not hasattr(self, '_discord_webhook') or not self._discord_webhook:
            self._discord_webhook = config.discord_webhook or ""

        # Per-window signal snapshots for dashboard KPIs
        self._window_signals: dict[str, dict] = {}  # slug -> signal data per cycle

        # Stats — initialized to 0, then overwritten by _load_persisted_state()
        # (which already ran above, so these only set defaults if not already loaded)
        if not hasattr(self, 'total_bets'):
            self.total_bets = 0
        if not hasattr(self, 'wins'):
            self.wins = 0
        if not hasattr(self, 'losses'):
            self.losses = 0
        if not hasattr(self, 'total_pnl'):
            self.total_pnl = 0.0

        # Ground-truth PNL tracking: initial deposit vs current balance
        # This is the ONLY reliable PNL — immune to book-price drift
        if not hasattr(self, '_initial_deposit'):
            self._initial_deposit = 0.0

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=8.0,
                headers={
                    "Accept-Encoding": "identity",
                    "User-Agent": "Mozilla/5.0",
                },
            )
        return self._http

    # ------------------------------------------------------------------
    # State Persistence — retain copy trader settings across restarts
    # ------------------------------------------------------------------

    def _load_persisted_state(self):
        """Load persisted bot state (copy trader config, trade mode, P&L, etc.)."""
        try:
            if self._state_file.exists():
                data = json.loads(self._state_file.read_text())
                # Trade mode is always kelly now
                self._trade_mode = "kelly"
                # Restore live/dry mode — defer actual LIVE switch until after
                # _poly_client init (flag checked post-init)
                if data.get("dry_run") is False:
                    self._restore_live = True
                if data.get("intervals"):
                    # Force 5m-only — skip restoring 15m from saved state
                    self.interval_labels = ["5m"]
                if data.get("discord_webhook"):
                    self._discord_webhook = data["discord_webhook"]
                # Restore W/L counts from DB (authoritative, deduplicated)
                try:
                    db_summary = history.get_live_pnl_summary()
                    self.wins = db_summary["wins"]
                    self.losses = db_summary["losses"]
                    self.total_bets = db_summary["total_bets"]
                except Exception as e:
                    print(f"  DB W/L load failed: {e}")
                    if "wins" in data:
                        self.wins = int(data["wins"])
                    if "losses" in data:
                        self.losses = int(data["losses"])
                    if "total_bets" in data:
                        self.total_bets = int(data["total_bets"])

                # Restore PNL — prefer balance-based (real) over trade-sum (book)
                initial_dep = float(data.get("initial_deposit", 0))
                if initial_dep > 0:
                    self._initial_deposit = initial_dep
                    # Compute real PNL from current balance vs deposit
                    try:
                        current_bal = config.get_usdce_balance(config.resolve_identity().funder)
                        if current_bal > 0:
                            self.total_pnl = round(current_bal - initial_dep, 4)
                            print(f"  P&L from balance: ${self.total_pnl:+.2f} (${current_bal:.2f} - ${initial_dep:.2f} deposit)")
                            print(f"  Record: {self.wins}W-{self.losses}L | {self.total_bets} bets")
                        else:
                            self.total_pnl = float(data.get("total_pnl", 0))
                            print(f"  P&L from state: ${self.total_pnl:+.2f} (balance unavailable)")
                    except Exception:
                        self.total_pnl = float(data.get("total_pnl", 0))
                        print(f"  P&L from state: ${self.total_pnl:+.2f} (balance check failed)")
                else:
                    # No initial deposit recorded — use state file PNL
                    self.total_pnl = float(data.get("total_pnl", 0))
                    print(f"  P&L from state: ${self.total_pnl:+.2f} (no initial deposit recorded)")
                    print(f"  Record: {self.wins}W-{self.losses}L | {self.total_bets} bets")
                # Restore tuneables
                # Restore Kelly settings
                if "kelly_aggression" in data:
                    self._kelly_aggression = max(1, min(5, int(data["kelly_aggression"])))
                # Restore max-bet-dollars (dashboard changes survive restart)
                if "max_bet_dollars" in data:
                    self._restore_max_bet = float(data["max_bet_dollars"])
                # Restore initial deposit for balance-based PNL
                if "initial_deposit" in data:
                    self._initial_deposit = float(data["initial_deposit"])
                # Restore runtime config from state
                self.rcfg.merge_state(data)
                self._time_gate_enabled = self.rcfg.time_gate_enabled
                self._time_gate_hours = self.rcfg.time_gate_hours
                self._live_min_conviction = self.rcfg.live_min_conviction
                self.wallet_signal.enabled = self.rcfg.wallet_confirm_enabled
                # Restore peak bankroll for drawdown tracking
                if "peak_bankroll" in data and data["peak_bankroll"] > 0:
                    self._restore_peak_bankroll = float(data["peak_bankroll"])
                # Restore redeem dedup cache (prevents gas waste on stale API after restart)
                if "redeemed_conditions" in data and isinstance(data["redeemed_conditions"], list):
                    self._redeemed_conditions = set(data["redeemed_conditions"])
                # Restore UP-bias state
                if "up_bias_auto_flip" in data:
                    self._restore_up_bias_auto_flip = data["up_bias_auto_flip"]
                if "up_bias_hard_enabled" in data:
                    self._restore_up_bias_hard_enabled = data["up_bias_hard_enabled"]
        except Exception as e:
            print(f"  Could not load state: {e}")

    def _save_persisted_state(self):
        """Save bot state for persistence across restarts.

        Also triggers PNL sync from balance truth (non-dry-run only).
        """
        if not self.dry_run:
            self._sync_pnl_from_balance()
        try:
            data = {
                "trade_mode": self._trade_mode,
                "dry_run": self.dry_run,
                "intervals": self.interval_labels,
                "discord_webhook": self._discord_webhook,
                # P&L tracking — persist across restarts
                "total_pnl": round(self.total_pnl, 4),
                "wins": self.wins,
                "losses": self.losses,
                "total_bets": self.total_bets,
                # Tuneables — persist across restarts
                "kelly_aggression": self._kelly_aggression,
                "max_bet_dollars": self.engine.max_bet_dollars,
                # Balance-based PNL tracking
                "initial_deposit": round(self._initial_deposit, 4),
                "peak_bankroll": round(getattr(self.engine, '_peak_bankroll', self.engine.bankroll), 4),
                # Toggle states (synced to rcfg)
                "time_gate_enabled": self.rcfg.time_gate_enabled,
                "time_gate_hours": self.rcfg.time_gate_hours,
                "wallet_confirm_enabled": self.rcfg.wallet_confirm_enabled,
                "live_min_conviction": self.rcfg.live_min_conviction,
                # Redeem dedup — survives restarts, prevents gas waste on stale API
                "redeemed_conditions": list(self._redeemed_conditions)[-200:],
                # UP-bias state
                "up_bias_auto_flip": getattr(self.engine, '_up_bias_auto_flip', False),
                "up_bias_hard_enabled": getattr(self.engine, '_up_bias_hard_enabled', False),
            }
            self._state_file.write_text(json.dumps(data, indent=2))
        except Exception:
            pass

    def _owner_wallet_address(self) -> str:
        """Wallet that owns tradeable balances and redeemable positions."""
        identity = getattr(self, "_identity", None)
        if identity and identity.funder:
            return identity.funder
        return config.get_wallet_address()

    def _signer_wallet_address(self) -> str:
        """Wallet that signs and pays Polygon gas."""
        identity = getattr(self, "_identity", None)
        if identity and identity.eoa:
            return identity.eoa
        return config.get_wallet_address()

    def _tradeable_cash_balance(self) -> float:
        """Cash the bot can actually deploy right now."""
        return max(self._usdce_balance, self._clob_balance, 0.0)

    def _display_bankroll(self) -> float:
        """Operator-facing bankroll: tradeable cash first, sim bankroll fallback."""
        tradeable = self._tradeable_cash_balance()
        return tradeable if tradeable > 0 else self.engine.bankroll

    def _sync_pnl_from_balance(self):
        """Compute REAL PNL from on-chain balance vs initial deposit.

        This is the ground truth — immune to book-price drift, slippage,
        missed fills, and counter bugs. Also syncs W/L counts from DB.
        """
        try:
            # Get current total balance (USDC.e + CLOB + unredeemed)
            current_bal = self._tradeable_cash_balance()
            if current_bal <= 0:
                return  # No balance data yet

            # Also count value of open positions (BOTH legs for hedged)
            open_value = 0.0
            for slug, w in self._active_windows.items():
                _up_sh = getattr(w, 'up_shares', 0) or 0
                _dn_sh = getattr(w, 'down_shares', 0) or 0
                if _up_sh > 0 or _dn_sh > 0:
                    # Hedged: value both sides using best bid (what we could sell for)
                    up_bid = getattr(w, 'up_best_bid', 0) or 0
                    dn_bid = getattr(w, 'down_best_bid', 0) or 0
                    # Fallback to mid-price if bid unavailable
                    if up_bid <= 0:
                        up_bid = w.up_price_direct if w.up_price_direct > 0 else w.up_price
                    if dn_bid <= 0:
                        dn_bid = w.down_price_direct if w.down_price_direct > 0 else w.down_price
                    open_value += _up_sh * up_bid + _dn_sh * dn_bid
                elif w.bet_side and w.bet_shares > 0:
                    # Legacy single-side fallback
                    if w.bet_side == "UP":
                        cur_price = w.up_price_direct if w.up_price_direct > 0 else w.up_price
                    else:
                        cur_price = w.down_price_direct if w.down_price_direct > 0 else w.down_price
                    if cur_price > 0:
                        open_value += w.bet_shares * cur_price

            total_value = current_bal + open_value

            # Balance-based PNL (only when we have a deposit reference)
            if self._initial_deposit > 0:
                self.total_pnl = round(total_value - self._initial_deposit, 4)

            # Sync W/L counts from DB (authoritative, deduplicated)
            try:
                db_summary = history.get_live_pnl_summary()
                self.wins = db_summary["wins"]
                self.losses = db_summary["losses"]
                self.total_bets = db_summary["total_bets"]
            except Exception:
                pass  # Keep in-memory counts if DB fails

            # Fix drawdown tracking: use deposit as base, not startup balance
            if self._initial_deposit > 0:
                self.engine.initial_bankroll = self._initial_deposit
            # Restore peak bankroll from persisted state
            if hasattr(self, '_restore_peak_bankroll') and self._restore_peak_bankroll > 0:
                self.engine._peak_bankroll = max(self._restore_peak_bankroll, self.engine.bankroll)
                del self._restore_peak_bankroll
            elif self.engine._peak_bankroll < self.engine.bankroll:
                self.engine._peak_bankroll = self.engine.bankroll
            # Restore UP-bias state
            if hasattr(self, '_restore_up_bias_auto_flip'):
                self.engine._up_bias_auto_flip = self._restore_up_bias_auto_flip
                if not self._restore_up_bias_auto_flip:
                    print(f"  UP-bias forced flips: DISABLED (restored from state)")
                del self._restore_up_bias_auto_flip
            if hasattr(self, '_restore_up_bias_hard_enabled'):
                self.engine._up_bias_hard_enabled = self._restore_up_bias_hard_enabled
                if not self._restore_up_bias_hard_enabled:
                    print(f"  UP-bias hard mode: DISABLED (restored from state)")
                del self._restore_up_bias_hard_enabled

        except Exception as e:
            print(f"  PNL sync error: {e}")

    # ------------------------------------------------------------------
    # Market Discovery
    # ------------------------------------------------------------------

    async def _discover_markets(self):
        """Find active and upcoming 5m + 15m markets.
        Cached for 30s — markets only change every 5 minutes.
        """
        now = time.time()
        if hasattr(self, '_last_discovery') and now - self._last_discovery < 30:
            return  # use cached markets
        self._last_discovery = now
        client = await self._client()

        for asset in self.assets:
            for label in self.interval_labels:
                interval = INTERVALS[label]
                current_start = math.floor(now / interval) * interval

                # Check current + next window
                for ts in [current_start - interval, current_start, current_start + interval]:
                    slug = f"{asset}-updown-{label}-{int(ts)}"

                    if slug in self._active_windows:
                        continue  # Already tracking

                    # Skip only fully settled windows (UP_WON/DOWN_WON)
                    # NOT early exits — we may want to re-enter on the same window
                    if any(w.slug == slug and w.result in ("UP_WON", "DOWN_WON")
                           for w in self._closed_windows[-20:]):
                        continue

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
                            if f"{asset}-updown-{label}" not in ev_slug.lower():
                                continue

                            markets = ev.get("markets", [])
                            if not markets:
                                continue

                            m = markets[0]
                            end_str = m.get("endDate", "") or ev.get("endDate", "")
                            end_ts = self._parse_ts(end_str)
                            if not end_ts:
                                try:
                                    end_ts = float(ev_slug.split("-")[-1]) + interval
                                except (ValueError, IndexError):
                                    continue

                            if end_ts <= now:
                                continue  # Already closed

                            start_ts = end_ts - interval

                            # Parse tokens
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

                            up_token = tokens[up_idx] if len(tokens) > up_idx else ""
                            down_token = tokens[down_idx] if len(tokens) > down_idx else ""

                            if not up_token or not down_token:
                                continue

                            # Read negRisk from market data (critical for order signing)
                            market_neg_risk = m.get("negRisk", False)
                            if isinstance(market_neg_risk, str):
                                market_neg_risk = market_neg_risk.lower() == "true"

                            window = MarketWindow(
                                asset=asset,
                                interval_label=label,
                                interval_secs=interval,
                                slug=ev_slug,
                                title=ev.get("title", "") or m.get("question", ""),
                                start_ts=start_ts,
                                end_ts=end_ts,
                                up_token=up_token,
                                down_token=down_token,
                                neg_risk=bool(market_neg_risk),
                                condition_id=m.get("conditionId", "") or ev.get("conditionId", ""),
                            )

                            if start_ts > now:
                                # Window hasn't started yet — upcoming
                                if ev_slug not in self._upcoming_windows:
                                    window.status = "upcoming"
                                    self._upcoming_windows[ev_slug] = window
                            else:
                                # Currently active
                                self._upcoming_windows.pop(ev_slug, None)
                                self._active_windows[ev_slug] = window
                                pass  # window activated

                    except Exception as e:
                        logger.debug(f"Discovery error for {slug}: {e}")

    def _restore_bet_states_from_db(self):
        """Restore full bet state on active windows using recent DB BUY records.

        On restart, in-memory state (bet_side, shares, price, order_id) is lost.
        This checks the DB for recent BUYs and restores everything needed for
        auto-exit, manual sell, and DIRECTIONAL cut/hold logic to work.
        """
        recent = history.get_recent_buys(max_age=900)  # 15 min covers longest window
        restored = 0
        for slug, w in self._active_windows.items():
            if w.bet_side:
                continue  # already has state
            if slug in recent:
                rec = recent[slug]
                side = rec["side"]
                shares = rec.get("fill_shares", 0) or rec.get("shares", 0)
                cost = rec.get("fill_cost", 0) or rec.get("cost", 0)
                price = rec.get("price", 0)
                approach = rec.get("approach", "DIRECTIONAL")

                # Basic bet fields
                w.bet_side = side
                w.bet_shares = shares
                w.bet_price = price
                w.bet_order_id = rec.get("order_id", "") or ""
                w.bet_ts = rec.get("ts", time.time())
                w.status = "bet_placed"

                # DIRECTIONAL position fields — critical for cut/hold/exit logic
                if side == "UP":
                    w.up_shares = shares
                    w.up_cost = cost
                    w.down_shares = 0.0
                    w.down_cost = 0.0
                else:
                    w.down_shares = shares
                    w.down_cost = cost
                    w.up_shares = 0.0
                    w.up_cost = 0.0
                w.total_cost = cost
                w.total_spent = cost
                w.budget = cost
                w.approach = approach
                w.scalp_mode = "SCALP"
                w.trade_count = 1
                w._entry_side = side

                self._kelly_sniped_slugs.add(slug)
                restored += 1
                print(f"  ↻ Restored {side} {w.asset} {w.interval_label}: "
                      f"{shares:.2f}sh @ ${price:.3f} cost=${cost:.2f} "
                      f"[{approach}] {'(live)' if w.bet_order_id else '(dry)'}")
        if restored:
            print(f"  Restored {restored} bet state(s) from DB after restart")

    # ------------------------------------------------------------------
    # Price Updates
    # ------------------------------------------------------------------

    async def _update_prices(self):
        """Fetch live CLOB prices for all active windows.

        When Polymarket WS is connected, prices are updated in real-time
        by the WebSocket handler. Only poll HTTP as fallback every 10 cycles,
        or when WS is disconnected.
        """
        # If Polymarket WS is feeding us data, skip HTTP poll most cycles
        if getattr(self, '_ws_poly_connected', False) and self._ws_poly_updates > 0:
            # Still do HTTP poll every ~10 cycles (2.5s) as sanity check
            if self._cycle_count % 10 != 0:
                # Just record price history from WS-updated values
                for slug, w in list(self._active_windows.items()):
                    if w.up_price > 0 and w.down_price > 0 and w.time_remaining > 0:
                        elapsed = time.time() - w.start_ts
                        w.price_history.append({
                            "t": round(elapsed, 1),
                            "up": round(w.up_price, 4),
                            "dn": round(w.down_price, 4),
                            "upD": int(w.up_depth),
                            "dnD": int(w.down_depth),
                            "btc": round(self._btc_price, 2),
                        })
                return

        client = await self._client()
        tasks = []
        windows = []

        for slug, w in list(self._active_windows.items()):
            if w.time_remaining <= 0:
                continue
            tasks.append(self._fetch_book_pair(client, w.up_token, w.down_token))
            windows.append(w)

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for w, result in zip(windows, results):
            if isinstance(result, Exception) or result is None:
                continue
            # Use token-specific midpoint from actual book bids/asks
            w.up_price = result["up_mid"]
            w.down_price = result["down_mid"]
            # REST books are token-specific (queried by token_id) — trustworthy
            w.up_price_direct = result["up_mid"]
            w.down_price_direct = result["down_mid"]
            w.up_depth = result["up_depth"]
            w.down_depth = result["down_depth"]
            w.up_last = result.get("up_last", 0.0)
            w.down_last = result.get("down_last", 0.0)
            w.up_spread = result.get("up_spread", 0.0)
            w.down_spread = result.get("down_spread", 0.0)
            w.up_best_bid = result.get("up_bid", 0.0)
            w.down_best_bid = result.get("down_bid", 0.0)
            w.up_best_ask = result.get("up_ask", 1.0)
            w.down_best_ask = result.get("down_ask", 1.0)
            w.total_volume = int(w.up_depth + w.down_depth)

            # Record BTC strike price — Chainlink is the resolution source
            # Priority: set at promotion time (exact). Fallback: first 30s (close enough).
            # After 30s: still set if 0 (better than nothing — needed after bot restart)
            if w.btc_at_open == 0 and self._btc_price > 0:
                w.btc_at_open = self._btc_price
            elapsed = time.time() - w.start_ts
            w.price_history.append({
                "t": round(elapsed, 1),
                "up": round(w.up_price, 4),
                "dn": round(w.down_price, 4),
                "upD": int(w.up_depth),
                "dnD": int(w.down_depth),
                "btc": round(self._btc_price, 2),
            })

    async def _fetch_book_pair(self, client, up_token, down_token) -> dict | None:
        """Fetch order books in parallel — compute midpoint locally.

        Dropped separate /midpoint API calls (saves 2 HTTP round-trips per window).
        Uses: last_trade_price > volume-weighted mid > naive (bid+ask)/2.
        """
        # 2 requests instead of 4 — halves network latency
        up_book_resp, down_book_resp = await asyncio.gather(
            client.get(f"{CLOB_API}/book", params={"token_id": up_token}),
            client.get(f"{CLOB_API}/book", params={"token_id": down_token}),
            return_exceptions=True,
        )

        if isinstance(up_book_resp, Exception) or isinstance(down_book_resp, Exception):
            return None
        if up_book_resp.status_code != 200 or down_book_resp.status_code != 200:
            return None

        ub = up_book_resp.json()
        db = down_book_resp.json()

        # Handle one-sided books (heavily skewed markets may have no asks OR no bids)
        if not ub.get("asks") and not ub.get("bids"):
            return None  # completely empty UP book
        if not db.get("asks") and not db.get("bids"):
            return None  # completely empty DOWN book

        # CLOB payload ordering is not guaranteed. Derive best prices explicitly.
        # Some responses are sorted worst→best (e.g. asks start at 0.99), so using
        # index 0 can create fake 0.98 spreads and false "no liquidity" gates.
        def _sorted_levels(book: dict, side: str):
            rows = []
            for lvl in book.get(side, []) or []:
                try:
                    p = float(lvl.get("price", 0))
                    s = float(lvl.get("size", 0))
                except (TypeError, ValueError):
                    continue
                if p <= 0 or s <= 0:
                    continue
                rows.append((p, s))
            if side == "bids":
                rows.sort(key=lambda x: x[0], reverse=True)  # best bid first
            else:
                rows.sort(key=lambda x: x[0])  # best ask first
            return rows

        up_bids = _sorted_levels(ub, "bids")
        up_asks = _sorted_levels(ub, "asks")
        down_bids = _sorted_levels(db, "bids")
        down_asks = _sorted_levels(db, "asks")

        up_bid = up_bids[0][0] if up_bids else 0.0
        down_bid = down_bids[0][0] if down_bids else 0.0
        up_ask = up_asks[0][0] if up_asks else 1.0
        down_ask = down_asks[0][0] if down_asks else 1.0

        # --- Token-specific midpoint ---
        # When the book spread is tight (<50¢), use bid/ask midpoint (most accurate).
        # When the book is wide (≥50¢ spread), the resting orders are far from fair value
        # and last_trade_price better reflects where the market actually trades.
        def _local_mid(book, ask, bid):
            spread = (ask - bid) if (bid > 0 and ask < 1.0) else 1.0
            # 1) Tight spread: use book midpoint (reliable)
            if bid > 0 and ask < 1.0 and spread < 0.50:
                return (bid + ask) / 2
            # 2) Wide spread: prefer last_trade_price (actual fills)
            ltp = book.get("last_trade_price")
            if ltp:
                try:
                    v = float(ltp)
                    if 0 < v < 1:
                        return v
                except (ValueError, TypeError):
                    pass
            # 3) Fallback to book midpoint even if wide
            if bid > 0 and ask < 1.0:
                return (bid + ask) / 2
            if bid > 0:
                return bid
            if ask < 1.0:
                return ask
            return 0.5  # complete fallback

        up_mid = _local_mid(ub, up_ask, up_bid)
        down_mid = _local_mid(db, down_ask, down_bid)

        # Safety net: if books somehow return identical midpoints (shouldn't happen
        # with actual bid/ask pricing), use complement. Only for near-identical values.
        if abs(up_mid - down_mid) < 0.005 and up_mid > 0.05 and up_mid < 0.95:
            down_mid = round(1.0 - up_mid, 4)

        # Capture last trade prices for dashboard display
        up_last = 0.0
        down_last = 0.0
        try:
            up_last = float(ub.get("last_trade_price", 0))
        except (ValueError, TypeError):
            pass
        try:
            down_last = float(db.get("last_trade_price", 0))
        except (ValueError, TypeError):
            pass
        # If books returned same data, derive down_last as complement
        if abs(up_last - down_last) < 0.02 and up_last > 0.01 and up_last < 0.99:
            down_last = round(1.0 - up_last, 4)

        # Depth near touch: sum top 3 levels after explicit sorting.
        up_depth = sum(sz for _, sz in up_asks[:3]) + sum(sz for _, sz in up_bids[:3])
        down_depth = sum(sz for _, sz in down_asks[:3]) + sum(sz for _, sz in down_bids[:3])

        return {
            "up_ask": up_ask,
            "down_ask": down_ask,
            "up_bid": up_bid,
            "down_bid": down_bid,
            "up_mid": up_mid,
            "down_mid": down_mid,
            "up_last": up_last,
            "down_last": down_last,
            "up_spread": round(up_ask - up_bid, 4) if up_bid > 0 else 0.0,
            "down_spread": round(down_ask - down_bid, 4) if down_bid > 0 else 0.0,
            "up_depth": up_depth,
            "down_depth": down_depth,
        }

    async def _update_btc_price(self):
        """Fetch current BTC price. Chainlink is PRIMARY (Polymarket resolution source).

        Chainlink BTC/USD on Polygon is what Polymarket actually resolves against.
        Binance/Coinbase are fallbacks only if Chainlink fails.
        """
        client = await self._client()

        # PRIMARY: Chainlink BTC/USD on Polygon — this is the RESOLUTION SOURCE
        try:
            resp = await client.post(POLYGON_RPC_URL, json={
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": CHAINLINK_BTC_USD, "data": CHAINLINK_LATEST_ROUND}, "latest"],
                "id": 1,
            })
            if resp.status_code == 200:
                result = resp.json().get("result", "0x")
                hex_data = result[2:] if result.startswith("0x") else result
                if len(hex_data) >= 320:
                    price_raw = int(hex_data[64:128], 16)
                    price = price_raw / 1e8  # Chainlink uses 8 decimals
                    if price > 10000:  # sanity check
                        self._btc_price = price
                        self.engine.record_btc_price(price)
                        return
        except Exception:
            pass  # Fall through to backup sources

        # FALLBACK: Binance / Coinbase / CoinGecko
        for url, parser in [
            ("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
             lambda d: float(d["price"])),
            ("https://api.coinbase.com/v2/prices/BTC-USD/spot",
             lambda d: float(d["data"]["amount"])),
            ("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd",
             lambda d: float(d["bitcoin"]["usd"])),
        ]:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    price = parser(resp.json())
                    if price > 0:
                        self._btc_price = price
                        self.engine.record_btc_price(price)
                        return
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Feature Logging — for offline calibration
    # ------------------------------------------------------------------

    async def _feature_log_flusher(self):
        """Background task: flush queued feature rows to CSV every 10s."""
        data_dir = Path(__file__).parent.parent / "data"
        data_dir.mkdir(exist_ok=True)
        features_file = data_dir / "features.csv"
        labels_file = data_dir / "labels.csv"

        # Write header if file doesn't exist
        if not features_file.exists():
            with open(features_file, "w") as f:
                f.write("ts,slug,start_ts,t_frac,q_mid,p_raw,p_smooth,z,book_imb,lag,pressure,"
                        "up_best_bid,up_best_ask,down_best_bid,down_best_ask\n")

        while self._running:
            try:
                await asyncio.sleep(10)
                if self._feature_log_q:
                    rows = []
                    while self._feature_log_q:
                        rows.append(self._feature_log_q.popleft())
                    with open(features_file, "a") as f:
                        f.writelines(rows)
            except Exception as e:
                logger.debug(f"Feature log flush error: {e}")

    def _log_label(self, slug: str, start_ts: float, label: int):
        """Append settlement label to labels CSV. label: 1=UP won, 0=DOWN won."""
        data_dir = Path(__file__).parent.parent / "data"
        data_dir.mkdir(exist_ok=True)
        labels_file = data_dir / "labels.csv"
        if not labels_file.exists():
            with open(labels_file, "w") as f:
                f.write("slug,start_ts,label\n")
        with open(labels_file, "a") as f:
            f.write(f"{slug},{start_ts},{label}\n")

    # ------------------------------------------------------------------
    # WebSocket Feeds — real-time data, no polling
    # ------------------------------------------------------------------

    async def _ws_binance_feed(self):
        """Background task: stream BTC/USDT trades from Binance WebSocket.

        Replaces polling Chainlink RPC every 0.25s with push-based ~10ms updates.
        Chainlink on-chain is still polled as fallback (it's the resolution source),
        but Binance gives us a much faster signal for the probability model.
        """
        import websockets
        self._ws_binance_connected = False
        self._ws_btc_updates = 0

        while self._running:
            try:
                async with websockets.connect(
                    BINANCE_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws_binance_connected = True
                    print(f"  ✓ Binance WS connected (btcusdt@aggTrade)")
                    self._log_event("BINANCE WS connected · real-time BTC feed", "info")

                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            data = json.loads(msg)
                            price = float(data.get("p", 0))
                            if price > 10000:  # sanity check
                                self._btc_price = price
                                self.engine.record_btc_price(price)
                                self._ws_btc_updates += 1
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass

            except Exception as e:
                self._ws_binance_connected = False
                if self._running:
                    print(f"  ⚠️ Binance WS error: {e}, reconnecting in 3s...")
                    await asyncio.sleep(3)

    async def _ws_polymarket_feed(self):
        """Background task: stream Polymarket CLOB order book updates.

        Replaces polling /book endpoint every 0.25s with push-based updates.
        Subscribes to all active token IDs and updates prices in real-time.
        """
        import websockets
        self._ws_poly_connected = False
        self._ws_poly_updates = 0
        self._ws_subscribed_tokens: set[str] = set()

        # Wait for main loop to discover markets first
        for _ in range(30):
            if self._active_windows:
                break
            await asyncio.sleep(1)

        while self._running:
            try:
                async with websockets.connect(
                    POLYMARKET_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws_poly_connected = True
                    self._ws_subscribed_tokens = set()
                    print(f"  ✓ Polymarket WS connected (market channel)")
                    self._log_event("POLYMARKET WS connected · real-time CLOB feed", "info")

                    # Subscribe to currently known tokens
                    await self._ws_poly_subscribe(ws)

                    last_resub = time.time()

                    while self._running:
                        # Use wait_for with timeout so we can periodically re-subscribe
                        # even when no messages are arriving
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=10)
                        except asyncio.TimeoutError:
                            # No messages — check if we need to re-subscribe
                            if time.time() - last_resub > 15:
                                await self._ws_poly_subscribe(ws)
                                last_resub = time.time()
                            continue

                        # Re-subscribe for new market windows every 15s
                        if time.time() - last_resub > 15:
                            await self._ws_poly_subscribe(ws)
                            last_resub = time.time()

                        try:
                            data = json.loads(msg)
                            # Initial message is a list of book snapshots
                            if isinstance(data, list):
                                for item in data:
                                    if isinstance(item, dict):
                                        self._ws_poly_handle_event(item)
                            elif isinstance(data, dict):
                                self._ws_poly_handle_event(data)
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass

            except Exception as e:
                self._ws_poly_connected = False
                self._ws_subscribed_tokens = set()
                if self._running:
                    print(f"  ⚠️ Polymarket WS error: {e}, reconnecting in 3s...")
                    await asyncio.sleep(3)

    async def _ws_poly_subscribe(self, ws):
        """Subscribe to all active market token IDs."""
        import websockets
        tokens = set()
        for w in list(self._active_windows.values()):
            if w.up_token:
                tokens.add(w.up_token)
            if w.down_token:
                tokens.add(w.down_token)
        for w in list(self._upcoming_windows.values()):
            if w.up_token:
                tokens.add(w.up_token)
            if w.down_token:
                tokens.add(w.down_token)

        # Only re-subscribe if token set changed
        new_tokens = tokens - self._ws_subscribed_tokens
        if not new_tokens:
            return

        all_tokens = list(tokens)
        if all_tokens:
            sub_msg = json.dumps({
                "assets_ids": all_tokens,
                "type": "market",
            })
            await ws.send(sub_msg)
            self._ws_subscribed_tokens = tokens

    def _ws_poly_handle_event(self, data: dict):
        """Process a Polymarket WebSocket event and update window prices."""
        _ws_t0 = time.monotonic()
        event_type = data.get("event_type", "")
        self._ws_poly_updates += 1

        if event_type == "book":
            # Full book snapshot — update best bid/ask and depth
            asset_id = data.get("asset_id", "")
            if not asset_id:
                return
            _ws_mono = time.monotonic()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if not asks:
                return

            # Bids sorted ascending (0.01 first), asks sorted ascending (best ask first)
            # Best bid = highest bid, best ask = lowest ask
            best_bid = max((float(b["price"]) for b in bids), default=0.0) if bids else 0.0
            best_ask = min((float(a["price"]) for a in asks), default=1.0)
            # Depth = bid-side + ask-side top-5 each (both sides of the book)
            bid_depth = sum(float(b.get("size", 0)) for b in bids[-5:]) if bids else 0.0
            ask_depth = sum(float(a.get("size", 0)) for a in asks[:5])
            depth = bid_depth + ask_depth
            spread = round(best_ask - best_bid, 4) if best_bid > 0 else 0.0
            # When spread is wide (≥50¢), book midpoint is meaningless — use last_trade_price
            if spread >= 0.50:
                ltp = data.get("last_trade_price")
                if ltp:
                    try:
                        mid = float(ltp)
                    except (ValueError, TypeError):
                        mid = (best_bid + best_ask) / 2 if best_bid > 0 else best_ask
                else:
                    mid = (best_bid + best_ask) / 2 if best_bid > 0 else best_ask
            else:
                mid = (best_bid + best_ask) / 2 if best_bid > 0 else best_ask

            for w in list(self._active_windows.values()):
                if w.up_token == asset_id:
                    w.up_price = mid
                    w.up_price_direct = mid  # direct match — trustworthy
                    w.down_price = round(1.0 - mid, 4)  # complement for display
                    # CRITICAL: clear stale down_price_direct when UP book updates
                    # Otherwise exit logic uses stale DOWN direct price instead of fresh complement
                    w.down_price_direct = 0.0
                    w.up_depth = depth
                    w.up_spread = spread
                    w.up_best_bid = best_bid  # actual best bid for sell pricing
                    w.up_best_ask = best_ask  # actual best ask for buy pricing
                    w.total_volume = int(w.up_depth + w.down_depth)
                    w._last_ws_recv_mono = _ws_mono
                    break
                elif w.down_token == asset_id:
                    w.down_price = mid
                    w.down_price_direct = mid  # direct match — trustworthy
                    w.up_price = round(1.0 - mid, 4)  # complement for display
                    # CRITICAL: clear stale up_price_direct when DOWN book updates
                    w.up_price_direct = 0.0
                    w.down_depth = depth
                    w.down_spread = spread
                    w.down_best_bid = best_bid  # actual best bid for sell pricing
                    w.down_best_ask = best_ask  # actual best ask for buy pricing
                    w.total_volume = int(w.up_depth + w.down_depth)
                    w._last_ws_recv_mono = _ws_mono
                    break

        elif event_type == "price_change":
            # Incremental price update — each change has best_bid/best_ask
            changes = data.get("price_changes", [])
            for ch in changes:
                aid = ch.get("asset_id", "")
                bb_str = ch.get("best_bid")
                ba_str = ch.get("best_ask")
                if not aid or bb_str is None or ba_str is None:
                    continue
                try:
                    bb = float(bb_str)
                    ba = float(ba_str)
                    _spread = round(ba - bb, 4) if bb > 0 else 0.0
                    # When spread is wide (≥50¢), keep existing price from last_trade
                    # (book midpoint is meaningless at 50¢ spread)
                    _ws_mono = time.monotonic()
                    if _spread >= 0.50:
                        # Still update spread/best_bid/best_ask but DON'T overwrite price
                        for w in list(self._active_windows.values()):
                            if w.up_token == aid:
                                w.up_spread = _spread
                                w.up_best_bid = bb  # always track best bid
                                w.up_best_ask = ba  # always track best ask
                                w._last_ws_recv_mono = _ws_mono
                                break
                            elif w.down_token == aid:
                                w.down_spread = _spread
                                w.down_best_bid = bb  # always track best bid
                                w.down_best_ask = ba  # always track best ask
                                w._last_ws_recv_mono = _ws_mono
                                break
                        continue
                    mid = (bb + ba) / 2 if bb > 0 else ba
                    for w in list(self._active_windows.values()):
                        if w.up_token == aid:
                            w.up_price = mid
                            w.up_price_direct = mid  # direct match
                            w.up_spread = _spread
                            w.up_best_bid = bb  # always track best bid
                            w.up_best_ask = ba  # always track best ask
                            w.down_price = round(1.0 - mid, 4)  # complement
                            w.down_price_direct = 0.0  # clear stale direct
                            w._last_ws_recv_mono = _ws_mono
                            break
                        elif w.down_token == aid:
                            w.down_price = mid
                            w.down_price_direct = mid  # direct match
                            w.down_spread = _spread
                            w.down_best_bid = bb  # always track best bid
                            w.down_best_ask = ba  # always track best ask
                            w.up_price = round(1.0 - mid, 4)  # complement
                            w.up_price_direct = 0.0  # clear stale direct
                            w._last_ws_recv_mono = _ws_mono
                            break
                except (ValueError, TypeError):
                    pass

        elif event_type == "last_trade_price":
            # Last trade — update display AND trading prices when book is wide
            asset_id = data.get("asset_id", "")
            price = data.get("price")
            if not asset_id or price is None:
                return
            try:
                _ws_mono = time.monotonic()
                p = float(price)
                if not (0 < p < 1):
                    return
                for w in list(self._active_windows.values()):
                    if w.up_token == asset_id:
                        w.up_last = p
                        w.down_last = round(1.0 - p, 4)
                        # When book spread is wide, last_trade is the best price signal
                        if w.up_spread >= 0.50:
                            w.up_price = p
                            w.up_price_direct = p
                            w.down_price = round(1.0 - p, 4)
                            w.down_price_direct = 0.0  # clear stale
                        w._last_ws_recv_mono = _ws_mono
                        break
                    elif w.down_token == asset_id:
                        w.down_last = p
                        w.up_last = round(1.0 - p, 4)
                        if w.down_spread >= 0.50:
                            w.down_price = p
                            w.down_price_direct = p
                            w.up_price = round(1.0 - p, 4)
                            w.up_price_direct = 0.0  # clear stale
                        w._last_ws_recv_mono = _ws_mono
                        break
            except (ValueError, TypeError):
                pass

        perf.record_ws_processing((time.monotonic() - _ws_t0) * 1000)

    # ------------------------------------------------------------------
    # Directional Betting
    # ------------------------------------------------------------------

    def _evaluate_signals_only(self):
        """Evaluate signals for dashboard display — no auto-betting."""
        for slug, w in list(self._active_windows.items()):
            if w.up_price > 0 and w.down_price > 0 and w.time_remaining > 0:
                self.engine.record_book_snapshot(w.up_price, w.down_price)
                sig_snap = self.engine.get_signal(
                    up_price=w.up_price, down_price=w.down_price,
                    up_depth=w.up_depth, down_depth=w.down_depth,
                    time_remaining=w.time_remaining, interval_secs=w.interval_secs,
                )
                self._window_signals[slug] = {
                    "interval": w.interval_label,
                    "pipeline": dict(self.engine._last_pipeline),
                    "signals": dict(self.engine._last_signals),
                    "signal": {
                        "side": sig_snap.side if sig_snap else None,
                        "confidence": sig_snap.confidence if sig_snap else 0,
                        "edge": sig_snap.edge_type if sig_snap else "",
                        "bet_size": sig_snap.bet_size if sig_snap else 0,
                        "kelly": sig_snap.kelly_fraction if sig_snap else 0,
                        "ev": sig_snap.expected_value if sig_snap else 0,
                    } if True else {},
                    "timeLeft": w.time_remaining,
                    "upPrice": round(w.up_price, 4),
                    "dnPrice": round(w.down_price, 4),
                    "hasBet": bool(w.bet_side),
                    "betSide": w.bet_side,
                }

    # ------------------------------------------------------------------
    # Robust Order Execution Helper
    # ------------------------------------------------------------------

    # ── Advisor integration (reads sidecar advice file) ──────────

    _ADVICE_FILE = Path("/tmp/polybot_advice.json")
    _ADVICE_MIN_CONFIDENCE = 0.50  # ignore low-confidence advice
    _ADVICE_ACTIONS_MAP = {
        "TRIM_UP": ("TRIM", "UP"),
        "TRIM_DOWN": ("TRIM", "DOWN"),
        "ADD_UP": ("ADD", "UP"),
        "ADD_DOWN": ("ADD", "DOWN"),
        "ADD_HEDGE": ("ADD", "HEDGE"),
        "EXIT_BOTH": ("TAKE_PROFIT", "BOTH"),
    }

    def _read_advisor_advice(self, w, current_act: str, current_action: dict) -> dict | None:
        """Read and validate advisor advice. Returns advice dict or None.

        DISABLED: Sidecar removed for deterministic-only operation.
        """
        return None  # sidecar disabled — deterministic engine only

        if not self._ADVICE_FILE.exists():
            return None

        try:
            raw = json.loads(self._ADVICE_FILE.read_text())
        except Exception:
            return None

        # 1. Stale check (advice write time)
        ts = raw.get("ts", 0)
        ttl = raw.get("ttl_sec", 3)
        if time.time() - ts > ttl:
            return None  # expired

        # 1b. Context-staleness guard: reject if the market snapshot the LLM
        #     based its decision on has drifted beyond tolerance.
        ctx_ts = raw.get("context_ts", 0)
        if ctx_ts > 0:
            ctx_age = time.time() - ctx_ts
            # Hard reject if context is older than 20s (allow Kimi k2.5 latency)
            if ctx_age > 20.0:
                self._metrics["advisor_ignored"] += 1
                return None

            # Price drift: if current bids have moved > 3 cents from snapshot
            ctx_up_bid = raw.get("context_up_bid", 0)
            ctx_dn_bid = raw.get("context_dn_bid", 0)
            if ctx_up_bid > 0 and ctx_dn_bid > 0:
                cur_up_bid = getattr(w, 'up_best_bid', 0) or 0
                cur_dn_bid = getattr(w, 'down_best_bid', 0) or 0
                drift_up = abs(cur_up_bid - ctx_up_bid)
                drift_dn = abs(cur_dn_bid - ctx_dn_bid)
                max_drift = max(drift_up, drift_dn)
                if max_drift > 0.03:
                    self._metrics["advisor_ignored"] += 1
                    self._metrics["quote_stale_at_decision"] += 1
                    return None

            # Time-phase drift: if time_left changed by > 30s, the position is
            # in a different phase than what the LLM saw
            ctx_time_left = raw.get("context_time_left", 0)
            if ctx_time_left > 0:
                time_drift = abs(w.time_remaining - ctx_time_left)
                if time_drift > 30:
                    self._metrics["advisor_ignored"] += 1
                    return None

        # 2. Wrong window
        if raw.get("window_slug", "") != w.slug:
            return None

        # 3. Low confidence
        conf = raw.get("confidence", 0)
        if conf < self._ADVICE_MIN_CONFIDENCE:
            self._metrics["advisor_ignored"] += 1
            return None

        adv_action = raw.get("action", "NO_ACTION").upper()

        # 4. No adding near expiry
        if adv_action.startswith("ADD") and w.time_remaining < 30:
            self._metrics["advisor_ignored"] += 1
            return None

        # 5. Never override emergency deterministic actions
        if current_act in ("CUT_LOSS",):
            self._metrics["advisor_ignored"] += 1
            return None

        # 6. Respect anti-churn cooldown for trims
        if adv_action.startswith("TRIM"):
            _last_trim = getattr(w, '_last_trim_ts', 0)
            if time.time() - _last_trim < 25.0:
                self._metrics["advisor_ignored"] += 1
                return None

        # 7. Dry-run mode — log only, don't apply
        if raw.get("_dry_run", True):
            self._metrics["advisor_calls"] += 1   # count call even in dry-run for visibility
            self._metrics["advisor_ignored"] += 1
            return raw  # return for logging but don't modify action

        # ── Phase 2: Bounded TRIM-only nudge path ──────────────────────
        #
        # The advisor can ONLY influence TRIM decisions. Specifically:
        #   - TRIM_UP / TRIM_DOWN: trigger a trim of the specified side
        #   - All other actions (ADD, EXIT, HOLD, etc) are logged but not applied.
        #
        # Bounds:
        #   - size_pct capped at 0.40 (max 40% of side shares)
        #   - confidence >= 0.65 for TRIM action
        #   - Only when deterministic engine says HOLD (don't interfere with engine trims)
        #   - Only when the trim side has enough shares (>= 2 shares)
        #   - Never trim to below hedge floor (30% minority)

        self._metrics["advisor_calls"] += 1

        # Only TRIM and ADJUST_PARAMS actions can influence execution
        if not adv_action.startswith("TRIM") and adv_action != "ADJUST_PARAMS":
            return raw  # log other advice but don't change behavior

        # P5: Handle ADJUST_PARAMS — bounded parameter optimizer
        if adv_action == "ADJUST_PARAMS":
            params = raw.get("param_adjustments", {})
            if not params:
                self._metrics["advisor_param_rejected"] += 1
                return raw

            # Validate bounds
            PARAM_BOUNDS = {
                "trim_aggressiveness": (0.5, 2.0),
                "hedge_target_adj": (-0.10, 0.10),
                "lock_buffer_adj": (-0.02, 0.02),
                "spread_tolerance_mult": (0.5, 1.5),
            }
            valid_params = {}
            for k, v in params.items():
                if k in PARAM_BOUNDS:
                    lo, hi = PARAM_BOUNDS[k]
                    clamped = max(lo, min(hi, float(v)))
                    valid_params[k] = clamped

            if not valid_params:
                self._metrics["advisor_param_rejected"] += 1
                return raw  # no valid params after bounds check

            if conf < 0.70:
                self._metrics["advisor_param_rejected"] += 1
                return raw  # confidence too low for param adjustment

            raw["_param_applied"] = valid_params
            w._advisor_params = valid_params
            w._advisor_params_ts = time.time()
            self._metrics["advisor_param_applied"] += 1
            print(f"  ADVISOR PARAMS: {valid_params} conf={conf:.2f}")
            return raw

        # Higher confidence bar for active TRIM nudge
        if conf < 0.65:
            self._metrics["advisor_ignored"] += 1
            return raw

        # Only nudge when deterministic engine said HOLD (don't stack with engine trims)
        if current_act != "HOLD":
            self._metrics["advisor_ignored"] += 1
            return raw

        # Determine which side to trim
        trim_side = "UP" if adv_action == "TRIM_UP" else "DOWN"
        trim_shares = float(w.up_shares if trim_side == "UP" else w.down_shares)

        # Minimum share threshold
        if trim_shares < 2.0:
            self._metrics["advisor_ignored"] += 1
            return raw

        # Cap size_pct to 40%
        size_pct = min(raw.get("size_pct", 0.20), 0.40)
        sell_amount = round(trim_shares * size_pct, 2)

        # Don't trim below hedge floor: ensure minority ratio stays >= 0.25
        total_sh = float(w.up_shares) + float(w.down_shares)
        remaining_after = trim_shares - sell_amount
        other_side = float(w.down_shares if trim_side == "UP" else w.up_shares)
        new_total = remaining_after + other_side
        if new_total > 0:
            new_minority = min(remaining_after, other_side) / new_total
            if new_minority < 0.25:
                # Cap sell to maintain hedge floor
                # min_remaining = 0.25 * (min_remaining + other_side) → solve
                min_remaining = 0.25 * other_side / 0.75  # = other/3
                sell_amount = max(0, trim_shares - min_remaining)
                sell_amount = round(sell_amount, 2)

        # Final sanity: must sell at least 1 share (Polymarket minimum)
        if sell_amount < 1.0:
            self._metrics["advisor_ignored"] += 1
            return raw

        # Inject trim nudge into current_action
        raw["_nudge_applied"] = True
        raw["_nudge_side"] = trim_side
        raw["_nudge_shares"] = sell_amount
        self._metrics["advisor_applied"] += 1
        return raw

    def _cap_sell_shares_to_balance(self, token_id: str, requested_shares: float) -> tuple[float, str]:
        """Cap SELL size to actual conditional-token balance.

        Returns (safe_shares, note). Note is non-empty when size was capped or balance missing.
        """
        req = float(requested_shares or 0.0)
        if req <= 0:
            return 0.0, "requested<=0"
        if self.dry_run or not self._poly_client or not token_id:
            return req, ""

        try:
            token_bal = self._poly_client.get_token_balance(token_id)
        except Exception:
            return req, "bal-query-error"
        if token_bal < 0:
            return req, "bal-query-error"

        cap = max(0.0, round(float(token_bal) - 0.01, 4))
        if cap < 0.5:
            return 0.0, f"bal={token_bal:.2f}"
        if cap + 1e-9 < req:
            capped = round(cap, 2)
            if capped < 0.5:
                return 0.0, f"cap={cap:.2f}"
            return capped, f"capped {req:.2f}->{capped:.2f}"
        return req, ""

    async def _sell_hedged(
        self, w, side: str, shares: float, bid: float,
        reason: str = "SELL", retry: bool = False,
    ) -> dict:
        """Unified sell pipeline for HEDGED positions.

        All sell paths (LOCK, TRIM, TP, CUT, Telegram, API) route through here.
        1. Caps shares to token balance
        2. Executes sell order (with optional retry at lower price)
        3. Uses actual fill data (last_fill_shares, last_fill_cost)
        4. Updates window state: shares, cost, realized_pnl, total_cost
        5. Updates engine bankroll
        6. Logs event

        Returns: {ok, fill_shares, proceeds, pnl, reason}
        """
        res = {"ok": False, "fill_shares": 0.0, "proceeds": 0.0, "pnl": 0.0, "reason": ""}
        self._metrics["sell_attempts"] += 1

        token = w.up_token if side == "UP" else w.down_token
        cur_shares = float(w.up_shares if side == "UP" else w.down_shares)
        cur_cost = float((w.up_cost if side == "UP" else w.down_cost) or 0)

        if shares < 0.5 or cur_shares < 0.5:
            res["reason"] = f"no {side} shares ({cur_shares:.1f})"
            return res
        shares = min(shares, cur_shares)
        if bid <= 0.01:
            res["reason"] = f"bid too low ({bid:.3f})"
            return res
        if not token:
            res["reason"] = "no token"
            return res

        fill_shares = 0.0
        fill_cost = 0.0  # USDC received (proceeds)

        if not self.dry_run and self._poly_client:
            shares, bal_note = self._cap_sell_shares_to_balance(token, shares)
            if shares < 0.5:
                # Retry once: wait 200ms for chain propagation, re-query balance
                import asyncio as _aio
                await _aio.sleep(0.20)
                shares_retry, bal_note2 = self._cap_sell_shares_to_balance(token, min(shares, cur_shares))
                if shares_retry >= 0.5:
                    shares = shares_retry
                    print(f"    SELL cap retry OK: {shares:.1f}sh (was: {bal_note})")
                else:
                    res["reason"] = f"balance cap: {bal_note} retry:{bal_note2}"
                    self._metrics["sell_bal_mismatch"] += 1
                    return res

            import asyncio as _aio
            oid = await _aio.to_thread(
                self._execute_order,
                token, "SELL", shares, bid,
                w.neg_risk, w.end_ts,
            )
            if not oid and retry:
                # Retry at lower price
                await _aio.sleep(0.3)
                retry_bid = max(0.01, bid - 0.01)
                oid = await _aio.to_thread(
                    self._execute_order,
                    token, "SELL", shares, retry_bid,
                    w.neg_risk, w.end_ts,
                )
                if oid:
                    bid = retry_bid
            if not oid:
                res["reason"] = "sell order failed"
                self._metrics["sell_failures"] += 1
                return res

            _raw_fill_sh = float(getattr(self._poly_client, 'last_fill_shares', 0) or 0)
            _raw_fill_cost = float(getattr(self._poly_client, 'last_fill_cost', 0) or 0)
            if _raw_fill_sh > 0:
                fill_shares = _raw_fill_sh
                fill_cost = _raw_fill_cost if _raw_fill_cost > 0 else (_raw_fill_sh * bid)
            else:
                # Order submitted but no fill data — query balance to confirm actual fill
                # Short delay: allow on-chain state to propagate before re-querying
                await _aio.sleep(0.15)  # 150ms settle
                post_bal = self._poly_client.get_token_balance(token)
                pre_bal = cur_shares  # shares we thought we had
                if post_bal >= 0:
                    actual_sold = min(max(0, pre_bal - post_bal), shares)  # cap to requested
                    if actual_sold < 0.5:
                        res["reason"] = "order OK but no fill confirmed"
                        self._metrics["sell_failures"] += 1
                        return res
                    fill_shares = actual_sold
                    fill_cost = actual_sold * bid  # approximate
                else:
                    fill_shares = shares  # fallback on balance error
                    fill_cost = shares * bid
        else:
            # Dry run: simulate fill
            fill_shares = shares
            fill_cost = shares * bid

        # Proportional cost basis of shares sold
        sold_frac = min(1.0, fill_shares / max(cur_shares, 1e-9))
        sold_cost = cur_cost * sold_frac
        proceeds = fill_cost
        pnl = proceeds - sold_cost

        # Snapshot pre-sell state for DB logging (before shares/cost are decremented)
        _pre_sell_up_shares = float(w.up_shares)
        _pre_sell_down_shares = float(w.down_shares)
        _pre_sell_total_spent = float(w.total_spent or 0)
        _pre_sell_mark_ratio = 0.0
        if w.up_best_bid > 0 and w.down_best_bid > 0 and _pre_sell_total_spent > 0:
            _pre_sell_mark_ratio = round(
                (_pre_sell_up_shares * _sell_proceeds_per_share(w.up_best_bid)
                 + _pre_sell_down_shares * _sell_proceeds_per_share(w.down_best_bid))
                / max(_pre_sell_total_spent, 0.01), 4)

        # Update window state
        if side == "UP":
            w.up_shares = max(0, float(w.up_shares) - fill_shares)
            w.up_cost = max(0, float(w.up_cost or 0) - sold_cost)
        else:
            w.down_shares = max(0, float(w.down_shares) - fill_shares)
            w.down_cost = max(0, float(w.down_cost or 0) - sold_cost)

        w.realized_pnl = getattr(w, 'realized_pnl', 0) + pnl
        w.total_cost = max(0, float(w.total_cost or 0) - sold_cost)
        w.trade_count += 1
        self.engine.bankroll += proceeds

        # Log
        print(f"  {reason} {side} -{fill_shares:.1f}sh @${bid:.3f} "
              f"proceeds=${proceeds:.2f} pnl=${pnl:+.3f}")
        self._log_event(
            f"{reason} {side} {w.asset} {getattr(w, 'interval_label', '5m')} "
            f"-{fill_shares:.1f}sh @${bid:.3f} P&L ${pnl:+.3f}",
            "sell" if pnl >= 0 else "loss",
        )

        res["ok"] = True
        res["fill_shares"] = fill_shares
        res["proceeds"] = proceeds
        res["pnl"] = pnl
        self._metrics["sell_successes"] += 1
        # Track per-reason metrics
        if "TRIM" in reason:
            self._metrics["trim_successes"] += 1
            self._metrics["trim_pnl_total"] += pnl
        elif "LOCK" in reason:
            self._metrics["lock_successes"] += 1

        # Record SELL to DB for audit trail
        db_queue.add_trade({
            "market": w.slug,
            "up_price": w.up_price, "down_price": w.down_price,
            "total_cost": round(sold_cost, 4), "profit_pct": 0,
            "shares": round(fill_shares, 3),
            "investment": round(proceeds, 4),
            "expected_profit": round(pnl, 4),
            "dry_run": self.dry_run,
            "asset": w.asset,
            "extra": {
                "side": side, "action": "SELL",
                "reason": reason,
                "pnl": round(pnl, 4),
                "exit_price": round(bid, 4),
                "approach": getattr(w, 'approach', ''),
                "dry_run": self.dry_run,
                # #10: Decision context for cut tuning calibration
                # Uses pre-sell snapshots so mark_ratio/shares reflect decision state
                "mark_ratio": _pre_sell_mark_ratio,
                "hold_secs": round(time.time() - w.bet_ts, 1) if w.bet_ts > 0 else 0,
                "reversal_secs": round((time.time() - getattr(w, '_reversal_start_ts', 0.0))
                    if getattr(w, '_reversal_start_ts', 0.0) > 0 else 0.0, 1),
                "p_smooth": round(getattr(self.engine, '_p_smooth', 0.5), 4),
                "conviction": round(getattr(self.engine, '_last_conviction_pipeline', {}).get('conviction', 0), 4),
                "wallet_lean": round(self._last_wallet_lean, 3) if getattr(self, '_last_wallet_lean', None) is not None else None,
                "wallet_mult": round(self._last_wallet_mult, 3) if getattr(self, '_last_wallet_mult', None) is not None else None,
                "total_spent": round(_pre_sell_total_spent, 4),
                "up_shares": round(_pre_sell_up_shares, 2),
                "down_shares": round(_pre_sell_down_shares, 2),
                # #8: Full cut context for calibration
                "ev_hold": round(getattr(self.engine, '_p_smooth', 0.5)
                                 * (_pre_sell_up_shares if side == "UP" else _pre_sell_down_shares)
                                 if side == "UP" else
                                 (1.0 - getattr(self.engine, '_p_smooth', 0.5))
                                 * _pre_sell_down_shares, 4),
                "ev_exit": round(proceeds, 4),
                "quote_age_ms": round(
                    (time.monotonic() - getattr(w, '_last_ws_recv_mono', time.monotonic())) * 1000, 0),
                "pathological": getattr(self.engine, '_last_quality_score', 1.0) < 0.30,
                "cut_stage": 2 if getattr(w, '_partial_cut_done', False) else (1 if "STAGE" in reason else 0),
                "time_remaining": int(getattr(w, 'time_remaining', 0)),
            },
        })

        # NOTE: Reconcile removed from post-sell — chain state is stale immediately
        # after a sell. Periodic reconcile (every 30s in SCALP) handles drift safely.

        return res

    def _reconcile_shares(self, w):
        """Sync in-memory share counts with on-chain balances.

        CRITICAL: Only ratchet DOWN (chain < mem), never UP.
        If chain shows MORE than we track, that's stale data from a prior
        window or settlement credit — we didn't buy those shares.

        Also adjusts w.up_cost/w.down_cost proportionally to prevent VWAP drift.

        SAFETY GUARDS:
        1. Skip if within 45s of entry or last ADD (chain lag after order fill)
        2. Two-consecutive-reads required before ratcheting down (prevents
           transient zero-balance reads from zeroing out live positions)
        3. Updates total_cost AND total_spent proportionally to prevent
           zero-ratio CUT_LOSS triggers after reconcile
        """
        if self.dry_run or not self._poly_client:
            return
        # Guard 1: Don't reconcile within 45s of entry or last ADD
        # On-chain state takes time to propagate after order fills
        _eft = getattr(w, '_entry_fill_ts', 0)
        _lat = getattr(w, '_last_add_ts', 0)
        _last_position_change = max(_eft, _lat)
        if _last_position_change > 0 and time.time() - _last_position_change < 45.0:
            return
        changed = False
        try:
            if w.up_token:
                up_bal = self._poly_client.get_token_balance(w.up_token)
                if up_bal >= 0:
                    old = float(w.up_shares)
                    if old - up_bal > 0.5:
                        # Track when chain returns zero for a live position
                        if up_bal < 0.5 and old >= 1.0:
                            self._metrics["reconcile_zero_hit"] += 1
                        # Guard 2: Two-read confirmation before ratcheting down
                        _pending = getattr(w, '_reconcile_pending_up', 0.0)
                        if _pending == 0.0 or abs(_pending - up_bal) > 0.5:
                            # First discrepancy or value changed — record, don't apply yet
                            w._reconcile_pending_up = up_bal
                            self._metrics.setdefault("reconcile_pending", 0)
                            self._metrics["reconcile_pending"] += 1
                            if self._cycle_count % 30 == 1:
                                print(f"    RECONCILE UP pending: mem={old:.1f} chain={up_bal:.1f} (need confirm)")
                        else:
                            # Second consecutive matching read — confirmed, apply
                            print(f"    RECONCILE UP ↓: mem={old:.1f} → chain={up_bal:.1f} (confirmed)")
                            ratio = up_bal / old if old > 0 else 0
                            w.up_cost = float(w.up_cost or 0) * ratio
                            w.up_shares = up_bal
                            w._reconcile_pending_up = 0.0
                            self._metrics["sell_deferred"] += 1
                            changed = True
                    else:
                        w._reconcile_pending_up = 0.0  # no discrepancy — clear pending
                        if up_bal - old > 0.5 and self._cycle_count % 60 == 0:
                            print(f"    RECONCILE UP skip: chain={up_bal:.1f} > mem={old:.1f} (ignoring ghost)")
            if w.down_token:
                dn_bal = self._poly_client.get_token_balance(w.down_token)
                if dn_bal >= 0:
                    old = float(w.down_shares)
                    if old - dn_bal > 0.5:
                        # Track when chain returns zero for a live position
                        if dn_bal < 0.5 and old >= 1.0:
                            self._metrics["reconcile_zero_hit"] += 1
                        _pending = getattr(w, '_reconcile_pending_dn', 0.0)
                        if _pending == 0.0 or abs(_pending - dn_bal) > 0.5:
                            w._reconcile_pending_dn = dn_bal
                            self._metrics.setdefault("reconcile_pending", 0)
                            self._metrics["reconcile_pending"] += 1
                            if self._cycle_count % 30 == 1:
                                print(f"    RECONCILE DN pending: mem={old:.1f} chain={dn_bal:.1f} (need confirm)")
                        else:
                            print(f"    RECONCILE DN ↓: mem={old:.1f} → chain={dn_bal:.1f} (confirmed)")
                            ratio = dn_bal / old if old > 0 else 0
                            w.down_cost = float(w.down_cost or 0) * ratio
                            w.down_shares = dn_bal
                            w._reconcile_pending_dn = 0.0
                            self._metrics["sell_deferred"] += 1
                            changed = True
                    else:
                        w._reconcile_pending_dn = 0.0
                        if dn_bal - old > 0.5 and self._cycle_count % 60 == 0:
                            print(f"    RECONCILE DN skip: chain={dn_bal:.1f} > mem={old:.1f} (ignoring ghost)")
            if changed:
                new_total_cost = max(0, float(w.up_cost or 0) + float(w.down_cost or 0))
                # Scale total_spent proportionally to prevent zero-ratio CUT triggers
                if w.total_cost > 0:
                    spent_ratio = new_total_cost / w.total_cost
                    w.total_spent = max(0, w.total_spent * spent_ratio)
                w.total_cost = new_total_cost
        except Exception as e:
            print(f"    Reconcile error: {e}")

    def _execute_order(self, token_id: str, side: str, shares: float,
                       price: float, neg_risk: bool, end_ts: float = 0,
                       max_retries: int = 2, use_maker: bool = False,
                       spend_dollars: float = 0.0) -> str | None:
        """Execute an order with multi-tier retries.

        Tier 1 (if use_maker): Post-only at best_bid or best_bid+0.01, wait 300-500ms.
        Tier 1 (normal): FOK market order (immediate fill at best available price)
        Tier 2: Limit order at current price (BUY: +1%, SELL: at bid)
        Tier 3: Last resort retry (BUY: +2%, SELL: at bid)
        Returns order_id or None.
        Also sets self._last_fill_shares with actual shares from FOK fill.

        Args:
            spend_dollars: For BUYs, the exact dollar amount to send as market order.
                           If >0, used instead of shares*price to avoid rounding errors.

        SELL orders use the actual best_bid price — no artificial slippage.
        On a CLOB, sells fill at whatever bids exist in the book.
        Allowances are cached to avoid redundant API calls (~300ms each).
        """
        _t0 = time.monotonic()
        self._last_fill_shares = 0.0

        # Client-side BUY min-size reject (Polymarket requires $1.00)
        PM_MIN_MARKET_BUY = 1.05
        _initial_buy_notional = 0.0
        if side == "BUY" and not self.dry_run:
            _buy_notional = spend_dollars if spend_dollars > 0 else (shares * price)
            _initial_buy_notional = float(_buy_notional)
            if _buy_notional < PM_MIN_MARKET_BUY:
                print(f"    ⛔ ORDER MIN-SIZE REJECT: ${_buy_notional:.2f} < ${PM_MIN_MARKET_BUY} PM minimum")
                self._metrics.setdefault("order_min_size_rejected", 0)
                self._metrics["order_min_size_rejected"] += 1
                self._entry_retry_after = time.time() + 5  # cooldown 5s
                return None

        # ── GUARDRAIL: per-order notional cap ──
        _notional = spend_dollars if (side == "BUY" and spend_dollars > 0) else (shares * price)
        _cap = getattr(self.engine, 'max_bet_dollars', 0)
        if _cap > 0 and _notional > _cap * 1.5:
            print(f"    ⛔ ORDER REJECTED: ${_notional:.2f} exceeds max_bet ${_cap:.2f}×1.5")
            self._metrics.setdefault("order_cap_rejected", 0)
            self._metrics["order_cap_rejected"] += 1
            return None
        if not self.dry_run:
            _br = getattr(self.engine, 'bankroll', 0)
            if _br > 0 and _notional > _br * 0.50:
                print(f"    ⛔ ORDER REJECTED: ${_notional:.2f} > 50% bankroll ${_br:.2f}")
                self._metrics.setdefault("order_bankroll_rejected", 0)
                self._metrics["order_bankroll_rejected"] += 1
                return None

        if self.dry_run:
            # Enforce same min-order rule in dry mode for realistic simulation
            if side == "BUY":
                _dry_notional = spend_dollars if spend_dollars > 0 else (shares * price)
                if _dry_notional < PM_MIN_MARKET_BUY:
                    self._metrics.setdefault("dry_order_min_size_rejected", 0)
                    self._metrics["dry_order_min_size_rejected"] += 1
                    return None
            # Simulate fill for dry-run bookkeeping
            sim_cost = round(shares * price, 4)
            self._last_fill_shares = shares
            if hasattr(self._poly_client, 'last_fill_cost'):
                self._poly_client.last_fill_cost = sim_cost
            if hasattr(self._poly_client, 'last_fill_shares'):
                self._poly_client.last_fill_shares = shares
            return f"DRY_{side}_{int(time.time()*1000)}"
        if not self._poly_client:
            return None

        # Cache allowances — only call API when needed (saves ~300ms per order)
        if not hasattr(self, '_allowance_set'):
            self._allowance_set = False
            self._token_allowances = set()  # token_ids already approved
        try:
            if side == "BUY" and not self._allowance_set:
                self._poly_client.set_allowances()
                self._allowance_set = True
            if side == "SELL" and token_id not in self._token_allowances:
                self._poly_client.set_token_allowance(token_id)
                self._token_allowances.add(token_id)
        except Exception as e:
            print(f"    Allowance setup warning: {str(e)[:60]}")

        # Tier 0: Maker-first attempt (post_only at best_bid or bid+0.01)
        if use_maker and side == "BUY":
            try:
                maker_price = max(0.01, min(0.99, round(price - 0.01, 2)))  # 1 tick below ask
                order_id = self._poly_client.place_order(
                    token_id=token_id, side=side,
                    size=shares, price=maker_price,
                    tick_size="0.01", neg_risk=neg_risk,
                    order_type="GTC", post_only=True,
                )
                if order_id:
                    time.sleep(0.4)  # wait 400ms for fill
                    # Check if filled
                    fill = getattr(self._poly_client, 'last_fill_shares', 0)
                    fill_cost = getattr(self._poly_client, 'last_fill_cost', 0)
                    if fill and fill > 0:
                        self._last_fill_shares = fill
                        eff = fill_cost / fill if fill > 0 else 0
                        print(f"    💰 MAKER BUY: eff={eff:.4f} vs post={maker_price:.4f} (saved taker fee)")
                        perf.record_order_time((time.monotonic() - _t0) * 1000, success=True)
                        return order_id
                    else:
                        # Cancel unfilled maker order, fall through to taker
                        try:
                            self._poly_client.cancel_orders([order_id])
                        except Exception:
                            pass
                        print(f"    Maker unfilled @{maker_price:.2f}, falling back to taker")
            except Exception as e:
                print(f"    Maker attempt failed: {str(e)[:60]}, falling back to taker")

        for attempt in range(max_retries):
            try:
                # Tier 1: FOK market order
                if side == "BUY":
                    amount = float(spend_dollars) if spend_dollars > 0 else float(shares * price)
                else:
                    amount = float(shares)

                order_id = self._poly_client.place_market_order(
                    token_id=token_id, side=side,
                    amount=amount, tick_size="0.01", neg_risk=neg_risk,
                )
                if order_id:
                    # Capture actual fill shares from FOK response
                    fill = getattr(self._poly_client, 'last_fill_shares', 0)
                    fill_cost = getattr(self._poly_client, 'last_fill_cost', 0)
                    if fill > 0:
                        self._last_fill_shares = fill
                        eff = fill_cost / fill if fill > 0 else 0
                        if side == "BUY":
                            print(f"    💰 BUY: eff={eff:.4f} vs ask={price:.4f} slip={eff-price:+.4f}")
                        else:
                            print(f"    💰 SELL: eff={eff:.4f} vs bid={price:.4f} slip={price-eff:+.4f}")
                    else:
                        # Fallback: treat requested size as filled when API doesn't return details.
                        self._last_fill_shares = shares
                    print(f"    Order filled (FOK attempt {attempt+1}): {order_id}")
                    if side == "BUY" and _initial_buy_notional > 0 and amount < (_initial_buy_notional - 0.04):
                        self._metrics.setdefault("buy_fok_downsize_success", 0)
                        self._metrics["buy_fok_downsize_success"] += 1
                    perf.record_order_time((time.monotonic() - _t0) * 1000, success=True)
                    perf.record_slippage(expected=price, actual=price, side=side, shares=shares)
                    return order_id

                # FOK didn't fill — no limit fallback (prevents phantom fills
                # and hidden orders that can drain balance after mode switch).
                # Retry FOK on next attempt with brief pause. BUY retries downsize
                # notional so thin books don't block entries unnecessarily.
                if attempt < max_retries - 1:
                    time.sleep(0.3)
                    if side == "BUY" and amount > (PM_MIN_MARKET_BUY + 0.04):
                        _cut = max(0.10, amount * 0.10)
                        _next_amount = round(max(PM_MIN_MARKET_BUY, amount - _cut), 2)
                        if _next_amount < amount - 0.04:
                            self._metrics.setdefault("buy_fok_downsize_retries", 0)
                            self._metrics["buy_fok_downsize_retries"] += 1
                            if spend_dollars > 0:
                                spend_dollars = _next_amount
                            shares = round(_next_amount / max(price, 0.01), 2)
                            print(
                                f"    FOK unfilled attempt {attempt+1}/{max_retries}, "
                                f"downsizing BUY ${amount:.2f} → ${_next_amount:.2f} "
                                f"({shares:.2f}sh) and retrying..."
                            )
                            continue
                    print(f"    FOK unfilled attempt {attempt+1}/{max_retries}, retrying...")
                else:
                    print(f"    FOK unfilled after {max_retries} attempts — giving up")
                    perf.record_order_time((time.monotonic() - _t0) * 1000, success=False)

            except Exception as e:
                err_str = str(e)
                self._last_order_error = err_str[:120]
                print(f"    Order attempt {attempt+1} error: {err_str[:80]}")
                if "balance" in err_str.lower() or "allowance" in err_str.lower():
                    if side == "SELL":
                        # Check if we bought very recently — blockchain lag, not actually sold
                        buy_age = time.time() - getattr(self, '_last_buy_ts', 0)
                        if buy_age < 15:
                            # Bought <15s ago — blockchain hasn't settled yet. Retry.
                            print(f"    SELL: not enough balance but bought {buy_age:.0f}s ago — blockchain lag, retrying")
                            if attempt < max_retries - 1:
                                time.sleep(3)  # Wait 3s for chain settlement
                                continue
                        # Balance errors on SELL are often share-accounting drift
                        # (fee-in-shares, partial fills, delayed sync), not true "already sold".
                        # FIX: Refresh balance and retry with capped size before giving up.
                        try:
                            _refresh_bal = self._poly_client.get_token_balance(token_id)
                            if _refresh_bal is not None and _refresh_bal >= 0.5:
                                _capped = round(float(_refresh_bal) - 0.01, 2)
                                if _capped >= 0.5 and _capped < shares:
                                    print(f"    SELL: bal mismatch — refreshed bal={_refresh_bal:.2f}, "
                                          f"retrying with {_capped:.2f}sh (was {shares:.2f})")
                                    shares = _capped
                                    time.sleep(0.2)
                                    continue  # retry with capped size
                        except Exception:
                            pass  # balance refresh failed — fall through to original behavior
                        self._last_order_error = f"SELL_BAL_MISMATCH: {err_str[:60]}"
                        print(f"    SELL: not enough token balance for requested size")
                        perf.record_order_time((time.monotonic() - _t0) * 1000, success=False)
                        return None
                    break  # Don't retry for buys
                if attempt < max_retries - 1:
                    time.sleep(0.5)  # Brief pause before retry

        self._last_order_error = f"no fill after {max_retries} attempts @ ${price:.2f} x{shares:.2f}"
        if side == "BUY":
            self._entry_retry_after = time.time() + 5  # cooldown 5s after failed BUY
        perf.record_order_time((time.monotonic() - _t0) * 1000, success=False)
        return None

    # ------------------------------------------------------------------
    # Settlement
    # ------------------------------------------------------------------

    def _auto_exit_positions(self):
        """Smart auto-exit using engine's should_exit_position() analysis.

        Evaluates every open position every cycle using:
        - Probability model edge (has it flipped?)
        - Momentum (reversal detection)
        - Profit taking (>82¢ lock in gains)
        - Stop-loss (cut at -40%)
        - Near-expiry profitable (sell in last 15s)

        In copy/value mode (late-snipe), we HOLD to expiry — skip auto-exit.
        Settlement at expiry is handled by _check_settlements().
        """
        if self._trade_mode in ("copy", "value"):
            return  # Late-snipe / value strategy: hold everything till settlement
        if self._trade_mode == "kelly":
            return  # v2 scalp loop handles exits via should_act()
        for slug, w in list(self._active_windows.items()):
            if not w.bet_side or w.status == "closed":
                continue

            # Use the BEST price we have for the bet side
            # Priority: best_bid (what we'd actually sell at) > direct WS > complement
            if w.bet_side == "UP":
                cur_price = w.up_price  # always freshest (updated by both direct + complement)
                cur_best_bid = w.up_best_bid
            else:
                cur_price = w.down_price  # always freshest
                cur_best_bid = w.down_best_bid
            if cur_price <= 0:
                continue

            # Track hold duration for logging
            hold_secs = time.time() - w.bet_ts if w.bet_ts > 0 else 999

            # NO GRACE PERIOD — user wants fast in/out execution.
            # The _execute_order SELL handler protects against false ALREADY_SOLD
            # by checking _last_buy_ts before marking as sold.

            # Update peak profit tracking — ONLY when bid exists (real sellable profit)
            if w.bet_price > 0 and cur_best_bid >= 0.01:
                cur_profit_pct = (cur_best_bid - w.bet_price) / w.bet_price
                w.peak_profit_pct = max(w.peak_profit_pct, cur_profit_pct)
            elif w.bet_price > 0:
                cur_profit_pct = (cur_price - w.bet_price) / w.bet_price  # for display only
            else:
                cur_profit_pct = 0
            # Log profit status every ~5 cycles for visibility
            if w.bet_price > 0:
                if not hasattr(self, '_profit_log_counter'):
                    self._profit_log_counter = 0
                self._profit_log_counter += 1
                if self._profit_log_counter % 5 == 0:
                    pnl_now = (cur_best_bid - w.bet_price) * w.bet_shares if cur_best_bid > 0 else 0
                    print(f"  📊 {w.bet_side} {w.asset} {w.interval_label}: entry=${w.bet_price:.3f} bid=${cur_best_bid:.2f} mid=${cur_price:.3f} P&L={cur_profit_pct:+.0%} (${pnl_now:+.2f}) peak={w.peak_profit_pct:+.0%} {int(w.time_remaining)}s")

            # Always trust prices — both direct and complement are from live WS
            has_direct = True  # simplified: trust all WS prices for exit decisions

            # Ask the engine whether we should exit
            # Now passes actual best_bid so exit logic can compare
            # CLOB sell vs settlement realistically
            should_exit, reason = self.engine.should_exit_position(
                bet_side=w.bet_side,
                entry_price=w.bet_price,
                current_price=cur_price,
                up_price=w.up_price,
                down_price=w.down_price,
                time_remaining=w.time_remaining,
                interval_secs=w.interval_secs,
                peak_profit_pct=w.peak_profit_pct,
                price_is_direct=has_direct,
                best_bid=cur_best_bid,
            )

            if not should_exit:
                continue

            pnl = (cur_price - w.bet_price) * w.bet_shares
            order_id = None

            # === LIVE EXECUTION ===
            # Sell if live mode with real shares (bet_order_id OR restored position)
            if not self.dry_run and self._poly_client and w.bet_shares > 0:
                token_id = w.up_token if w.bet_side == "UP" else w.down_token

                # Cap SELL size to actual token balance to avoid false "not enough balance".
                sell_shares, bal_note = self._cap_sell_shares_to_balance(token_id, float(w.bet_shares))
                if sell_shares < 0.5:
                    if self._cycle_count % 25 == 1:
                        print(f"  EXIT BLOCKED ({slug}): {bal_note or 'token balance too small'}")
                    continue
                if bal_note and self._cycle_count % 25 == 1:
                    print(f"  EXIT SIZE CAPPED ({slug}): {bal_note}")

                # Position too small to sell on CLOB — let it settle on-chain
                sell_value = sell_shares * cur_price
                if sell_value < 0.50:
                    continue

                # Track sell failures per slug — give up after 3 attempts (don't spam)
                if not hasattr(self, '_exit_fail_count'):
                    self._exit_fail_count = {}
                fail_count = self._exit_fail_count.get(slug, 0)
                if fail_count >= 3:
                    # Already failed 3 times — let it settle on-chain instead
                    if fail_count == 3:  # Log once
                        print(f"  ℹ️  Giving up on auto-exit {slug} after 3 failed attempts — will settle on-chain")
                        self._log_event(f"EXIT GAVE UP {w.bet_side} {w.asset} · {reason} · will settle on-chain", "info")
                        self._exit_fail_count[slug] = fail_count + 1
                    continue

                # Use actual best_bid as sell price — not mid price
                sell_price = cur_best_bid if cur_best_bid > 0.01 else cur_price
                order_id = self._execute_order(
                    token_id=token_id, side="SELL", shares=sell_shares,
                    price=sell_price, neg_risk=w.neg_risk,
                )
                if not order_id:
                    err = self._last_order_error or ""
                    if "ALREADY_SOLD" in err:
                        # Position was already sold/settled — fully clean up internal state
                        print(f"  ℹ️  Position already sold on-chain — fully clearing: {slug}")
                        w.bet_shares = 0
                        w.bet_side = ""  # MUST clear so Kelly doesn't skip this window
                        w.status = "closed"
                        w.bet_price = 0
                        w.peak_profit_pct = 0
                        self._log_event(f"ALREADY SOLD {w.bet_side or '?'} {w.asset} · {reason} · cleared", "info")
                        continue
                    self._exit_fail_count[slug] = fail_count + 1
                    print(f"  ⚠️  Auto-exit sell FAILED ({fail_count+1}/3): {slug} [{reason}] — {err}")
                    logger.warning(f"Auto-exit sell failed for {slug}: {err}")
                    continue

                # Use ACTUAL sell fill data — mid-price can differ from real fill
                sell_fill_cost = getattr(self._poly_client, 'last_fill_cost', 0)
                sell_fill_shares = getattr(self._poly_client, 'last_fill_shares', 0)
                if sell_fill_shares > 0 and sell_fill_cost > 0:
                    real_sell_price = sell_fill_cost / sell_fill_shares
                    if abs(real_sell_price - cur_price) > 0.01:
                        print(f"  SELL FILL: mid=${cur_price:.3f} → actual=${real_sell_price:.3f} "
                              f"(received ${sell_fill_cost:.2f} for {sell_fill_shares:.2f}sh)")
                    cur_price = round(real_sell_price, 4)
                    pnl = (cur_price - w.bet_price) * w.bet_shares
                    # Update reason text with actual fill price/pct
                    real_pct = (cur_price - w.bet_price) / w.bet_price if w.bet_price > 0 else 0
                    reason = f"{reason.split(':')[0]}: ${cur_price:.3f} ({real_pct:+.0%})"

            mode_str = "LIVE" if not self.dry_run else "DRY"
            pnl_pct = (cur_price - w.bet_price) / w.bet_price * 100 if w.bet_price > 0 else 0
            win_icon = "🟢" if pnl >= 0 else "🔴"
            action_word = "SOLD" if pnl >= 0 else "STOPPED"
            print(f"\n  {win_icon} {action_word} [{mode_str}] {w.bet_side} {w.asset.upper()} {w.interval_label}")
            print(f"  {w.bet_shares:.2f}sh: ${w.bet_price:.3f} → ${cur_price:.3f} ({pnl_pct:+.0f}%) | P&L: ${pnl:+.2f}")
            print(f"  Reason: {reason}")
            lvl = "sell" if pnl >= 0 else "loss"
            self._log_event(f"{action_word} {w.bet_side} {w.asset} · {w.bet_shares:.1f}sh ${w.bet_price:.3f}→${cur_price:.3f} · P&L ${pnl:+.2f} · {reason}", lvl)
            # CRITICAL: Mark closed FIRST — prevents re-sell if anything below fails
            w.pnl = pnl
            w.result = f"EXIT_{w.bet_side}"
            w.status = "closed"
            w.btc_at_close = self._btc_price
            # Keep window in _active_windows — prevents re-entry on same window
            # Window will be cleaned up naturally at expiry/settlement
            self._closed_windows.append(w)

            # Update PNL and stats
            self.total_pnl += pnl
            if not self.dry_run:
                self.engine.bankroll += pnl
            self.engine.record_result(pnl >= 0, pnl)
            if pnl >= 0:
                self.wins += 1
            else:
                self.losses += 1

            # Display fields (safe now that position is closed)
            w.exit_price = cur_price
            w.exit_reason = reason

            # Log EV details if available
            exit_eval = getattr(self.engine, '_last_exit_eval', {})
            if exit_eval:
                _bb = exit_eval.get('best_bid', 0)
                print(f"  EV: hold={exit_eval.get('hold_ev',0):.3f} sell={exit_eval.get('sell_ev',0):.3f} "
                      f"bid=${_bb:.2f} p_win={exit_eval.get('p_win_adj',0):.1%} σ={exit_eval.get('sigma',0):.6f}")

            # NO RE-ENTRY: one trade per window. Buy once → sell for profit → done.
            # The model + Kelly + time-remaining handle hold vs sell each second.
            # Keep slug marked as sniped so we don't re-enter this window.
            self._kelly_sniped_slugs.add(slug)  # ensure it stays marked

            # Log exit to DB
            db_queue.add_trade({
                "market": w.slug, "up_price": w.up_price, "down_price": w.down_price,
                "total_cost": w.total_cost, "profit_pct": 0,
                "shares": w.bet_shares,
                "investment": round(w.bet_shares * cur_price, 4),
                "expected_profit": round(pnl, 4), "dry_run": self.dry_run,
                "asset": w.asset,
                "extra": {
                    "side": w.bet_side, "action": "SELL",
                    "exit_reason": reason, "pnl": round(pnl, 4),
                    "exit_price": round(cur_price, 4),
                    "order_id": order_id,
                },
            })
    def _check_settlements(self):
        """Check if any active windows have closed and settle bets."""
        now = time.time()
        to_close = []

        for slug, w in list(self._active_windows.items()):
            if w.time_remaining > 0:
                continue
            # Guard against double-settlement: skip if already closed
            if w.status == "closed":
                continue

            to_close.append(slug)
            w.status = "closed"  # Mark immediately to prevent double-settlement
            w.btc_at_close = self._btc_price

            # Determine result from final prices — with BTC price fallback
            # Primary: use market prices. The winning side should be well
            # above 0.5 at close. Old threshold of 0.9 was too strict —
            # prices like 0.875 clearly indicate the winner but missed the
            # gate, causing 5 wrong results (-$15.56 phantom losses).
            # Now: if one side > 0.6 and clearly dominant, trust it.
            if w.up_price > 0.6 and w.up_price > w.down_price:
                w.result = "UP_WON"
            elif w.down_price > 0.6 and w.down_price > w.up_price:
                w.result = "DOWN_WON"
            elif w.btc_at_open > 0 and w.btc_at_close > 0:
                # Fallback: use BTC open vs close for BTC Up/Down markets
                # Only used when prices are truly ambiguous (near 0.50/0.50)
                if w.btc_at_close > w.btc_at_open:
                    w.result = "UP_WON"
                elif w.btc_at_close < w.btc_at_open:
                    w.result = "DOWN_WON"
                else:
                    w.result = "PENDING"  # Exact same price — rare
            else:
                w.result = "PENDING"

            # Settle bet if we had one
            if w.bet_side and w.result in ("UP_WON", "DOWN_WON"):
                realized = getattr(w, 'realized_pnl', 0.0)  # profit from intra-window sells

                if getattr(w, 'approach', '') == "HEDGED":
                    # HEDGED: winning side shares pay $1, losing side = $0
                    # realized_pnl already tracks all sell/buy trades done mid-window
                    if w.result == "UP_WON":
                        settlement = getattr(w, 'up_shares', 0) * 1.00
                    else:
                        settlement = getattr(w, 'down_shares', 0) * 1.00
                    w.pnl = settlement + realized - w.total_cost
                    won = w.pnl > 0
                else:
                    # DIRECTIONAL: single-side settlement
                    # Use REMAINING shares + total_cost (not original bet_shares * bet_price)
                    # to correctly account for partial sells (CUT-STAGE1, trims, etc.)
                    won = (w.bet_side == "UP" and w.result == "UP_WON") or \
                          (w.bet_side == "DOWN" and w.result == "DOWN_WON")
                    _remain_sh = float(getattr(w, 'up_shares', 0) if w.bet_side == "UP"
                                       else getattr(w, 'down_shares', 0))
                    if won:
                        settlement = _remain_sh * 1.00  # NO fee on Polymarket settlement!
                        w.pnl = settlement + realized - w.total_cost
                    else:
                        w.pnl = 0.0 + realized - w.total_cost  # remaining shares worth $0

                if w.pnl > 0:
                    self.wins += 1
                else:
                    self.losses += 1

                # Update result ring for risk scaling + streak guard
                _settle_side = getattr(w, 'bet_side', '') or ''
                self._result_ring.append((_settle_side, w.pnl))
                if len(self._result_ring) > self._result_ring_max:
                    self._result_ring = self._result_ring[-self._result_ring_max:]
                # Streak guard: track consecutive same-side negative outcomes
                if _settle_side and w.pnl < 0:
                    if _settle_side == self._streak_side:
                        self._streak_count += 1
                        self._streak_pnl += w.pnl
                    else:
                        self._streak_side = _settle_side
                        self._streak_count = 1
                        self._streak_pnl = w.pnl
                else:
                    self._streak_count = 0
                    self._streak_pnl = 0.0
                    if _settle_side:
                        self._streak_side = _settle_side

                self.total_pnl += w.pnl
                # Bankroll update: buy costs were already deducted, sell proceeds already added.
                # At settlement, only the remaining shares' redemption value is new cash.
                if getattr(w, 'approach', '') == "HEDGED":
                    # HEDGED: winning shares pay $1, losing = $0. Cost already deducted at entry.
                    # Sells already credited via _sell_hedged(). Only add redemption of remaining shares.
                    if w.result == "UP_WON":
                        _settle_cash = getattr(w, 'up_shares', 0) * 1.00
                    else:
                        _settle_cash = getattr(w, 'down_shares', 0) * 1.00
                    self.engine.bankroll += _settle_cash
                else:
                    # DIRECTIONAL: only remaining winning shares redeem for $1
                    if won:
                        _remain_settle_sh = float(getattr(w, 'up_shares', 0) if w.bet_side == "UP"
                                                  else getattr(w, 'down_shares', 0))
                        self.engine.bankroll += _remain_settle_sh * 1.00
                    # Loser: $0 redemption, cost already deducted — nothing to add
                self.engine.record_result(w.pnl > 0, w.pnl)
                self._metrics["windows_total"] += 1
                self._metrics["window_pnl_total"] += w.pnl
                if w.pnl > 0:
                    self._metrics["windows_profitable"] += 1

                # UP-bias PNL attribution (split by mode)
                if getattr(w, '_up_bias_entry', False):
                    _ub_m = getattr(w, '_up_bias_mode', "hard")
                    if _ub_m == "soft":
                        self._metrics["up_bias_soft_pnl"] += w.pnl
                    else:
                        self._metrics["up_bias_hard_pnl"] += w.pnl
                    self._up_bias_entry_windows += 1
                    # Auto-disable forced flips after 30 bias-entry windows if net negative
                    _total_bias_pnl = (self._metrics["up_bias_soft_pnl"]
                                       + self._metrics["up_bias_hard_pnl"])
                    if (self._up_bias_entry_windows >= 30
                            and _total_bias_pnl < 0
                            and getattr(self.engine, '_up_bias_auto_flip', False)):
                        self.engine._up_bias_auto_flip = False
                        print(f"  ⚠ UP-BIAS AUTO-DISABLE: {self._up_bias_entry_windows} windows, "
                              f"PNL=${_total_bias_pnl:+.2f} → forced flips OFF")
                        self._log_event(
                            f"UP-BIAS: forced flips disabled (PNL=${_total_bias_pnl:+.2f} "
                            f"after {self._up_bias_entry_windows} windows)", "error")
                        self._save_persisted_state()

                # result_str must reflect P&L sign, not directional correctness.
                # A directionally-correct bet can still lose money (drag, partial sells).
                # Display must never show "WIN" with negative P&L.
                result_str = "WIN" if w.pnl > 0 else "LOSS"
                realized_str = f" (realized: ${realized:+.2f})" if abs(realized) > 0.01 else ""
                print(f"\n  {'*'*50}")
                print(f"  SETTLED: {w.asset.upper()} {w.interval_label} → {w.result}")
                print(f"  Bet: {w.bet_side} | Result: {result_str} | P&L: ${w.pnl:+.4f}{realized_str}")
                print(f"  Bankroll: ${self.engine.bankroll:.4f} | "
                      f"Record: {self.wins}W-{self.losses}L | Total P&L: ${self.total_pnl:+.4f}")
                print(f"  {'*'*50}\n")

                # ── Discord alert + event log ──
                self._send_settlement_alert(w, won, result_str)
                lvl = "win" if won else "loss"
                _up_sh_log = getattr(w, 'up_shares', 0) or 0
                _dn_sh_log = getattr(w, 'down_shares', 0) or 0
                _approach_log = getattr(w, 'approach', '')
                _settle_detail = (f"↑{_up_sh_log:.1f} ↓{_dn_sh_log:.1f} " if _approach_log == "HEDGED" else f"{w.bet_side} ")
                self._log_event(
                    f"SETTLED {w.asset} {w.interval_label} · {_settle_detail}→ {result_str} · P&L ${w.pnl:+.2f} · Bank ${self.engine.bankroll:.2f}",
                    lvl,
                )

                # KPI: track settlement PnL for sell_pnl/settle_pnl ratio
                if getattr(w, 'approach', '') == "DIRECTIONAL":
                    self._metrics.setdefault("kpi_settle_pnl", 0.0)
                    self._metrics["kpi_settle_pnl"] += w.pnl

                # Start settlement cooldown + early redeem for REAL bets
                # Trigger even if bot is currently in dry mode — the BUY was real,
                # so tokens exist on-chain and need redeeming.
                _oid_settle = getattr(w, 'bet_order_id', None)
                _was_real_trade = (_oid_settle and not str(_oid_settle).startswith("DRY_"))
                if _was_real_trade or not self.dry_run:
                    cd_secs = 2
                    self._settle_cooldown_until = time.time() + cd_secs
                    print(f"  Settlement cooldown: waiting {cd_secs}s for token redemption"
                          f"{' (real trade in dry mode)' if self.dry_run else ''}")
                    # Force immediate redeem check + burst mode (10s checks for 2 min)
                    self._last_redeem_check = 0
                    self._redeem_burst_until = time.time() + 120

                # Log settlement to DB
                # Use the ORIGINAL trade's dry_run status — not current mode.
                # If the BUY was real (has a real CLOB order ID, not DRY_*), the
                # settlement is real regardless of whether we switched to dry since.
                _settle_remain_sh = float(getattr(w, 'up_shares', 0) if w.bet_side == "UP"
                                          else getattr(w, 'down_shares', 0))
                _oid = getattr(w, 'bet_order_id', None)
                _was_real = (_oid and not str(_oid).startswith("DRY_"))
                _settle_dry = (not _was_real) if _oid else self.dry_run
                db_queue.add_trade({
                    "market": w.slug, "up_price": w.up_price, "down_price": w.down_price,
                    "total_cost": w.total_cost, "profit_pct": 0,
                    "shares": round(_settle_remain_sh, 3),
                    "investment": round(w.total_cost, 4),
                    "expected_profit": round(w.pnl, 4), "dry_run": _settle_dry,
                    "asset": w.asset,
                    "extra": {
                        "side": w.bet_side, "action": "SETTLE",
                        "result": result_str, "pnl": round(w.pnl, 4),
                        "approach": getattr(w, 'approach', ''),
                        "dry_run": _settle_dry,
                        "entry_price": round(w.bet_price, 4) if w.bet_price else 0,
                        "remaining_shares": round(_settle_remain_sh, 3),
                        "remaining_cost": round(w.total_cost, 4),
                        "realized_pnl": round(realized, 4),
                        "stage1_done": getattr(w, '_partial_cut_done', False),
                        "entry_shares": round(w.bet_shares, 3) if w.bet_shares else 0,
                        "wallet_lean": round(self._last_wallet_lean, 3) if getattr(self, '_last_wallet_lean', None) is not None else None,
                        "wallet_dir": getattr(self, '_last_wallet_dir', None),
                    },
                })

                # ── Cut-quality evaluation (Imp 6c) ──
                if w.result in ("UP_WON", "DOWN_WON"):
                    _remaining_audit = []
                    for ca in self._cut_audit:
                        if ca["slug"] != w.slug or ca["start_ts"] != w.start_ts:
                            _remaining_audit.append(ca)
                            continue
                        _would_won = ((ca["bet_side"] == "UP" and w.result == "UP_WON") or
                                      (ca["bet_side"] == "DOWN" and w.result == "DOWN_WON"))
                        if _would_won:
                            self._metrics.setdefault("cut_then_would_have_won", 0)
                            self._metrics["cut_then_would_have_won"] += 1
                            print(f"    CUT AUDIT: would have WON (p={ca['p_smooth']:.2f})")
                        else:
                            self._metrics.setdefault("cut_saved_loss", 0)
                            self._metrics["cut_saved_loss"] += 1
                            print(f"    CUT AUDIT: saved loss (p={ca['p_smooth']:.2f})")
                        # Hour-bucket tracking
                        self._metrics.setdefault("cut_quality_by_hour", {})
                        h = ca.get("hour", "??")
                        self._metrics["cut_quality_by_hour"].setdefault(h, {"won": 0, "saved": 0})
                        self._metrics["cut_quality_by_hour"][h]["won" if _would_won else "saved"] += 1
                        # DB record for persistent analysis
                        db_queue.add_trade({
                            "market": ca["slug"], "up_price": w.up_price, "down_price": w.down_price,
                            "total_cost": 0, "profit_pct": 0, "shares": 0,
                            "investment": 0, "expected_profit": 0,
                            "dry_run": self.dry_run, "asset": w.asset,
                            "extra": {
                                "action": "CUT_AUDIT", "bet_side": ca["bet_side"],
                                "result": w.result, "would_have_won": _would_won,
                                "exit_pnl": ca["exit_pnl"], "total_spent": ca["total_spent"],
                                "p_smooth_at_cut": ca["p_smooth"], "hour": h,
                                "dry_run": self.dry_run,
                            },
                        })
                    self._cut_audit = _remaining_audit

            # status already set to "closed" at top of loop

        for slug in to_close:
            w = self._active_windows.pop(slug)
            self._closed_windows.append(w)
            # Clean up exit fail counter for closed windows
            if hasattr(self, '_exit_fail_count'):
                self._exit_fail_count.pop(slug, None)
            # Keep only last 50 closed
            if len(self._closed_windows) > 50:
                self._closed_windows = self._closed_windows[-50:]
        if to_close:
            self._save_persisted_state()  # Persist updated P&L after settlement

    # ------------------------------------------------------------------
    # Real-time Event Log (for dashboard terminal feed)
    # ------------------------------------------------------------------

    def _log_event(self, msg: str, level: str = "info"):
        """Append a timestamped event to the dashboard terminal log.

        Collapses repeated identical messages into a counter to prevent
        terminal flooding from burst-filtered signals.
        """
        now = time.time()
        # Collapse repeated messages (same level + msg within 5s)
        if self._event_log:
            last = self._event_log[-1]
            if last.get("msg") == msg and last.get("level") == level and now - last["ts"] < 5:
                last["repeat"] = last.get("repeat", 1) + 1
                last["ts"] = now  # update timestamp to latest
                return

        self._event_log.append({
            "ts": now,
            "level": level,  # info, buy, sell, win, loss, error, redeem
            "msg": msg,
            "repeat": 1,
        })
        if len(self._event_log) > 200:
            self._event_log = self._event_log[-200:]

    # ------------------------------------------------------------------
    # Telegram Command Bot — receive commands from phone
    # ------------------------------------------------------------------

    async def _tg_send(self, text: str, parse_mode: str = "HTML"):
        """Send a message to the Telegram chat."""
        if not self._tg_enabled:
            return
        try:
            url = f"https://api.telegram.org/bot{self._tg_token}/sendMessage"
            async with httpx.AsyncClient(timeout=10) as c:
                await c.post(url, json={
                    "chat_id": self._tg_chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                })
        except Exception as e:
            print(f"  TG send error: {e}")

    async def _tg_poll(self):
        """Poll Telegram for new commands. Non-blocking, self-throttled."""
        if not self._tg_enabled:
            return
        now = time.time()
        if now - self._tg_last_poll < self._tg_poll_interval:
            return
        self._tg_last_poll = now

        try:
            url = f"https://api.telegram.org/bot{self._tg_token}/getUpdates"
            async with httpx.AsyncClient(timeout=8) as c:
                resp = await c.get(url, params={
                    "offset": self._tg_offset,
                    "timeout": 0,  # non-blocking
                    "allowed_updates": '["message"]',
                })
            if resp.status_code != 200:
                return
            data = resp.json()
            if not data.get("ok"):
                return

            for update in data.get("result", []):
                self._tg_offset = update["update_id"] + 1
                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text = (msg.get("text") or "").strip()

                # Only respond to our chat
                if chat_id != str(self._tg_chat_id):
                    continue
                if not text:
                    continue

                await self._tg_handle_command(text)

        except Exception as e:
            # Silently fail — non-critical
            if "timeout" not in str(e).lower():
                print(f"  TG poll error: {e}")

    async def _tg_handle_command(self, text: str):
        """Handle an incoming Telegram command."""
        cmd = text.lower().strip().lstrip("/")
        parts = cmd.split()
        cmd_name = parts[0] if parts else ""
        args = parts[1:] if len(parts) > 1 else []

        if cmd_name in ("status", "s", "stat"):
            await self._tg_cmd_status()
        elif cmd_name in ("pnl", "pl", "profit"):
            await self._tg_cmd_pnl()
        elif cmd_name in ("sell", "exit", "close"):
            await self._tg_cmd_sell()
        elif cmd_name in ("pause", "p", "stop"):
            self._bot_paused = True
            self._log_event("BOT PAUSED (via Telegram)", "info")
            await self._tg_send("⏸ Bot paused. Send /resume to continue.")
        elif cmd_name in ("resume", "r", "go", "start"):
            self._bot_paused = False
            # Also reset engine halt if it was halted (session stop-loss)
            _was_halted = getattr(self.engine, '_halted', False)
            if _was_halted:
                self.engine.reset_halt()
                self._log_event("BOT RESUMED + HALT RESET (via Telegram)", "info")
                await self._tg_send(
                    "▶️ Bot resumed.\n"
                    f"🔄 Session stop-loss RESET — peak rebased to ${self.engine.bankroll:.2f}"
                )
            else:
                self._log_event("BOT RESUMED (via Telegram)", "info")
                await self._tg_send("▶️ Bot resumed.")
        elif cmd_name in ("mode",):
            if args and args[0] in ("copy", "value", "kelly"):
                new_mode = args[0]
                self._trade_mode = new_mode
                self._save_persisted_state()
                self._log_event(f"MODE → {new_mode.upper()} (via Telegram)", "info")
                await self._tg_send(f"🔄 Mode switched to <b>{new_mode.upper()}</b>")
            else:
                await self._tg_send(f"Current mode: <b>{self._trade_mode.upper()}</b>\nUsage: /mode kelly|copy|value")
        elif cmd_name in ("aggression", "agg", "kelly"):
            if args:
                try:
                    agg = int(args[0])
                    agg = max(1, min(5, agg))
                    self._kelly_aggression = agg
                    self._save_persisted_state()
                    labels = {1: "Conservative", 2: "Moderate", 3: "Aggressive", 4: "Very Aggressive", 5: "YOLO"}
                    await self._tg_send(f"🎰 Kelly aggression: <b>{agg}/5</b> ({labels.get(agg, '')})")
                except ValueError:
                    await self._tg_send(f"Current: {self._kelly_aggression}/5\nUsage: /aggression 3")
            else:
                labels = {1: "Conservative", 2: "Moderate", 3: "Aggressive", 4: "Very Aggressive", 5: "YOLO"}
                await self._tg_send(f"🎰 Kelly aggression: <b>{self._kelly_aggression}/5</b> ({labels.get(self._kelly_aggression, '')})\nUsage: /aggression 1-5")
        elif cmd_name in ("balance", "bal", "b"):
            await self._tg_cmd_balance()
        elif cmd_name in ("gate", "g"):
            await self._tg_cmd_gate(args)
        elif cmd_name in ("hours",):
            await self._tg_cmd_hours(args)
        elif cmd_name in ("wallet", "w", "abrak"):
            await self._tg_cmd_wallet()
        elif cmd_name in ("metrics", "m", "stats"):
            await self._tg_cmd_metrics()
        elif cmd_name in ("unhalt", "reset"):
            if getattr(self.engine, '_halted', False):
                self.engine.reset_halt()
                self._bot_paused = False
                self._log_event("HALT RESET (via Telegram)", "info")
                self._save_persisted_state()  # persist new peak across restarts
                await self._tg_send(
                    "🔄 Session stop-loss RESET\n"
                    f"Peak rebased to ${self.engine.bankroll:.2f}\n"
                    "▶️ Bot is active again."
                )
            else:
                await self._tg_send("✅ Bot is not halted. No action needed.")
        elif cmd_name in ("maxbet", "bet", "setbet"):
            if args:
                try:
                    amt = float(args[0])
                    if amt < 0.10:
                        await self._tg_send("⚠️ Min $0.10")
                    elif amt > 100.0:
                        await self._tg_send("⚠️ Max $100.00")
                    else:
                        self.engine.max_bet_dollars = amt
                        self._save_persisted_state()
                        await self._tg_send(f"✅ Max bet set to <b>${amt:.2f}</b>")
                except ValueError:
                    await self._tg_send("Usage: /maxbet 2.50")
            else:
                await self._tg_send(
                    f"💰 Max bet: <b>${self.engine.max_bet_dollars:.2f}</b>\n"
                    f"Bankroll: ${self.engine.bankroll:.2f}\n"
                    f"Usage: /maxbet <amount>"
                )
        elif cmd_name in ("streak",):
            _cl = getattr(self.engine, '_consecutive_losses', 0)
            # Compute current multiplier (purely defensive)
            if _cl == 0:
                _sm = 1.00
            elif _cl == 1:
                _sm = 0.85
            elif _cl == 2:
                _sm = 0.75
            elif _cl == 3:
                _sm = 0.65
            else:
                _sm = 0.50
            _base = self.engine.max_bet_dollars
            _effective = round(_base * _sm, 2)
            await self._tg_send(
                f"📊 <b>Streak Status</b>\n"
                f"Consecutive losses: <b>{_cl}</b>\n"
                f"Streak multiplier: <b>{_sm:.2f}x</b>\n"
                f"Base bet: ${_base:.2f}\n"
                f"Effective bet: <b>${_effective:.2f}</b>\n\n"
                f"<i>Table: 0L=1.0x, 1L=0.85x, 2L=0.75x, 3L=0.65x, 4+L=0.50x</i>"
            )
        elif cmd_name in ("log", "events", "e"):
            await self._tg_cmd_log()
        elif cmd_name in ("live",):
            # SAFETY: require "/live CONFIRM" to prevent accidental activation
            if len(parts) < 2 or parts[1].upper() != "CONFIRM":
                await self._tg_send(
                    "⚠️ Live mode requires confirmation.\n"
                    "Send: <code>/live CONFIRM</code>\n"
                    f"Current balance: ${self._usdce_balance:.2f} USDC.e"
                )
            elif self.dry_run:
                self.dry_run = False
                self._save_persisted_state()
                self._log_event("LIVE MODE (via Telegram)", "info")
                await self._tg_send("🔴 <b>LIVE MODE</b> activated")
            else:
                await self._tg_send("Already in LIVE mode")
        elif cmd_name in ("dry", "dryrun"):
            if not self.dry_run:
                self.dry_run = True
                # Reset halt so dry-run starts collecting data immediately
                if getattr(self.engine, '_halted', False):
                    self.engine.reset_halt()
                self._save_persisted_state()
                self._log_event("DRY RUN (via Telegram)", "info")
                await self._tg_send("🟡 <b>DRY RUN</b> mode activated\n💡 Halt reset, data collection active")
            else:
                await self._tg_send("Already in DRY RUN mode")
        elif cmd_name in ("help", "h", "?"):
            await self._tg_cmd_help()
        else:
            await self._tg_send(
                f"Unknown: <code>{cmd_name}</code>\nSend /help for commands."
            )

    async def _tg_cmd_status(self):
        """Send bot status summary."""
        mode = self._trade_mode.upper()
        _halted = getattr(self.engine, '_halted', False)
        if _halted:
            paused = "⛔ HALTED (stop-loss)"
        elif self._bot_paused:
            paused = "⏸ PAUSED"
        else:
            paused = "▶️ RUNNING"
        live = "🔴 LIVE" if not self.dry_run else "🟡 DRY RUN"
        clob = self._clob_balance if self._clob_balance > 0 else self.engine.bankroll

        # Active windows with P&L
        windows_info = ""
        for slug, w in self._active_windows.items():
            tr = int(w.time_remaining)
            if w.bet_side:
                _sh = float(w.up_shares if w.bet_side == "UP" else w.down_shares)
                _cost = float(w.up_cost if w.bet_side == "UP" else w.down_cost) if hasattr(w, 'up_cost') else 0
                _bid = w.up_best_bid if w.bet_side == "UP" else w.down_best_bid
                _mark = _sh * _bid if _sh > 0 and _bid > 0 else 0
                _upnl = _mark - _cost if _cost > 0 else 0
                emoji = "🟢" if _upnl >= 0 else "🔴"
                _mode = getattr(w, 'scalp_mode', '?')
                windows_info += f"\n{emoji} {w.bet_side} {_sh:.1f}sh · ${_upnl:+.2f} · {_mode} · {tr}s"
            else:
                _mode = getattr(w, 'scalp_mode', 'WAIT')
                windows_info += f"\n⏱ {w.asset} {w.interval_label}: {_mode} · {tr}s"

        stats = f"📊 {self.wins}W/{self.losses}L ({self.wins/(self.wins+self.losses)*100:.0f}%)" if (self.wins + self.losses) > 0 else ""

        # Gate + wallet info
        _gate_str = ""
        if self._time_gate_enabled:
            if getattr(self, 'wallet_signal', None) and self.wallet_signal.enabled:
                _wu = self.wallet_signal._cached_up_shares
                _wd = self.wallet_signal._cached_down_shares
                _wt = _wu + _wd
                _gate_str = f"\n🔒 Gate: Abrak {'ACTIVE' if _wt >= 10 else 'INACTIVE'} ({_wt:.0f}sh)"
            else:
                _gate_str = "\n🔒 Gate: time-of-day"
        else:
            _gate_str = "\n🔓 Gate: OFF"

        # Wallet direction
        _wdir_str = ""
        if getattr(self, 'wallet_signal', None) and self.wallet_signal.enabled:
            _wu = self.wallet_signal._cached_up_shares
            _wd = self.wallet_signal._cached_down_shares
            if _wu + _wd > 0:
                _lean = (_wu - _wd) / (_wu + _wd)
                _dir = "UP" if _lean > 0.1 else ("DOWN" if _lean < -0.1 else "NEUTRAL")
                _wdir_str = f"\n📡 Wallet: {_dir} (lean={_lean:+.2f} · ↑{_wu:.0f}/↓{_wd:.0f})"

        _eff_bet = round(self.engine.max_bet_dollars * (self._last_wallet_mult if getattr(self, '_last_wallet_mult', None) is not None else 1.0), 2)

        # Drawdown info
        _dd_str = ""
        _peak = getattr(self.engine, '_peak_bankroll', 0)
        if _peak > 0 and self.engine.bankroll < _peak:
            _dd_pct = (_peak - self.engine.bankroll) / _peak * 100
            _dd_str = f"\n📉 Drawdown: {_dd_pct:.1f}% from peak ${_peak:.2f}"

        _halt_hint = "\n💡 Send /unhalt to reset and resume" if _halted else ""

        _dep = self._initial_deposit
        _net = clob - _dep
        _net_str = f"${_net:+.2f}" if _dep > 0 else f"${self.total_pnl:+.2f}"

        msg = (
            f"{paused} · {live}\n"
            f"💰 <b>${clob:.2f}</b> · Net: {_net_str}"
            f"{_dd_str}\n"
            f"📋 {mode} · Bet: ${self.engine.max_bet_dollars:.0f}"
            f"{_gate_str}{_wdir_str}{windows_info}\n"
            f"{stats}{_halt_hint}"
        )
        await self._tg_send(msg)

    async def _tg_cmd_balance(self):
        """Send balance breakdown."""
        _clob = self._clob_balance if self._clob_balance > 0 else self.engine.bankroll
        _usdce = self._usdce_balance if self._usdce_balance > 0 else 0
        _dep = self._initial_deposit
        _net = _clob - _dep
        _pct = (_net / _dep * 100) if _dep > 0 else 0
        _mode = "🔴 LIVE" if not self.dry_run else "🟡 DRY"
        msg = (
            f"💰 <b>Balance</b>\n"
            f"CLOB: <b>${_clob:.2f}</b> (tradeable)\n"
            f"USDC.e: ${_usdce:.2f} (on-chain)\n"
            f"Deposit: ${_dep:.2f}\n"
            f"Net: <b>${_net:+.2f}</b> ({_pct:+.1f}%)\n"
            f"{_mode}"
        )
        await self._tg_send(msg)

    async def _tg_cmd_pnl(self):
        """Show P&L summary."""
        total = self.wins + self.losses
        wr = (self.wins / total * 100) if total > 0 else 0
        clob = self._clob_balance if self._clob_balance > 0 else self.engine.bankroll
        dep_pnl = clob - self._initial_deposit

        # Recent trades
        recent_wins = 0
        recent_losses = 0
        recent_pnl = 0.0
        for w in list(self._closed_windows)[-20:]:
            if w.pnl >= 0:
                recent_wins += 1
            else:
                recent_losses += 1
            recent_pnl += w.pnl

        _pct = (dep_pnl / self._initial_deposit * 100) if self._initial_deposit > 0 else 0
        _avg_win = 0.0
        _avg_loss = 0.0
        _win_count = 0
        _loss_count = 0
        for w in list(self._closed_windows)[-100:]:
            if w.pnl >= 0:
                _avg_win += w.pnl
                _win_count += 1
            else:
                _avg_loss += w.pnl
                _loss_count += 1
        _avg_win = _avg_win / _win_count if _win_count > 0 else 0
        _avg_loss = _avg_loss / _loss_count if _loss_count > 0 else 0

        msg = (
            f"📊 <b>P&L Summary</b>\n"
            f"💰 Balance: <b>${clob:.2f}</b>\n"
            f"📈 Net: <b>${dep_pnl:+.2f}</b> ({_pct:+.1f}%) from ${self._initial_deposit:.2f}\n"
            f"🎯 {self.wins}W/{self.losses}L ({wr:.0f}%) · {self.total_bets} bets\n"
            f"Avg win: ${_avg_win:+.2f} · Avg loss: ${_avg_loss:+.2f}"
        )
        if recent_wins + recent_losses > 0:
            rwr = recent_wins / (recent_wins + recent_losses) * 100
            msg += f"\n\n<b>Last {recent_wins + recent_losses}:</b> {recent_wins}W/{recent_losses}L ({rwr:.0f}%) · ${recent_pnl:+.2f}"
        await self._tg_send(msg)

    async def _tg_cmd_sell(self):
        """Force sell the current position (HEDGED-aware: sells both sides)."""
        sold = False
        for slug, w in list(self._active_windows.items()):
            if not w.bet_side or w.status == "closed":
                continue

            approach = getattr(w, 'approach', '')
            total_pnl = 0.0
            total_proceeds = 0.0
            sell_msgs = []

            # HEDGED: sell both sides via unified pipeline
            if approach == "HEDGED":
                for sell_side in ["UP", "DOWN"]:
                    sh = float(w.up_shares if sell_side == "UP" else w.down_shares)
                    bid = w.up_best_bid if sell_side == "UP" else w.down_best_bid
                    if sh < 0.5:
                        continue
                    if bid <= 0.01:
                        await self._tg_send(f"⚠️ {sell_side} bid too low ({bid:.3f}), skipping")
                        continue

                    if not self.dry_run:
                        r = await self._sell_hedged(
                            w, sell_side, sh, bid,
                            reason="TG-SELL", retry=True,
                        )
                        if r["ok"]:
                            total_pnl += r["pnl"]
                            total_proceeds += r["proceeds"]
                            sell_msgs.append(f"  {sell_side}: {r['fill_shares']:.1f}sh → ${r['proceeds']:.2f} (P&L ${r['pnl']:+.2f})")
                        else:
                            await self._tg_send(f"❌ {sell_side} sell failed: {r['reason']}")
                    else:
                        # Dry run: estimate
                        cost = float((w.up_cost if sell_side == "UP" else w.down_cost) or 0)
                        est_proceeds = sh * bid
                        est_pnl = est_proceeds - cost
                        total_pnl += est_pnl
                        total_proceeds += est_proceeds
                        sell_msgs.append(f"  {sell_side}: {sh:.1f}sh → ~${est_proceeds:.2f} (P&L ~${est_pnl:+.2f})")

            else:
                # Legacy single-side sell
                if w.bet_side == "UP":
                    cur_price = w.up_price_direct if w.up_price_direct > 0 else w.up_price
                else:
                    cur_price = w.down_price_direct if w.down_price_direct > 0 else w.down_price
                if cur_price <= 0:
                    continue

                sh = float(getattr(w, 'bet_shares', 0))
                if sh < 0.5:
                    continue

                if not self.dry_run and self._poly_client:
                    r = await self._sell_hedged(
                        w, w.bet_side, sh, cur_price,
                        reason="TG-SELL", retry=True,
                    )
                    if r["ok"]:
                        total_pnl = r["pnl"]
                        total_proceeds = r["proceeds"]
                        sell_msgs.append(f"  {w.bet_side}: {r['fill_shares']:.1f}sh → ${r['proceeds']:.2f}")
                    else:
                        await self._tg_send(f"❌ Sell failed: {r['reason']}")
                        continue
                else:
                    est_pnl = (cur_price - w.bet_price) * sh
                    total_pnl = est_pnl
                    sell_msgs.append(f"  {w.bet_side}: {sh:.1f}sh @ ${cur_price:.3f}")

            if not sell_msgs:
                continue

            # Record
            self._log_event(
                f"TG SELL {w.asset} {w.interval_label} · P&L ${total_pnl:+.2f}",
                "sell" if total_pnl >= 0 else "loss"
            )
            w.pnl = total_pnl
            w.result = "TG_SELL"
            w.status = "closed"
            w.btc_at_close = self._btc_price
            self.total_pnl += total_pnl
            self.engine.record_result(total_pnl >= 0, total_pnl)
            if total_pnl >= 0:
                self.wins += 1
            else:
                self.losses += 1
            self._active_windows.pop(slug, None)
            self._closed_windows.append(w)
            self._save_persisted_state()

            emoji = "🟢" if total_pnl >= 0 else "🔴"
            detail = "\n".join(sell_msgs)
            await self._tg_send(
                f"{emoji} <b>SOLD</b> {w.asset} {w.interval_label}\n"
                f"{detail}\n"
                f"  Total P&L: <b>${total_pnl:+.2f}</b>"
            )
            sold = True

        if not sold:
            await self._tg_send("No active positions to sell.")

    async def _tg_cmd_log(self):
        """Show recent event log."""
        events = self._event_log[-10:]
        if not events:
            await self._tg_send("No events yet.")
            return
        lines = ["<b>Recent Events</b>\n"]
        for e in events:
            ts = datetime.fromtimestamp(e["ts"]).strftime("%H:%M:%S")
            lvl = e.get("level", "info")
            icon = {"buy": "🟢", "sell": "🔵", "win": "🏆", "loss": "❌", "error": "⚠️", "info": "ℹ️"}.get(lvl, "·")
            lines.append(f"{icon} <code>{ts}</code> {e['msg'][:60]}")
        await self._tg_send("\n".join(lines))

    async def _tg_cmd_gate(self, args: list):
        """Toggle or show Abrak activity gate status."""
        if args and args[0] in ("on", "enable"):
            self._time_gate_enabled = True
            self._save_persisted_state()
            await self._tg_send("🔒 Gate <b>ENABLED</b>")
            return
        elif args and args[0] in ("off", "disable"):
            self._time_gate_enabled = False
            self._save_persisted_state()
            await self._tg_send("🔓 Gate <b>DISABLED</b>")
            return

        # Show current gate state
        _lines = [f"🔒 Gate: <b>{'ON' if self._time_gate_enabled else 'OFF'}</b>"]
        if getattr(self, 'wallet_signal', None) and self.wallet_signal.enabled:
            _wu = self.wallet_signal._cached_up_shares
            _wd = self.wallet_signal._cached_down_shares
            _wt = _wu + _wd
            _lines.append(f"  Mode: Abrak activity (≥10 shares)")
            _lines.append(f"  Abrak: ↑{_wu:.0f} ↓{_wd:.0f} = {_wt:.0f}sh")
            _lines.append(f"  Status: {'✅ ACTIVE' if _wt >= 10 else '❌ INACTIVE'}")
        else:
            _hrs = ",".join(str(h) for h in sorted(self._time_gate_hours))
            _lines.append(f"  Mode: time-of-day [{_hrs}] ET")
        _hrs_str = ",".join(str(h) for h in sorted(self._time_gate_hours))
        _lines.append(f"  Hours: [{_hrs_str}] ET")
        _gi = self._metrics.get("gate_abrak_inactive", 0)
        _gt = self._metrics.get("gate_time_of_day", 0)
        if _gi or _gt:
            _lines.append(f"  Blocked: abrak={_gi} time={_gt}")
        await self._tg_send("\n".join(_lines))

    async def _tg_cmd_hours(self, args: list):
        """View or set allowed ET hours for trading.

        Usage:
          /hours              — show current hours
          /hours 22,23,3-8    — set hours (ranges and commas)
          /hours all          — allow all hours (disable time gate)
        """
        if not args:
            _hrs = ",".join(str(h) for h in sorted(self._time_gate_hours))
            await self._tg_send(
                f"⏰ Hours (ET): <b>[{_hrs}]</b>\n"
                f"Gate: <b>{'ON' if self._time_gate_enabled else 'OFF'}</b>\n\n"
                f"/hours safe — 8h proven winners\n"
                f"/hours relaxed — 15h, blocks losers\n"
                f"/hours all — 24h no gate\n"
                f"/hours 0-4,7,18 — custom"
            )
            return

        raw = " ".join(args).strip().lower()
        if raw == "all":
            self._time_gate_hours = list(range(24))
            self._save_persisted_state()
            await self._tg_send("⏰ Hours: <b>ALL</b> (0-23 ET)")
            return
        if raw == "safe":
            # Narrow recent winners only.
            self._time_gate_hours = [0, 3, 4, 14, 19]
            self._save_persisted_state()
            _hrs = ",".join(str(h) for h in self._time_gate_hours)
            await self._tg_send(f"⏰ Hours: <b>SAFE</b> [{_hrs}] ET\n5h · recent dry-run winners only")
            return
        if raw == "relaxed":
            # Safe core + small positive / near-flat recent hours.
            self._time_gate_hours = [0, 3, 4, 14, 17, 19, 22]
            self._save_persisted_state()
            _hrs = ",".join(str(h) for h in self._time_gate_hours)
            await self._tg_send(f"⏰ Hours: <b>RELAXED</b> [{_hrs}] ET\n7h · adds only marginal recent hours")
            return

        # Parse ranges and commas: "22,23,3-8" → [22, 23, 3, 4, 5, 6, 7, 8]
        new_hours = set()
        for part in raw.replace(" ", ",").split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                try:
                    a, b = part.split("-", 1)
                    a, b = int(a), int(b)
                    if a <= b:
                        new_hours.update(range(a, b + 1))
                    else:
                        # Wrap around midnight: 22-3 → 22,23,0,1,2,3
                        new_hours.update(range(a, 24))
                        new_hours.update(range(0, b + 1))
                except ValueError:
                    await self._tg_send(f"❌ Bad range: {part}")
                    return
            else:
                try:
                    h = int(part)
                    if 0 <= h < 24:
                        new_hours.add(h)
                except ValueError:
                    await self._tg_send(f"❌ Bad hour: {part}")
                    return

        if not new_hours:
            await self._tg_send("❌ No valid hours parsed")
            return

        self._time_gate_hours = sorted(new_hours)
        self._save_persisted_state()
        _hrs = ",".join(str(h) for h in self._time_gate_hours)
        await self._tg_send(f"⏰ Hours set: <b>[{_hrs}]</b> ET")

    async def _tg_cmd_wallet(self):
        """Show Abrak wallet signal details."""
        ws = getattr(self, 'wallet_signal', None)
        if not ws or not ws.enabled:
            await self._tg_send("📡 Wallet signal: <b>DISABLED</b>")
            return
        _wu = ws._cached_up_shares
        _wd = ws._cached_down_shares
        _wt = _wu + _wd
        _lean = (_wu - _wd) / _wt if _wt > 0 else 0
        _dir = "UP" if _lean > 0.1 else ("DOWN" if _lean < -0.1 else "NEUTRAL")
        _mult = round(self._last_wallet_mult, 3) if getattr(self, '_last_wallet_mult', None) is not None else 1.0
        msg = (
            f"📡 <b>Wallet Signal</b>\n\n"
            f"Direction: <b>{_dir}</b>\n"
            f"Lean: {_lean:+.3f}\n"
            f"Shares: ↑{_wu:.0f} / ↓{_wd:.0f} ({_wt:.0f} total)\n"
            f"Size mult: {_mult:.2f}x\n"
            f"Active: {'✅' if _wt >= 10 else '❌'}\n\n"
            f"Confirms: {self._metrics.get('wallet_confirm', 0)} · "
            f"Disagree: {self._metrics.get('gate_wallet_disagree', 0)} · "
            f"No pos: {self._metrics.get('wallet_no_position', 0)}"
        )
        await self._tg_send(msg)

    async def _tg_cmd_metrics(self):
        """Show key performance metrics."""
        m = self._metrics
        # Pick the most meaningful counters
        msg = (
            f"📈 <b>Session Metrics</b>\n\n"
            f"<b>Gates</b>\n"
            f"  passed: {m.get('gate_passed', 0)}\n"
            f"  abrak_inactive: {m.get('gate_abrak_inactive', 0)}\n"
            f"  time_of_day: {m.get('gate_time_of_day', 0)}\n"
            f"  streak_block: {m.get('gate_streak_block', 0)}\n"
            f"  conviction_low: {m.get('gate_conviction_low', 0)}\n"
            f"  wallet_disagree: {m.get('gate_wallet_disagree', 0)}\n\n"
            f"<b>Cuts</b>\n"
            f"  staged_partial: {m.get('cut_staged_partial', 0)}\n"
            f"  staged_full: {m.get('cut_staged_full', 0)}\n"
            f"  staged_recovered: {m.get('cut_staged_recovered', 0)}\n"
            f"  flip_tp_recovered: {m.get('flip_tp_recovered', 0)}\n"
            f"  dir_flip_exits: {m.get('dir_flip_exits', 0)}\n\n"
            f"<b>Risk</b>\n"
            f"  risk_scale_half: {m.get('risk_scale_half', 0)}\n"
            f"  wallet_confirm: {m.get('wallet_confirm', 0)}\n"
            f"  wallet_sizing: {m.get('wallet_sizing_applied', 0)}\n"
            f"  mid_price_skip: {m.get('entry_mid_price_skip', 0)}\n"
            f"  high_price_skip: {m.get('entry_high_price_skip', 0)}\n\n"
            f"<b>Windows</b>\n"
            f"  total: {m.get('windows_total', 0)} · "
            f"profitable: {m.get('windows_profitable', 0)} · "
            f"P&L: ${m.get('window_pnl_total', 0):.2f}\n\n"
            f"<b>UP Bias</b>\n"
            f"  windows: {m.get('up_bias_windows_seen', 0)} · "
            f"soft: {m.get('up_bias_soft_entries', 0)} · "
            f"hard: {m.get('up_bias_hard_entries', 0)} · "
            f"flips: {m.get('up_bias_force_flips', 0)}\n"
            f"  soft_pnl: ${m.get('up_bias_soft_pnl', 0):.2f} · "
            f"hard_pnl: ${m.get('up_bias_hard_pnl', 0):.2f} · "
            f"auto_flip: {'ON' if getattr(self.engine, '_up_bias_auto_flip', False) else 'OFF'}"
        )
        await self._tg_send(msg)

    async def _tg_cmd_help(self):
        """Show available commands."""
        msg = (
            "<b>PolyBot Commands</b>\n\n"
            "<b>Monitor</b>\n"
            "/status — Positions + gate + wallet\n"
            "/pnl — P&L vs deposit\n"
            "/balance — CLOB bankroll\n"
            "/wallet — Abrak signal details\n"
            "/metrics — Gate/cut/risk counters\n"
            "/log — Recent events\n\n"
            "<b>Control</b>\n"
            "/pause — Pause bot\n"
            "/resume — Resume bot (+ reset halt)\n"
            "/unhalt — Reset session stop-loss\n"
            "/sell — Force sell position\n"
            "/gate on|off — Toggle Abrak gate\n"
            "/hours — Show/set allowed ET hours\n"
            "/live — Switch to live mode\n"
            "/dry — Switch to dry run\n"
            "/aggression 1-5 — Kelly sizing\n"
        )
        await self._tg_send(msg)

    # ------------------------------------------------------------------
    # Discord Alerts
    # ------------------------------------------------------------------

    def _send_settlement_alert(self, w, won: bool, result_str: str):
        """Send settlement alerts to Telegram and Discord."""
        if not w.bet_order_id and not self.dry_run:
            return  # No on-chain order — skip phantom alerts (allow dry for visibility)

        emoji = "💰" if won else "💀"
        _sh = float(w.up_shares if w.bet_side == "UP" else w.down_shares) if w.bet_side else 0
        cost = round(w.total_cost or (_sh * w.bet_price), 2)
        payout = round(_sh * 1.0, 2) if won else 0.0
        total = self.wins + self.losses
        wr = f"{self.wins/total:.0%}" if total > 0 else "N/A"
        pnl_str = f"+${w.pnl:.2f}" if w.pnl >= 0 else f"-${abs(w.pnl):.2f}"
        # Use CLOB balance as authoritative (not engine.bankroll which drifts in dry)
        _bank = self._clob_balance if self._clob_balance > 0 else self.engine.bankroll
        total_pnl_str = f"+${self.total_pnl:.2f}" if self.total_pnl >= 0 else f"-${abs(self.total_pnl):.2f}"
        _side_icon = "↑" if w.bet_side == "UP" else "↓"
        _mode_tag = " · DRY" if self.dry_run else ""

        # ── Telegram notification ──
        if self._tg_enabled:
            tg_msg = (
                f"{emoji} <b>{result_str}</b> · {_side_icon} {w.bet_side} BTC {w.interval_label}{_mode_tag}\n"
                f"{_sh:.1f}sh @${w.bet_price:.3f} → ${payout:.2f}\n"
                f"P&L: <b>{pnl_str}</b> · Bank: <b>${_bank:.2f}</b>\n"
                f"{self.wins}W-{self.losses}L ({wr}) · Total: {total_pnl_str}"
            )
            try:
                url = f"https://api.telegram.org/bot{self._tg_token}/sendMessage"
                httpx.post(url, json={"chat_id": self._tg_chat_id, "text": tg_msg, "parse_mode": "HTML"}, timeout=5.0)
            except Exception:
                pass  # fire and forget — don't crash settlement flow

        # ── Discord notification ──
        if not self._discord_webhook:
            return
        mode = "LIVE"
        embed = {
            "title": f"{emoji} {result_str} — {w.bet_side} BTC {w.interval_label}",
            "color": 0x00ff88 if won else 0xff3b5c,
            "fields": [
                {"name": "Side", "value": f"**{w.bet_side}**", "inline": True},
                {"name": "Entry", "value": f"${w.bet_price:.3f}", "inline": True},
                {"name": "Cost", "value": f"${cost:.2f}", "inline": True},
                {"name": "P&L", "value": f"**{pnl_str}**", "inline": True},
                {"name": "Record", "value": f"{self.wins}W-{self.losses}L ({wr})", "inline": True},
                {"name": "Total P&L", "value": f"**{total_pnl_str}**", "inline": True},
                {"name": "Bank", "value": f"${self.engine.bankroll:.2f}", "inline": True},
                {"name": "Mode", "value": mode, "inline": True},
            ],
            "footer": {"text": f"PolyBot Late-Snipe • {w.slug[-25:]}"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        _discord_post(self._discord_webhook, embeds=[embed])

    def _send_balance_alert(self, event_type: str, amount: float, new_balance: float):
        """Send Discord alert for balance changes (deposits/payouts)."""
        if not self._discord_webhook:
            return
        if self.dry_run:
            return  # Don't spam Discord with dry run balance events
        emoji = "💵" if event_type == "DEPOSIT" else "🏆"
        embed = {
            "title": f"{emoji} {event_type} — {'+' if amount >= 0 else ''}${abs(amount):.2f}",
            "color": 0x00d4aa if event_type == "PAYOUT" else 0x4d7cfe,
            "fields": [
                {"name": "New Balance", "value": f"${new_balance:.2f}", "inline": True},
                {"name": "Total P&L", "value": f"{'+'if self.total_pnl>=0 else ''}${self.total_pnl:.4f}", "inline": True},
            ],
            "footer": {"text": "PolyBot"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        _discord_post(self._discord_webhook, embeds=[embed])

    # ------------------------------------------------------------------
    # Console Display
    # ------------------------------------------------------------------

    def _print_dashboard(self):
        """Print live console dashboard — expanded real-time hub."""
        now = time.time()
        utc_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        mode_str = "DRY RUN" if self.dry_run else "🟢 LIVE"
        total = self.wins + self.losses
        wr = f"{self.wins/total:.0%}" if total > 0 else "N/A"
        W = 100  # terminal width
        display_bankroll = self._display_bankroll()
        if self.dry_run:
            bankroll_str = f"Tradeable: ${display_bankroll:.2f}  |  Sim: ${self.engine.bankroll:.2f}"
        else:
            bankroll_str = f"Bankroll: ${display_bankroll:.2f}"

        print(f"\033[2J\033[H", end="")  # Clear screen
        print(f"{'═'*W}")
        print(f"  POLYMARKET BTC 5m SCALPER — {mode_str} | ADAPTIVE DIRECTIONAL")
        print(f"  {utc_str} | Cycle #{self._cycle_count} | BTC: ${self._btc_price:,.2f}")
        print(f"{'─'*W}")
        print(f"  {bankroll_str}  |  "
              f"Session P&L: ${self.total_pnl:+.4f}  |  "
              f"Bets: {self.total_bets}  |  "
              f"W/L: {self.wins}/{self.losses} ({wr})")
        clob_str = f"${self._clob_balance:.2f}" if self._clob_balance > 0 else "?"
        usdce_str = f"${self._usdce_balance:.2f}" if self._usdce_balance > 0 else "?"
        print(f"  CLOB: {clob_str} (tradeable)  |  "
              f"USDC.e: {usdce_str} (on-chain)  |  "
              f"Kelly Cap: {self.engine.kelly_fraction_cap:.0%}  |  "
              f"Max Bet: ${self.engine.max_bet_dollars:.2f}")
        print(f"{'═'*W}")

        # ── ACTIVE MARKET ──
        active = sorted(self._active_windows.values(), key=lambda w: w.time_remaining)
        if active:
            print(f"\n  ┌─ ACTIVE MARKET {'─'*(W-19)}┐")
            for w in active:
                tr = w.time_remaining
                mins, secs = tr // 60, tr % 60
                time_into = (w.interval_secs or 300) - tr

                up_str = f"${w.up_price:.3f}" if w.up_price > 0 else " ... "
                dn_str = f"${w.down_price:.3f}" if w.down_price > 0 else " ... "

                print(f"  │  {w.asset.upper()} {w.interval_label}  │  "
                      f"⏱ {mins:02d}:{secs:02d} left ({time_into:.0f}s in)  │  "
                      f"↑ Up {up_str}  │  ↓ Dn {dn_str}  │  "
                      f"Depth: {w.up_depth:.0f}/{w.down_depth:.0f}")

                # Position line
                scalp_mode = getattr(w, 'scalp_mode', 'WAIT')
                approach = getattr(w, 'approach', '')
                up_sh = getattr(w, 'up_shares', 0.0)
                dn_sh = getattr(w, 'down_shares', 0.0)
                tc = getattr(w, 'trade_count', 0)
                ma = getattr(w, 'max_adds', 0)

                if w.bet_side:
                    up_cost = getattr(w, 'up_cost', 0.0)
                    dn_cost = getattr(w, 'down_cost', 0.0)
                    up_vwap = f"@${up_cost/up_sh:.3f}" if up_sh > 0 else ""
                    dn_vwap = f"@${dn_cost/dn_sh:.3f}" if dn_sh > 0 else ""
                    pos_str = f"↑{up_sh:.1f}sh{up_vwap}  ↓{dn_sh:.1f}sh{dn_vwap}"
                    total_cost = getattr(w, 'total_cost', 0.0) or 0.0
                    rpnl = getattr(w, 'realized_pnl', 0.0)
                    # Unrealized: current bid * shares - cost (use bid for realistic exit value)
                    u_pnl = 0.0
                    _up_bid = getattr(w, 'up_best_bid', 0) or w.up_price
                    _dn_bid = getattr(w, 'down_best_bid', 0) or w.down_price
                    if up_sh > 0 and _up_bid > 0:
                        u_pnl += up_sh * _up_bid - up_cost
                    if dn_sh > 0 and _dn_bid > 0:
                        u_pnl += dn_sh * _dn_bid - dn_cost
                    print(f"  │  MODE: {scalp_mode:6s}  │  "
                          f"APPROACH: {approach or 'N/A':13s}  │  "
                          f"BET: {w.bet_side}  │  "
                          f"{pos_str}  │  "
                          f"Trades: {tc}/{ma}")
                    print(f"  │  Cost: ${total_cost:.2f}  │  "
                          f"Realized: ${rpnl:+.2f}  │  "
                          f"Unrealized: ${u_pnl:+.2f}  │  "
                          f"Net: ${rpnl+u_pnl:+.2f}")
                else:
                    print(f"  │  MODE: {scalp_mode:6s}  │  "
                          f"APPROACH: {approach or 'waiting':13s}  │  "
                          f"No position  │  "
                          f"↑ 0sh  ↓ 0sh")
            print(f"  └{'─'*(W-2)}┘")
        else:
            print(f"\n  ⏳ No active markets — waiting for next window...")

        # ── CONVICTION ENGINE ──
        kl = self._kelly_live
        if kl:
            conv_val = kl.get("conviction", 0)
            side = kl.get("side", "?")
            p_sm = kl.get("p_smooth", 0.50)
            q_mid = kl.get("q_mid", 0.50)
            edge = kl.get("edge", 0)
            z_val = kl.get("z", 0)
            book_imb = kl.get("book_imb", 0)
            lag_val = kl.get("lag", 0)
            regime = kl.get("regime", "CHOP")

            # Visual conviction bar
            bar_len = 30
            filled = int(conv_val * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)

            print(f"\n  ┌─ PROBABILITY ENGINE {'─'*(W-23)}┐")
            print(f"  │  Signal: {side:4s}  │  "
                  f"Conviction: [{bar}] {conv_val:.3f}  │  "
                  f"p={p_sm:.3f} q={q_mid:.3f} edge={edge:.3f}")
            print(f"  │  "
                  f"z: {z_val:6.3f}  │  "
                  f"book: {book_imb:6.3f}  │  "
                  f"lag: {lag_val:6.3f}  │  "
                  f"regime: {regime}")

            # GBM model data from conviction pipeline (merged GBM + conviction data).
            # Old: _last_pipeline (legacy, stale when conviction pipeline is primary)
            pipeline = getattr(self.engine, '_last_conviction_pipeline', {})
            if not pipeline:
                pipeline = getattr(self.engine, '_last_pipeline', {})  # fallback
            if pipeline:
                z = pipeline.get('z_score', 0)
                p_up = pipeline.get('p_up_model', 0.5)
                mu = pipeline.get('cex_mu', 0)
                sigma = pipeline.get('cex_sigma', 0)
                edge_up = pipeline.get('edge_up', 0)
                edge_dn = pipeline.get('edge_dn', 0)
                pm_implied = pipeline.get('pm_implied', 50)
                _conv_pipe = getattr(self.engine, '_last_conviction_pipeline', {})
                gate = _conv_pipe.get('gate_reason', '')

                print(f"  │  GBM: z={z:+.3f}  P(up)={p_up:.3f}  │  "
                      f"drift={mu:.2e}  σ={sigma:.2e}  │  "
                      f"Edge: ↑{edge_up:+.3f} ↓{edge_dn:+.3f}  │  "
                      f"Mkt implied: {pm_implied:.1f}%")
                if gate:
                    print(f"  │  Gate: {gate}")

            print(f"  └{'─'*(W-2)}┘")

        # ── RECENT RESULTS ──
        recent_closed = [w for w in self._closed_windows if w.bet_side][-10:]
        if recent_closed:
            print(f"\n  ┌─ RECENT RESULTS {'─'*(W-20)}┐")
            for w in reversed(recent_closed):
                won = w.pnl > 0 if w.pnl else (
                    (w.bet_side == "UP" and w.result == "UP_WON") or
                    (w.bet_side == "DOWN" and w.result == "DOWN_WON"))
                icon = "🟢" if won else "🔴"
                ep = getattr(w, 'exit_price', 0)
                shares_str = f"{w.bet_shares:.1f}sh"
                # How it closed
                if "SELL" in (w.result or "") or "EXIT" in (w.result or ""):
                    if ep > 0:
                        exit_str = f"${w.bet_price:.3f}→${ep:.3f}"
                    else:
                        exit_str = f"@${w.bet_price:.3f} (sold)"
                    reason = getattr(w, 'exit_reason', '')
                    if reason:
                        exit_str += f" [{reason[:20]}]"
                elif "WON" in (w.result or ""):
                    # Use P&L sign, not market outcome — bet can be directionally right but negative P&L
                    _pnl_label = "WIN" if w.pnl > 0 else "LOSS"
                    _mkt_label = w.result  # e.g. UP_WON, DOWN_WON
                    exit_str = f"@${w.bet_price:.3f} → {_pnl_label} (mkt={_mkt_label})"
                elif w.result in ("WIN", "LOSS"):
                    exit_str = f"@${w.bet_price:.3f} → {'WIN' if w.pnl > 0 else 'LOSS'}"
                else:
                    exit_str = f"@${w.bet_price:.3f}"
                side_icon = "↑" if w.bet_side == "UP" else "↓"
                print(f"  │  {icon} {side_icon} {w.bet_side:4s} {w.asset.upper()} {w.interval_label} │ "
                      f"{shares_str:>8s} {exit_str:<30s} │ "
                      f"P&L: ${w.pnl:+.2f}")
            print(f"  └{'─'*(W-2)}┘")

        # ── ENGINE STATUS ──
        stats = self.engine.get_stats()
        halt_str = f" ⛔ HALTED: {stats['halt_reason']}" if stats['halted'] else ""
        cooldown_str = ""
        if stats['consecutive_losses'] >= 3:
            cooldown_str = f" | ⚠ Consec losses: {stats['consecutive_losses']}"

        print(f"\n  ENGINE: conviction+GBM | "
              f"stop-loss={self.engine.session_stop_loss_pct:.0%} | "
              f"ticks={stats['btc_ticks']}"
              f"{cooldown_str}{halt_str}")
        _dep = self._initial_deposit if self._initial_deposit > 0 else stats['initial_bankroll']
        print(f"  Drawdown: {stats['drawdown_pct']:.1f}% | "
              f"Deposit: ${_dep:.2f} → Current: ${stats['bankroll']:.2f}")
        print(f"{'═'*W}")

    # ------------------------------------------------------------------
    # Main Loop
    # ------------------------------------------------------------------

    async def run(self):
        """Main async loop — runs forever."""
        self._running = True
        self._ws_clients = []
        print(f"\nStarting continuous runner...")
        print(f"  Mode: {self._trade_mode} (engine={self.engine.mode})")
        print(f"  Intervals: {', '.join(self.interval_labels)}")
        print(f"  Assets: {', '.join(a.upper() for a in self.assets)}")
        print(f"  Dry Run: {self.dry_run}")
        print(f"  Bankroll: ${self.engine.bankroll:.2f}")
        print(f"  Max Bet: ${self.engine.max_bet_dollars:.2f}")
        print(f"  Kelly Cap: {self.engine.kelly_fraction_cap:.0%}")

        # Fetch initial balances (POL + USDC.e + native USDC + CLOB + proxy)
        owner_addr = self._owner_wallet_address()
        signer_addr = self._signer_wallet_address()
        try:
            self._pol_balance = config.get_pol_balance(signer_addr)
            self._usdce_balance = config.get_usdce_balance(owner_addr)
            self._native_usdc_balance = config.get_native_usdc_balance(owner_addr)
            self._proxy_balance = config.get_proxy_usdce_balance()
        except Exception:
            pass
        try:
            if self._poly_client:
                self._clob_balance = self._poly_client.get_clob_balance()
        except Exception:
            pass

        # === LIVE MODE PRE-FLIGHT CHECKS ===
        if not self.dry_run:
            print(f"\n  LIVE MODE PRE-FLIGHT CHECKS:")
            print(f"  Signer: {signer_addr}")
            if owner_addr.lower() != signer_addr.lower():
                print(f"  Trading wallet: {owner_addr}")

            usdce_bal = config.get_usdce_balance(owner_addr)
            native_bal = config.get_onchain_balance(owner_addr)
            pol_bal = self._pol_balance
            print(f"  USDC.e (on-chain): ${usdce_bal:.4f}")
            print(f"  Native USDC:       ${native_bal:.4f}")
            print(f"  POL (gas):         {pol_bal:.4f}")

            # Check CLOB balance FIRST (funds already deposited on Polymarket)
            clob_bal = 0.0
            if self._poly_client:
                try:
                    self._poly_client.set_allowances()
                    print(f"  Allowances: OK")
                except Exception as e:
                    print(f"  Allowances: FAILED ({e})")
                # Cancel orphaned orders from previous session
                try:
                    self._poly_client.cancel_all_orders()
                    print(f"  Cancel stale orders: OK")
                except Exception as e:
                    print(f"  Cancel stale orders: FAILED ({e})")
                try:
                    clob_bal = self._poly_client.get_clob_balance()
                    self._clob_balance = clob_bal
                    print(f"  CLOB Balance:      ${clob_bal:.4f}")
                except Exception:
                    pass

            # CLOB balance is what's actually tradeable on the exchange
            # USDC.e on-chain needs to be deposited first
            total_available = clob_bal if clob_bal > 0 else usdce_bal
            print(f"  Tradeable (CLOB):  ${clob_bal:.4f}")
            if usdce_bal > clob_bal + 0.50:
                print(f"  Undeposited USDC.e: ${usdce_bal - clob_bal:.4f} (deposit via Polymarket to trade)")

            if total_available < 0.50:
                print(f"\n  WARNING: Insufficient funds (${total_available:.2f})")
                if native_bal > 1.0:
                    print(f"  You have ${native_bal:.2f} in native USDC — swap to USDC.e first!")
                print(f"  Falling back to DRY RUN mode.")
                print(f"  Use dashboard GO LIVE button when ready.\n")
                self.dry_run = True
            elif pol_bal < 0.01:
                print(f"\n  WARNING: No POL for gas fees. Transactions will fail.")
                print(f"  Get some POL from a faucet or exchange.")
                print(f"  Falling back to DRY RUN mode.\n")
                self.dry_run = True
            else:
                self.engine.bankroll = total_available
                print(f"  PRE-FLIGHT: ALL CHECKS PASSED")
                print(f"  Trading with ${total_available:.2f}\n")

        # Start web dashboard server
        await self._start_web_server()

        # Send Telegram startup notification
        if self._tg_enabled:
            live = "🔴 LIVE" if not self.dry_run else "🟡 DRY"
            mode = self._trade_mode.upper()
            bank = f"${self.engine.bankroll:.2f}"
            await self._tg_send(
                f"🚀 <b>PolyBot Started</b> {live}\n"
                f"Mode: {mode} · Bank: {bank}\n"
                f"Send /help for commands"
            )
            # Flush any old pending updates so we don't process stale commands
            try:
                url = f"https://api.telegram.org/bot{self._tg_token}/getUpdates"
                async with httpx.AsyncClient(timeout=5) as c:
                    resp = await c.get(url, params={"offset": -1, "timeout": 0})
                    if resp.status_code == 200:
                        results = resp.json().get("result", [])
                        if results:
                            self._tg_offset = results[-1]["update_id"] + 1
            except Exception:
                pass
            print(f"  Telegram bot linked: chat {self._tg_chat_id}")

        print()

        # Launch WebSocket feeds as background tasks
        self._ws_binance_connected = False
        self._ws_poly_connected = False
        self._ws_btc_updates = 0
        self._ws_poly_updates = 0
        ws_tasks = [
            asyncio.create_task(self._ws_binance_feed()),
            asyncio.create_task(self._ws_polymarket_feed()),
            asyncio.create_task(self._feature_log_flusher()),
        ]
        print(f"  WebSocket feeds launching (Binance + Polymarket)...")

        # Start async DB writer thread
        db_queue.start()

        try:
            while self._running:
              try:
                _cycle_t0 = time.monotonic()
                self._cycle_count += 1

                # 1+2. BTC price + market discovery
                # If Binance WS is connected, skip Chainlink HTTP poll (WS is faster)
                # Still poll Chainlink every 30s as ground truth (it's the resolution source)
                btc_tasks = [self._discover_markets()]
                if not self._ws_binance_connected or self._cycle_count % 120 == 0:
                    btc_tasks.append(self._update_btc_price())
                await asyncio.gather(*btc_tasks)

                # 2b. Promote upcoming → active
                now_ts = time.time()
                for slug in list(self._upcoming_windows):
                    uw = self._upcoming_windows[slug]
                    if uw.start_ts <= now_ts:
                        if slug not in self._active_windows and uw.end_ts > now_ts:
                            uw.status = "active"
                            # Capture BTC price at window start (Chainlink = resolution source)
                            if self._btc_price > 0 and uw.btc_at_open == 0:
                                uw.btc_at_open = self._btc_price
                            self._active_windows[slug] = uw
                            # Reset probability state for new window — prevents stale
                            # directional bias from previous window leaking through EMA
                            self.engine._p_smooth = 0.50
                            if hasattr(self.engine, '_p_raw_history'):
                                self.engine._p_raw_history.clear()
                        del self._upcoming_windows[slug]

                # 2c. Restore bet states from DB (prevents double-buy after restart)
                # Only run for first 5 cycles to avoid unnecessary DB queries
                if self._cycle_count <= 5:
                    self._restore_bet_states_from_db()

                # 3. Update prices
                await self._update_prices()

                # 4. Auto-exit profitable positions (live only)
                # Thread-wrapped: _execute_order() inside is SYNC with up to 5 HTTP calls
                await asyncio.to_thread(self._auto_exit_positions)

                # 4b. Check settlements (SYNC — thread-wrapped)
                await asyncio.to_thread(self._check_settlements)

                # 4c. Periodic balance refresh (every 30s) — update display only
                # NOTE: NO auto DRY→LIVE switch. User must confirm via dashboard.
                # Skip during settlement cooldown — winning tokens haven't been redeemed yet
                # so on-chain USDC.e is artificially low
                in_settle_cooldown = self._settle_cooldown_until > time.time()
                if time.time() - self._last_balance_refresh > 10 and not in_settle_cooldown:
                    try:
                        _bal_t0 = time.monotonic()
                        new_bal = await asyncio.to_thread(config.get_usdce_balance)
                        perf.record_blocking_call("get_usdce_balance", (time.monotonic() - _bal_t0) * 1000)
                        if new_bal > 0:
                            # Track balance changes (deposit vs payout)
                            if self._prev_usdce_balance > 0:
                                delta = new_bal - self._prev_usdce_balance
                                if abs(delta) >= 0.10:  # Meaningful change
                                    open_pos = sum(1 for w in self._active_windows.values() if w.bet_side)
                                    cooldown_active = self._settle_cooldown_until > time.time()
                                    # Heuristic: if in cooldown or just settled, it's likely a prize payout
                                    # If not in cooldown and no open positions, it's likely a deposit
                                    if delta > 0:
                                        if cooldown_active or (self._settle_cooldown_until > 0 and time.time() - self._settle_cooldown_until < 120):
                                            event_type = "PAYOUT"
                                        else:
                                            event_type = "DEPOSIT"
                                        self._balance_events.append({
                                            "type": event_type,
                                            "amount": round(delta, 4),
                                            "balance": round(new_bal, 4),
                                            "ts": time.time(),
                                        })
                                        print(f"  💰 {event_type}: +${delta:.2f} → ${new_bal:.2f} USDC.e")
                                        self._send_balance_alert(event_type, delta, new_bal)
                                    elif delta < -0.10:
                                        self._balance_events.append({
                                            "type": "TRADE",
                                            "amount": round(delta, 4),
                                            "balance": round(new_bal, 4),
                                            "ts": time.time(),
                                        })
                                    # Keep last 50 events
                                    self._balance_events = self._balance_events[-50:]
                            self._prev_usdce_balance = new_bal
                            self._usdce_balance = new_bal
                            # Refresh CLOB balance too (prevents stale display after trades)
                            if self._poly_client:
                                try:
                                    self._clob_balance = await asyncio.to_thread(self._poly_client.get_clob_balance)
                                except Exception:
                                    pass
                            self._last_balance_refresh = time.time()
                            # Sync engine bankroll with real wallet balance (LIVE only).
                            # In dry mode, engine.bankroll tracks simulated P&L — never
                            # overwrite it with live balances or the sim becomes unreliable.
                            tradeable = self._clob_balance if self._clob_balance > 0 else new_bal
                            if not self.dry_run:
                                if tradeable > 0 and abs(tradeable - self.engine.bankroll) > 0.05:
                                    self.engine.bankroll = tradeable
                            # Sync PNL from balance truth (overrides incremental counters)
                            if not self.dry_run:
                                self._sync_pnl_from_balance()
                    except Exception:
                        pass

                # 4d. Settlement cooldown — refresh balance when cooldown expires
                if self._settle_cooldown_until > 0:
                    remaining = self._settle_cooldown_until - time.time()
                    if remaining <= 0:
                        # Cooldown done — refresh balance, but only update bankroll
                        # if the on-chain balance is HIGHER (winning tokens may not be redeemed yet)
                        try:
                            new_bal = await asyncio.to_thread(config.get_usdce_balance)
                            clob = 0.0
                            if self._poly_client:
                                try:
                                    clob = await asyncio.to_thread(self._poly_client.get_clob_balance)
                                except Exception:
                                    pass
                            tradeable = clob if clob > 0 else new_bal
                            if tradeable > 0:
                                self._usdce_balance = new_bal
                                self._clob_balance = clob
                                # Sync engine bankroll with CLOB balance (actual tradeable funds)
                                if not self.dry_run:
                                    self.engine.bankroll = tradeable
                                print(f"\n  COOLDOWN DONE: USDC.e ${new_bal:.2f} | CLOB ${clob:.2f}")
                                if self.dry_run:
                                    print(f"  Still DRY RUN — click GO LIVE on dashboard to trade\n")
                                else:
                                    print(f"  LIVE — bankroll ${self.engine.bankroll:.2f} — ready to trade\n")
                        except Exception:
                            pass
                        self._settle_cooldown_until = 0
                        self._last_balance_refresh = time.time()

                # 4e. Auto-redeem winning positions (dedup by conditionId)
                # After settlement: burst-check 5s/10s/20s/40s then fall back to 60s.
                # Stale API returns are filtered by _redeemed_conditions cache.
                _since_last = time.time() - self._last_redeem_check
                _post_settle = getattr(self, '_redeem_burst_until', 0)
                if _post_settle and time.time() < _post_settle:
                    _redeem_interval = 10  # burst mode: check every 10s for 2 min after settle
                else:
                    _redeem_interval = 60  # normal background scan
                    if _post_settle:
                        self._redeem_burst_until = 0  # burst window expired
                if self._poly_client and _since_last > _redeem_interval and not self.dry_run:
                    # FIX: never auto-redeem in dry mode (burns real POL gas
                    # and mutates on-chain state while user thinks sim-only)
                    self._last_redeem_check = time.time()
                    self._metrics.setdefault("redeem_checks", 0)
                    self._metrics["redeem_checks"] += 1
                    try:
                        wallet = self._owner_wallet_address()
                        signer_wallet = self._signer_wallet_address()
                        positions = await asyncio.to_thread(self._poly_client.get_redeemable_positions, wallet)
                        if positions:
                            # Filter out already-redeemed conditions (stale API data)
                            fresh = [p for p in positions
                                     if p.get("conditionId", "") not in self._redeemed_conditions]
                            _stale = len(positions) - len(fresh)
                            if _stale > 0:
                                self._metrics.setdefault("redeem_stale_skipped", 0)
                                self._metrics["redeem_stale_skipped"] += _stale
                            positions = fresh
                        # Track redeemable value for dashboard
                        self._redeemable_value = sum(
                            _redeem_position_value(p) for p in (positions or [])
                        )
                        if positions:
                            # Check POL gas before attempting on-chain TX
                            _has_gas = True
                            try:
                                from web3 import Web3 as _W3
                                _w3 = _W3(_W3.HTTPProvider("https://polygon.drpc.org", request_kwargs={"timeout": 8}))
                                _matic = _w3.eth.get_balance(_W3.to_checksum_address(signer_wallet))
                                _gas_price = _w3.eth.gas_price
                                _min_matic = 300_000 * _gas_price
                                if _matic < _min_matic:
                                    _has_gas = False
                                    _matic_f = f"{_matic / 1e18:.4f}"
                                    self._metrics.setdefault("redeem_blocked_no_gas", 0)
                                    self._metrics["redeem_blocked_no_gas"] += 1
                                    if self._cycle_count % 60 == 1:
                                        print(f"\n  ⚠ LOW POL: {_matic_f} — cannot redeem")
                                        self._log_event(
                                            f"REDEEM BLOCKED · low POL {_matic_f} for ~${self._redeemable_value:.2f}",
                                            "error",
                                        )
                            except Exception:
                                pass
                            if _has_gas:
                                total_val = sum(float(p.get("size", 0)) for p in positions)
                                _cids_batch = {p.get("conditionId", "") for p in positions if p.get("conditionId")}
                                print(f"\n  AUTO-REDEEM: {len(positions)} positions (${total_val:.2f})")
                                self._metrics.setdefault("redeem_found", 0)
                                self._metrics["redeem_found"] += 1
                                redeem_results = await asyncio.to_thread(
                                    self._poly_client.redeem_positions, wallet, self._identity
                                )
                                result = self._poly_client.summarize_redeem_results(redeem_results)
                                # FIX: only mark conditions done on confirmed success.
                                # If errors occurred, leave conditions for retry — transient
                                # RPC/nonce/revert issues should not permanently suppress retries.
                                if result["errors"] == 0 and result["skipped"] == 0:
                                    self._redeemed_conditions.update(_cids_batch)
                                    if len(self._redeemed_conditions) > 500:
                                        self._redeemed_conditions = set(list(self._redeemed_conditions)[-500:])
                                if result["redeemed"] > 0:
                                    print(f"  ✓ Redeemed {result['redeemed']} positions, ~${result['value']:.2f}")
                                    self._log_event(f"REDEEMED {result['redeemed']} wins · ~${result['value']:.2f}", "redeem")
                                    self._metrics.setdefault("redeem_success", 0)
                                    self._metrics["redeem_success"] += result["redeemed"]
                                    self._metrics.setdefault("redeem_value", 0.0)
                                    self._metrics["redeem_value"] += result["value"]
                                    await asyncio.sleep(2)
                                    try:
                                        new_bal = await asyncio.to_thread(config.get_usdce_balance, wallet)
                                        self._usdce_balance = new_bal
                                        clob = await asyncio.to_thread(self._poly_client.get_clob_balance)
                                        self._clob_balance = clob
                                        tradeable = clob if clob > 0 else new_bal
                                        if tradeable > 0 and not self.dry_run:
                                            self.engine.bankroll = tradeable
                                        print(f"  Balance after redeem: USDC.e ${new_bal:.2f} | CLOB ${clob:.2f} → Bankroll ${tradeable:.2f}")
                                    except Exception as bal_err:
                                        print(f"  Balance refresh after redeem failed: {bal_err}")
                                    self._save_persisted_state()
                                if result["errors"] > 0:
                                    print(f"  ✗ {result['errors']} redeem errors (will retry next cycle)")
                                    self._metrics.setdefault("redeem_errors", 0)
                                    self._metrics["redeem_errors"] += result["errors"]
                                if result["skipped"] > 0:
                                    print(f"  ⚠ {result['skipped']} positions skipped (wrong owner)")
                                    self._metrics.setdefault("redeem_skipped", 0)
                                    self._metrics["redeem_skipped"] += result["skipped"]
                    except Exception as e:
                        _emsg = str(e)[:80]
                        if "insufficient funds" in _emsg.lower():
                            print(f"  Auto-redeem: out of gas (POL)")
                        else:
                            print(f"  Auto-redeem error: {_emsg}")
                        self._metrics.setdefault("redeem_check_errors", 0)
                        self._metrics["redeem_check_errors"] += 1

                # 5. Evaluate signals (for dashboard display only — no auto-betting)
                in_cooldown = self._settle_cooldown_until > time.time()
                if not in_cooldown:
                    # Always evaluate signals for KPI display, but never auto-bet
                    # User trades only via copy mode or manual dashboard buttons
                    self._evaluate_signals_only()

                # ─────────────────────────────────────────────────────────
                # 5b. ADAPTIVE SCALP LOOP — Conviction Engine + abrak25 Pattern
                #
                # Per-window lifecycle: WAIT → ENTER → SCALP → LOCK → SETTLE
                # Uses multi-channel conviction (BTC momentum, market confirmation,
                # reprice lag, book imbalance) + GBM model as confirmation.
                # Maker orders (post_only=True) for zero fees.
                # ─────────────────────────────────────────────────────────
                if self._trade_mode == "kelly" and not self._bot_paused:
                    if not hasattr(self, '_scalp_loop_ctr'):
                        self._scalp_loop_ctr = 0
                    self._scalp_loop_ctr += 1

                    # ── PERIODIC WALLET SIGNAL POLL ──
                    # Poll Abrak wallet every cycle (WalletSignal internally rate-limits
                    # to POLL_INTERVAL=15s).  Populates cached values for dashboard +
                    # pre-warms the gate so entry decisions use fresh data.
                    if self.wallet_signal.enabled and self._active_windows:
                        _aw = next(iter(self._active_windows.values()))
                        if getattr(_aw, 'up_token', None) and getattr(_aw, 'down_token', None):
                            try:
                                await self.wallet_signal.get_direction(
                                    _aw.up_token, _aw.down_token,
                                )
                            except Exception:
                                pass  # fail-open: don't crash loop

                    for slug, w in self._active_windows.items():
                        time_into = w.interval_secs - w.time_remaining

                        # Need valid prices
                        if not (0 < w.up_price < 1.0 and 0 < w.down_price < 1.0):
                            continue

                        # Compute probability every cycle for all windows
                        conv = self.engine._calc_probability(
                            time_into, w.time_remaining,
                            w.up_price, w.down_price,
                            w.up_depth, w.down_depth,
                            w.up_spread, w.down_spread,
                            w.up_best_bid, w.down_best_bid,
                            w.up_best_ask, w.down_best_ask,
                        )
                        # Track conviction history for debugging
                        w.conviction_history.append(round(conv["conviction"], 3))
                        if len(w.conviction_history) > 50:
                            w.conviction_history.pop(0)

                        # Update dashboard live view
                        sigs = conv.get("signals", {})
                        self._kelly_live = {
                            "side": conv["side"],
                            "conviction": conv["conviction"],
                            "p_smooth": conv.get("p_smooth", 0.50),
                            "q_mid": conv.get("q_mid", 0.50),
                            "edge": conv.get("edge", 0),
                            "z": sigs.get("z", 0),
                            "book_imb": sigs.get("book_imb", 0),
                            "lag": sigs.get("lag", 0),
                            "regime": conv.get("regime", "CHOP"),
                            "mode": w.scalp_mode,
                            "approach": w.approach,
                            "gate_reason": conv.get("gate_reason", ""),
                            "trade_count": w.trade_count,
                            "time_into": int(time_into),
                            "time_left": int(w.time_remaining),
                            "up_shares": w.up_shares,
                            "down_shares": w.down_shares,
                            "total_spent": w.total_spent,
                            "budget": w.budget,
                        }

                        if in_cooldown:
                            continue

                        # ══ SCALP LIFECYCLE ══

                        # ── LOCK phase: final 30s — ITERATIVE ──
                        # Retry sells each cycle with fresh bid checks.
                        if w.time_remaining < 30:
                            if hasattr(self.engine, "_last_conviction_pipeline"):
                                self.engine._last_conviction_pipeline["gate_reason"] = "too late in window"

                            # First entry into LOCK: cancel open orders
                            if w.scalp_mode != "LOCK" and w.scalp_mode != "SETTLE":
                                w.scalp_mode = "LOCK"
                                if w.open_orders:
                                    stale_ids = list(w.open_orders.keys())
                                    try:
                                        await asyncio.to_thread(
                                            self._poly_client.cancel_orders, stale_ids
                                        )
                                    except Exception:
                                        pass
                                    w.open_orders.clear()

                            # HEDGED LOCK: hold everything to settlement.
                            # Settlement pays $1/share on winning side, $0 on losing.
                            # Selling before settlement (even at 0.90+) throws away
                            # guaranteed profit. Only cancel open unfilled orders.
                            _approach_lock = getattr(w, 'approach', '')
                            if _approach_lock in ("HEDGED", "DIRECTIONAL"):
                                # HEDGED/DIRECTIONAL: hold to settlement for $1 payout
                                if w.bet_side and self._cycle_count % 60 == 1:
                                    print(f"  LOCK ({_approach_lock}): holding to settlement "
                                          f"↑{w.up_shares:.1f}sh ↓{w.down_shares:.1f}sh "
                                          f"t_left={w.time_remaining}s")
                            else:
                                # Non-HEDGED: sell if profitable (original logic)
                                if w.bet_side:
                                    self._reconcile_shares(w)
                                    _lock_cd = 5.0
                                    _last_lock = getattr(w, '_last_lock_sell_ts', 0)
                                    if time.time() - _last_lock >= _lock_cd:
                                        for _lk_side in ["UP", "DOWN"]:
                                            _lk_shares = float(w.up_shares if _lk_side == "UP" else w.down_shares)
                                            _lk_bid = w.up_best_bid if _lk_side == "UP" else w.down_best_bid
                                            _lk_cost = float((w.up_cost if _lk_side == "UP" else w.down_cost) or 0)
                                            if _lk_shares < 1 or _lk_bid <= 0.01:
                                                continue
                                            _lk_vwap = _lk_cost / _lk_shares if _lk_shares > 0 else 0
                                            if _lk_bid > _lk_vwap * 1.01 or _lk_bid > 0.90:
                                                self._metrics["lock_attempts"] += 1
                                                r = await self._sell_hedged(
                                                    w, _lk_side, _lk_shares, _lk_bid,
                                                    reason="LOCK-SELL", retry=True,
                                                )
                                                if r["ok"]:
                                                    w._last_lock_sell_ts = time.time()

                            continue  # hold remaining to settlement

                        # ── WAIT/ENTER phase: no position yet ──
                        if not w.bet_side:
                            if slug in self._kelly_sniped_slugs:
                                continue  # already attempted and failed/exited

                            # FLIP/CUT cooldown: prevent whipsaw re-entry (#5)
                            # 12s cooldown + require 3s opposite signal persistence + positive EV
                            _last_cut = getattr(w, '_last_cut_ts', 0)
                            _flip_cooldown = 12.0
                            if _last_cut > 0 and time.time() - _last_cut < _flip_cooldown:
                                if self._cycle_count % 30 == 1:
                                    print(f"    FLIP COOLDOWN: {_flip_cooldown - (time.time() - _last_cut):.0f}s remaining")
                                continue
                            # After cooldown, require signal persistence (3s) on new side
                            if _last_cut > 0 and time.time() - _last_cut < 30.0:
                                _rev_s = getattr(w, '_reversal_start_ts', 0)
                                _rev_dur = time.time() - _rev_s if _rev_s > 0 else 0
                                if _rev_dur < 3.0:
                                    if self._cycle_count % 30 == 1:
                                        print(f"    FLIP RE-ENTRY GATE: signal persist {_rev_dur:.1f}s < 3.0s")
                                    continue

                            w.scalp_mode = "WAIT"

                            # Clear stale gate_reason from previous LOCK phase
                            if hasattr(self.engine, "_last_conviction_pipeline"):
                                _prev_gr = self.engine._last_conviction_pipeline.get("gate_reason", "")
                                if _prev_gr in ("too late in window",):
                                    self.engine._last_conviction_pipeline["gate_reason"] = ""

                            # Liquidity check: skip if book is empty/too wide
                            _max_spread = max(w.up_spread, w.down_spread)
                            if _max_spread >= 0.50 or (w.up_best_ask >= 1.0 and w.down_best_ask >= 1.0):
                                if hasattr(self.engine, "_last_conviction_pipeline"):
                                    self.engine._last_conviction_pipeline["gate_reason"] = "no liquidity"
                                if self._cycle_count % 100 == 1:
                                    print(f"    SKIP {w.asset}: no liquidity "
                                          f"(spread={_max_spread:.2f} "
                                          f"ask_up={w.up_best_ask:.2f} "
                                          f"ask_dn={w.down_best_ask:.2f})")
                                continue

                            # ── TIME-OF-DAY GATE → DRY MODE SWITCH ──
                            # Controlled by /gate on|off + /hours.
                            # Outside gate hours: switch to dry_run (simulate only).
                            # Inside gate hours: switch back to live.
                            if self._time_gate_enabled:
                                import datetime as _dt
                                try:
                                    import zoneinfo as _zi
                                    _et_tz = _zi.ZoneInfo("America/New_York")
                                except ImportError:
                                    from dateutil import tz as _dtz
                                    _et_tz = _dtz.gettz("America/New_York")
                                _now_et = _dt.datetime.now(_et_tz)
                                _et_hour = _now_et.hour
                                _in_window = _et_hour in self._time_gate_hours
                                if not _in_window:
                                    # Outside gate hours → switch to dry mode (still run for data)
                                    if not self.dry_run:
                                        if not getattr(self, '_time_gate_paused', False):
                                            self._time_gate_paused = True
                                            self.dry_run = True
                                            # FIX: cancel all open orders on forced DRY transition
                                            # Prevents fills after mode change ("hidden drains")
                                            if self._poly_client:
                                                try:
                                                    await asyncio.to_thread(self._poly_client.cancel_all_orders)
                                                    print(f"  Cancelled open orders (time gate → DRY)")
                                                except Exception:
                                                    pass
                                            _hrs = ",".join(str(h) for h in sorted(self._time_gate_hours))
                                            print(f"\n  🔒 TIME GATE: {_et_hour:02d} ET not in [{_hrs}] → DRY MODE")
                                            self._log_event(f"TIME GATE → DRY MODE ({_et_hour:02d} ET outside [{_hrs}])", "info")
                                    # Fall through — bot continues in dry mode for data collection
                                else:
                                    # Inside gate hours → restore live if gate paused us.
                                    # Only restores if the gate itself forced dry (_time_gate_paused).
                                    # Manual /dry or dashboard dry is NOT touched.
                                    if getattr(self, '_time_gate_paused', False):
                                        self._time_gate_paused = False
                                        self.dry_run = False
                                        print(f"\n  ⏰ TIME GATE: {_et_hour:02d} ET in window → LIVE RESTORED")
                                        self._log_event(f"TIME GATE → LIVE ({_et_hour:02d} ET in window)", "info")

                            # Fetch fee params on first token encounter
                            if w.up_token and not w._fees_fetched and self._poly_client:
                                _fetch_fee_params(self._poly_client, w.up_token)
                                w._fees_fetched = True

                            # Dry-run: auto-reset halt (stop-loss doesn't apply to simulated bankroll)
                            if self.dry_run and getattr(self.engine, '_halted', False):
                                self.engine.reset_halt()

                            # Get conviction signal (probability engine + GBM + entry gate)
                            _kp = self._kelly_params()
                            signal, w._entry_ready_count = self.engine.get_conviction_signal(
                                w.up_price, w.down_price,
                                w.up_depth, w.down_depth,
                                int(w.time_remaining), w.interval_secs,
                                self.engine.bankroll,
                                up_spread=w.up_spread,
                                down_spread=w.down_spread,
                                up_best_bid=w.up_best_bid,
                                down_best_bid=w.down_best_bid,
                                up_best_ask=w.up_best_ask,
                                down_best_ask=w.down_best_ask,
                                entry_ready_count=w._entry_ready_count,
                                edge_history=w._edge_history,
                                kelly_entry_time=_kp["entry_time"],
                                kelly_min_edge=_kp["min_edge"],
                                rcfg_gates={
                                    "conv_floor_up": self.rcfg.conviction_floor_up,
                                    "conv_floor_down": self.rcfg.conviction_floor_down,
                                    "chop_conv": self.rcfg.chop_conviction_floor,
                                    "chop_edge": self.rcfg.chop_edge_floor,
                                    "chop_div": self.rcfg.chop_div_floor,
                                    "price_div_early": self.rcfg.price_div_early,
                                    "price_div_mid": self.rcfg.price_div_mid,
                                    "price_div_late": self.rcfg.price_div_late,
                                },
                            )

                            # UP-bias window dedup: count distinct schedule targets (day-aware)
                            _ub_target = getattr(self.engine, '_up_bias_last_target_secs', -1)
                            if _ub_target >= 0:
                                import datetime as _dt_ub
                                try:
                                    import zoneinfo as _zi_ub
                                    _et_now_ub = _dt_ub.datetime.now(_zi_ub.ZoneInfo("America/New_York"))
                                except ImportError:
                                    from dateutil import tz as _dtz_ub
                                    _et_now_ub = _dt_ub.datetime.now(_dtz_ub.gettz("America/New_York"))
                                _ub_key = (int(_et_now_ub.strftime("%Y%m%d")), _ub_target)
                                if _ub_key != self._up_bias_last_target_key:
                                    self._up_bias_last_target_key = _ub_key
                                    self._metrics["up_bias_windows_seen"] += 1

                            if not signal:
                                # ── Gate funnel attribution ──
                                _gr = getattr(self.engine, '_last_conviction_pipeline', {}).get('gate_reason', '')
                                _metric_key = _gate_metric_key(_gr)
                                if _metric_key:
                                    self._metrics.setdefault(_metric_key, 0)
                                    self._metrics[_metric_key] += 1
                                continue

                            # ── ONE-WAY STREAK GUARD ──
                            # If 3+ consecutive losses on the SAME side, require
                            # higher conviction AND wallet agreement to re-enter that side.
                            if (self._streak_count >= 3
                                    and self._streak_side == signal.side):
                                _streak_needs_conv = signal.conviction >= 0.30
                                _streak_needs_wallet = False
                                if self.wallet_signal.enabled:
                                    _sw, _sc, _su, _sd = await self.wallet_signal.get_direction(
                                        w.up_token, w.down_token)
                                    _streak_needs_wallet = (_sw == signal.side)
                                else:
                                    _streak_needs_wallet = True  # no wallet → skip check
                                if not (_streak_needs_conv and _streak_needs_wallet):
                                    self._metrics.setdefault("gate_streak_block", 0)
                                    self._metrics["gate_streak_block"] += 1
                                    if self._cycle_count % 30 == 1:
                                        print(f"    STREAK GUARD: {self._streak_count}x {self._streak_side} "
                                              f"losses (${self._streak_pnl:+.2f}) — "
                                              f"conv={signal.conviction:.3f} wallet={'agree' if _streak_needs_wallet else 'disagree'}")
                                    continue

                            # ── WALLET CONFIRMATION GATE ──
                            if self.wallet_signal.enabled:
                                _wdir, _wconf, _wup, _wdn = await self.wallet_signal.get_direction(
                                    w.up_token, w.down_token,
                                )
                                _confirms, _wreason = self.wallet_signal.should_confirm(signal.side)
                                if not _confirms:
                                    self._metrics.setdefault("gate_wallet_disagree", 0)
                                    self._metrics["gate_wallet_disagree"] += 1
                                    if self._cycle_count % 200 == 1:
                                        print(f"    WALLET GATE: engine={signal.side} wallet={_wdir} ({_wreason})")
                                    continue
                                if _wdir:
                                    self._metrics.setdefault("wallet_confirm", 0)
                                    self._metrics["wallet_confirm"] += 1
                                else:
                                    self._metrics.setdefault("wallet_no_position", 0)
                                    self._metrics["wallet_no_position"] += 1

                            # P2: COMPLEMENT SANITY — up_mid + down_mid should ≈ 1.0
                            # Check both mids and asks — WS can lag on one feed path
                            _up_mid = max(0.02, w.up_price)
                            _dn_mid = max(0.02, w.down_price)
                            _sum_mid = _up_mid + _dn_mid
                            _sum_ask_entry = (getattr(w, 'up_best_ask', 0) or 0) + (getattr(w, 'down_best_ask', 0) or 0)
                            if _sum_mid > 1.12 or _sum_mid < 0.70 or _sum_ask_entry > 1.15 or _sum_ask_entry < 0.50:
                                self._metrics["quote_pathological"] += 1
                                self._metrics["gate_pathological"] += 1
                                if self._cycle_count % 20 == 1:
                                    print(f"    PATHOLOGICAL QUOTES: mid={_sum_mid:.3f} ask={_sum_ask_entry:.3f}")
                                continue  # skip this window's entry decision

                            # --- Drag + EV checks via unified policy ---
                            ask_up = w.up_best_ask if 0.02 < w.up_best_ask < 1.0 and w.up_best_ask > _up_mid * 0.70 else _up_mid + 0.01
                            ask_dn = w.down_best_ask if 0.02 < w.down_best_ask < 1.0 and w.down_best_ask > _dn_mid * 0.70 else _dn_mid + 0.01
                            entry_drag = ask_up + ask_dn - 1.0

                            # Cooldown after entry-order rejection (min-size / 400 error)
                            if time.time() < self._entry_retry_after:
                                self._metrics.setdefault("entry_cooldown_skip", 0)
                                self._metrics["entry_cooldown_skip"] += 1
                                continue

                            p_smooth = getattr(self.engine, '_p_smooth', 0.50)
                            fav_side = signal.side
                            fav_ask = ask_up if fav_side == "UP" else ask_dn
                            hedge_ask = ask_dn if fav_side == "UP" else ask_up

                            fav_cost = _buy_cost_per_share(fav_ask)
                            hedge_cost = _buy_cost_per_share(hedge_ask)
                            slip = 0.001; buf = 0.001; fee_rnd = 0.0001
                            fav_ev = (p_smooth if fav_side == "UP" else 1 - p_smooth) - fav_cost - slip - buf - fee_rnd
                            hedge_ev = ((1 - p_smooth) if fav_side == "UP" else p_smooth) - hedge_cost - slip - buf - fee_rnd
                            combined_ev = 1.0 - fav_cost - hedge_cost - 2 * (slip + buf + fee_rnd)

                            _time_into = w.interval_secs - w.time_remaining
                            _book = {
                                "fav_ask": fav_ask, "hedge_ask": hedge_ask,
                                "fav_ev": fav_ev, "combined_ev": combined_ev,
                                "entry_drag": entry_drag,
                                "q_mid": getattr(self.engine, '_q_mid', 0.50),
                                "edge": signal.edge,
                                "time_into": _time_into,
                                "fav_cost": fav_cost,
                            }
                            _pass, _reason = policy_should_enter(signal, _book, self.rcfg, not self.dry_run)
                            if not _pass:
                                # Categorize block for metrics
                                if "drag" in _reason:
                                    self._metrics.setdefault("entry_skipped_drag", 0)
                                    self._metrics["entry_skipped_drag"] += 1
                                elif "ev" in _reason.lower() or "fav" in _reason.lower() or "hedged" in _reason.lower():
                                    self._metrics["entry_skipped_ev"] += 1
                                elif "live conv" in _reason:
                                    self._metrics.setdefault("entry_live_conv_block", 0)
                                    self._metrics["entry_live_conv_block"] += 1
                                    # Time-bucketed live conviction blocks
                                    if "early" in _reason:
                                        self._metrics.setdefault("entry_live_conv_block_early", 0)
                                        self._metrics["entry_live_conv_block_early"] += 1
                                    elif "mid" in _reason:
                                        self._metrics.setdefault("entry_live_conv_block_mid", 0)
                                        self._metrics["entry_live_conv_block_mid"] += 1
                                    elif "late" in _reason:
                                        self._metrics.setdefault("entry_live_conv_block_late", 0)
                                        self._metrics["entry_live_conv_block_late"] += 1
                                elif "mid price" in _reason:
                                    self._metrics.setdefault("entry_price_skip", 0)
                                    self._metrics["entry_price_skip"] += 1
                                    if "early" in _reason:
                                        self._metrics.setdefault("entry_mid_price_skip_early", 0)
                                        self._metrics["entry_mid_price_skip_early"] += 1
                                    else:
                                        self._metrics.setdefault("entry_mid_price_skip_std", 0)
                                        self._metrics["entry_mid_price_skip_std"] += 1
                                elif "price" in _reason:
                                    self._metrics.setdefault("entry_price_skip", 0)
                                    self._metrics["entry_price_skip"] += 1
                                elif "edge slope" in _reason:
                                    self._metrics.setdefault("entry_edge_slope_block", 0)
                                    self._metrics["entry_edge_slope_block"] += 1
                                elif "early window" in _reason or "warm" in _reason.lower():
                                    self._metrics.setdefault("entry_warmup_block", 0)
                                    self._metrics["entry_warmup_block"] += 1
                                else:
                                    self._metrics.setdefault("entry_policy_block", 0)
                                    self._metrics["entry_policy_block"] += 1
                                if self._cycle_count % 50 == 1:
                                    print(f"    ENTRY BLOCKED ({_reason}): "
                                          f"fav_ev={fav_ev:.4f} conv={signal.conviction:.3f} "
                                          f"ask={fav_ask:.2f} drag={entry_drag:.3f}")
                                continue

                            # All policy gates passed
                            self._metrics["gate_passed"] += 1
                            self._metrics["gate_ev_block"] = self._metrics.get("entry_skipped_ev", 0)

                            # Entry-timing telemetry
                            _time_into = w.interval_secs - w.time_remaining
                            if _time_into < 60:
                                self._metrics.setdefault("entry_t_0_60", 0)
                                self._metrics["entry_t_0_60"] += 1
                            elif _time_into < 120:
                                self._metrics.setdefault("entry_t_60_120", 0)
                                self._metrics["entry_t_60_120"] += 1
                            elif _time_into < 180:
                                self._metrics.setdefault("entry_t_120_180", 0)
                                self._metrics["entry_t_120_180"] += 1
                            else:
                                self._metrics.setdefault("entry_t_180_plus", 0)
                                self._metrics["entry_t_180_plus"] += 1

                            # Portfolio exposure cap (40% of bankroll)
                            if self.dry_run:
                                # Dry run: no exposure cap — bet the configured amount
                                remaining = self.engine.max_bet_dollars
                            else:
                                bankroll = self.engine.bankroll
                                open_exposure = sum(
                                    ow.total_cost for ow in self._active_windows.values()
                                    if ow.bet_side and ow.total_cost > 0
                                )
                                max_exposure = bankroll * 0.40
                                remaining = max(0, max_exposure - open_exposure)
                            sizing = self._compute_entry_sizing(
                                signal.approach,
                                side=signal.side,
                                remaining=remaining,
                            )
                            _wallet_mult = sizing["wallet_mult"]
                            _streak_mult = sizing["streak_mult"]
                            _base_bet = sizing["base_bet"]
                            _consec_losses = sizing["consec_losses"]
                            alloc = sizing["alloc"]
                            self._last_wallet_lean = sizing["wallet_lean"]
                            self._last_wallet_mult = sizing["wallet_mult"]
                            self._last_wallet_dir = _wdir if self.wallet_signal.enabled else ""
                            if sizing["wallet_applied"]:
                                self._metrics.setdefault("wallet_sizing_applied", 0)
                                self._metrics["wallet_sizing_applied"] += 1
                            if _streak_mult != 1.0:
                                self._metrics.setdefault("streak_sizing_applied", 0)
                                self._metrics["streak_sizing_applied"] += 1
                            if sizing["tod_mult"] != 1.0:
                                self._metrics.setdefault("risk_scale_half", 0)
                                self._metrics["risk_scale_half"] += 1
                            # Minimum viable spend — Polymarket requires $1.00 for market orders
                            PM_MIN_MARKET_BUY = 1.05  # $1.05 to cover rounding
                            if alloc < PM_MIN_MARKET_BUY:
                                if self._cycle_count % 50 == 1:
                                    print(f"    ENTRY BLOCKED: alloc={alloc:.2f} < ${PM_MIN_MARKET_BUY} PM min "
                                          f"(base={_base_bet:.2f} "
                                          f"streak={_streak_mult:.2f}x "
                                          f"wallet={_wallet_mult:.2f}x "
                                          f"remaining={remaining:.2f})")
                                self._metrics.setdefault("entry_below_pm_min", 0)
                                self._metrics["entry_below_pm_min"] += 1
                                continue

                            initial_spend = sizing["initial_spend"]
                            _adds_dampened = False  # flag for high-price dampener

                            # ── High-price chasing gate ──
                            # Payoff math: win pays (1-ask), loss costs ask.
                            # At ask=0.58: win=+$0.42, loss=-$0.58 → need 58% WR.
                            # At ask=0.50: win=+$0.50, loss=-$0.50 → need 50% WR.
                            # Old gate at 0.78 was useless. Tiered approach:
                            fav_ask = w.up_best_ask if signal.side == "UP" else w.down_best_ask
                            if fav_ask > 0.65:
                                # >0.65: hard skip unless very high conviction
                                if signal.conviction < 0.10:
                                    self._metrics.setdefault("entry_high_price_skip", 0)
                                    self._metrics["entry_high_price_skip"] += 1
                                    continue
                            elif fav_ask > 0.55 and fav_ask <= 0.65:
                                # 0.55-0.65: hard skip unless conviction >= 0.30
                                # Apply in dry and live so validation matches runtime behavior.
                                if signal.conviction < 0.06:
                                    self._metrics.setdefault("entry_mid_price_skip", 0)
                                    self._metrics["entry_mid_price_skip"] += 1
                                    continue

                            # Live sizing uses the centralized helper so dashboard
                            # telemetry and execution stay in sync.
                            w.reserve_budget = round(max(0.0, alloc - initial_spend), 2)

                            # Compute favored + hedge spend (capped to budget)
                            remaining_budget = max(0, alloc - w.total_spent)
                            entry_cap = min(initial_spend, remaining_budget)
                            if entry_cap < PM_MIN_MARKET_BUY:
                                self._metrics.setdefault("entry_below_pm_min", 0)
                                self._metrics["entry_below_pm_min"] += 1
                                continue  # not enough budget

                            PM_MIN_ORDER = PM_MIN_MARKET_BUY
                            hedge_spend = 0
                            _mt = 0.0  # minority target (set below if hedging)
                            # Hedge when drag isn't extreme (matched to relaxed EV gate)
                            want_hedge = entry_drag <= 0.06 and combined_ev > -0.06
                            # DIRECTIONAL: no hedge — all capital on favored side
                            if signal.approach == "DIRECTIONAL":
                                want_hedge = False

                            if want_hedge and entry_cap >= 2 * PM_MIN_ORDER:
                                # ── SHARE-BASED hedge sizing (matches directional.py minority ratio) ──
                                # Compute minority target the same way directional.py will check it,
                                # so we don't trigger overhedge trim immediately after entry.
                                _conv = signal.conviction
                                # Use actual market regime from conviction pipeline
                                # (was signal.approach="HEDGED" — wrong input to minority_target)
                                _regime = conv.get("regime", "CHOP")
                                _mt = self.engine._compute_minority_target(
                                    _conv, w.time_remaining,
                                    max(w.up_spread, w.down_spread), _regime
                                ) if hasattr(self.engine, '_compute_minority_target') else 0.30

                                # Total shares we can buy with entry_cap at current asks
                                fav_cps = _buy_cost_per_share(fav_ask) if fav_ask > 0 else fav_ask
                                hedge_cps = _buy_cost_per_share(hedge_ask) if hedge_ask > 0 else hedge_ask
                                if fav_cps <= 0 or hedge_cps <= 0:
                                    fav_spend = round(entry_cap, 2)
                                    hedge_spend = 0
                                else:
                                    # Solve: fav_sh * fav_cps + hedge_sh * hedge_cps = entry_cap
                                    #         hedge_sh / (fav_sh + hedge_sh) = _mt
                                    # => fav_sh = entry_cap * (1-_mt) / (fav_cps*(1-_mt) + hedge_cps*_mt)
                                    #    hedge_sh = entry_cap * _mt / (fav_cps*(1-_mt) + hedge_cps*_mt)
                                    denom = fav_cps * (1 - _mt) + hedge_cps * _mt
                                    fav_shares_target = entry_cap * (1 - _mt) / denom
                                    hedge_shares_target = entry_cap * _mt / denom
                                    fav_spend = round(fav_shares_target * fav_cps, 2)
                                    hedge_spend = round(hedge_shares_target * hedge_cps, 2)

                                    # Enforce PM minimums — but check if doing so would
                                    # distort the minority ratio beyond acceptable limits.
                                    # Bug fix: when hedge_ask is very cheap (e.g. $0.08),
                                    # forcing $1.00 minimum buys ~12.5 hedge shares vs ~2
                                    # expensive favored shares → 85% minority → instant
                                    # HARD overhedge trim → CUT cycle → fee burn.
                                    if hedge_spend < PM_MIN_ORDER:
                                        # Would forcing PM_MIN produce an acceptable ratio?
                                        _forced_h_sh = PM_MIN_ORDER / hedge_cps if hedge_cps > 0 else 0
                                        _forced_f_spend = max(PM_MIN_ORDER, round(entry_cap - PM_MIN_ORDER, 2))
                                        _forced_f_sh = _forced_f_spend / fav_cps if fav_cps > 0 else 0
                                        _forced_total = _forced_f_sh + _forced_h_sh
                                        _forced_minority = _forced_h_sh / _forced_total if _forced_total > 0 else 0
                                        if _forced_minority <= _mt + 0.15:
                                            # Acceptable distortion — force PM minimum
                                            hedge_spend = PM_MIN_ORDER
                                            fav_spend = round(entry_cap - hedge_spend, 2)
                                        else:
                                            # Severe distortion — skip hedge, go favored-only
                                            hedge_spend = 0
                                            fav_spend = round(entry_cap, 2)
                                            if self._cycle_count % 20 == 1:
                                                print(f"    HEDGE SKIP: PM_MIN would force "
                                                      f"minority={_forced_minority:.2f} "
                                                      f"(target={_mt:.2f}) — entering fav-only")
                                    if fav_spend < PM_MIN_ORDER:
                                        fav_spend = PM_MIN_ORDER
                                    # Final budget check
                                    if fav_spend + hedge_spend > entry_cap:
                                        hedge_spend = round(entry_cap - fav_spend, 2)
                                        if hedge_spend < PM_MIN_ORDER:
                                            hedge_spend = 0
                                            fav_spend = round(entry_cap, 2)
                            elif want_hedge:
                                # Budget too small for both sides — just enter favored
                                fav_spend = round(entry_cap, 2)
                            else:
                                # No hedge wanted (very high drag or very negative EV)
                                fav_raw = round(entry_cap * signal.dominant_pct, 2)
                                fav_spend = max(PM_MIN_ORDER, fav_raw)
                                if fav_spend > entry_cap:
                                    fav_spend = round(entry_cap, 2)

                            # Final min-size check on fav_spend
                            if not self.dry_run and fav_spend < PM_MIN_MARKET_BUY:
                                self._metrics.setdefault("entry_below_pm_min", 0)
                                self._metrics["entry_below_pm_min"] += 1
                                continue

                            # FIRE — favored side entry
                            fav_token = w.up_token if fav_side == "UP" else w.down_token
                            fav_shares = round(fav_spend / fav_ask, 2) if fav_ask > 0 else 0
                            if fav_shares < 1 or not fav_token:
                                if self._cycle_count % 50 == 1:
                                    print(f"    ENTRY BLOCKED: shares={fav_shares:.2f} "
                                          f"spend=${fav_spend:.2f} ask={fav_ask:.2f}")
                                continue

                            print(f"  >>> ENTRY: {fav_side} ${fav_spend:.2f} "
                                  f"@ {fav_ask:.2f} ({fav_shares:.1f}sh) "
                                  f"cev={combined_ev:+.4f} p={p_smooth:.3f} "
                                  f"hedge=${hedge_spend:.2f} "
                                  f"{'DRY' if self.dry_run else 'LIVE'}")
                            order_id = await asyncio.to_thread(
                                self._execute_order,
                                fav_token, "BUY", fav_shares, fav_ask,
                                w.neg_risk, w.end_ts,
                                2, False, fav_spend,  # max_retries, use_maker, spend_dollars
                            )
                            if order_id:
                                # Update from actual fill data
                                actual_cost = getattr(self._poly_client, 'last_fill_cost', 0) or fav_spend
                                fill_sh = getattr(self._poly_client, 'last_fill_shares', 0) or getattr(self, '_last_fill_shares', 0)
                                if fill_sh and fill_sh > 0:
                                    actual_shares = float(fill_sh)
                                else:
                                    # Fallback when API omits fill shares: infer from fee-adjusted cost/share.
                                    cps = _buy_cost_per_share(fav_ask) if fav_ask > 0 else 0.0
                                    actual_shares = round(actual_cost / cps, 2) if cps > 0 else fav_shares

                                w.bet_side = fav_side
                                w.bet_price = fav_ask
                                w.bet_shares = actual_shares
                                w.bet_order_id = order_id
                                w.bet_ts = time.time()
                                w._entry_fill_ts = time.time()  # overhedge hold gate
                                # Snapshot Abrak's direction at entry for wallet-flip detection
                                if self.wallet_signal.enabled:
                                    w._wallet_entry_direction = self.wallet_signal._cached_direction or ""
                                    # Snapshot opposition % at entry for delta-opposition gating
                                    _weu = self.wallet_signal._cached_up_shares
                                    _wed = self.wallet_signal._cached_down_shares
                                    _wet = _weu + _wed
                                    if _wet >= 5.0:
                                        w._wallet_entry_against_pct = (
                                            _wed / _wet if fav_side == "UP" else _weu / _wet
                                        )
                                    else:
                                        w._wallet_entry_against_pct = 0.0
                                else:
                                    w._wallet_entry_direction = ""
                                    w._wallet_entry_against_pct = 0.0
                                w.status = "bet_placed"
                                w.total_cost = actual_cost
                                w.total_spent = actual_cost
                                # Deduct buy cost from bankroll (cash-flow model)
                                # LIVE: CLOB already deducted; this keeps bankroll in sync until next CLOB sync
                                # DRY: no CLOB, so this is the only place cost is tracked
                                self.engine.bankroll -= actual_cost
                                w.budget = alloc
                                w.scalp_mode = "SCALP"
                                w.approach = signal.approach  # "HEDGED"
                                w.max_adds = 0 if _adds_dampened else signal.max_adds
                                w.trade_count = 1
                                w.realized_pnl = 0.0
                                if fav_side == "UP":
                                    w.up_shares = (w.up_shares or 0) + actual_shares
                                    w.up_cost = (w.up_cost or 0) + actual_cost
                                else:
                                    w.down_shares = (w.down_shares or 0) + actual_shares
                                    w.down_cost = (w.down_cost or 0) + actual_cost

                                self._kelly_sniped_slugs.add(slug)
                                self._last_buy_ts = time.time()
                                self.total_bets += 1

                                # Tag UP-bias entry (after successful fill)
                                _ub_active = getattr(self.engine, '_up_bias_active', False)
                                if _ub_active:
                                    _ub_mode = getattr(self.engine, '_up_bias_mode', "")
                                    w._up_bias_entry = True
                                    w._up_bias_mode = _ub_mode
                                    if _ub_mode == "soft":
                                        self._metrics["up_bias_soft_entries"] += 1
                                    elif _ub_mode == "flip":
                                        self._metrics["up_bias_force_flips"] += 1
                                        self._metrics["up_bias_hard_entries"] += 1
                                    elif _ub_mode == "hard":
                                        self._metrics["up_bias_hard_entries"] += 1

                                # HEDGE side entry (if conditions allow)
                                if hedge_spend < PM_MIN_ORDER:
                                    if signal.approach != "DIRECTIONAL":
                                        print(f"    NO HEDGE: spend=${hedge_spend:.2f} "
                                              f"drag={entry_drag:.3f} "
                                              f"cev={combined_ev:.4f}")
                                        # Flag window so ADDs prioritize hedge
                                        w._needs_initial_hedge = True
                                if hedge_spend >= PM_MIN_ORDER:
                                    hedge_token = w.down_token if fav_side == "UP" else w.up_token
                                    hedge_shares = round(hedge_spend / hedge_ask, 2) if hedge_ask > 0 else 0
                                    h_oid = None
                                    if hedge_shares >= 1 and hedge_token:
                                        h_oid = await asyncio.to_thread(
                                            self._execute_order,
                                            hedge_token, "BUY", hedge_shares, hedge_ask,
                                            w.neg_risk, w.end_ts,
                                            2, False, hedge_spend,  # spend_dollars
                                        )
                                        if not h_oid:
                                            # Retry once with slightly higher price
                                            await asyncio.sleep(0.5)
                                            retry_ask = min(hedge_ask + 0.01, 0.95)
                                            retry_sh = round(hedge_spend / retry_ask, 2)
                                            if retry_sh >= 1:
                                                h_oid = await asyncio.to_thread(
                                                    self._execute_order,
                                                    hedge_token, "BUY", retry_sh, retry_ask,
                                                    w.neg_risk, w.end_ts,
                                                    2, False, hedge_spend,  # spend_dollars
                                                )
                                                if h_oid:
                                                    hedge_ask = retry_ask
                                                    hedge_shares = retry_sh
                                                    print(f"    HEDGE RETRY OK @${retry_ask:.3f}")
                                    if not h_oid:
                                        print(f"    ⚠ HEDGE FAILED — will retry via ADDs")
                                        w._needs_initial_hedge = True
                                    if h_oid:
                                            h_cost = getattr(self._poly_client, 'last_fill_cost', 0) or hedge_spend
                                            fill_h = getattr(self._poly_client, 'last_fill_shares', 0) or getattr(self, '_last_fill_shares', 0)
                                            if fill_h and fill_h > 0:
                                                h_shares = float(fill_h)
                                            else:
                                                h_cps = _buy_cost_per_share(hedge_ask) if hedge_ask > 0 else 0.0
                                                h_shares = round(h_cost / h_cps, 2) if h_cps > 0 else hedge_shares
                                            w.total_cost += h_cost
                                            w.total_spent += h_cost
                                            w.trade_count += 1
                                            self.engine.bankroll -= h_cost  # deduct hedge buy cost
                                            if fav_side == "UP":
                                                w.down_shares = (w.down_shares or 0) + h_shares
                                                w.down_cost = (w.down_cost or 0) + h_cost
                                            else:
                                                w.up_shares = (w.up_shares or 0) + h_shares
                                                w.up_cost = (w.up_cost or 0) + h_cost

                                # Post-entry ratio validation
                                _pe_total = (w.up_shares or 0) + (w.down_shares or 0)
                                if _pe_total > 0:
                                    _pe_minority_sh = (w.down_shares if fav_side == "UP" else w.up_shares) or 0
                                    _pe_minority_r = _pe_minority_sh / _pe_total
                                    if _pe_minority_r > _mt + 0.20:
                                        print(f"    ⚠ ENTRY OVER-HEDGED: minority={_pe_minority_r:.2f} "
                                              f"target={_mt:.2f} ↑{w.up_shares:.1f}sh ↓{w.down_shares:.1f}sh")

                                self._save_persisted_state()
                                _h_sh = (w.down_shares if fav_side == "UP" else w.up_shares) or 0
                                _h_ask_log = hedge_ask if hedge_spend > 0 else 0
                                self._log_event(
                                    f"ENTER {signal.approach} {w.asset} {w.interval_label} · "
                                    f"↑{w.up_shares:.1f}sh ↓{w.down_shares:.1f}sh "
                                    f"fav={fav_side} @${fav_ask:.3f} "
                                    + (f"hedge @${_h_ask_log:.3f} " if _h_ask_log > 0 else "NO-HEDGE ")
                                    + f"cev={combined_ev:+.3f} drag={entry_drag:.3f} conv={signal.conviction:.2f}",
                                    "buy",
                                )
                                self._kelly_decisions.append({
                                    "t": time.time(), "type": "enter",
                                    "side": fav_side, "price": fav_ask,
                                    "alloc": alloc, "approach": signal.approach,
                                    "conviction": signal.conviction,
                                    "zone": signal.approach,  # HEDGED/AGGRESSIVE/etc
                                    "p": round(p_smooth, 3),
                                    "cv": round(signal.conviction, 3),
                                    "dd95": round(entry_drag, 3),  # entry drag as proxy
                                })
                                if len(self._kelly_decisions) > 20:
                                    self._kelly_decisions.pop(0)

                                # Record BUY to DB for audit trail
                                db_queue.add_trade({
                                    "market": w.slug,
                                    "up_price": w.up_price, "down_price": w.down_price,
                                    "total_cost": actual_cost, "profit_pct": 0,
                                    "shares": actual_shares,
                                    "investment": actual_cost,
                                    "expected_profit": 0,
                                    "dry_run": self.dry_run,
                                    "asset": w.asset,
                                    "extra": {
                                        "side": fav_side, "action": "BUY",
                                        "approach": signal.approach,
                                        "dry_run": self.dry_run,
                                        "price": round(fav_ask, 4),
                                        "fill_cost": round(actual_cost, 4),
                                        "fill_shares": round(actual_shares, 3),
                                        "conviction": round(signal.conviction, 4),
                                        "p_smooth": round(p_smooth, 4),
                                        "q_mid": round(_book.get("q_mid", 0.50), 4),
                                        "fav_ev": round(fav_ev, 5),
                                        "time_into": round(_time_into, 1),
                                        "edge": round(_book.get("edge", 0), 5),
                                        "regime": getattr(signal, "regime", ""),
                                        "entry_drag": round(entry_drag, 4),
                                        "wallet_lean": round(self._last_wallet_lean, 3) if getattr(self, '_last_wallet_lean', None) is not None else None,
                                        "wallet_mult": round(self._last_wallet_mult, 3) if getattr(self, '_last_wallet_mult', None) is not None else None,
                                        "wallet_dir": getattr(self, '_last_wallet_dir', None),
                                        "streak_mult": _streak_mult,
                                        "consec_losses": _consec_losses,
                                        "rcfg_hash": f"ev{self.rcfg.directional_ev_floor}_lmc{self.rcfg.live_min_conviction}",
                                    },
                                })

                                # ── TELEGRAM ENTRY ALERT ──
                                if self._tg_enabled:
                                    _tg_side_icon = "🟢 ↑" if fav_side == "UP" else "🔴 ↓"
                                    _tg_bank = self._clob_balance if self._clob_balance > 0 else self.engine.bankroll
                                    _tg_mode = "DRY" if self.dry_run else "LIVE"
                                    _tg_entry_msg = (
                                        f"{_tg_side_icon} <b>BUY {fav_side}</b> · "
                                        f"{actual_shares:.1f}sh @${fav_ask:.3f}\n"
                                        f"Cost: ${actual_cost:.2f} · Conv: {signal.conviction:.2f}\n"
                                        f"Bank: <b>${_tg_bank:.2f}</b> · {_tg_mode}"
                                    )
                                    try:
                                        await self._tg_send(_tg_entry_msg)
                                    except Exception:
                                        pass

                            else:
                                print(f"  ENTER FAILED {fav_side} {fav_shares:.1f}sh @${fav_ask:.3f}")

                        # ── SCALP phase: position management (ADD/TAKE_PROFIT/CUT_LOSS) ──
                        elif w.bet_side and w.scalp_mode == "SCALP":
                            _bcs = _buy_cost_per_share  # alias for brevity

                            # P1: Periodic share reconciliation (every 30s)
                            if time.time() - w._last_reconcile_ts >= 30.0:
                                self._reconcile_shares(w)
                                w._last_reconcile_ts = time.time()

                            # Recompute conviction for this cycle
                            p_smooth_val = getattr(self.engine, '_p_smooth', 0.50)

                            # Latency: time from last WS recv to decision start
                            _t_decide_start = time.monotonic()
                            _t_ws_last = getattr(w, '_last_ws_recv_mono', _t_decide_start)
                            perf.record_recv_to_decision((_t_decide_start - _t_ws_last) * 1000)

                            # P5: Read advisor params (if fresh, < 30s old)
                            _adv_params = getattr(w, '_advisor_params', {})
                            _adv_ts = getattr(w, '_advisor_params_ts', 0)
                            _advisor_trim_mult = 1.0
                            _advisor_hedge_adj = 0.0
                            if _adv_params and time.time() - _adv_ts < 30:
                                _advisor_trim_mult = _adv_params.get("trim_aggressiveness", 1.0)
                                _advisor_hedge_adj = _adv_params.get("hedge_target_adj", 0.0)

                            # Update reversal state BEFORE computing reversal_secs
                            # (fixes one-cycle lag where stale _reversal_start_ts
                            # was passed to should_act before being updated)
                            if getattr(w, 'approach', '') == "DIRECTIONAL" and w.bet_side:
                                _p_rev = p_smooth_val
                                _opposing = (_p_rev < 0.50 and w.bet_side == "UP") or \
                                            (_p_rev >= 0.50 and w.bet_side == "DOWN")
                                if _opposing:
                                    if getattr(w, '_reversal_start_ts', 0.0) <= 0:
                                        w._reversal_start_ts = time.time()
                                else:
                                    w._reversal_start_ts = 0.0

                            # ── S2 deterioration: track mark_ratio history ──
                            _mr_15s_ago = -1.0  # default: not available
                            if getattr(w, 'approach', '') == "DIRECTIONAL" and w.bet_side:
                                _now_ts = time.time()
                                _dir_sh = float(w.up_shares if w.bet_side == "UP" else w.down_shares)
                                _dir_bd = w.up_best_bid if w.bet_side == "UP" else w.down_best_bid
                                _dir_ct = float(w.total_spent or 0)
                                _cur_mr = 0.0
                                if _dir_ct > 0 and _dir_bd > 0 and _dir_sh > 0:
                                    _cur_mr = (_dir_sh * _sell_proceeds_per_share(_dir_bd)) / _dir_ct
                                _mrh = getattr(w, '_mark_ratio_history', [])
                                _mrh.append((_now_ts, _cur_mr))
                                # Prune entries older than 30s
                                _mrh = [(t, v) for t, v in _mrh if _now_ts - t <= 30.0]
                                w._mark_ratio_history = _mrh
                                # Find value closest to 15s ago
                                _target = _now_ts - 15.0
                                _best = None
                                for t, v in _mrh:
                                    if t <= _target:
                                        _best = v
                                if _best is not None:
                                    _mr_15s_ago = _best

                            # Call should_act with v2 params + advisor params
                            action = self.engine.should_act(
                                bet_side=w.bet_side,
                                entry_price=w.bet_price,
                                current_price=w.up_price if w.bet_side == "UP" else w.down_price,
                                up_price=w.up_price,
                                down_price=w.down_price,
                                time_remaining=int(w.time_remaining),
                                interval_secs=w.interval_secs,
                                conviction_result=conv,
                                trade_count=w.trade_count,
                                max_adds=w.max_adds,
                                up_shares=w.up_shares,
                                down_shares=w.down_shares,
                                best_bid=w.up_best_bid if w.bet_side == "UP" else w.down_best_bid,
                                peak_profit_pct=w.peak_profit_pct,
                                approach=getattr(w, 'approach', ''),
                                up_best_bid=w.up_best_bid,
                                down_best_bid=w.down_best_bid,
                                # v2 params
                                budget=w.budget,
                                total_spent=w.total_spent,
                                p_smooth=p_smooth_val,
                                up_best_ask=w.up_best_ask,
                                down_best_ask=w.down_best_ask,
                                up_spread=w.up_spread,
                                down_spread=w.down_spread,
                                hold_secs=(time.time() - w.bet_ts) if w.bet_ts > 0 else 999.0,
                                up_cost=getattr(w, 'up_cost', 0) or 0,
                                down_cost=getattr(w, 'down_cost', 0) or 0,
                                # P5: advisor param adjustments
                                advisor_trim_mult=_advisor_trim_mult,
                                advisor_hedge_adj=_advisor_hedge_adj,
                                # Rehedge hysteresis counter
                                rh_consec=getattr(w, '_rh_consec', 0),
                                # DIRECTIONAL reversal persistence (wall-clock secs)
                                reversal_secs=(time.time() - getattr(w, '_reversal_start_ts', 0.0))
                                    if getattr(w, '_reversal_start_ts', 0.0) > 0 else 0.0,
                                # Wallet signal (Abrak) for wallet-flip cut
                                wallet_up_shares=self.wallet_signal._cached_up_shares if self.wallet_signal.enabled else 0.0,
                                wallet_down_shares=self.wallet_signal._cached_down_shares if self.wallet_signal.enabled else 0.0,
                                wallet_prev_direction=getattr(w, '_wallet_entry_direction', ''),
                                wallet_entry_against_pct=getattr(w, '_wallet_entry_against_pct', 0.0),
                                # Abrak position delta (share changes between polls)
                                wallet_delta_up=self.wallet_signal._delta_up if self.wallet_signal.enabled else 0.0,
                                wallet_delta_down=self.wallet_signal._delta_down if self.wallet_signal.enabled else 0.0,
                                wallet_delta_age=(time.time() - self.wallet_signal._delta_ts)
                                    if (self.wallet_signal.enabled and self.wallet_signal._delta_ts > 0)
                                    else 999.0,
                                # S2 deterioration speed: mark_ratio from ~15s ago
                                mark_ratio_15s_ago=_mr_15s_ago,
                            )

                            # Capture rehedge hysteresis counter from engine
                            w._rh_consec = action.get("_rh_consec", 0)

                            # Track EV gate blocks (Imp 2)
                            if action.get("_ev_block", False):
                                self._metrics.setdefault("cut_ev_blocked", 0)
                                self._metrics["cut_ev_blocked"] += 1

                            act = action["action"]
                            _t_decide_done = time.monotonic()

                            # Reset CUT hysteresis when engine says non-CUT
                            if act != "CUT_LOSS":
                                w._cut_consec = 0
                                w._cut_first_ts = 0.0
                                # Staged-cut recovery: only reset AFTER cooldown has
                                # elapsed.  During cooldown the S3 handler (below) does
                                # the deterioration-vs-recovery check.
                                _staged_cd = 25.0
                                _pc_ts = getattr(w, '_partial_cut_ts', 0.0)
                                if (getattr(w, '_partial_cut_done', False)
                                        and _pc_ts > 0
                                        and (time.time() - _pc_ts) >= _staged_cd):
                                    w._partial_cut_done = False
                                    w._partial_cut_ts = 0.0
                                    # Only fully reset stage-1 count if mark is healthy.
                                    # If mark is still bad, preserve count so next CUT
                                    # escalates via _stage1_exhausted instead of restarting.
                                    _rec_mr = 0.0
                                    if (w.up_best_bid > 0 and w.down_best_bid > 0
                                            and float(w.total_spent or 0) > 0):
                                        _rec_mr = ((float(w.up_shares) * _sell_proceeds_per_share(w.up_best_bid)
                                                    + float(w.down_shares) * _sell_proceeds_per_share(w.down_best_bid))
                                                   / max(float(w.total_spent), 0.01))
                                    if _rec_mr > 0.40:
                                        w._stage1_count = 0  # true recovery — mark healthy
                                    # else: keep count — mark still bad, escalate on next CUT
                                    self._metrics.setdefault("cut_staged_recovered", 0)
                                    self._metrics["cut_staged_recovered"] += 1

                            # Reset flip-TP partial state when engine stops saying TAKE_PROFIT
                            if act != "TAKE_PROFIT":
                                if getattr(w, '_flip_tp_partial_done', False):
                                    w._flip_tp_partial_done = False
                                    w._flip_tp_first_ts = 0.0
                                    self._metrics.setdefault("flip_tp_recovered", 0)
                                    self._metrics["flip_tp_recovered"] += 1

                            # P2: Pathological quote gate in SCALP phase
                            # Check BOTH mid prices AND best asks — WS can update asks
                            # while mids lag (e.g. 0.99/0.99 ask with stale mid)
                            _sum_mid_scalp = w.up_price + w.down_price
                            _sum_ask_scalp = (getattr(w, 'up_best_ask', 0) or 0) + (getattr(w, 'down_best_ask', 0) or 0)
                            _patho = (_sum_mid_scalp > 1.12 or _sum_mid_scalp < 0.70
                                      or _sum_ask_scalp > 1.15 or _sum_ask_scalp < 0.50)
                            if _patho:
                                self._metrics["quote_pathological"] += 1
                                if act in ("ADD", "REHEDGE", "TRIM"):
                                    if self._cycle_count % 20 == 1:
                                        print(f"    PATHOLOGICAL QUOTES: blocking {act} "
                                              f"mid_sum={_sum_mid_scalp:.3f} ask_sum={_sum_ask_scalp:.3f}")
                                    continue
                                # DIRECTIONAL CUT gate (Imp 3): pathological quotes → unreliable mark_ratio
                                # Exception: extreme fail-safe (mark_ratio < 0.10) still fires
                                if act == "CUT_LOSS" and getattr(w, 'approach', '') == "DIRECTIONAL":
                                    _mr_patho = 1.0
                                    if w.total_spent > 0:
                                        _mk_patho = (float(w.up_shares) * (w.up_best_bid or 0)
                                                     + float(w.down_shares) * (w.down_best_bid or 0))
                                        _mr_patho = _mk_patho / w.total_spent
                                    if _mr_patho >= 0.05:  # only S1 HARD_CAT (<0.05) passes
                                        self._metrics.setdefault("cut_patho_blocked", 0)
                                        self._metrics["cut_patho_blocked"] += 1
                                        if self._cycle_count % 20 == 1:
                                            print(f"    PATHOLOGICAL: blocking DIR CUT mark~{_mr_patho:.2f}")
                                        continue
                                    # mark_ratio < 0.10 → extreme fail-safe, let CUT proceed
                                # Non-DIRECTIONAL CUT_LOSS, TAKE_PROFIT, LOCK proceed

                            # ── TRADE-COUNT GOVERNOR: prevent churn collapse ──
                            # In a 5min window, 20+ round-trips is excessive churn.
                            # Soft cap (20): block ADD/REHEDGE — stop adding exposure
                            # Hard cap (30): block TRIM too — stop selling into noise
                            # CUT_LOSS and TAKE_PROFIT always pass (protective exits).
                            _tc = w.trade_count
                            if _tc >= 30 and act in ("ADD", "REHEDGE", "TRIM"):
                                self._metrics.setdefault("gov_hard_blocks", 0)
                                self._metrics["gov_hard_blocks"] += 1
                                if self._cycle_count % 30 == 1:
                                    print(f"    TRADE GOV HARD: {act} blocked (trades={_tc}/30)")
                                continue
                            if _tc >= 20 and act in ("ADD", "REHEDGE"):
                                self._metrics.setdefault("gov_soft_blocks", 0)
                                self._metrics["gov_soft_blocks"] += 1
                                if self._cycle_count % 30 == 1:
                                    print(f"    TRADE GOV SOFT: {act} blocked (trades={_tc}/20)")
                                continue

                            # ── ADVISOR NUDGE: read advice file if available ──
                            try:
                                _adv = self._read_advisor_advice(w, act, action)
                                if _adv:
                                    _adv_act = _adv.get("action", "")
                                    _nudge = _adv.get("_nudge_applied", False)

                                    if _nudge:
                                        # ── TRIM NUDGE: advisor wants to trim a side ──
                                        _trim_side = _adv["_nudge_side"]
                                        _trim_shares = _adv["_nudge_shares"]
                                        _trim_bid = w.up_best_bid if _trim_side == "UP" else w.down_best_bid
                                        print(f"  🤖 ADVISOR TRIM: {_trim_side} {_trim_shares:.1f}sh @${_trim_bid:.3f} "
                                              f"(conf={_adv.get('confidence', 0):.2f}) | {_adv.get('reason', '?')[:60]}")
                                        if _trim_bid > 0 and _trim_shares >= 1.0:
                                            r = await self._sell_hedged(
                                                w, _trim_side, _trim_shares, _trim_bid,
                                                reason="ADVISOR-TRIM", retry=True,
                                            )
                                            if r["ok"]:
                                                w._last_trim_ts = time.time()
                                                self._metrics["trim_successes"] += 1
                                                self._metrics["trim_pnl_total"] += r.get("pnl", 0)
                                                print(f"    ✅ ADVISOR TRIM OK: pnl=${r.get('pnl', 0):+.3f}")
                                            else:
                                                print(f"    ❌ ADVISOR TRIM FAIL: {r.get('reason', '?')}")
                                    elif _adv_act and _adv_act != act:
                                        print(f"  🤖 ADVISOR: {_adv_act} (conf={_adv.get('confidence', 0):.2f}) "
                                              f"| {_adv.get('reason', '?')[:60]}")
                            except Exception:
                                pass  # never break trading loop

                            # Feature logging (every ~1s) — wrapped so it can't break trading
                            try:
                                if time.time() - w._last_feature_log > 1.0:
                                    prob = conv if isinstance(conv, dict) else {}
                                    sigs = prob.get("signals", {})
                                    row = (f"{time.time()},{w.slug},{w.start_ts},"
                                           f"{(w.interval_secs - w.time_remaining) / max(1, w.interval_secs):.3f},"
                                           f"{prob.get('q_mid', 0):.4f},"
                                           f"{prob.get('p_raw', 0):.4f},{prob.get('p_smooth', 0):.4f},"
                                           f"{sigs.get('z', 0):.4f},{sigs.get('book_imb', 0):.4f},"
                                           f"{sigs.get('lag', 0):.4f},{sigs.get('pressure', 0):.4f},"
                                           f"{w.up_best_bid:.4f},{w.up_best_ask:.4f},"
                                           f"{w.down_best_bid:.4f},{w.down_best_ask:.4f}\n")
                                    self._feature_log_q.append(row)
                                    w._last_feature_log = time.time()
                            except Exception:
                                pass

                            # Periodic action visibility log (every 30s)
                            if time.time() - w._last_act_log > 30:
                                up_b = w.up_best_bid
                                dn_b = w.down_best_bid
                                real = getattr(w, 'realized_pnl', 0)
                                _tsr = action.get("trim_skip_reason", "")
                                _trim_info = f" trim_skip={_tsr}" if _tsr else ""
                                # Rehedge telemetry
                                _rh_act = action.get("_rh_actual", 0)
                                _rh_tgt = action.get("_rh_target", 0)
                                _rh_dev = action.get("_rh_dev", 0)
                                _rh_blk = action.get("_rh_block", "")
                                _rh_info = f" min={_rh_act:.2f}/{_rh_tgt:.2f} d={_rh_dev:+.3f}" if _rh_act > 0 else ""
                                if _rh_blk:
                                    _rh_info += f" ({_rh_blk})"
                                print(f"  📊 {getattr(w, 'approach', 'HEDGED')} {w.asset}: {act} | ↑{w.up_shares:.1f}sh@${up_b:.2f} ↓{w.down_shares:.1f}sh@${dn_b:.2f} "
                                      f"| spent=${w.total_spent:.2f}/{w.budget:.2f} rsv=${w.reserve_budget:.2f} real=${real:+.2f} "
                                      f"| {action['reason']}{_trim_info}{_rh_info}")
                                w._last_act_log = time.time()

                            if act == "ADD" and w.trade_count < w.max_adds and w.trade_count < 12:
                                self._metrics["add_attempts"] += 1
                                # Adaptive throttle: 10s early, 5s mid, 3s late
                                t_frac = (w.interval_secs - w.time_remaining) / max(1, w.interval_secs)
                                throttle = 10.0 if t_frac < 0.33 else (5.0 if t_frac < 0.66 else 3.0)
                                last_add = getattr(w, '_last_add_ts', 0)
                                if time.time() - last_add < throttle:
                                    continue  # too soon

                                fav_side = action["side"]
                                fav_spend = action.get("fav_spend", 0)
                                hedge_spend = action.get("hedge_spend", 0)

                                # ── HEDGE PRIORITY: if entry hedge failed, force hedge on first ADD ──
                                # DIRECTIONAL: skip hedge priority — no hedge side
                                if getattr(w, 'approach', '') != "DIRECTIONAL" and \
                                   getattr(w, '_needs_initial_hedge', False) and hedge_spend < 1.00:
                                    hedge_side_chk = "DOWN" if w.bet_side == "UP" else "UP"
                                    h_shares_chk = w.up_shares if hedge_side_chk == "UP" else w.down_shares
                                    if (h_shares_chk or 0) < 0.5:
                                        # No hedge shares yet — redirect spend to hedge
                                        hedge_spend = max(1.00, fav_spend * 0.50)
                                        fav_spend = max(1.00, fav_spend - hedge_spend)
                                        print(f"    HEDGE PRIORITY: redirecting ${hedge_spend:.2f} to {hedge_side_chk}")
                                        w._needs_initial_hedge = False  # only once

                                # Buy favored side
                                if fav_spend >= 1.00:  # Polymarket min $1.00
                                    fav_ask = w.up_best_ask if fav_side == "UP" else w.down_best_ask
                                    if fav_ask >= 1.0:
                                        fav_ask = (w.up_price if fav_side == "UP" else w.down_price) + 0.01
                                    fav_token = w.up_token if fav_side == "UP" else w.down_token
                                    fav_shares = round(fav_spend / _bcs(fav_ask), 2)

                                    if fav_shares >= 1 and fav_token:
                                        perf.record_decision_to_send((time.monotonic() - _t_decide_done) * 1000)
                                        # Use maker for ADDs only when >60s AND >= 5 shares (maker minimum)
                                        _use_maker = w.time_remaining > 60 and fav_shares >= 5
                                        oid = await asyncio.to_thread(
                                            self._execute_order,
                                            fav_token, "BUY", fav_shares, fav_ask,
                                            w.neg_risk, w.end_ts,
                                            2, _use_maker, fav_spend,  # spend_dollars
                                        )
                                        if oid:
                                            self._metrics["add_successes"] += 1
                                            ac = getattr(self._poly_client, 'last_fill_cost', 0) or fav_spend
                                            fill_a = getattr(self._poly_client, 'last_fill_shares', 0) or getattr(self, '_last_fill_shares', 0)
                                            if fill_a and fill_a > 0:
                                                ash = float(fill_a)
                                            else:
                                                a_cps = _buy_cost_per_share(fav_ask) if fav_ask > 0 else 0.0
                                                ash = round(ac / a_cps, 2) if a_cps > 0 else fav_shares
                                            w.total_spent += ac
                                            w.total_cost += ac
                                            w.trade_count += 1
                                            w._last_add_ts = time.time()
                                            self.engine.bankroll -= ac  # deduct ADD buy cost
                                            # Decrement reserve when ADD eats into it
                                            if w.total_spent > (w.budget - w.reserve_budget):
                                                overshoot = w.total_spent - (w.budget - w.reserve_budget)
                                                w.reserve_budget = max(0, w.reserve_budget - overshoot)
                                            if fav_side == "UP":
                                                w.up_shares += ash
                                                w.up_cost += ac
                                            else:
                                                w.down_shares += ash
                                                w.down_cost += ac
                                            w.bet_shares = w.up_shares if w.bet_side == "UP" else w.down_shares
                                            print(f"  ADD {fav_side} +{ash:.1f}sh @${fav_ask:.3f}=${ac:.2f} (#{w.trade_count}) rsv=${w.reserve_budget:.2f}")
                                            self._log_event(
                                                f"ADD {fav_side} {w.asset} {w.interval_label} "
                                                f"+{ash:.1f}sh @${fav_ask:.3f}=${ac:.2f} "
                                                f"[↑{w.up_shares:.1f} ↓{w.down_shares:.1f} spent=${w.total_spent:.2f}/{w.budget:.2f}]",
                                                "buy",
                                            )

                                # Buy hedge side (only if should_act says so)
                                # Polymarket min marketable order is $1.00
                                if hedge_spend >= 1.00:
                                    hedge_side = "DOWN" if fav_side == "UP" else "UP"
                                    hedge_ask = w.up_best_ask if hedge_side == "UP" else w.down_best_ask
                                    if hedge_ask >= 1.0:
                                        hedge_ask = (w.up_price if hedge_side == "UP" else w.down_price) + 0.01
                                    hedge_token = w.up_token if hedge_side == "UP" else w.down_token
                                    hedge_shares = round(hedge_spend / _bcs(hedge_ask), 2)

                                    if hedge_shares >= 1 and hedge_token:
                                        h_oid = await asyncio.to_thread(
                                            self._execute_order,
                                            hedge_token, "BUY", hedge_shares, hedge_ask,
                                            w.neg_risk, w.end_ts,
                                            2, False, hedge_spend,  # spend_dollars
                                        )
                                        if h_oid:
                                            hc = getattr(self._poly_client, 'last_fill_cost', 0) or hedge_spend
                                            fill_h = getattr(self._poly_client, 'last_fill_shares', 0) or getattr(self, '_last_fill_shares', 0)
                                            if fill_h and fill_h > 0:
                                                hsh = float(fill_h)
                                            else:
                                                h_cps = _buy_cost_per_share(hedge_ask) if hedge_ask > 0 else 0.0
                                                hsh = round(hc / h_cps, 2) if h_cps > 0 else hedge_shares
                                            w.total_spent += hc
                                            w.total_cost += hc
                                            w.trade_count += 1
                                            self.engine.bankroll -= hc  # deduct hedge ADD buy cost
                                            # Decrement reserve when hedge ADD eats into it
                                            if w.total_spent > (w.budget - w.reserve_budget):
                                                overshoot = w.total_spent - (w.budget - w.reserve_budget)
                                                w.reserve_budget = max(0, w.reserve_budget - overshoot)
                                            if hedge_side == "UP":
                                                w.up_shares += hsh
                                                w.up_cost += hc
                                            else:
                                                w.down_shares += hsh
                                                w.down_cost += hc
                                            print(f"  ADD hedge {hedge_side} +{hsh:.1f}sh @${hedge_ask:.3f}=${hc:.2f}")
                                            self._log_event(
                                                f"ADD hedge {hedge_side} {w.asset} {w.interval_label} "
                                                f"+{hsh:.1f}sh @${hedge_ask:.3f}=${hc:.2f} "
                                                f"[↑{w.up_shares:.1f} ↓{w.down_shares:.1f}]",
                                                "buy",
                                            )

                            elif act == "TRIM":
                                # Smart trim via unified sell pipeline
                                # Track flip metrics (from 2-stage flip detection)
                                _flip_label = action.get("_flip_label", "")
                                if _flip_label:
                                    self._metrics["flip_detected"] += 1
                                    _flip_dur = action.get("_flip_duration", 0)
                                    self._metrics["flip_latency_sum"] += _flip_dur
                                    if _flip_label == "soft":
                                        self._metrics["flip_soft"] += 1
                                    elif _flip_label == "hard":
                                        self._metrics["flip_hard"] += 1
                                trim_side = action.get("sell_side", "")
                                trim_shares = float(action.get("fraction", 0))
                                # ── TRIM COOLDOWN ──
                                _is_overhedge = "overhedge" in action.get("reason", "").lower()
                                _trim_cd = 15.0 if _is_overhedge else 20.0
                                _last_trim = getattr(w, '_last_trim_ts', 0)
                                if time.time() - _last_trim < _trim_cd:
                                    trim_shares = 0
                                # ── OVERHEDGE HOLD GATE ──
                                # Don't trim for overhedge within 30s of entry fill OR last ADD
                                # (ADDs can transiently over-hedge; give position time to settle)
                                _eft = getattr(w, '_entry_fill_ts', 0)
                                _lat = getattr(w, '_last_add_ts', 0)
                                _last_position_change = max(_eft, _lat)
                                if _is_overhedge and _last_position_change > 0 and time.time() - _last_position_change < 30.0:
                                    trim_shares = 0
                                # Count trim_attempts AFTER eligibility gates
                                if trim_side and trim_shares >= 1:
                                    self._metrics["trim_attempts"] += 1
                                if trim_side and trim_shares >= 1:
                                    trim_bid = w.up_best_bid if trim_side == "UP" else w.down_best_bid
                                    r = await self._sell_hedged(
                                        w, trim_side, trim_shares, trim_bid,
                                        reason="TRIM", retry=False,
                                    )
                                    if r["ok"]:
                                        w._last_trim_ts = time.time()
                                        proceeds = r["proceeds"]

                                        # Refill reserve from trim proceeds (30% of proceeds)
                                        _refill = round(proceeds * 0.30, 2)
                                        w.reserve_budget += _refill

                                        # ── TRIM REINVEST: buy favored/winning side with proceeds ──
                                        # GATE: only reinvest if position is healthy enough.
                                        # If mark_ratio is already weak, reinvesting just raises
                                        # total_spent and can trigger immediate CUT_LOSS.
                                        _approach_tr = getattr(w, 'approach', '')
                                        # Reinvest % of trim proceeds into favored side
                                        # Flip-confirmed trims get higher reinvest to accelerate rotation
                                        _is_flip_trim = action.get("_flip_label", "") in ("soft", "hard", "roll")
                                        _reinvest_pct = 0.55 if _is_flip_trim else 0.40
                                        _reinvest_amt = round(float(proceeds) * _reinvest_pct, 2)
                                        # Health check: estimate mark_ratio before committing
                                        _up_bid_ri = w.up_best_bid if w.up_best_bid > 0 else w.up_price
                                        _dn_bid_ri = w.down_best_bid if w.down_best_bid > 0 else w.down_price
                                        _mark_ri = (w.up_shares * _up_bid_ri + w.down_shares * _dn_bid_ri)
                                        _spent_ri = w.total_spent if w.total_spent > 0 else 1.0
                                        _mark_ratio_ri = _mark_ri / _spent_ri
                                        # Flip trims get lower health threshold (more aggressive reinvest)
                                        _ri_threshold = 0.65 if _is_flip_trim else 0.75
                                        _ri_healthy = _mark_ratio_ri > _ri_threshold
                                        if (_approach_tr == "HEDGED"
                                                and _reinvest_amt >= 1.00
                                                and w.time_remaining > 30
                                                and _ri_healthy):
                                            ri_side = "DOWN" if trim_side == "UP" else "UP"
                                            ri_ask = w.up_best_ask if ri_side == "UP" else w.down_best_ask
                                            if ri_ask >= 1.0:
                                                ri_ask = (w.up_price if ri_side == "UP" else w.down_price) + 0.01
                                            ri_token = w.up_token if ri_side == "UP" else w.down_token
                                            # Allow buying winning side even at higher asks (heading to $1)
                                            if ri_ask > 0.01 and ri_ask < 0.98 and ri_token:
                                                ri_shares = round(_reinvest_amt / _buy_cost_per_share(ri_ask), 2)
                                                if ri_shares >= 1:
                                                    ri_oid = await asyncio.to_thread(
                                                        self._execute_order,
                                                        ri_token, "BUY", ri_shares, ri_ask,
                                                        w.neg_risk, w.end_ts,
                                                        2, False, _reinvest_amt,  # spend_dollars
                                                    )
                                                    if ri_oid:
                                                        ri_cost = getattr(self._poly_client, 'last_fill_cost', 0) or _reinvest_amt
                                                        ri_fill = getattr(self._poly_client, 'last_fill_shares', 0) or ri_shares
                                                        ri_fill = float(ri_fill) if ri_fill else ri_shares
                                                        w.total_spent += ri_cost
                                                        w.total_cost += ri_cost
                                                        w.trade_count += 1
                                                        if ri_side == "UP":
                                                            w.up_shares += ri_fill
                                                            w.up_cost += ri_cost
                                                        else:
                                                            w.down_shares += ri_fill
                                                            w.down_cost += ri_cost
                                                        # Deduct reinvested amount from realized (it's re-deployed)
                                                        w.realized_pnl -= ri_cost
                                                        self.engine.bankroll -= ri_cost
                                                        # Deduct from reserve when reinvest eats into it
                                                        if w.total_spent > (w.budget - w.reserve_budget):
                                                            _ri_over = w.total_spent - (w.budget - w.reserve_budget)
                                                            w.reserve_budget = max(0, w.reserve_budget - _ri_over)
                                                        w._last_reinvest_ts = time.time()  # CUT cooldown after reinvest
                                                        print(f"  REINVEST {ri_side} +{ri_fill:.1f}sh @${ri_ask:.3f}=${ri_cost:.2f} (from trim)")
                                                        self._log_event(
                                                            f"REINVEST {ri_side} {w.asset} {w.interval_label} "
                                                            f"+{ri_fill:.1f}sh @${ri_ask:.3f}=${ri_cost:.2f} "
                                                            f"[↑{w.up_shares:.1f} ↓{w.down_shares:.1f}]",
                                                            "buy")
                                        self._save_persisted_state()

                            elif act == "REHEDGE":
                                # DIRECTIONAL: no hedge side to rebalance — skip
                                if getattr(w, 'approach', '') == "DIRECTIONAL":
                                    continue
                                # P3: Throttle — max once per 15s (was 7s — reduce churn)
                                if time.time() - w._last_rehedge_ts < 15.0:
                                    self._metrics.setdefault("rehedge_blocked_throttle", 0)
                                    self._metrics["rehedge_blocked_throttle"] += 1
                                else:
                                    _rh_side = action.get("side", "")
                                    # Cap each rehedge to 20% of reserve (was 30% — less oversizing)
                                    _rh_max = max(1.00, w.reserve_budget * 0.20)
                                    _rh_spend = min(action.get("rehedge_spend", 0), _rh_max, w.reserve_budget)
                                    if _rh_spend < 1.00 or w.reserve_budget < 1.00:
                                        # Blocked: insufficient spend or reserve
                                        self._metrics.setdefault("rehedge_blocked_budget", 0)
                                        self._metrics["rehedge_blocked_budget"] += 1
                                    else:
                                        _rh_token = w.up_token if _rh_side == "UP" else w.down_token
                                        _rh_ask = w.up_best_ask if _rh_side == "UP" else w.down_best_ask
                                        _rh_cps = _buy_cost_per_share(_rh_ask) if _rh_ask > 0 else _rh_ask
                                        _rh_shares = round(_rh_spend / _rh_cps, 2) if _rh_cps > 0 else 0
                                        if _rh_shares < 1 or not _rh_token:
                                            self._metrics.setdefault("rehedge_blocked_shares", 0)
                                            self._metrics["rehedge_blocked_shares"] += 1
                                        else:
                                            # ── Feasibility passed: NOW count as real attempt ──
                                            self._metrics["rehedge_attempts"] += 1
                                            _rh_oid = await asyncio.to_thread(
                                                self._execute_order,
                                                _rh_token, "BUY", _rh_shares, _rh_ask,
                                                w.neg_risk, w.end_ts,
                                                2, False, _rh_spend,  # spend_dollars
                                            )
                                            if _rh_oid:
                                                _rh_cost = getattr(self._poly_client, 'last_fill_cost', 0) or _rh_spend
                                                _rh_fill = getattr(self._poly_client, 'last_fill_shares', 0) or _rh_shares
                                                _rh_fill = float(_rh_fill) if _rh_fill else _rh_shares
                                                if _rh_side == "UP":
                                                    w.up_shares += float(_rh_fill)
                                                    w.up_cost += float(_rh_cost)
                                                else:
                                                    w.down_shares += float(_rh_fill)
                                                    w.down_cost += float(_rh_cost)
                                                w.total_spent += float(_rh_cost)
                                                w.total_cost += float(_rh_cost)
                                                w.reserve_budget = max(0, w.reserve_budget - float(_rh_cost))
                                                self.engine.bankroll -= float(_rh_cost)
                                                w.trade_count += 1
                                                self._metrics["rehedge_successes"] += 1
                                                w._last_rehedge_ts = time.time()
                                                print(f"  REHEDGE {_rh_side} +{float(_rh_fill):.1f}sh "
                                                      f"@${_rh_ask:.3f} reserve=${w.reserve_budget:.2f}")

                            elif act == "FLIP_ROLL":
                                # ── FLIP_ROLL: sell old favored → buy new favored ──
                                _fr_sell_side = action.get("sell_side", "")
                                _fr_sell_shares = float(action.get("sell_shares", 0))
                                _fr_buy_side = action.get("buy_side", "")
                                _fr_buy_spend = float(action.get("buy_spend", 0))
                                _fr_buy_ask = float(action.get("buy_ask", 0))
                                _fr_sell_bid = w.up_best_bid if _fr_sell_side == "UP" else w.down_best_bid

                                if _fr_sell_shares >= 1 and _fr_sell_bid > 0.01:
                                    self._metrics["flip_roll_attempts"] += 1
                                    # Step 1: Sell old favored
                                    _fr_sell_res = await self._sell_hedged(
                                        w, _fr_sell_side, _fr_sell_shares, _fr_sell_bid,
                                        reason="FLIP_SELL", retry=True)
                                    if _fr_sell_res["ok"]:
                                        _fr_proceeds = _fr_sell_res["proceeds"]
                                        # Step 2: Buy new favored with portion of proceeds
                                        _fr_actual_buy = min(_fr_buy_spend, _fr_proceeds * 0.60)
                                        _fr_buy_token = w.up_token if _fr_buy_side == "UP" else w.down_token
                                        if _fr_actual_buy >= 1.00 and _fr_buy_ask > 0.01 and _fr_buy_ask < 0.95 and _fr_buy_token:
                                            _fr_buy_shares = round(_fr_actual_buy / _buy_cost_per_share(_fr_buy_ask), 2)
                                            if _fr_buy_shares >= 1:
                                                _fr_buy_oid = await asyncio.to_thread(
                                                    self._execute_order,
                                                    _fr_buy_token, "BUY", _fr_buy_shares, _fr_buy_ask,
                                                    w.neg_risk, w.end_ts,
                                                    2, False, _fr_actual_buy,  # spend_dollars
                                                )
                                                if _fr_buy_oid:
                                                    _fr_buy_cost = float(getattr(self._poly_client, 'last_fill_cost', 0) or _fr_actual_buy)
                                                    _fr_buy_fill = float(getattr(self._poly_client, 'last_fill_shares', 0) or _fr_buy_shares)
                                                    if _fr_buy_side == "UP":
                                                        w.up_shares += _fr_buy_fill
                                                        w.up_cost += _fr_buy_cost
                                                    else:
                                                        w.down_shares += _fr_buy_fill
                                                        w.down_cost += _fr_buy_cost
                                                    w.total_spent += _fr_buy_cost
                                                    w.total_cost += _fr_buy_cost
                                                    self.engine.bankroll -= _fr_buy_cost
                                                    w.trade_count += 1
                                                    w._last_add_ts = time.time()
                                                    self._metrics["flip_roll_successes"] += 1
                                                    print(f"  FLIP_ROLL {_fr_sell_side}→{_fr_buy_side} "
                                                          f"sold {_fr_sell_res['fill_shares']:.1f}sh "
                                                          f"bought +{_fr_buy_fill:.1f}sh @${_fr_buy_ask:.3f}")
                                                    self._log_event(
                                                        f"FLIP_ROLL {w.asset} {w.interval_label} "
                                                        f"{_fr_sell_side}→{_fr_buy_side} "
                                                        f"-{_fr_sell_res['fill_shares']:.1f}/+{_fr_buy_fill:.1f}sh",
                                                        "info",
                                                    )
                                        # Even if buy leg fails, sell already happened — that's OK
                                        # proceeds return to bankroll via _sell_hedged

                            elif act in ("TAKE_PROFIT", "CUT_LOSS"):
                                # ── POST-REINVEST CUT COOLDOWN ──
                                # Block CUT_LOSS for 30s after a reinvest fill to prevent
                                # TRIM→REINVEST→CUT chain (reinvest raises total_spent,
                                # which tanks mark_ratio and triggers immediate CUT).
                                _lri = getattr(w, '_last_reinvest_ts', 0)
                                _cut_cooldown = 30.0
                                if act == "CUT_LOSS" and _lri > 0 and time.time() - _lri < _cut_cooldown:
                                    if self._cycle_count % 30 == 1:
                                        print(f"    CUT BLOCKED: post-reinvest cooldown "
                                              f"({_cut_cooldown - (time.time() - _lri):.0f}s left)")
                                    continue  # skip this CUT — let position stabilize
                                # ── POST-ADD CUT COOLDOWN ──
                                # Block CUT_LOSS for 20s after any ADD fill. ADDs change
                                # the position composition; give the hedge time to settle
                                # before evaluating cut worthiness. Also prevents
                                # ADD → instant CUT → ADD cycle that burns fees.
                                _lat_cut = getattr(w, '_last_add_ts', 0)
                                if act == "CUT_LOSS" and _lat_cut > 0 and time.time() - _lat_cut < 20.0:
                                    if self._cycle_count % 30 == 1:
                                        print(f"    CUT BLOCKED: post-ADD cooldown "
                                              f"({20.0 - (time.time() - _lat_cut):.0f}s left)")
                                    continue  # skip — position just changed
                                # ── #4: QUOTE-FRESHNESS GATE ──
                                # Block all sell actions (CUT + TP + flip exits) when quotes
                                # are stale (>1.5s). Selling on outdated bids means we price
                                # the exit wrong. Catastrophic cuts bypass.
                                _quote_age_ms = (time.monotonic() - getattr(w, '_last_ws_recv_mono', time.monotonic())) * 1000
                                _is_catastrophic_q = action.get("_cut_catastrophic", False)
                                if (act in ("CUT_LOSS", "TAKE_PROFIT")
                                        and not _is_catastrophic_q
                                        and _quote_age_ms > 1500):
                                    self._metrics.setdefault("exit_stale_quote_block", 0)
                                    self._metrics["exit_stale_quote_block"] += 1
                                    if self._cycle_count % 20 == 1:
                                        print(f"    {act} BLOCKED: stale quote ({_quote_age_ms:.0f}ms)")
                                    continue
                                # ── CUT HYSTERESIS (time-based) ──
                                # Require cut condition to persist for 4s before firing.
                                # At ~4 cycles/s, 3 cycles was only ~0.75s — too fast.
                                # Now uses wall-clock time for consistent behavior.
                                # Catastrophic cuts bypass this.
                                _CUT_HYSTERESIS_SECS = 4.0
                                _is_catastrophic = action.get("_cut_catastrophic", False)
                                _wants_hysteresis = action.get("_cut_hysteresis", False)
                                if act == "CUT_LOSS" and _wants_hysteresis and not _is_catastrophic:
                                    if not hasattr(w, '_cut_first_ts') or w._cut_first_ts <= 0:
                                        w._cut_first_ts = time.time()
                                    _cut_elapsed = time.time() - w._cut_first_ts
                                    if _cut_elapsed < _CUT_HYSTERESIS_SECS:
                                        self._metrics.setdefault("cut_hysteresis_block", 0)
                                        self._metrics["cut_hysteresis_block"] += 1
                                        if self._cycle_count % 20 == 1:
                                            print(f"    CUT HYSTERESIS: {_cut_elapsed:.1f}s/{_CUT_HYSTERESIS_SECS:.0f}s "
                                                  f"({action.get('reason', '')})")
                                        continue
                                    # Passed hysteresis — fall through to execute
                                elif act != "CUT_LOSS":
                                    # Non-CUT action (TAKE_PROFIT) — reset timer
                                    if hasattr(w, '_cut_first_ts'):
                                        w._cut_first_ts = 0.0
                                # Zero-inventory guard: engine flagged stale inventory
                                if action.get("_zero_inventory", False):
                                    self._metrics.setdefault("zero_inventory_with_spent", 0)
                                    self._metrics["zero_inventory_with_spent"] += 1
                                    continue  # don't execute — position data is stale
                                # Track zero-ratio CUT events (likely stale state)
                                if act == "CUT_LOSS" and w.total_spent > 0:
                                    _cut_settle_ratio = (float(w.up_shares) + float(w.down_shares)) / w.total_spent
                                    if _cut_settle_ratio < 0.01:
                                        self._metrics["cut_zero_ratio"] += 1
                                # ── STAGED CUT for DIRECTIONAL (S2/S3/S4) ──
                                # S1 (HARD_CAT): full liquidation, no staging
                                # S2 (DETERIORATION): 35-45% partial, then S3 check
                                # S4 (REGULAR_CUT): uses old staged 20% logic
                                # TAKE_PROFIT always executes fully.
                                _is_dir_cut = (getattr(w, 'approach', '') == "DIRECTIONAL"
                                               and act == "CUT_LOSS")
                                _is_cat_cut = action.get("_cut_catastrophic", False)
                                _is_det_cut = action.get("_cut_deterioration", False)
                                _partial_done = getattr(w, '_partial_cut_done', False)
                                _staged_cooldown = 25.0  # 20-30s per spec

                                _stage1_exhausted = getattr(w, '_stage1_count', 0) >= 2

                                if _is_dir_cut and not _is_cat_cut and not _partial_done and not _stage1_exhausted:
                                    # STAGE 1: Partial cut
                                    # S2 deterioration: 35-45% (larger de-risk)
                                    # S4 regular: 20% (softer)
                                    _s1_pct = 0.40 if _is_det_cut else 0.20
                                    _s1_label = "S2-DET" if _is_det_cut else "S4-REG"
                                    _s1_sold = False
                                    for sell_side in ["UP", "DOWN"]:
                                        sell_sh = float(w.up_shares if sell_side == "UP" else w.down_shares)
                                        sell_bid = w.up_best_bid if sell_side == "UP" else w.down_best_bid
                                        if sell_sh < 1.0:
                                            continue
                                        partial_sh = max(1.0, math.floor(sell_sh * _s1_pct))
                                        r = await self._sell_hedged(
                                            w, sell_side, partial_sh, sell_bid,
                                            reason=f"CUT-{_s1_label}", retry=True,
                                        )
                                        if r["ok"]:
                                            _s1_sold = True
                                    if _s1_sold:
                                        w._partial_cut_done = True
                                        w._partial_cut_ts = time.time()
                                        w._stage1_count = getattr(w, '_stage1_count', 0) + 1
                                        w._cut_consec = 0
                                        w._cut_first_ts = 0.0
                                        # Snapshot stage-1 state for S3 check
                                        _s1_mr = 0.0
                                        if w.up_best_bid > 0 and w.down_best_bid > 0 and float(w.total_spent or 0) > 0:
                                            _s1_mr = ((float(w.up_shares) * _sell_proceeds_per_share(w.up_best_bid)
                                                        + float(w.down_shares) * _sell_proceeds_per_share(w.down_best_bid))
                                                       / max(float(w.total_spent), 0.01))
                                        w._stage1_mark_ratio = _s1_mr
                                        w._stage1_p_smooth = getattr(self.engine, '_p_smooth', 0.50)
                                        self._metrics.setdefault("cut_staged_partial", 0)
                                        self._metrics["cut_staged_partial"] += 1
                                        # Track exit price for KPI
                                        if sell_bid > 0:
                                            self._metrics.setdefault("_exit_prices", [])
                                            self._metrics["_exit_prices"].append(sell_bid)
                                        print(f"    STAGED CUT: {_s1_label} partial ({_s1_pct:.0%}) mark={_s1_mr:.3f} bid={sell_bid:.2f} — reassess in {_staged_cooldown:.0f}s")
                                        continue  # reassess next cycle
                                    # Stage-1 fill failed
                                    self._metrics.setdefault("cut_stage1_nofill", 0)
                                    self._metrics["cut_stage1_nofill"] += 1
                                    print(f"    STAGED CUT: {_s1_label} no fill — escalating to full liquidation")

                                elif _is_dir_cut and not _is_cat_cut and _partial_done:
                                    # S3: DETERIORATION_STAGE2 — conditional full exit
                                    if time.time() - getattr(w, '_partial_cut_ts', 0) < _staged_cooldown:
                                        if self._cycle_count % 20 == 1:
                                            _cd_left = _staged_cooldown - (time.time() - getattr(w, '_partial_cut_ts', 0))
                                            print(f"    STAGED CUT: cooldown ({_cd_left:.0f}s left)")
                                        continue  # still cooling down
                                    # S3 conditions: exit remainder if ANY:
                                    #   - mark_ratio < 0.18
                                    #   - further deterioration >= -0.05 since stage1
                                    #   - p_ours < 0.18 with reversal persistence
                                    #   - stage1 exhaustion (>=2 partials)
                                    _s2_mr = 0.0
                                    if w.up_best_bid > 0 and w.down_best_bid > 0 and float(w.total_spent or 0) > 0:
                                        _s2_mr = ((float(w.up_shares) * _sell_proceeds_per_share(w.up_best_bid)
                                                    + float(w.down_shares) * _sell_proceeds_per_share(w.down_best_bid))
                                                   / max(float(w.total_spent), 0.01))
                                    _s2_ps = getattr(self.engine, '_p_smooth', 0.50)
                                    _s1_mr = getattr(w, '_stage1_mark_ratio', 0.0)
                                    _s1_ps = getattr(w, '_stage1_p_smooth', 0.50)
                                    # S3 condition 1: mark fell below 0.18
                                    _mark_floor = (_s2_mr < 0.18)
                                    # S3 condition 2: further deterioration >= 0.05 since stage1
                                    _further_det = (_s2_mr < _s1_mr - 0.05)
                                    # S3 condition 3: p_ours < 0.18 + reversal persistent
                                    _p_ours_s3 = _s2_ps if w.bet_side == "UP" else (1.0 - _s2_ps)
                                    _rev_s = (time.time() - getattr(w, '_reversal_start_ts', 0.0)
                                              if getattr(w, '_reversal_start_ts', 0.0) > 0 else 0.0)
                                    _p_collapsed = (_p_ours_s3 < 0.18 and _rev_s >= 15.0)
                                    # S3 condition 4: stage1 exhaustion
                                    _stages_exhausted = getattr(w, '_stage1_count', 0) >= 2
                                    if _mark_floor or _further_det or _p_collapsed or _stages_exhausted:
                                        _s3_reason = ""
                                        if _stages_exhausted:
                                            _s3_reason = " [max stage-1s]"
                                        elif _mark_floor:
                                            _s3_reason = f" [mark<0.18: {_s2_mr:.3f}]"
                                        elif _further_det:
                                            _s3_reason = f" [det -0.05: {_s2_mr:.3f}<{_s1_mr:.3f}]"
                                        elif _p_collapsed:
                                            _s3_reason = f" [p_ours={_p_ours_s3:.2f} rev={_rev_s:.0f}s]"
                                        self._metrics.setdefault("cut_staged_full", 0)
                                        self._metrics["cut_staged_full"] += 1
                                        print(f"    S3 STAGE2: full liquidation{_s3_reason} "
                                              f"mark={_s2_mr:.3f}(was {_s1_mr:.3f}) "
                                              f"p={_s2_ps:.3f}(was {_s1_ps:.3f})")
                                        # Fall through to existing full liquidation code below
                                    else:
                                        # Position stabilized or improved → hold to settlement
                                        w._partial_cut_done = False
                                        w._partial_cut_ts = 0.0
                                        w._cut_first_ts = 0.0
                                        w._stage1_count = 0
                                        self._metrics.setdefault("cut_staged_recovered", 0)
                                        self._metrics["cut_staged_recovered"] += 1
                                        print(f"    STAGED CUT: recovered — holding to settle "
                                              f"mark={_s2_mr:.3f}(was {_s1_mr:.3f})")
                                        continue  # back to HOLD
                                    # Fall through to existing full liquidation code below

                                # ── #1 + #5: EARLY FLIP-TP PARTIAL + RE-CHECK ──
                                # When >180s remaining and flip TP fires, do partial sell
                                # (30%) first, then wait 6s re-check before full liquidation.
                                _is_early_flip_tp = (
                                    act == "TAKE_PROFIT"
                                    and action.get("_flip_tp_early", False)
                                    and not action.get("_cut_catastrophic", False))

                                if _is_early_flip_tp:
                                    _ftp_done = getattr(w, '_flip_tp_partial_done', False)
                                    if not _ftp_done:
                                        # Stage 1: partial sell (30%)
                                        _ftp_sold = False
                                        for _ftp_side in ["UP", "DOWN"]:
                                            _ftp_sh = float(w.up_shares if _ftp_side == "UP" else w.down_shares)
                                            _ftp_bid = w.up_best_bid if _ftp_side == "UP" else w.down_best_bid
                                            if _ftp_sh < 1.0:
                                                continue
                                            _ftp_partial = max(1.0, math.floor(_ftp_sh * 0.35))
                                            r = await self._sell_hedged(
                                                w, _ftp_side, _ftp_partial, _ftp_bid,
                                                reason="FLIP-TP-PARTIAL", retry=True,
                                            )
                                            if r["ok"]:
                                                _ftp_sold = True
                                        if _ftp_sold:
                                            w._flip_tp_partial_done = True
                                            w._flip_tp_first_ts = time.time()
                                            self._metrics.setdefault("flip_tp_partial", 0)
                                            self._metrics["flip_tp_partial"] += 1
                                            print(f"    FLIP-TP: partial (30%) sold — re-check in 6s")
                                        continue  # re-check next cycle
                                    else:
                                        # #5: Re-check delay — wait 6s before full liquidation
                                        _ftp_elapsed = time.time() - w._flip_tp_first_ts
                                        if _ftp_elapsed < 8.0:
                                            if self._cycle_count % 20 == 1:
                                                print(f"    FLIP-TP: re-check delay "
                                                      f"{_ftp_elapsed:.1f}s/8.0s")
                                            continue  # still waiting
                                        # Passed 8s — check if flip still strong (F >= 0.80 for full)
                                        _flip_score = action.get("_flip_score", 0)
                                        if _flip_score >= 0.80:
                                            self._metrics.setdefault("flip_tp_confirmed", 0)
                                            self._metrics["flip_tp_confirmed"] += 1
                                            print(f"    FLIP-TP: confirmed F={_flip_score:.2f} after {_ftp_elapsed:.1f}s — full rotate")
                                        else:
                                            # F weakened — hold partial, don't escalate
                                            self._metrics.setdefault("flip_tp_held_partial", 0)
                                            self._metrics["flip_tp_held_partial"] += 1
                                            print(f"    FLIP-TP: F={_flip_score:.2f} weakened — holding partial position")
                                            continue

                                # ── Snapshot total_spent for cut audit before sells clear it ──
                                _total_spent_snapshot = float(w.total_spent or 0)

                                sold_any = False
                                blocked_reasons: list[str] = []

                                # Sell BOTH sides via unified pipeline
                                for sell_side in ["UP", "DOWN"]:
                                    sell_sh = float(w.up_shares if sell_side == "UP" else w.down_shares)
                                    sell_bid = w.up_best_bid if sell_side == "UP" else w.down_best_bid
                                    if sell_sh < 0.5:
                                        continue
                                    r = await self._sell_hedged(
                                        w, sell_side, sell_sh, sell_bid,
                                        reason=act, retry=True,
                                    )
                                    if r["ok"]:
                                        sold_any = True
                                    else:
                                        blocked_reasons.append(f"{sell_side}:{r['reason']}")

                                w.total_cost = max(0.0, float(w.up_cost or 0.0) + float(w.down_cost or 0.0))
                                w.total_spent = w.total_cost
                                both_empty = w.up_shares < 0.5 and w.down_shares < 0.5

                                if both_empty:
                                    exit_pnl = float(getattr(w, 'realized_pnl', 0.0))
                                    w.bet_shares = 0.0
                                    self._log_event(
                                        f"{act} {w.asset} {w.interval_label} · "
                                        f"↑{w.up_shares:.1f} ↓{w.down_shares:.1f} · "
                                        f"{action['reason']} · pnl=${exit_pnl:+.2f}",
                                        "sell" if exit_pnl >= 0 else "loss",
                                    )

                                    # Realized exit: book PnL now (window settlement is bypassed once flat).
                                    # NOTE: bankroll already adjusted by _sell_hedged() proceeds.
                                    # DO NOT add exit_pnl again — that would double-count.
                                    self.total_pnl += exit_pnl
                                    if abs(exit_pnl) > 1e-9:
                                        self.engine.record_result(exit_pnl > 0, exit_pnl)
                                        if exit_pnl > 0:
                                            self.wins += 1
                                        else:
                                            self.losses += 1

                                    # Update result ring + streak tracker
                                    _exit_side = getattr(w, '_entry_side', w.bet_side) or ""
                                    self._result_ring.append((_exit_side, exit_pnl))
                                    if len(self._result_ring) > self._result_ring_max:
                                        self._result_ring = self._result_ring[-self._result_ring_max:]
                                    # Streak guard: track consecutive same-side negative outcomes
                                    if _exit_side and exit_pnl < 0:
                                        if _exit_side == self._streak_side:
                                            self._streak_count += 1
                                            self._streak_pnl += exit_pnl
                                        else:
                                            self._streak_side = _exit_side
                                            self._streak_count = 1
                                            self._streak_pnl = exit_pnl
                                    else:
                                        # Win or break-even resets streak
                                        self._streak_count = 0
                                        self._streak_pnl = 0.0
                                        if _exit_side:
                                            self._streak_side = _exit_side

                                    # Clear position state — NO re-entry in same window.
                                    # Full TP/CUT exits should not re-enter; wait for
                                    # next 5-min window for fresh signal.
                                    w.bet_side = ""
                                    w.status = "active"
                                    w.bet_order_id = ""
                                    w.bet_price = 0.0
                                    w.bet_ts = 0.0
                                    w.scalp_mode = "WAIT"
                                    w.up_shares = 0.0
                                    w.down_shares = 0.0
                                    w.up_cost = 0.0
                                    w.down_cost = 0.0
                                    w.total_cost = 0.0
                                    w.total_spent = 0.0
                                    w.budget = 0.0
                                    w._last_cut_ts = time.time()  # CUT cooldown gate
                                    w._rh_consec = 0  # reset rehedge hysteresis
                                    w._partial_cut_done = False  # reset staged cut
                                    w._partial_cut_ts = 0.0
                                    w._stage1_count = 0
                                    w._reversal_start_ts = 0.0
                                    # Abrak pivot: DON'T snipe slug — allow re-entry
                                    # on new direction in same window.
                                    _is_pivot = action.get("_abrak_pivot", False)
                                    _is_hb_flip = action.get("_high_bid_tp", False)
                                    if _is_pivot:
                                        _pivot_dir = action.get("_pivot_direction", "")
                                        self._metrics.setdefault("abrak_pivot_exits", 0)
                                        self._metrics["abrak_pivot_exits"] += 1
                                        print(f"    ABRAK PIVOT: exited {_exit_side} pnl=${exit_pnl:.3f} "
                                              f"→ re-entry allowed toward {_pivot_dir}")
                                        # Don't add to sniped slugs — allow re-entry
                                    elif _is_hb_flip:
                                        _new_dir = action.get("_new_direction", "")
                                        self._metrics.setdefault("high_bid_flip_exits", 0)
                                        self._metrics["high_bid_flip_exits"] += 1
                                        print(f"    HIGH-BID FLIP TP: exited {_exit_side} pnl=${exit_pnl:.3f} "
                                              f"→ re-entry allowed toward {_new_dir}")
                                        # Don't add to sniped slugs — allow re-entry in new direction
                                    else:
                                        # Block re-entry for this window — slug stays sniped
                                        self._kelly_sniped_slugs.add(slug)

                                    # Track DIRECTIONAL flip-exits
                                    if action.get("_flip_exit", False) and not _is_pivot:
                                        self._metrics.setdefault("dir_flip_exits", 0)
                                        self._metrics["dir_flip_exits"] += 1
                                        _new_dir = action.get("_new_direction", "")
                                        print(f"    FLIP-EXIT: {act} → ready to re-enter {_new_dir}")

                                    # Track wallet-flip cuts/exits
                                    if action.get("_wallet_flip", False):
                                        self._metrics.setdefault("wallet_flip_exits", 0)
                                        self._metrics["wallet_flip_exits"] += 1
                                        print(f"    WALLET-FLIP: Abrak switched sides → {act} pnl=${exit_pnl:.3f}")

                                    # ── KPI TRACKING ──
                                    if getattr(w, 'approach', '') == "DIRECTIONAL":
                                        _kpi_bid = (w.up_best_bid if (action.get("side", "") or w.bet_side) == "UP"
                                                    else w.down_best_bid)
                                        if act == "CUT_LOSS":
                                            self._metrics.setdefault("kpi_sell_pnl", 0.0)
                                            self._metrics["kpi_sell_pnl"] += exit_pnl
                                            # Track exit prices for median calculation
                                            self._metrics.setdefault("kpi_exit_prices", [])
                                            self._metrics["kpi_exit_prices"].append(round(_kpi_bid, 3))
                                            if len(self._metrics["kpi_exit_prices"]) > 200:
                                                self._metrics["kpi_exit_prices"] = self._metrics["kpi_exit_prices"][-200:]
                                            # Deep cut count (exit_price <= 0.04)
                                            if _kpi_bid <= 0.04:
                                                self._metrics.setdefault("kpi_deep_cuts", 0)
                                                self._metrics["kpi_deep_cuts"] += 1

                                    # Cut-quality audit: record for settlement eval
                                    if getattr(w, 'approach', '') == "DIRECTIONAL":
                                        import datetime as _dtca
                                        self._cut_audit.append({
                                            "slug": w.slug, "start_ts": w.start_ts,
                                            "end_ts": w.end_ts, "cut_ts": time.time(),
                                            "bet_side": action.get("side", w.bet_side) or w.bet_side,
                                            "total_spent": _total_spent_snapshot,
                                            "exit_pnl": exit_pnl,
                                            "p_smooth": p_smooth_val,
                                            "hour": _dtca.datetime.now(_dtca.timezone.utc).strftime("%H"),
                                        })
                                        if len(self._cut_audit) > 200:
                                            self._cut_audit = self._cut_audit[-200:]

                                    # ── TELEGRAM EXIT ALERT ──
                                    if self._tg_enabled:
                                        _tg_exit_icon = "✅" if exit_pnl >= 0 else "❌"
                                        _tg_act_label = "TAKE PROFIT" if act == "TAKE_PROFIT" else "CUT LOSS"
                                        _tg_exit_msg = (
                                            f"{_tg_exit_icon} <b>{_tg_act_label}</b> · "
                                            f"P&L: <b>${exit_pnl:+.2f}</b>\n"
                                            f"📋 {action.get('reason', '')[:80]}\n"
                                            f"📊 Record: {self.wins}W-{self.losses}L | "
                                            f"Bank: ${self.engine.bankroll:.2f}"
                                        )
                                        try:
                                            await self._tg_send(_tg_exit_msg)
                                        except Exception:
                                            pass

                                else:
                                    # Still holding inventory (no fill / partial fill): keep position open.
                                    if w.bet_side == "UP" and w.up_shares < 0.5 and w.down_shares >= 0.5:
                                        w.bet_side = "DOWN"
                                    elif w.bet_side == "DOWN" and w.down_shares < 0.5 and w.up_shares >= 0.5:
                                        w.bet_side = "UP"

                                    if w.bet_side == "UP" and w.up_shares > 0:
                                        w.bet_shares = w.up_shares
                                        if w.up_cost > 0:
                                            w.bet_price = w.up_cost / max(w.up_shares, 1e-9)
                                    elif w.bet_side == "DOWN" and w.down_shares > 0:
                                        w.bet_shares = w.down_shares
                                        if w.down_cost > 0:
                                            w.bet_price = w.down_cost / max(w.down_shares, 1e-9)
                                    else:
                                        w.bet_shares = 0.0

                                    w.status = "bet_placed"
                                    w.scalp_mode = "SCALP"
                                    if sold_any:
                                        self._log_event(
                                            f"{act} PARTIAL {w.asset} · open ↑{w.up_shares:.1f} ↓{w.down_shares:.1f}",
                                            "info",
                                        )
                                    elif self._cycle_count % 25 == 1:
                                        why = ", ".join(blocked_reasons) if blocked_reasons else "no actionable bids"
                                        print(f"    EXIT DEFERRED {act}: {why[:120]}")

                            elif act == "LOCK":
                                w.scalp_mode = "LOCK"

                    # Cleanup sniped slugs — only remove for windows whose time
                    # has fully passed (end_ts < now). Do NOT remove just because the
                    # window was closed/sold — that would allow re-entry into the
                    # same time slot.
                    now_ts = int(time.time())
                    expired = set()
                    for ss in list(self._kelly_sniped_slugs):
                        sw = self._active_windows.get(ss)
                        if sw and getattr(sw, 'end_ts', 0) > 0 and sw.end_ts < now_ts:
                            expired.add(ss)
                        elif not sw:
                            # Window no longer tracked — check closed windows
                            cw = next((c for c in self._closed_windows if c.slug == ss), None)
                            if cw and getattr(cw, 'end_ts', 0) > 0 and cw.end_ts < now_ts:
                                expired.add(ss)
                            # If not in closed_windows either, keep it sniped for safety
                    self._kelly_sniped_slugs -= expired

                await self._tg_poll()

                # 6. Display (console + web)
                # Throttle: console every 4th cycle (1s), WS broadcast every 4th cycle (1s)
                if self._cycle_count % 4 == 0:
                    self._print_dashboard()
                if self._cycle_count % 4 == 0:
                    await self._broadcast_state()

                # Record cycle time for instrumentation
                perf.record_cycle((time.monotonic() - _cycle_t0) * 1000)

                # Log perf stats every 120 cycles (~30s)
                if self._cycle_count % 120 == 0:
                    _perf_line = perf.one_liner()
                    if _perf_line:
                        print(f"  📐 PERF: {_perf_line}")

                # 7. Sleep — fast cycle for quick execution (0.25s = ~4 cycles/sec)
                await asyncio.sleep(0.25)

              except asyncio.CancelledError:
                raise  # Don't swallow cancel
              except BrokenPipeError:
                pass  # Stdout pipe closed — harmless, ignore silently
              except Exception as loop_err:
                # Per-iteration safety net — log and continue, don't crash overnight
                if isinstance(loop_err, OSError) and loop_err.errno == 32:
                    continue  # Broken pipe — ignore
                import traceback
                err_msg = str(loop_err)[:120]
                tb_short = traceback.format_exc().strip().split('\n')
                # Show last 3 lines of traceback for context
                tb_tail = ' | '.join(line.strip() for line in tb_short[-3:])[:200]
                print(f"\n  ⚠️ LOOP ERROR (cycle {self._cycle_count}): {err_msg}")
                print(f"    TB: {tb_tail}")
                self._log_event(f"LOOP ERR: {err_msg} · {tb_tail[:80]}", "error")
                await asyncio.sleep(3)  # Brief pause before retry

        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            db_queue.stop()  # Flush pending DB writes
            # Cancel WebSocket background tasks
            for t in ws_tasks:
                t.cancel()
            await asyncio.gather(*ws_tasks, return_exceptions=True)
            if self._http:
                await self._http.aclose()

    def stop(self):
        self._running = False

    # ------------------------------------------------------------------
    # Dashboard State (JSON for web UI)
    # ------------------------------------------------------------------

    def _get_cached_live_trades(self) -> list[dict]:
        """Cache live trades for 5s to avoid DB+API hits every cycle."""
        now = time.time()
        if now - self._live_trades_cache_ts > 5.0:
            self._live_trades_cache = history.get_live_trades(limit=10)
            self._live_trades_cache_ts = now
        return self._live_trades_cache

    def _get_gate_status(self) -> dict:
        """Compact trading-state summary for low-bandwidth views."""
        gate_reason = ""
        if isinstance(getattr(self, "_kelly_live", None), dict):
            gate_reason = self._kelly_live.get("gate_reason", "") or ""
        if not gate_reason:
            gate_reason = (getattr(self.engine, "_last_conviction_pipeline", {}) or {}).get("gate_reason", "") or ""

        active_market = None
        for _, w in sorted(self._active_windows.items(), key=lambda kv: kv[1].time_remaining):
            active_market = w
            break

        settle_cooldown = max(0, int(self._settle_cooldown_until - time.time()))
        if self._bot_paused:
            return {"title": "PAUSED", "detail": "Bot is manually paused", "tone": "red"}
        if self._time_gate_paused:
            return {"title": "TIME GATE", "detail": "Trading blocked by time-gate hours", "tone": "amber"}
        if settle_cooldown > 0:
            return {"title": "COOLDOWN", "detail": f"Settlement cooldown active ({settle_cooldown}s)", "tone": "amber"}
        if gate_reason:
            return {"title": "BLOCKED", "detail": gate_reason, "tone": "amber"}
        if active_market and getattr(active_market, "bet_side", ""):
            spent = float(getattr(active_market, "total_spent", 0) or 0)
            return {
                "title": "ACTIVE",
                "detail": f"{active_market.asset.upper()} {active_market.interval_label} {active_market.bet_side} live · ${spent:.2f} deployed",
                "tone": "green",
            }
        if self.dry_run:
            return {"title": "DRY RUN", "detail": "Monitoring only", "tone": "blue"}
        return {"title": "SCANNING", "detail": "Watching live markets for entry", "tone": "blue"}

    def _get_perf_summary(self) -> dict:
        """Compact perf metrics for dashboard (called every cycle)."""
        s = perf.summary()
        return {
            "cycle": s["cycle"],
            "orders": s["orders"],
            "slippage": s["slippage"],
            "dbQueue": db_queue.stats,
        }

    def _compute_entry_sizing(
        self,
        approach: str = "DIRECTIONAL",
        side: str = "",
        remaining: float | None = None,
    ) -> dict:
        """Compute live entry sizing using the same logic as the runner."""
        bankroll = float(self.engine.bankroll)
        if remaining is None:
            if self.dry_run:
                remaining = self.engine.max_bet_dollars
            else:
                open_exposure = sum(
                    float(ow.total_cost or 0)
                    for ow in self._active_windows.values()
                    if ow.bet_side and float(getattr(ow, "total_cost", 0) or 0) > 0
                )
                remaining = max(0.0, bankroll * 0.40 - open_exposure)

        wallet_mult = 1.0
        wallet_lean = None
        wallet_applied = False
        if getattr(self, "wallet_signal", None) and self.wallet_signal.enabled and approach == "DIRECTIONAL":
            _wu = float(self.wallet_signal._cached_up_shares or 0)
            _wd = float(self.wallet_signal._cached_down_shares or 0)
            _wt = _wu + _wd
            if _wt >= 15.0:
                wallet_applied = True
                if side == "UP":
                    wallet_lean = _wu / _wt
                else:
                    wallet_lean = _wd / _wt
                # Compact live mode: wallet remains a confirmation input, not a
                # bet-sizing input. Keep the lean for telemetry, but size from
                # bankroll policy only to avoid extra shrink/boost branches.
                wallet_mult = 1.0

        base_bet = float(self.engine.max_bet_dollars)
        pct_bet_cap = None
        if not self.dry_run:
            pct_bet_cap = round(bankroll * float(self.engine.max_bet_pct), 2)
            base_bet = min(base_bet, max(pct_bet_cap, 1.20))
        adjusted_bet = round(base_bet * wallet_mult, 2)

        consec_losses = int(getattr(self.engine, "_consecutive_losses", 0) or 0)
        if consec_losses == 0:
            streak_mult = 1.00
        elif consec_losses == 1:
            streak_mult = 0.85
        elif consec_losses == 2:
            streak_mult = 0.75
        elif consec_losses == 3:
            streak_mult = 0.65
        else:
            streak_mult = 0.50
        adjusted_bet = round(adjusted_bet * streak_mult, 2)

        tod_mult = 1.0
        try:
            try:
                from zoneinfo import ZoneInfo as _ZI2
                _et2 = _ZI2("America/New_York")
            except ImportError:
                from dateutil import tz as _dtz2
                _et2 = _dtz2.gettz("America/New_York")
            import datetime as _dt2
            _h_et = _dt2.datetime.now(_et2).hour
            if 1 <= _h_et < 3:
                _recent4 = self._result_ring[-4:] if len(self._result_ring) >= 4 else self._result_ring
                _recent_losses = sum(1 for _, p in _recent4 if p < 0)
                if _recent_losses >= 2:
                    tod_mult = 0.50
                    adjusted_bet = round(adjusted_bet * tod_mult, 2)
        except Exception:
            pass

        quality_score = float(getattr(self.engine, "_last_quality_score", 1.0) or 1.0)
        quality_mult = 0.50 if quality_score < 0.50 else 1.0
        deploy_pct = 1.00 if approach == "DIRECTIONAL" else 0.60
        alloc = min(float(remaining), adjusted_bet)
        initial_spend = round(alloc * quality_mult * deploy_pct, 2)

        return {
            "approach": approach,
            "bankroll": round(bankroll, 4),
            "remaining": round(float(remaining), 4),
            "base_bet": round(base_bet, 2),
            "pct_bet_cap": pct_bet_cap,
            "wallet_mult": round(wallet_mult, 3),
            "wallet_lean": round(wallet_lean, 3) if wallet_lean is not None else None,
            "wallet_applied": wallet_applied,
            "consec_losses": consec_losses,
            "streak_mult": round(streak_mult, 2),
            "tod_mult": round(tod_mult, 2),
            "quality_score": round(quality_score, 3),
            "quality_mult": round(quality_mult, 2),
            "deploy_pct": round(deploy_pct, 2),
            "alloc": round(alloc, 2),
            "initial_spend": round(initial_spend, 2),
        }

    def _get_dashboard_state(self, compact: bool = False) -> dict:
        """Build dashboard state as JSON-serialisable dict."""
        now = time.time()
        stats = self.engine.get_stats()
        total = self.wins + self.losses
        wr = (self.wins / total * 100) if total > 0 else 0

        history_points = 0 if compact else 60
        closed_limit = 4 if compact else 8
        event_log_limit = 15 if compact else 50
        balance_event_limit = 6 if compact else 10
        kelly_decision_limit = 8 if compact else 15

        def _window_to_dict(w, include_history: bool = False):
            bet_label = ""
            if w.bet_side:
                approach = getattr(w, 'approach', '')
                up_sh = getattr(w, 'up_shares', 0)
                dn_sh = getattr(w, 'down_shares', 0)
                if approach == "HEDGED" and up_sh > 0 and dn_sh > 0:
                    bet_label = f"HEDGED ↑{up_sh:.1f} ↓{dn_sh:.1f}"
                else:
                    bet_label = f"{w.bet_side} x{w.bet_shares}"
            d = {
                "slug": w.slug,
                "asset": w.asset.upper(),
                "interval": w.interval_label,
                "intervalSecs": w.interval_secs,
                "timeLeft": w.time_remaining,
                "upPrice": round(w.up_price, 4),
                "dnPrice": round(w.down_price, 4),
                "upLast": round(getattr(w, 'up_last', 0), 4),
                "dnLast": round(getattr(w, 'down_last', 0), 4),
                "upSpread": round(getattr(w, 'up_spread', 0), 4),
                "dnSpread": round(getattr(w, 'down_spread', 0), 4),
                "upBid": round(getattr(w, 'up_best_bid', 0), 4),
                "dnBid": round(getattr(w, 'down_best_bid', 0), 4),
                "depthUp": int(w.up_depth),
                "depthDn": int(w.down_depth),
                "bet": bet_label,
                "status": w.status,
                "result": w.result,
                "btcOpen": round(w.btc_at_open, 2),
                "btcClose": round(w.btc_at_close, 2),
                "volume": w.total_volume,
                "betSide": w.bet_side,
                "betPnl": round(w.pnl, 4),
                "approach": getattr(w, 'approach', ''),
                "upShares": round(getattr(w, 'up_shares', 0), 2),
                "dnShares": round(getattr(w, 'down_shares', 0), 2),
                "totalCost": round(getattr(w, 'total_cost', 0), 4),
                "realizedPnl": round(getattr(w, 'realized_pnl', 0), 4),
                "budget": round(getattr(w, 'budget', 0), 2),
                "totalSpent": round(getattr(w, 'total_spent', 0), 2),
                "scalpMode": getattr(w, 'scalp_mode', 'WAIT'),
                "tradeCount": getattr(w, 'trade_count', 0),
                "upAsk": round(getattr(w, 'up_best_ask', 1.0), 4),
                "dnAsk": round(getattr(w, 'down_best_ask', 1.0), 4),
                "upCost": round(getattr(w, 'up_cost', 0), 4),
                "dnCost": round(getattr(w, 'down_cost', 0), 4),
            }
            if include_history and history_points > 0:
                # Downsample to max 60 points for bandwidth
                ph = w.price_history
                if len(ph) > history_points:
                    step = len(ph) / history_points
                    ph = [ph[int(i * step)] for i in range(history_points)]
                d["history"] = ph
            return d

        markets = []
        for slug, w in sorted(self._active_windows.items(), key=lambda kv: kv[1].time_remaining):
            markets.append(_window_to_dict(w, include_history=not compact))

        # Event history — closed windows with full price timeline
        event_history = []
        for w in reversed(self._closed_windows[-closed_limit:]):
            event_history.append(_window_to_dict(w, include_history=not compact))

        bets = []
        # Active windows with bets — show BOTH sides for HEDGED
        for w in self._active_windows.values():
            if w.bet_side:
                approach = getattr(w, 'approach', '')
                up_sh = getattr(w, 'up_shares', 0)
                dn_sh = getattr(w, 'down_shares', 0)
                if approach == "HEDGED" and up_sh > 0:
                    bets.append({
                        "id": w.slug,
                        "asset": w.asset.upper(),
                        "interval": w.interval_label,
                        "side": "UP",
                        "price": round(getattr(w, 'up_cost', 0) / max(up_sh, 0.01), 4),
                        "shares": round(up_sh, 2),
                        "result": "PENDING",
                        "pnl": 0,
                        "approach": "HEDGED",
                        "sideCost": round(getattr(w, 'up_cost', 0), 4),
                    })
                if approach == "HEDGED" and dn_sh > 0:
                    bets.append({
                        "id": w.slug,
                        "asset": w.asset.upper(),
                        "interval": w.interval_label,
                        "side": "DOWN",
                        "price": round(getattr(w, 'down_cost', 0) / max(dn_sh, 0.01), 4),
                        "shares": round(dn_sh, 2),
                        "result": "PENDING",
                        "pnl": 0,
                        "approach": "HEDGED",
                        "sideCost": round(getattr(w, 'down_cost', 0), 4),
                    })
                if approach != "HEDGED":
                    bets.append({
                        "id": w.slug,
                        "asset": w.asset.upper(),
                        "interval": w.interval_label,
                        "side": w.bet_side,
                        "price": round(w.bet_price, 4),
                        "shares": w.bet_shares,
                        "result": "PENDING",
                        "pnl": 0,
                    })
        # Closed windows with bets
        for w in reversed(self._closed_windows):
            if w.bet_side:
                approach = getattr(w, 'approach', '')
                # Determine correct result label
                if w.result and (w.result.startswith("EXIT_") or w.result.startswith("COPY_EXIT_")):
                    if w.pnl > 0:
                        res_label = "EXIT_WIN"
                    elif w.pnl < 0:
                        res_label = "EXIT_LOSS"
                    else:
                        res_label = "EXIT"  # Breakeven
                elif w.pnl > 0:
                    res_label = "WIN"
                elif w.pnl < 0:
                    res_label = "LOSS"
                else:
                    res_label = "FLAT"
                # For HEDGED settled bets, show winning side
                _settled_side = w.bet_side
                _settled_shares = w.bet_shares
                _settled_price = round(w.bet_price, 4)
                if approach == "HEDGED" and w.result in ("UP_WON", "DOWN_WON"):
                    # Show the winning side for clarity
                    _up_sh = getattr(w, 'up_shares', 0) or 0
                    _dn_sh = getattr(w, 'down_shares', 0) or 0
                    if w.result == "UP_WON" and _up_sh > 0:
                        _settled_side = "UP"
                        _settled_shares = round(_up_sh, 2)
                        _settled_price = round((getattr(w, 'up_cost', 0) or 0) / max(_up_sh, 0.01), 4)
                    elif w.result == "DOWN_WON" and _dn_sh > 0:
                        _settled_side = "DOWN"
                        _settled_shares = round(_dn_sh, 2)
                        _settled_price = round((getattr(w, 'down_cost', 0) or 0) / max(_dn_sh, 0.01), 4)
                bets.append({
                    "id": w.slug,
                    "asset": w.asset.upper(),
                    "interval": w.interval_label,
                    "side": _settled_side,
                    "price": _settled_price,
                    "shares": _settled_shares,
                    "result": res_label,
                    "marketResult": w.result,
                    "pnl": round(w.pnl, 4),
                    "approach": approach,
                    "confidence": 0,
                    "edge": "",
                })
            if len(bets) >= (10 if compact else 15):
                break

        # Upcoming windows
        upcoming = []
        for slug, w in sorted(self._upcoming_windows.items(), key=lambda kv: kv[1].start_ts):
            upcoming.append({
                "slug": w.slug,
                "asset": w.asset.upper(),
                "interval": w.interval_label,
                "startTs": w.start_ts,
                "endTs": w.end_ts,
                "startsIn": max(0, int(w.start_ts - time.time())),
                "status": "upcoming",
            })

        # Signal values for display
        signals = {}
        if hasattr(self.engine, '_last_signals') and self.engine._last_signals:
            signals = self.engine._last_signals

        # Execution pipeline from probability engine
        pipeline = getattr(self.engine, '_last_pipeline', {})
        conv_pipeline = getattr(self.engine, '_last_conviction_pipeline', {})

        active_approach = ((self._kelly_live or {}).get("approach") if self._kelly_live else "") or "DIRECTIONAL"
        active_side = ((self._kelly_live or {}).get("side") if self._kelly_live else "") or ""
        entry_sizing = self._compute_entry_sizing(active_approach, side=active_side)
        display_bankroll = self._display_bankroll()
        owner_addr = self._owner_wallet_address()
        signer_addr = self._signer_wallet_address()
        state = {
            "btcPrice": self._btc_price,
            "priceSource": "binance_ws" if getattr(self, '_ws_binance_connected', False) else "chainlink",
            "wsBinance": getattr(self, '_ws_binance_connected', False),
            "wsPolymarket": getattr(self, '_ws_poly_connected', False),
            "wsBtcUpdates": getattr(self, '_ws_btc_updates', 0),
            "wsPolyUpdates": getattr(self, '_ws_poly_updates', 0),
            "bankroll": round(display_bankroll, 4),
            "tradeableBalance": round(self._tradeable_cash_balance(), 4),
            "simBankroll": round(self.engine.bankroll, 4),
            "initialBankroll": round(self._initial_deposit if self._initial_deposit > 0 else stats.get("initial_bankroll", self.engine.bankroll), 4),
            "initialDeposit": round(self._initial_deposit, 4),
            "pnl": round(self.total_pnl, 4),
            "totalBets": self.total_bets,
            "wins": self.wins,
            "losses": self.losses,
            "winRate": round(wr, 1),
            "drawdown": round(stats.get("drawdown_pct", 0), 1),
            "cycle": self._cycle_count,
            "ticks": stats.get("btc_ticks", 0),
            "dryRun": self.dry_run,
            "mode": self.engine.mode,
            "kellyCap": self.engine.kelly_fraction_cap,
            "minConfidence": self.engine.min_confidence,
            "liveMinConviction": round(self.rcfg.live_min_conviction, 4),
            "convictionGateLabel": f"LIVE {(self.rcfg.live_min_conviction * 100):.0f}%"
                                  if not self.dry_run
                                  else f"DRY {self.rcfg.conviction_floor_up*100:.0f}-{self.rcfg.conviction_floor_down*100:.0f}%",
            "maxBetPct": self.engine.max_bet_pct,
            "maxBetDollars": self.engine.max_bet_dollars,
            "markets": markets,
            "eventHistory": event_history,
            "bets": bets,
            "pastLiveTrades": self._get_cached_live_trades(),
            "signals": signals,
            "consecutiveLosses": stats.get("consecutive_losses", 0),
            "cooldownUntil": getattr(self.engine, '_cooldown_until', 0),
            "lastEdge": stats.get("last_edge_type", ""),
            "walletAddress": owner_addr,
            "tradingWallet": owner_addr,
            "signerWallet": signer_addr,
            "proxyAddress": self._identity.display_proxy,
            "identity": self._identity.to_dict(),
            "polBalance": round(self._pol_balance, 4),
            "usdceBalance": round(self._usdce_balance, 4),
            "nativeUsdcBalance": round(self._native_usdc_balance, 4),
            "clobBalance": round(self._clob_balance, 4),
            "proxyBalance": round(self._proxy_balance, 4),
            "redeemableValue": round(self._redeemable_value, 4),
            "upcoming": upcoming,
            "pipeline": pipeline,
            "tradeMode": self._trade_mode,
            "intervals": self.interval_labels,
            "settleCooldown": max(0, int(self._settle_cooldown_until - time.time())),
            "balanceEvents": self._balance_events[-balance_event_limit:],
            "discordWebhook": bool(self._discord_webhook),
            "botPaused": self._bot_paused,
            "timeGateEnabled": self._time_gate_enabled,
            "timeGatePaused": self._time_gate_paused,
            "timeGateHours": sorted(self._time_gate_hours),
            "walletConfirmEnabled": getattr(self, 'wallet_signal', None) and self.wallet_signal.enabled or False,
            "walletDirection": getattr(self, 'wallet_signal', None) and self.wallet_signal._cached_direction or None,
            "walletUpShares": round(getattr(self, 'wallet_signal', None) and self.wallet_signal._cached_up_shares or 0, 2),
            "walletDownShares": round(getattr(self, 'wallet_signal', None) and self.wallet_signal._cached_down_shares or 0, 2),
            "walletLean": entry_sizing["wallet_lean"],
            "walletSizeMult": entry_sizing["wallet_mult"],
            "effectiveBet": entry_sizing["alloc"],
            "entrySizing": entry_sizing,
            "upBiasHardEnabled": getattr(self.engine, '_up_bias_hard_enabled', False),
            "upBiasAutoFlip": getattr(self.engine, '_up_bias_auto_flip', False),
            "kellyAggression": self._kelly_aggression,
            "kellyDecisions": self._kelly_decisions[-kelly_decision_limit:],
            "kellyLive": self._kelly_live,  # Real-time conviction + scalp mode for current window
            "convictionEngine": {
                "conviction": self._kelly_live.get("conviction", 0) if self._kelly_live else 0,
                "side": self._kelly_live.get("side", "NONE") if self._kelly_live else "NONE",
                "p_smooth": self._kelly_live.get("p_smooth", 0.50) if self._kelly_live else 0.50,
                "q_mid": self._kelly_live.get("q_mid", 0.50) if self._kelly_live else 0.50,
                "edge": self._kelly_live.get("edge", 0) if self._kelly_live else 0,
                "signals": {
                    "z": self._kelly_live.get("z", 0) if self._kelly_live else 0,
                    "book_imb": self._kelly_live.get("book_imb", 0) if self._kelly_live else 0,
                    "lag": self._kelly_live.get("lag", 0) if self._kelly_live else 0,
                },
                "regime": self._kelly_live.get("regime", "CHOP") if self._kelly_live else "CHOP",
                "mode": self._kelly_live.get("mode", "WAIT") if self._kelly_live else "WAIT",
                "approach": self._kelly_live.get("approach", "") if self._kelly_live else "",
                "trade_count": self._kelly_live.get("trade_count", 0) if self._kelly_live else 0,
                "total_spent": self._kelly_live.get("total_spent", 0) if self._kelly_live else 0,
                "budget": self._kelly_live.get("budget", 0) if self._kelly_live else 0,
                "gbm": {
                    "z_score": pipeline.get("z_score", 0),
                    "p_up": pipeline.get("p_up_model", 0.5),
                    "p_down": pipeline.get("p_down_model", 0.5),
                    "drift": pipeline.get("cex_mu", 0),
                    "sigma": pipeline.get("cex_sigma", 0),
                    "edge_up": pipeline.get("edge_up", 0),
                    "edge_down": pipeline.get("edge_dn", 0),
                    "market_implied": pipeline.get("pm_implied", 50),
                },
                "gate_reason": (self._kelly_live.get("gate_reason", "") if self._kelly_live else conv_pipeline.get("gate_reason", "")),
                "approach_label": (self._kelly_live.get("approach", "") if self._kelly_live else conv_pipeline.get("approach", "")),
            },
            "position": {
                "up_shares": self._kelly_live.get("up_shares", 0) if self._kelly_live else 0,
                "down_shares": self._kelly_live.get("down_shares", 0) if self._kelly_live else 0,
                "time_into": self._kelly_live.get("time_into", 0) if self._kelly_live else 0,
                "time_left": self._kelly_live.get("time_left", 0) if self._kelly_live else 0,
            },
            "eventLog": self._event_log[-event_log_limit:],
            "gateStatus": self._get_gate_status(),
        }
        if compact:
            state["pastLiveTrades"] = self._get_cached_live_trades()
            return state

        state["pastLiveTrades"] = self._get_cached_live_trades()
        state["windowSignals"] = self._window_signals
        state["perf"] = self._get_perf_summary()
        state["metrics"] = dict(self._metrics)
        state["kpi"] = self._compute_kpi()
        return state

    def _get_mobile_state(self) -> dict:
        """Very small phone-friendly status payload."""
        dashboard = self._get_dashboard_state(compact=True)
        active_market = (dashboard.get("markets") or [{}])[0]
        last_event = (dashboard.get("eventLog") or [None])[-1]
        return {
            "ts": time.time(),
            "btcPrice": dashboard.get("btcPrice", 0),
            "dryRun": dashboard.get("dryRun", True),
            "bankroll": dashboard.get("bankroll", 0),
            "tradeableBalance": dashboard.get("tradeableBalance", 0),
            "pnl": dashboard.get("pnl", 0),
            "wins": dashboard.get("wins", 0),
            "losses": dashboard.get("losses", 0),
            "drawdown": dashboard.get("drawdown", 0),
            "maxBetDollars": dashboard.get("maxBetDollars", 0),
            "wsBinance": dashboard.get("wsBinance", False),
            "wsPolymarket": dashboard.get("wsPolymarket", False),
            "botPaused": dashboard.get("botPaused", False),
            "timeGatePaused": dashboard.get("timeGatePaused", False),
            "gateStatus": dashboard.get("gateStatus", {}),
            "market": {
                "slug": active_market.get("slug", ""),
                "asset": active_market.get("asset", ""),
                "interval": active_market.get("interval", ""),
                "timeLeft": active_market.get("timeLeft", 0),
                "status": active_market.get("status", ""),
                "betSide": active_market.get("betSide", ""),
                "upPrice": active_market.get("upPrice", 0),
                "dnPrice": active_market.get("dnPrice", 0),
                "upBid": active_market.get("upBid", 0),
                "dnBid": active_market.get("dnBid", 0),
                "upAsk": active_market.get("upAsk", 0),
                "dnAsk": active_market.get("dnAsk", 0),
                "volume": active_market.get("volume", 0),
                "btcOpen": active_market.get("btcOpen", 0),
                "upShares": active_market.get("upShares", 0),
                "dnShares": active_market.get("dnShares", 0),
                "totalSpent": active_market.get("totalSpent", 0),
            },
            "convictionEngine": dashboard.get("convictionEngine", {}),
            "balances": {
                "usdce": dashboard.get("usdceBalance", 0),
                "pol": dashboard.get("polBalance", 0),
                "clob": dashboard.get("clobBalance", 0),
                "redeemable": dashboard.get("redeemableValue", 0),
            },
            "lastEvent": last_event,
        }

    def _compute_kpi(self) -> dict:
        """Compute KPI metrics for cut quality monitoring."""
        sell_pnl = self._metrics.get("kpi_sell_pnl", 0.0)
        settle_pnl = self._metrics.get("kpi_settle_pnl", 0.0)
        exit_prices = self._metrics.get("kpi_exit_prices", [])
        deep_cuts = self._metrics.get("kpi_deep_cuts", 0)

        # sell_pnl / settle_pnl ratio (target: < 0.35)
        ratio = abs(sell_pnl / settle_pnl) if settle_pnl > 0 else 0.0

        # Median exit price (target: > 0.10)
        median_exit = 0.0
        if exit_prices:
            _sorted = sorted(exit_prices)
            _mid = len(_sorted) // 2
            median_exit = (_sorted[_mid] if len(_sorted) % 2
                          else (_sorted[_mid - 1] + _sorted[_mid]) / 2)

        return {
            "sell_settle_ratio": round(ratio, 3),
            "median_exit_price": round(median_exit, 3),
            "deep_cuts": deep_cuts,
            "sell_pnl": round(sell_pnl, 2),
            "settle_pnl": round(settle_pnl, 2),
            "n_cuts": len(exit_prices),
        }

    # ------------------------------------------------------------------
    # Embedded Web Server (aiohttp)
    # ------------------------------------------------------------------

    _ws_clients: list = []

    async def _broadcast_state(self):
        """Push dashboard state to all connected WebSocket clients."""
        if not self._ws_clients:
            return
        data = json.dumps({"type": "dashboard_update", "data": self._get_dashboard_state(compact=True)})
        dead = []
        for ws in self._ws_clients:
            try:
                await ws.send_str(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            try:
                self._ws_clients.remove(ws)
            except ValueError:
                pass  # already removed by ws_handler

    async def _ws_handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self._ws_clients.append(ws)

        # Send initial state
        try:
            await ws.send_str(json.dumps({
                "type": "full_state",
                "data": self._get_dashboard_state(),
            }))
        except Exception:
            pass

        try:
            async for msg in ws:
                pass  # Client messages (future: config changes)
        finally:
            if ws in self._ws_clients:
                self._ws_clients.remove(ws)
        return ws

    async def _api_status(self, request):
        return web.json_response(self._get_dashboard_state())

    async def _api_status_compact(self, request):
        return web.json_response(self._get_dashboard_state(compact=True))

    async def _api_status_lite(self, request):
        return web.json_response(self._get_mobile_state())

    async def _api_refresh_balance(self, request):
        """Force refresh on-chain + CLOB balances."""
        try:
            owner_addr = self._owner_wallet_address()
            signer_addr = self._signer_wallet_address()
            balances = await asyncio.to_thread(
                config.get_all_balances,
                owner_addr,
                signer_addr,
            )
            self._pol_balance = balances["pol"]
            self._usdce_balance = balances.get("usdce", 0)
            self._native_usdc_balance = balances.get("nativeUsdc", 0)
            self._proxy_balance = balances.get("proxyUsdce", 0)
            # Fetch CLOB balance (deposited on Polymarket exchange)
            clob = 0.0
            try:
                if self._poly_client:
                    clob = await asyncio.to_thread(self._poly_client.get_clob_balance)
                    self._clob_balance = clob
            except Exception:
                pass
            self._last_balance_refresh = time.time()
            return web.json_response({
                "usdc": balances["usdc"],
                "usdce": balances.get("usdce", 0),
                "nativeUsdc": balances.get("nativeUsdc", 0),
                "proxyUsdce": balances.get("proxyUsdce", 0),
                "clob": clob,
                "pol": balances["pol"],
                "address": balances["address"],
                "gasAddress": balances.get("gasAddress", signer_addr),
                "proxyAddress": balances.get("proxyAddress", ""),
                "bankroll": round(self._display_bankroll(), 4),
                "tradeableBalance": round(self._tradeable_cash_balance(), 4),
                "simBankroll": round(self.engine.bankroll, 4),
                "liveMinConviction": round(self.rcfg.live_min_conviction, 4),
                "pnl": round(self.total_pnl, 4),
                "redeemableValue": round(self._redeemable_value, 4),
                "initialDeposit": round(self._initial_deposit, 4),
                "ok": True,
            })
        except Exception as e:
            return web.json_response({"usdc": 0, "pol": 0, "clob": 0, "nativeUsdc": 0, "ok": False, "error": str(e)})

    async def _api_set_trade_mode(self, request):
        """Switch between probability and copy mode."""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        mode = data.get("mode", "kelly")
        if mode == "kelly":
            self._trade_mode = "kelly"
            self._kelly_sniped_slugs.clear()
            kp = self._kelly_params()
            self._log_event(
                f"KELLY MODE · aggression {self._kelly_aggression}/5 · entry@{kp['entry_time']}s · edge>{kp['min_edge']:.0%}",
                "info",
            )
        else:
            return web.json_response({"ok": False, "error": f"Unknown mode: {mode}. Use 'kelly'."})
        self._save_persisted_state()  # Persist across restarts
        return web.json_response({"mode": self._trade_mode, "ok": True})

    async def _api_go_live(self, request):
        """Switch from DRY RUN to LIVE mode. Requires explicit user action."""
        if not self.dry_run:
            return web.json_response({"ok": True, "msg": "Already LIVE", "dryRun": False})

        # Check balance before switching
        try:
            owner_addr = self._owner_wallet_address()
            signer_addr = self._signer_wallet_address()
            new_bal = config.get_usdce_balance(owner_addr)
            pol = config.get_pol_balance(signer_addr)
            self._usdce_balance = new_bal
            self._pol_balance = pol
        except Exception as e:
            return web.json_response({"ok": False, "error": f"Balance check failed: {e}", "dryRun": True})

        # Check CLOB balance too (funds deposited on Polymarket exchange)
        clob = 0.0
        if self._poly_client:
            try:
                clob = self._poly_client.get_clob_balance()
                self._clob_balance = clob
            except Exception:
                pass

        # Use the HIGHER of CLOB or USDC.e as bankroll.
        # CLOB = funds deposited on Polymarket exchange (tradeable).
        # USDC.e = on-chain token balance (includes CLOB deposit).
        # They often match; using max() avoids double-counting.
        total_available = max(clob, new_bal)
        if total_available < 0.50:
            return web.json_response({
                "ok": False,
                "error": f"Insufficient balance: CLOB=${clob:.2f}, USDC.e=${new_bal:.2f}. Deposit funds first.",
                "dryRun": True, "balance": round(total_available, 4),
            })
        if pol < 0.005:
            return web.json_response({
                "ok": False,
                "error": f"Insufficient POL for gas ({pol:.4f}). Need at least 0.005 POL.",
                "dryRun": True,
            })

        # Init the trading client
        if not self._poly_client:
            try:
                self._poly_client = create_poly_client(config)
            except Exception as e:
                return web.json_response({
                    "ok": False, "error": f"Client init failed: {e}", "dryRun": True,
                })

        # Verify CLOB client can actually place orders (py_clob_client required)
        try:
            self._poly_client.set_allowances()
        except Exception as e:
            err_msg = str(e)
            if "py_clob_client" in err_msg or "No module" in err_msg:
                return web.json_response({
                    "ok": False,
                    "error": "py_clob_client not installed — cannot trade LIVE. Install with: pip3 install py-clob-client",
                    "dryRun": True,
                })
            # Other errors are OK (network timeout etc)
        try:
            self._clob_balance = self._poly_client.get_clob_balance()
        except Exception:
            pass

        # Switch to LIVE
        self.dry_run = False
        self.engine.bankroll = total_available
        # Reset stop-loss tracking for fresh session
        self.engine.initial_bankroll = total_available
        self.engine.reset_halt()
        self._save_persisted_state()  # Persist live mode across restarts
        # Create sentinel file — survives state file overwrites
        _live_sentinel = Path(__file__).parent.parent / ".live_mode"
        _live_sentinel.touch()

        # Track initial deposit for balance-based PNL (only set once — first GO LIVE)
        if self._initial_deposit <= 0:
            self._initial_deposit = total_available
            print(f"  Initial deposit recorded: ${self._initial_deposit:.2f}")
            self._save_persisted_state()

        # One-time: ensure exchange contracts are approved for selling tokens
        print("  Checking exchange approvals for SELL orders...")
        if self._poly_client:
            try:
                ok = self._poly_client.ensure_exchange_approvals()
                print(f"  Exchange approvals: {'OK' if ok else 'FAILED'}")
            except Exception as e:
                print(f"  Exchange approval error: {str(e)[:120]}")
        else:
            print("  ⚠ No poly_client — skipping exchange approvals")
        # Preserve session stats (PNL, wins, losses, total_bets) across DRY→LIVE switch
        # Clear dry-run bets that have no on-chain backing (including DRY_BUY_* IDs)
        for slug, w in self._active_windows.items():
            _is_dry_pos = (not w.bet_order_id or
                           str(w.bet_order_id).startswith("DRY_"))
            if w.bet_side and _is_dry_pos:
                print(f"  Clearing dry-run bet: {w.bet_side} on {slug[-25:]} (oid={w.bet_order_id})")
                w.bet_side = ""
                w.bet_shares = 0
                w.bet_price = 0.0
                w.total_cost = 0.0
                w.bet_order_id = None
                w.status = ""
        # Clear redeem cache — fresh start for live mode, don't skip real positions
        self._redeemed_conditions.clear()
        self._last_redeem_check = 0.0
        self._last_balance_refresh = time.time()
        print(f"\n  {'*'*55}")
        print(f"  USER CONFIRMED: SWITCHING TO LIVE MODE")
        print(f"  Bankroll: ${total_available:.2f}  |  USDC.e: ${new_bal:.2f}  |  CLOB: ${clob:.2f}  |  Gas: {pol:.4f} POL")
        print(f"  {'*'*55}\n")

        # Discord alert
        if self._discord_webhook:
            _discord_post(self._discord_webhook, embeds=[{
                "title": "🟢 LIVE MODE ACTIVATED",
                "color": 0x00ff88,
                "fields": [
                    {"name": "Bankroll", "value": f"${total_available:.2f}", "inline": True},
                    {"name": "Mode", "value": self._trade_mode.title(), "inline": True},
                    {"name": "Max Bet", "value": f"${self.engine.max_bet_dollars:.2f}", "inline": True},
                ],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }])

        return web.json_response({
            "ok": True,
            "msg": f"LIVE MODE ACTIVE — ${total_available:.2f} bankroll",
            "dryRun": False,
            "balance": round(total_available, 4),
        })

    async def _api_go_dry(self, request):
        """Switch from LIVE to DRY RUN mode."""
        if self.dry_run:
            return web.json_response({"ok": True, "msg": "Already DRY RUN", "dryRun": True})

        self.dry_run = True
        # FIX: cancel ALL open exchange orders before going dry.
        # Prevents fills after mode change ("hidden drains").
        if self._poly_client:
            try:
                self._poly_client.cancel_all_orders()
                print(f"  Cancelled all open orders (go_dry safety)")
            except Exception as _ce:
                print(f"  Cancel orders on go_dry failed: {str(_ce)[:60]}")
        # Reset halt so dry-run starts collecting data immediately
        if getattr(self.engine, '_halted', False):
            self.engine.reset_halt()
        # Remove sentinel file so restarts stay in dry run
        _live_sentinel = Path(__file__).parent.parent / ".live_mode"
        _live_sentinel.unlink(missing_ok=True)
        self._save_persisted_state()
        print(f"\n  USER REQUESTED: Switching to DRY RUN\n")
        return web.json_response({"ok": True, "msg": "DRY RUN active, orders cancelled, halt reset", "dryRun": True})

    async def _api_set_intervals(self, request):
        """Set which intervals the bot trades on.

        POST /api/intervals { intervals: "5m" | "15m" | "5m,15m" }
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        raw = data.get("intervals", "5m,15m")
        valid = {"5m", "15m"}
        # Accept both "5m,15m" string and ["5m","15m"] list
        if isinstance(raw, list):
            chosen = [x.strip() for x in raw if x.strip() in valid]
        else:
            chosen = [x.strip() for x in raw.split(",") if x.strip() in valid]
        if not chosen:
            return web.json_response({"ok": False, "error": "Must include at least one of: 5m, 15m"})

        self.interval_labels = chosen
        self._save_persisted_state()
        print(f"  Intervals changed → {','.join(chosen)}")
        return web.json_response({"ok": True, "intervals": chosen})

    async def _api_set_max_bet(self, request):
        """Set the hard dollar cap per bet.

        POST /api/max_bet { amount: 1.50 }
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        amount = float(data.get("amount", 1.0))
        if amount < 0.10:
            return web.json_response({"ok": False, "error": "Min $0.10"})
        if amount > 100.0:
            return web.json_response({"ok": False, "error": "Max $100.00 hard limit"})

        self.engine.max_bet_dollars = amount
        self._save_persisted_state()
        print(f"  Max bet changed → ${amount:.2f}")
        return web.json_response({"ok": True, "maxBetDollars": amount})

    async def _api_set_discord_webhook(self, request):
        """Set Discord webhook URL for alerts.

        POST /api/discord_webhook { url: "https://discord.com/api/webhooks/..." }
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        url = str(data.get("url", "")).strip()
        if url and not url.startswith("https://discord.com/api/webhooks/"):
            return web.json_response({"ok": False, "error": "Invalid Discord webhook URL"}, status=400)

        self._discord_webhook = url
        self._save_persisted_state()
        status = "enabled" if url else "disabled"
        print(f"  Discord webhook {status}")
        return web.json_response({"ok": True, "status": status})

    async def _api_bot_pause(self, request):
        """Pause/resume bot trading. Stops all auto-trades while paused.

        POST /api/bot_pause { action: "pause" | "resume" | "toggle" }
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        action = data.get("action", "toggle")
        if action == "pause":
            self._bot_paused = True
        elif action == "resume":
            self._bot_paused = False
        else:
            self._bot_paused = not self._bot_paused

        status = "PAUSED" if self._bot_paused else "ACTIVE"
        self._log_event(f"BOT {status}", "info")
        print(f"\n  {'⏸️' if self._bot_paused else '▶️'} Bot {status.lower()}")

        # Discord notification (live mode only)
        if self._discord_webhook and not self.dry_run:
            emoji = "⏸️" if self._bot_paused else "▶️"
            _discord_post(self._discord_webhook, content=f"{emoji} **Bot {status.lower()}**")

        return web.json_response({"ok": True, "paused": self._bot_paused, "status": status.lower()})

    async def _api_unhalt(self, request):
        """Reset session stop-loss halt. Rebases peak to current bankroll.

        POST /api/unhalt
        """
        was_halted = getattr(self.engine, '_halted', False)
        if was_halted:
            self.engine.reset_halt()
            self._bot_paused = False
            self._log_event("HALT RESET (via API)", "info")
            self._save_persisted_state()  # persist new peak across restarts
            print(f"\n  🔄 Session stop-loss RESET — peak rebased to ${self.engine.bankroll:.2f}")
            return web.json_response({
                "ok": True, "was_halted": True,
                "new_peak": round(self.engine.bankroll, 2),
            })
        return web.json_response({"ok": True, "was_halted": False, "msg": "not halted"})

    async def _api_time_gate(self, request):
        """Toggle time-of-day gate on/off.

        POST /api/time_gate { action: "enable" | "disable" | "toggle" }
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        action = data.get("action", "toggle")
        if action == "enable":
            self._time_gate_enabled = True
        elif action == "disable":
            self._time_gate_enabled = False
        else:
            self._time_gate_enabled = not self._time_gate_enabled
        self.rcfg.time_gate_enabled = self._time_gate_enabled

        self._save_persisted_state()
        status = "ENABLED" if self._time_gate_enabled else "DISABLED"
        self._log_event(f"Time gate {status}", "info")
        print(f"\n  ⏰ Time gate {status}")
        return web.json_response({"ok": True, "enabled": self._time_gate_enabled})

    async def _api_time_gate_hours(self, request):
        """Get or set allowed trading hours (ET).

        POST /api/time_gate_hours { hours: [22, 23, 3, 4, 5, 6, 7, 8] }
        GET  /api/time_gate_hours  (returns current hours)
        """
        if request.method == "GET" or request.content_length == 0:
            return web.json_response({
                "ok": True,
                "hours": sorted(self._time_gate_hours),
                "enabled": self._time_gate_enabled,
            })
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        hours = data.get("hours", [])
        if not isinstance(hours, list):
            return web.json_response({"ok": False, "error": "hours must be a list of ints 0-23"}, status=400)
        hours = [int(h) for h in hours if 0 <= int(h) < 24]
        if not hours:
            return web.json_response({"ok": False, "error": "no valid hours"}, status=400)

        self._time_gate_hours = sorted(set(hours))
        self._save_persisted_state()
        _hrs = ",".join(str(h) for h in self._time_gate_hours)
        print(f"\n  ⏰ Time gate hours set: [{_hrs}] ET")
        return web.json_response({"ok": True, "hours": self._time_gate_hours})

    async def _api_wallet_confirm(self, request):
        """Toggle wallet confirmation signal on/off.

        POST /api/wallet_confirm { action: "enable" | "disable" | "toggle" }
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        action = data.get("action", "toggle")
        if action == "enable":
            self.wallet_signal.enabled = True
        elif action == "disable":
            self.wallet_signal.enabled = False
        else:
            self.wallet_signal.enabled = not self.wallet_signal.enabled
        self.rcfg.wallet_confirm_enabled = self.wallet_signal.enabled
        # When disabled, clear stale wallet state from dashboard
        if not self.wallet_signal.enabled:
            self._last_wallet_mult = 1.0
            self._last_wallet_lean = None
            self._last_wallet_dir = None

        self._save_persisted_state()
        status = "ON" if self.wallet_signal.enabled else "OFF"
        self._log_event(f"Wallet confirmation {status}", "info")
        print(f"\n  Wallet confirmation {status}")
        return web.json_response({"ok": True, "enabled": self.wallet_signal.enabled})

    def _kelly_params(self) -> dict:
        """Derive Kelly parameters from aggression level (1-5).
        Tuned for zero-fee Polymarket: fast entry, tight profit/loss gaps.
        Higher aggression = enter earlier + lower edge threshold + bigger size.
        """
        a = self._kelly_aggression
        return {
            # Entry time: seconds into window before Kelly can trade
            # HFT: enter FAST, scalp 3-5%, repeat
            "entry_time": {1: 25, 2: 15, 3: 12, 4: 3, 5: 2}.get(a, 12),
            "min_edge":   {1: 0.05, 2: 0.03, 3: 0.02, 4: 0.005, 5: 0.003}.get(a, 0.02),
            "kelly_mult": {1: 0.50, 2: 0.60, 3: 0.70, 4: 0.90, 5: 0.95}.get(a, 0.70),
            "mid_mult":   {1: 0.0, 2: 0.0, 3: 0.6, 4: 0.8, 5: 1.0}.get(a, 0.6),
        }

    async def _api_kelly_settings(self, request):
        """Adjust Kelly aggressiveness.

        POST /api/kelly_settings { aggression: 3 }
        Range 1 (conservative) to 5 (aggressive).
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        if "aggression" in data:
            val = int(data["aggression"])
            val = max(1, min(val, 5))
            self._kelly_aggression = val
            params = self._kelly_params()
            self._log_event(
                f"KELLY AGGRESSION: {val}/5 · entry@{params['entry_time']}s · edge>{params['min_edge']:.0%} · kelly×{params['kelly_mult']:.0%}",
                "info",
            )
            self._save_persisted_state()

        params = self._kelly_params()
        return web.json_response({
            "ok": True,
            "aggression": self._kelly_aggression,
            "entryTime": params["entry_time"],
            "minEdge": params["min_edge"],
            "kellyMult": params["kelly_mult"],
        })

    async def _api_test_discord(self, request):
        """Send a test message to Discord.

        POST /api/test_discord
        """
        if not self._discord_webhook:
            return web.json_response({"ok": False, "error": "No webhook URL set"}, status=400)

        mode = "LIVE" if not self.dry_run else "DRY RUN"
        total = self.wins + self.losses
        wr = f"{self.wins/total:.0%}" if total > 0 else "N/A"
        embed = {
            "title": "🤖 PolyBot Connected!",
            "color": 0x4d7cfe,
            "fields": [
                {"name": "Mode", "value": mode, "inline": True},
                {"name": "Balance", "value": f"${self.engine.bankroll:.2f}", "inline": True},
                {"name": "Record", "value": f"{self.wins}W-{self.losses}L ({wr})", "inline": True},
                {"name": "Total P&L", "value": f"{'+'if self.total_pnl>=0 else ''}${self.total_pnl:.4f}", "inline": True},
                {"name": "Trade Mode", "value": self._trade_mode.title(), "inline": True},
                {"name": "Intervals", "value": ",".join(self.interval_labels), "inline": True},
            ],
            "footer": {"text": "PolyBot • Test Alert"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        ok = _discord_post(self._discord_webhook, embeds=[embed])
        if ok:
            return web.json_response({"ok": True, "msg": "Test sent!"})
        else:
            return web.json_response({"ok": False, "error": "Failed to send — check webhook URL"}, status=500)

    async def _api_manual_trade(self, request):
        """Place a manual trade on a specific market.

        POST /api/trade { slug, side, amount }
        side: "UP" or "DOWN"
        amount: USDC to spend (e.g. 1.0)
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        slug = data.get("slug", "")
        side = data.get("side", "").upper()
        amount = float(data.get("amount", 0))

        if side not in ("UP", "DOWN"):
            return web.json_response({"ok": False, "error": "side must be UP or DOWN"})
        if amount <= 0 or amount > self.engine.bankroll:
            return web.json_response({"ok": False, "error": f"invalid amount (max ${self.engine.bankroll:.2f})"})

        # Find the market window
        w = self._active_windows.get(slug)
        if not w:
            return web.json_response({"ok": False, "error": f"market {slug} not found or not active"})
        if w.bet_side:
            return web.json_response({"ok": False, "error": "already have a bet on this market"})
        if w.time_remaining < 20:
            return web.json_response({"ok": False, "error": "too close to expiry"})

        token_id = w.up_token if side == "UP" else w.down_token
        buy_price = w.up_price if side == "UP" else w.down_price

        if buy_price <= 0 or buy_price >= 1.0:
            return web.json_response({"ok": False, "error": f"invalid price {buy_price}"})

        shares = round(amount / buy_price, 2)
        if shares < 0.1:
            return web.json_response({"ok": False, "error": f"need at least 0.1 shares (price ${buy_price:.2f})"})

        cost = round(shares * buy_price, 4)
        order_id = None

        # === LIVE EXECUTION WITH ROBUST AUTO-SLIPPAGE ===
        if not self.dry_run and self._poly_client:
            order_id = await asyncio.to_thread(
                self._execute_order,
                token_id=token_id, side="BUY", shares=shares,
                price=buy_price, neg_risk=w.neg_risk, end_ts=w.end_ts,
                spend_dollars=cost,
            )
            if not order_id:
                return web.json_response({"ok": False, "error": "order failed - no fill after retries"})

        # Record the bet
        w.bet_side = side
        w.bet_shares = shares
        w.bet_price = buy_price
        w.bet_ts = time.time()
        w.status = "bet_placed"
        self.total_bets += 1

        mode_str = "LIVE" if not self.dry_run else "DRY"
        fill_info = f" (order:{order_id[:12]})" if order_id else ""
        print(f"\n  MANUAL BET [{mode_str}]{fill_info} {w.asset.upper()} {w.interval_label} → {side}")
        print(f"  {shares} shares @ ${buy_price:.4f} = ${cost:.4f}")

        # Log to history
        db_queue.add_trade({
            "market": w.slug, "up_price": w.up_price, "down_price": w.down_price,
            "total_cost": w.total_cost, "profit_pct": 0, "shares": shares,
            "investment": cost, "expected_profit": 0, "dry_run": self.dry_run,
            "asset": w.asset,
            "extra": {"side": side, "manual": True, "order_id": order_id},
        })

        return web.json_response({
            "ok": True,
            "mode": mode_str,
            "side": side,
            "shares": shares,
            "price": round(buy_price, 4),
            "cost": round(cost, 4),
            "slug": slug,
            "orderId": order_id,
        })

    async def _api_withdraw(self, request):
        """Withdraw USDC.e or POL from the bot wallet to an external address.

        POST /api/withdraw { to, amount, token }
        token: "usdce" or "pol"
        to: 0x destination address
        amount: number (USDC.e in dollars, POL in units)
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        to_addr = data.get("to", "").strip()
        amount = float(data.get("amount", 0))
        token = data.get("token", "usdce").lower()

        if not to_addr or len(to_addr) != 42 or not to_addr.startswith("0x"):
            return web.json_response({"ok": False, "error": "Invalid destination address"})
        if amount <= 0:
            return web.json_response({"ok": False, "error": "Amount must be > 0"})
        if token not in ("usdce", "usdc", "pol"):
            return web.json_response({"ok": False, "error": "Token must be 'usdce', 'usdc', or 'pol'"})

        # Safety: require private key
        if not config.private_key:
            return web.json_response({"ok": False, "error": "No private key configured"})

        try:
            from eth_account import Account
            from eth_account.signers.local import LocalAccount
            import httpx as hx

            account: LocalAccount = Account.from_key(config.private_key)
            # Use reliable RPC — polygon-rpc.com disabled their free tier
            rpcs = ["https://polygon-bor-rpc.publicnode.com", "https://polygon.drpc.org"]
            rpc = rpcs[0]
            for r in rpcs:
                try:
                    test = hx.post(r, json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}, timeout=5.0)
                    if "result" in test.json():
                        rpc = r
                        break
                except Exception:
                    continue

            # Get nonce
            nonce_resp = hx.post(rpc, json={
                "jsonrpc": "2.0", "method": "eth_getTransactionCount",
                "params": [account.address, "latest"], "id": 1,
            }, timeout=10.0)
            nonce = int(nonce_resp.json()["result"], 16)

            # Get gas price
            gas_resp = hx.post(rpc, json={
                "jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1,
            }, timeout=10.0)
            gas_price = int(gas_resp.json()["result"], 16)
            # Use 50% above current gas price for reliable confirmation
            gas_price = int(gas_price * 1.5)

            if token == "pol":
                # Native POL transfer
                value_wei = int(amount * 1e18)
                tx = {
                    "nonce": nonce,
                    "gasPrice": gas_price,
                    "gas": 21000,
                    "to": to_addr,
                    "value": value_wei,
                    "data": b"",
                    "chainId": 137,
                }
            else:
                # ERC20 transfer — pick contract by token type
                if token == "usdc":
                    contract = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # Native USDC
                else:
                    contract = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e (bridged)
                value_units = int(amount * 1e6)  # Both USDC variants have 6 decimals

                # ERC20 transfer(address,uint256) function selector + encoded args
                transfer_data = (
                    "0xa9059cbb"
                    + to_addr[2:].lower().zfill(64)
                    + hex(value_units)[2:].zfill(64)
                )
                tx = {
                    "nonce": nonce,
                    "gasPrice": gas_price,
                    "gas": 100000,  # ERC20 transfer gas limit (USDC.e proxy can use ~60-80k)
                    "to": contract,
                    "value": 0,
                    "data": transfer_data,
                    "chainId": 137,
                }

            # Sign and send
            signed = account.sign_transaction(tx)
            raw_tx = signed.raw_transaction.hex()
            if not raw_tx.startswith("0x"):
                raw_tx = "0x" + raw_tx

            send_resp = hx.post(rpc, json={
                "jsonrpc": "2.0", "method": "eth_sendRawTransaction",
                "params": [raw_tx], "id": 1,
            }, timeout=15.0)
            result = send_resp.json()

            if "error" in result:
                err_msg = result["error"].get("message", str(result["error"]))
                return web.json_response({"ok": False, "error": f"TX failed: {err_msg}"})

            tx_hash = result.get("result", "")
            token_label = {"pol": "POL", "usdc": "USDC", "usdce": "USDC.e"}[token]
            print(f"\n  WITHDRAW: {amount} {token_label} → {to_addr[:10]}...")
            print(f"  TX: {tx_hash}")

            return web.json_response({
                "ok": True,
                "txHash": tx_hash,
                "token": token_label,
                "amount": amount,
                "to": to_addr,
                "explorer": f"https://polygonscan.com/tx/{tx_hash}",
            })

        except ImportError:
            return web.json_response({"ok": False, "error": "eth_account not installed"})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)[:200]})

    async def _api_exit_position(self, request):
        """Exit/sell an open position on a specific market.

        POST /api/exit { slug }
        Sells (counter-bets) the position at current market price.
        In dry-run: simulates the exit instantly at current mid price.
        In live: places a sell order (opposite side) on the CLOB.
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        slug = data.get("slug", "")
        sell_side = data.get("side", "")  # "UP", "DOWN", or "" (sell bet_side)

        # Find the market window with an active bet
        w = self._active_windows.get(slug)
        if not w:
            return web.json_response({"ok": False, "error": f"market {slug} not found"})
        if not w.bet_side:
            return web.json_response({"ok": False, "error": "no active position on this market"})

        # Default: sell specified side. "BOTH" sells both sides.
        if not sell_side:
            sell_side = w.bet_side
        sell_both = sell_side.upper() == "BOTH"
        sides_to_sell = ["UP", "DOWN"] if sell_both else [sell_side]

        mode_str = "DRY" if self.dry_run else "LIVE"
        total_pnl = 0.0
        total_proceeds = 0.0
        total_shares_sold = 0.0
        sold_sides = []

        for _side in sides_to_sell:
            _shares = float(w.up_shares if _side == "UP" else w.down_shares)
            _bid = w.up_best_bid if _side == "UP" else w.down_best_bid
            _bid = _bid if _bid > 0.01 else (w.up_price if _side == "UP" else w.down_price)
            if _shares < 0.5 or _bid <= 0:
                continue
            r = await self._sell_hedged(
                w, _side, _shares, _bid,
                reason="API-EXIT", retry=True,
            )
            if r["ok"]:
                total_pnl += r["pnl"]
                total_proceeds += r["proceeds"]
                total_shares_sold += r["fill_shares"]
                sold_sides.append(_side)

        if not sold_sides:
            return web.json_response({"ok": False, "error": "no shares sold"})

        # Check if fully flat
        both_empty = w.up_shares < 0.5 and w.down_shares < 0.5
        self._kelly_sniped_slugs.add(slug)
        if both_empty:
            exit_pnl = float(getattr(w, 'realized_pnl', 0.0))
            w.result = f"EXIT_{'+'.join(sold_sides)}"
            w.status = "closed"
            w.btc_at_close = self._btc_price
            w.bet_side = ""
            self.total_pnl += exit_pnl
            if abs(exit_pnl) > 1e-9:
                self.engine.record_result(exit_pnl > 0, exit_pnl)
                if exit_pnl > 0:
                    self.wins += 1
                else:
                    self.losses += 1
            self._active_windows.pop(slug, None)
            self._closed_windows.append(w)
        else:
            w.bet_shares = w.up_shares if w.bet_side == "UP" else w.down_shares

        self._save_persisted_state()

        close_str = " [CLOSED]" if both_empty else " [PARTIAL]"
        win_icon = "🟢" if total_pnl >= 0 else "🔴"
        print(f"\n  {win_icon} API-EXIT [{mode_str}] {'+'.join(sold_sides)} {w.asset.upper()}{close_str} P&L ${total_pnl:+.2f}")

        return web.json_response({
            "ok": True,
            "mode": mode_str,
            "side": "+".join(sold_sides),
            "shares": round(total_shares_sold, 2),
            "pnl": round(total_pnl, 4),
            "slug": slug,
            "closed": both_empty,
            "upShares": round(getattr(w, 'up_shares', 0), 2),
            "dnShares": round(getattr(w, 'down_shares', 0), 2),
        })

    async def _index_handler(self, request):
        html_path = DASHBOARD_DIR / "index.html"
        if html_path.exists():
            return web.FileResponse(html_path)
        return web.Response(text="Dashboard not found", status=404)

    async def _mobile_handler(self, request):
        html_path = DASHBOARD_DIR / "mobile.html"
        if html_path.exists():
            return web.FileResponse(html_path)
        return web.Response(text="Mobile dashboard not found", status=404)

    async def _api_redeem_winnings(self, request):
        """Manually trigger redemption of all winning positions.

        POST /api/redeem
        Redeems settled winning shares back to USDC on Polymarket exchange.
        """
        if not self._poly_client:
            return web.json_response({"ok": False, "error": "No Polymarket client"})
        if self.dry_run:
            return web.json_response({"ok": False, "error": "Cannot redeem in DRY RUN mode (burns real gas). Switch to LIVE first."})
        try:
            wallet = self._owner_wallet_address()
            positions = self._poly_client.get_redeemable_positions(wallet)
            if not positions:
                return web.json_response({"ok": True, "msg": "No winning positions to redeem", "redeemed": 0})
            total_val = sum(float(p.get("size", 0)) for p in positions)
            print(f"\n  MANUAL REDEEM: {len(positions)} winning positions (~${total_val:.2f})")
            redeem_results = self._poly_client.redeem_positions(wallet, self._identity)
            result = self._poly_client.summarize_redeem_results(redeem_results)
            # Refresh balance — wait for on-chain propagation
            await asyncio.sleep(5)
            try:
                new_bal = config.get_usdce_balance(wallet)
                self._usdce_balance = new_bal
                clob = self._poly_client.get_clob_balance()
                self._clob_balance = clob
                tradeable = clob if clob > 0 else new_bal
                if tradeable > 0 and not self.dry_run:
                    self.engine.bankroll = tradeable
                    # Don't reset initial_bankroll on redeem — it should stay = deposit
                print(f"  Balance after redeem: USDC.e ${new_bal:.2f} | CLOB ${clob:.2f} → Bankroll ${tradeable:.2f}")
            except Exception as bal_err:
                print(f"  Balance refresh after manual redeem failed: {bal_err}")
            self._save_persisted_state()
            # Discord alert for manual redeem (live mode only)
            if self._discord_webhook and not self.dry_run and result["redeemed"] > 0:
                _discord_post(self._discord_webhook,
                    content=f"\U0001f4b0 **Manual Redeem: {result['redeemed']} positions** (~${result['value']:.2f}) \u2192 Balance: ${self.engine.bankroll:.2f}"
                )
            return web.json_response({
                "ok": True,
                "redeemed": result["redeemed"],
                "value": result["value"],
                "errors": result["errors"],
                "skipped": result["skipped"],
                "results": result["results"],
                "canSelfRedeem": self._identity.can_self_redeem,
                "clobBalance": round(self._clob_balance, 4),
                "usdceBalance": round(self._usdce_balance, 4),
                "bankroll": round(self._display_bankroll(), 4),
                "tradeableBalance": round(self._tradeable_cash_balance(), 4),
                "simBankroll": round(self.engine.bankroll, 4),
            })
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)[:100]})

    async def _api_perf(self, request):
        """GET /api/perf — return latency + slippage instrumentation data."""
        data = perf.summary()
        data["db_queue"] = db_queue.stats
        return web.json_response(data)

    async def _api_advisor_context(self, request):
        """GET /api/advisor_context — slim EV-enriched context for advisor sidecar.

        Returns the same position data as /api/status plus:
        - p_smooth, q_mid, edge, conviction, regime from probability engine
        - fav_ev, hedge_ev from should_act() EV computation
        - settlement_ev (expected value at settlement given current shares)
        - GBM confirmation signals
        - combined_ev and entry drag
        This gives the LLM the SAME math the deterministic engine uses.
        """
        from src.directional import _buy_cost_per_share as _bcs_adv, _sell_proceeds_per_share as _sps_adv

        now = time.time()
        windows = []

        for slug, w in self._active_windows.items():
            if not w.bet_side or w.status == "closed":
                continue

            # Core position data
            up_sh = float(getattr(w, 'up_shares', 0))
            dn_sh = float(getattr(w, 'down_shares', 0))
            up_bid = float(getattr(w, 'up_best_bid', 0))
            dn_bid = float(getattr(w, 'down_best_bid', 0))
            up_ask = float(getattr(w, 'up_best_ask', 1.0))
            dn_ask = float(getattr(w, 'down_best_ask', 1.0))
            total_cost = float(getattr(w, 'total_cost', 0))
            total_spent = float(getattr(w, 'total_spent', 0))
            budget = float(getattr(w, 'budget', 0))
            realized = float(getattr(w, 'realized_pnl', 0))
            t_left = float(getattr(w, 'time_remaining', 0))
            t_into = w.interval_secs - t_left if w.interval_secs > 0 else 0

            # Probability engine state
            p_smooth = getattr(self.engine, '_p_smooth', 0.50)
            q_mid_val = 0.50
            if up_bid > 0 and up_ask < 1.0:
                q_mid_val = (up_bid + up_ask) / 2.0
            elif w.up_price > 0:
                q_mid_val = w.up_price

            edge = abs(p_smooth - q_mid_val)
            fav_side = "UP" if p_smooth > 0.50 else "DOWN"

            # EV per share (same formula as should_act)
            slip = 0.002
            buffer = 0.003
            try:
                cost_up = _bcs_adv(up_ask) if 0 < up_ask < 1.0 else up_ask
                cost_dn = _bcs_adv(dn_ask) if 0 < dn_ask < 1.0 else dn_ask
            except Exception:
                cost_up = up_ask
                cost_dn = dn_ask

            ev_up = p_smooth - cost_up - slip - buffer
            ev_dn = (1.0 - p_smooth) - cost_dn - slip - buffer
            fav_ev = ev_up if fav_side == "UP" else ev_dn
            hedge_ev = ev_dn if fav_side == "UP" else ev_up

            # Settlement EV: expected value of holding to expiry
            settlement_ev = p_smooth * up_sh + (1.0 - p_smooth) * dn_sh - total_cost + realized

            # Mark-to-market (liquidation value)
            try:
                mark_up = up_sh * _sps_adv(up_bid) if up_bid > 0 else 0
                mark_dn = dn_sh * _sps_adv(dn_bid) if dn_bid > 0 else 0
            except Exception:
                mark_up = up_sh * up_bid
                mark_dn = dn_sh * dn_bid
            mark_total = mark_up + mark_dn

            # Entry drag
            entry_drag = up_ask + dn_ask - 1.0

            # Conviction pipeline
            conv_pipeline = getattr(self.engine, '_last_conviction_pipeline', {})

            # Regime
            regime = conv_pipeline.get("regime", "UNKNOWN")

            # GBM signals from conviction pipeline (ret_5, z, vol, etc.)
            gbm_data = {}
            conv_sigs = conv_pipeline.get("conv_signals", {})
            if conv_sigs:
                gbm_data = {
                    "ret_5": round(conv_sigs.get("ret_5", 0), 6),
                    "ret_15": round(conv_sigs.get("ret_15", 0), 6),
                    "ret_30": round(conv_sigs.get("ret_30", 0), 6),
                    "vol": round(conv_sigs.get("vol", 0), 6),
                    "book_imb": round(conv_sigs.get("book_imb", 0), 4),
                    "z": round(conv_sigs.get("z", 0), 4),
                    "lag": round(conv_sigs.get("lag", 0), 4),
                    "pressure": round(conv_sigs.get("pressure", 0), 4),
                }

            # Minority ratio
            total_sh = up_sh + dn_sh
            minority = min(up_sh, dn_sh) / max(total_sh, 0.01) if total_sh > 0 else 0

            windows.append({
                "slug": w.slug,
                "asset": w.asset.upper(),
                "interval_secs": w.interval_secs,
                "time_left": round(t_left, 1),
                "time_into": round(t_into, 1),
                "bet_side": w.bet_side,
                "approach": getattr(w, 'approach', ''),
                # Prices
                "up_bid": round(up_bid, 4),
                "up_ask": round(up_ask, 4),
                "dn_bid": round(dn_bid, 4),
                "dn_ask": round(dn_ask, 4),
                "up_spread": round(getattr(w, 'up_spread', 0), 4),
                "dn_spread": round(getattr(w, 'down_spread', 0), 4),
                # Position
                "up_shares": round(up_sh, 2),
                "dn_shares": round(dn_sh, 2),
                "up_cost": round(float(getattr(w, 'up_cost', 0)), 4),
                "dn_cost": round(float(getattr(w, 'down_cost', 0)), 4),
                "total_cost": round(total_cost, 4),
                "total_spent": round(total_spent, 4),
                "budget": round(budget, 2),
                "realized_pnl": round(realized, 4),
                "trade_count": getattr(w, 'trade_count', 0),
                "minority_ratio": round(minority, 3),
                # EV math (the critical part the sidecar needs)
                "p_smooth": round(p_smooth, 4),
                "q_mid": round(q_mid_val, 4),
                "edge": round(edge, 4),
                "fav_side": fav_side,
                "fav_ev": round(fav_ev, 4),
                "hedge_ev": round(hedge_ev, 4),
                "settlement_ev": round(settlement_ev, 4),
                "entry_drag": round(entry_drag, 4),
                "mark_total": round(mark_total, 4),
                "regime": regime,
                "conviction": round(conv_pipeline.get("conviction", 0), 4),
                "signals": gbm_data,
            })

        return web.json_response({
            "ts": now,
            "btc_price": self._btc_price,
            "bankroll": round(self.engine.bankroll, 4),
            "dry_run": self.dry_run,
            "trade_mode": self._trade_mode,
            "windows": windows,
        })

    async def _api_metrics(self, request):
        """GET /api/metrics — reliability metrics for live tuning."""
        m = dict(self._metrics)  # copy
        # Compute derived rates
        m["trim_success_rate"] = round(m["trim_successes"] / max(1, m["trim_attempts"]), 3)
        m["sell_success_rate"] = round(m["sell_successes"] / max(1, m["sell_attempts"]), 3)
        m["lock_success_rate"] = round(m["lock_successes"] / max(1, m["lock_attempts"]), 3)
        m["window_win_rate"] = round(m["windows_profitable"] / max(1, m["windows_total"]), 3)
        m["avg_flip_latency"] = round(m["flip_latency_sum"] / max(1, m["flip_detected"]), 2)
        m["avg_trim_pnl"] = round(m["trim_pnl_total"] / max(1, m["trim_successes"]), 4)
        m["avg_window_pnl"] = round(m["window_pnl_total"] / max(1, m["windows_total"]), 4)
        # P2+: new derived rates
        m["rehedge_success_rate"] = round(m["rehedge_successes"] / max(1, m["rehedge_attempts"]), 3)
        m["advisor_param_rate"] = round(m["advisor_param_applied"] / max(1, m["advisor_calls"]), 3)
        m["quote_pathological_rate"] = round(m["quote_pathological"] / max(1, self._cycle_count), 4)
        m["flip_roll_success_rate"] = round(m.get("flip_roll_successes", 0) / max(1, m.get("flip_roll_attempts", 0)), 3)
        # UP-bias derived
        m["up_bias_auto_flip"] = getattr(self.engine, '_up_bias_auto_flip', False)
        m["up_bias_hard_enabled"] = getattr(self.engine, '_up_bias_hard_enabled', False)
        m["up_bias_total_pnl"] = round(m.get("up_bias_soft_pnl", 0) + m.get("up_bias_hard_pnl", 0), 4)
        return web.json_response(m)

    async def _start_web_server(self):
        """Start embedded aiohttp web server in background."""
        app = web.Application()
        app.router.add_get("/", self._index_handler)
        app.router.add_get("/mobile", self._mobile_handler)
        app.router.add_get("/ws", self._ws_handler)
        app.router.add_get("/api/status", self._api_status)
        app.router.add_get("/api/status_compact", self._api_status_compact)
        app.router.add_get("/api/status_lite", self._api_status_lite)
        app.router.add_get("/api/refresh_balance", self._api_refresh_balance)
        app.router.add_post("/api/trade_mode", self._api_set_trade_mode)
        app.router.add_post("/api/go_live", self._api_go_live)
        app.router.add_post("/api/go_dry", self._api_go_dry)
        app.router.add_post("/api/trade", self._api_manual_trade)
        app.router.add_post("/api/exit", self._api_exit_position)
        app.router.add_post("/api/withdraw", self._api_withdraw)
        app.router.add_post("/api/intervals", self._api_set_intervals)
        app.router.add_post("/api/max_bet", self._api_set_max_bet)
        app.router.add_post("/api/discord_webhook", self._api_set_discord_webhook)
        app.router.add_post("/api/test_discord", self._api_test_discord)
        app.router.add_post("/api/bot_pause", self._api_bot_pause)
        app.router.add_post("/api/unhalt", self._api_unhalt)
        app.router.add_post("/api/time_gate", self._api_time_gate)
        app.router.add_post("/api/time_gate_hours", self._api_time_gate_hours)
        app.router.add_post("/api/wallet_confirm", self._api_wallet_confirm)
        app.router.add_post("/api/kelly_settings", self._api_kelly_settings)
        app.router.add_post("/api/redeem", self._api_redeem_winnings)
        app.router.add_get("/api/perf", self._api_perf)
        app.router.add_get("/api/metrics", self._api_metrics)
        app.router.add_get("/api/advisor_context", self._api_advisor_context)
        # Serve static files from dashboard dir
        if DASHBOARD_DIR.exists():
            app.router.add_static("/static", DASHBOARD_DIR)

        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", WEB_PORT)
        try:
            await site.start()
            print(f"  Dashboard: http://localhost:{WEB_PORT}")
            # Open browser
            webbrowser.open(f"http://localhost:{WEB_PORT}")
        except OSError as e:
            print(f"  Dashboard server failed: {e} (port {WEB_PORT} in use?)")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_ts(s: str) -> float:
        if not s:
            return 0
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError):
            return 0


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Polymarket BTC Up/Down Continuous Runner")
    parser.add_argument("--live", action="store_true", help="Enable live trading (default: dry run)")
    parser.add_argument("--mode", default="composite", choices=["momentum", "contrarian", "book_imbalance", "composite"])
    parser.add_argument("--bankroll", type=float, default=None, help="Starting bankroll ($)")
    parser.add_argument("--kelly-cap", type=float, default=0.25, help="Kelly fraction cap (0.05-1.0)")
    parser.add_argument("--min-confidence", type=float, default=0.52, help="Minimum confidence to bet")
    parser.add_argument("--live-min-conviction", type=float, default=0.05, help="Minimum conviction for LIVE entries")
    parser.add_argument("--max-bet-pct", type=float, default=0.25, help="Max bankroll pct per bet")
    parser.add_argument("--max-bet-dollars", type=float, default=4.00, help="Hard $ cap per bet (default: $4.00)")
    parser.add_argument("--intervals", default="5m", help="Intervals to trade (5m,15m)")
    args = parser.parse_args()

    # Get bankroll from on-chain balance if not specified
    bankroll = args.bankroll
    if bankroll is None:
        try:
            identity = config.resolve_identity()
            # Prefer USDC.e (what Polymarket CLOB uses) over native USDC
            usdce = config.get_usdce_balance(identity.funder)
            native = config.get_onchain_balance(identity.funder)
            bankroll = usdce if usdce > 0.5 else native
            if bankroll <= 0:
                bankroll = 10.0
                print(f"Could not fetch balance, using default ${bankroll}")
            else:
                label = "USDC.e" if usdce > 0.5 else "USDC"
                print(f"Bankroll from {label}: ${bankroll:.2f}")
        except Exception:
            bankroll = 10.0

    # --live flag controls initial mode; dashboard can toggle at runtime
    dry_run = not args.live
    if dry_run:
        print("\n  Starting in DRY RUN mode. Use --live or dashboard to enable live trading.")
    else:
        print("\n  ⚠️  LIVE TRADING enabled via --live flag.")
    intervals = [i.strip() for i in args.intervals.split(",") if i.strip() in INTERVALS]

    # Detect if --max-bet-dollars was explicitly passed on CLI
    import sys as _sys
    _max_bet_explicit = any(a.startswith("--max-bet-dollars") for a in _sys.argv[1:])

    runner = ContinuousRunner(
        dry_run=dry_run,
        mode=args.mode,
        bankroll=bankroll,
        kelly_cap=args.kelly_cap,
        min_confidence=args.min_confidence,
        live_min_conviction=args.live_min_conviction,
        max_bet_pct=args.max_bet_pct,
        max_bet_dollars=args.max_bet_dollars,
        intervals=intervals,
        max_bet_explicit=_max_bet_explicit,
    )

    try:
        asyncio.run(runner.run())
    except KeyboardInterrupt:
        print(f"\n\nStopped. Final stats:")
        print(f"  Bets: {runner.total_bets} | W/L: {runner.wins}/{runner.losses}")
        print(f"  Total P&L: ${runner.total_pnl:+.4f}")
        print(f"  Final bankroll: ${runner.engine.bankroll:.4f}")


if __name__ == "__main__":
    main()
