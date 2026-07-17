#!/usr/bin/env python3
"""
Advisor Monitor — AI-powered sidecar for PolyBot.

Polls the runner's local API for position context, sends structured snapshots
to Kimi 2 (via NVIDIA NIM) or OpenClaw as fallback, then writes bounded
advisory nudges to a local JSON file that the runner reads opportunistically.

Architecture:
  Runner (port 8420) ──[/api/advisor_context]──► Advisor Monitor
                                                     │
                                                     ▼
                                              Kimi 2 / OpenClaw
                                                     │
                                                     ▼
                                        /tmp/polybot_advice.json
                                                     │
  Runner ◄──[reads file with TTL check]──────────────┘

Hard constraints:
  - LLM CANNOT directly place/cancel orders
  - Output must be strict JSON schema; invalid outputs rejected
  - Timeout + circuit breaker + cooldown per call
  - If advisor unavailable, bot behaviour unchanged (deterministic baseline)
"""

import asyncio
import json
import hashlib
import os
import sys
import time
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx

# ── Configuration ──────────────────────────────────────────────────

RUNNER_URL = os.environ.get("RUNNER_URL", "http://localhost:8420")
ADVICE_FILE = Path(os.environ.get("ADVICE_FILE", "/tmp/polybot_advice.json"))

# GPT-4o via OpenAI (primary — fast, 1-3s latency)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")

# Kimi via NVIDIA NIM (fallback)
NVIDIA_API_KEY = os.environ.get("NVIDIA_API_KEY", "")
NVIDIA_BASE_URL = os.environ.get("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")
KIMI_MODEL = os.environ.get("KIMI_MODEL", "moonshotai/kimi-k2.5")  # faster variant

# Legacy fallback
OPENCLAW_MODEL = os.environ.get("OPENCLAW_MODEL", "moonshotai/kimi-k2-instruct")

# Timing
POLL_INTERVAL = float(os.environ.get("ADVISOR_POLL_INTERVAL", "3.0"))  # seconds
ADVICE_TTL = int(os.environ.get("ADVICE_TTL", "3"))  # seconds
LLM_TIMEOUT = float(os.environ.get("LLM_TIMEOUT", "30.0"))  # seconds per call (NVIDIA NIM cold starts can be very slow)
MAX_RPM = int(os.environ.get("ADVISOR_MAX_RPM", "15"))  # max requests/minute
PER_WINDOW_COOLDOWN = float(os.environ.get("ADVISOR_WINDOW_COOLDOWN", "10.0"))

# Circuit breaker
CB_FAIL_THRESHOLD = 5  # consecutive failures to open circuit
CB_RESET_TIMEOUT = 30.0  # seconds before half-open retry

# Mode
DRY_RUN_ADVISOR = os.environ.get("ADVISOR_DRY_RUN", "true").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ADVISOR] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("advisor")


# ── Schemas ────────────────────────────────────────────────────────

VALID_ACTIONS = {
    "HOLD", "TRIM_UP", "TRIM_DOWN", "EXIT_BOTH",
    "ADD_UP", "ADD_DOWN", "ADD_HEDGE", "LOCK_ONLY", "NO_ACTION",
    "ADJUST_PARAMS",  # P5: bounded parameter optimization
}

VALID_RISK_FLAGS = {"NONE", "LOW", "MEDIUM", "HIGH"}


@dataclass
class Advice:
    version: str = "1"
    ts: float = 0.0
    context_ts: float = 0.0       # when the runner snapshot was captured
    context_time_left: float = 0.0  # time_left at snapshot (for drift detection)
    context_up_bid: float = 0.0   # up bid at snapshot
    context_dn_bid: float = 0.0   # down bid at snapshot
    window_slug: str = ""
    action: str = "NO_ACTION"
    confidence: float = 0.0
    size_pct: float = 0.0
    reason_code: str = ""
    reason: str = ""
    ttl_sec: int = 3
    risk_flag: str = "NONE"
    param_adjustments: dict = field(default_factory=dict)  # P5: bounded param tweaks

    def is_valid(self) -> bool:
        return (
            self.version == "1"
            and self.action in VALID_ACTIONS
            and 0.0 <= self.confidence <= 1.0
            and 0.0 <= self.size_pct <= 1.0
            and self.risk_flag in VALID_RISK_FLAGS
            and 1 <= self.ttl_sec <= 5
            and len(self.window_slug) > 0
            and self.context_ts > 0  # must have context timestamp
        )

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "ts": self.ts,
            "context_ts": self.context_ts,
            "context_time_left": self.context_time_left,
            "context_up_bid": self.context_up_bid,
            "context_dn_bid": self.context_dn_bid,
            "window_slug": self.window_slug,
            "action": self.action,
            "confidence": self.confidence,
            "size_pct": self.size_pct,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "ttl_sec": self.ttl_sec,
            "risk_flag": self.risk_flag,
            "param_adjustments": self.param_adjustments,
        }


# ── Circuit Breaker ───────────────────────────────────────────────

@dataclass
class CircuitBreaker:
    fail_count: int = 0
    state: str = "closed"  # closed, open, half-open
    last_failure: float = 0.0

    def record_success(self):
        self.fail_count = 0
        self.state = "closed"

    def record_failure(self):
        self.fail_count += 1
        self.last_failure = time.time()
        if self.fail_count >= CB_FAIL_THRESHOLD:
            self.state = "open"
            log.warning(f"Circuit OPEN after {self.fail_count} failures")

    def allow_request(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            if time.time() - self.last_failure > CB_RESET_TIMEOUT:
                self.state = "half-open"
                return True
            return False
        # half-open: allow one attempt
        return True


# ── Rate Limiter ──────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, max_rpm: int):
        self.max_rpm = max_rpm
        self.timestamps: list[float] = []

    def allow(self) -> bool:
        now = time.time()
        self.timestamps = [t for t in self.timestamps if now - t < 60.0]
        return len(self.timestamps) < self.max_rpm

    def record(self):
        self.timestamps.append(time.time())


# ── Counters ──────────────────────────────────────────────────────

@dataclass
class Stats:
    calls: int = 0
    timeouts: int = 0
    parse_fails: int = 0
    applied: int = 0
    ignored: int = 0
    provider_used: dict = field(default_factory=lambda: {"kimi": 0, "openclaw": 0, "none": 0})
    latency_sum: float = 0.0
    latency_count: int = 0

    def avg_latency(self) -> float:
        return self.latency_sum / max(1, self.latency_count)

    def to_dict(self) -> dict:
        return {
            "calls": self.calls,
            "timeouts": self.timeouts,
            "parse_fails": self.parse_fails,
            "applied": self.applied,
            "ignored": self.ignored,
            "provider_used": dict(self.provider_used),
            "avg_latency_ms": round(self.avg_latency() * 1000, 1),
        }


# ── Trigger Policy ────────────────────────────────────────────────

def should_call_advisor(ctx: dict) -> tuple[bool, str]:
    """Determine if we should call the advisor for this context.
    Returns (should_call, reason).

    Uses /api/advisor_context format (windows array) with EV-aware triggers.
    Falls back to /api/status format (markets array) if needed.
    """
    # Try new format first (from /api/advisor_context)
    windows = ctx.get("windows", [])
    if windows:
        for w in windows:
            time_left = w.get("time_left", 999)

            # Skip if not in actionable zone (30-270s, broadened for P5)
            if time_left <= 30 or time_left > 270:
                continue

            # Trigger 1: significant EV opportunity (fav_ev positive = buying is +EV)
            fav_ev = w.get("fav_ev", 0)
            if fav_ev > 0.005:
                remaining_budget = w.get("budget", 0) - w.get("total_spent", 0)
                if remaining_budget > 1.0:
                    return True, f"fav_ev={fav_ev:+.4f}_budget=${remaining_budget:.1f}"

            # Trigger 2: settlement EV diverges significantly from mark
            settle_ev = w.get("settlement_ev", 0)
            mark = w.get("mark_total", 0)
            total_cost = w.get("total_cost", 0)
            if total_cost > 0 and abs(settle_ev - (mark - total_cost)) > 0.03:
                return True, f"ev_diverge_settle={settle_ev:+.3f}_mark={mark - total_cost:+.3f}"

            # Trigger 3: minority ratio dangerously low
            minority = w.get("minority_ratio", 0.5)
            if minority < 0.15 and time_left > 45:
                return True, f"minority_low={minority:.2f}"

            # Trigger 4: regime change (TREND/MEAN_REVERT detection)
            regime = w.get("regime", "UNKNOWN")
            edge = w.get("edge", 0)
            if regime in ("TREND",) and edge > 0.03:
                return True, f"trend_edge={edge:.3f}"

            # Trigger 5: actionable time window approaching LOCK
            if 30 < time_left < 60:
                return True, f"pre_lock={time_left:.0f}s"

        return False, "no_trigger"

    # Fallback: legacy /api/status format
    markets = ctx.get("markets", [])
    if not markets:
        return False, "no_markets"

    for m in markets:
        if not m.get("betSide"):
            continue

        time_left = m.get("timeLeft", 999)
        if time_left <= 30 or time_left > 270:
            continue

        up_sh = m.get("upShares", 0)
        dn_sh = m.get("dnShares", 0)
        up_bid = m.get("upBid", 0)
        dn_bid = m.get("dnBid", 0)
        total_cost = m.get("totalCost", 0)
        if total_cost > 0 and (up_sh > 0 or dn_sh > 0):
            mark = up_sh * up_bid + dn_sh * dn_bid
            unreal_pnl = mark - total_cost
            if abs(unreal_pnl) > 0.02:
                return True, f"unrealized_pnl={unreal_pnl:+.3f}"

        if 30 < time_left < 180:
            return True, f"actionable_time={time_left:.0f}s"

    return False, "no_trigger"


# ── LLM Interaction ───────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert quantitative trading advisor for a Polymarket BTC 5-minute binary options bot.

The bot uses a HEDGED strategy: buying both UP and DOWN sides, tilting toward the favored direction.
At settlement (5 min), one side pays $1/share, the other pays $0.

Your role: analyze the current position context and recommend ONE action to optimize profit.

RULES:
- You cannot place orders. You advise only.
- Your advice is bounded: the bot applies guardrails before executing.
- Focus on: (1) profitable trim timing, (2) direction adjustment, (3) avoiding bad exits.
- Consider: current bid/ask, spread, time remaining, EV metrics, position balance.
- Be conservative: HOLD is often the best action. Only recommend action with clear reasoning.

RESPOND WITH EXACTLY THIS JSON (no markdown, no explanation outside JSON):
{
  "version": "1",
  "action": "HOLD|TRIM_UP|TRIM_DOWN|EXIT_BOTH|ADD_UP|ADD_DOWN|ADD_HEDGE|LOCK_ONLY|NO_ACTION|ADJUST_PARAMS",
  "confidence": 0.0 to 1.0,
  "size_pct": 0.0 to 1.0 (fraction of position to act on),
  "reason_code": "short_code",
  "reason": "one-line explanation",
  "ttl_sec": 3,
  "risk_flag": "NONE|LOW|MEDIUM|HIGH",
  "param_adjustments": {}
}

For ADJUST_PARAMS action, include a "param_adjustments" field with any subset of:
{
  "trim_aggressiveness": 0.5 to 2.0 (multiplier on trim percentage, default 1.0),
  "hedge_target_adj": -0.10 to +0.10 (additive adjustment to minority_target, default 0),
  "lock_buffer_adj": -0.02 to +0.02 (adjustment to lock trigger sensitivity, default 0),
  "spread_tolerance_mult": 0.5 to 1.5 (multiplier on spread gate threshold, default 1.0)
}
Use ADJUST_PARAMS when you see opportunity to tune behavior without direct order actions."""


def _build_prompt(ctx: dict, trigger: str) -> str:
    """Build the user prompt from advisor_context endpoint (enriched with EV math)."""
    windows = ctx.get("windows", [])
    if not windows:
        # Fallback: try legacy /api/status format
        return _build_prompt_legacy(ctx, trigger)

    w = windows[0]  # first active window with position

    # EV math section
    sigs = w.get("signals", {})
    sig_str = ""
    if sigs:
        sig_str = (
            f"\n  Momentum signals: z={sigs.get('z', 0):.4f} "
            f"ret_5={sigs.get('ret_5', 0):.6f} ret_15={sigs.get('ret_15', 0):.6f} "
            f"ret_30={sigs.get('ret_30', 0):.6f} vol={sigs.get('vol', 0):.6f}"
            f"\n  Book imbalance: {sigs.get('book_imb', 0):.4f}"
        )

    prompt = f"""Current BTC position snapshot (trigger: {trigger}):

Window: {w.get('slug', '?')[:40]}
Time: {w.get('time_left', 0):.0f}s remaining of {w.get('interval_secs', 300)}s
BTC: ${ctx.get('btc_price', 0):,.2f}

Prices:
  UP:   bid={w.get('up_bid', 0):.4f}  ask={w.get('up_ask', 1):.4f}  spread={w.get('up_spread', 0):.4f}
  DOWN: bid={w.get('dn_bid', 0):.4f}  ask={w.get('dn_ask', 1):.4f}  spread={w.get('dn_spread', 0):.4f}

Position:
  Side: {w.get('bet_side', '?')}  Approach: {w.get('approach', '?')}
  UP shares: {w.get('up_shares', 0):.2f} (cost ${w.get('up_cost', 0):.4f})
  DOWN shares: {w.get('dn_shares', 0):.2f} (cost ${w.get('dn_cost', 0):.4f})
  Total spent: ${w.get('total_spent', 0):.4f}  Budget: ${w.get('budget', 0):.2f}
  Realized P&L: ${w.get('realized_pnl', 0):.4f}
  Trade count: {w.get('trade_count', 0)}
  Minority ratio: {w.get('minority_ratio', 0):.3f}

EV Analysis (from probability engine):
  p_smooth={w.get('p_smooth', 0.50):.4f}  q_mid={w.get('q_mid', 0.50):.4f}  edge={w.get('edge', 0):.4f}
  Favored: {w.get('fav_side', '?')}  conviction={w.get('conviction', 0):.4f}
  Fav EV/share: {w.get('fav_ev', 0):+.4f}  Hedge EV/share: {w.get('hedge_ev', 0):+.4f}
  Settlement EV: ${w.get('settlement_ev', 0):+.4f}
  Entry drag: {w.get('entry_drag', 0):.4f}
  Regime: {w.get('regime', 'UNKNOWN')}{sig_str}

Mark-to-market:
  Mark value: ${w.get('mark_total', 0):.4f}
  Unrealized P&L: ${w.get('mark_total', 0) - w.get('total_cost', 0):.4f}

Bot state:
  Bankroll: ${ctx.get('bankroll', 0):.2f}
  Mode: {ctx.get('trade_mode', '?')}  Dry run: {ctx.get('dry_run', True)}

Key decisions:
- fav_ev > 0 means buying favored side is +EV after fees
- settlement_ev > mark_total suggests holding beats selling
- minority_ratio < 0.30 means hedge might be needed
- entry_drag > 0.04 means spreads are too wide

What action do you recommend? Respond with JSON only."""

    return prompt


def _build_prompt_legacy(ctx: dict, trigger: str) -> str:
    """Fallback prompt builder for /api/status format (no EV math)."""
    markets = ctx.get("markets", [])
    if not markets:
        return ""

    m = None
    for mk in markets:
        if mk.get("betSide"):
            m = mk
            break
    if not m:
        return ""

    prompt = f"""Current BTC position snapshot (trigger: {trigger}):

Window: {m.get('slug', '?')[:40]}
Time: {m.get('timeLeft', 0):.0f}s remaining of {m.get('intervalSecs', 300)}s
BTC: ${ctx.get('btcPrice', 0):,.2f}

Position:
  Side: {m.get('betSide', '?')}  Approach: {m.get('approach', '?')}
  UP shares: {m.get('upShares', 0):.2f}  DOWN shares: {m.get('dnShares', 0):.2f}
  Total cost: ${m.get('totalCost', 0):.4f}  Budget: ${m.get('budget', 0):.2f}

Prices:
  UP:  bid={m.get('upBid', 0):.4f}  ask={m.get('upAsk', 1):.4f}
  DOWN: bid={m.get('dnBid', 0):.4f}  ask={m.get('dnAsk', 1):.4f}

Bankroll: ${ctx.get('bankroll', 0):.2f}  Dry run: {ctx.get('dryRun', True)}

What action do you recommend? Respond with JSON only."""

    return prompt


async def call_openai(
    client: httpx.AsyncClient,
    model: str,
    prompt: str,
) -> Optional[dict]:
    """Call OpenAI API (GPT-4o). Returns parsed JSON or None."""
    if not OPENAI_API_KEY:
        return None

    try:
        resp = await client.post(
            f"{OPENAI_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "max_tokens": 300,
                "response_format": {"type": "json_object"},
            },
            timeout=10.0,  # GPT-4o is fast — 10s hard cap
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()

        # Strip markdown code fences if present
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(
                l for l in lines if not l.strip().startswith("```")
            )

        return json.loads(content)

    except httpx.TimeoutException:
        log.warning(f"openai timeout (10s)")
        return None
    except httpx.HTTPStatusError as e:
        log.warning(f"openai HTTP {e.response.status_code}: {e.response.text[:200]}")
        return None
    except json.JSONDecodeError as e:
        log.warning(f"openai JSON parse error: {e}")
        return None
    except Exception as e:
        log.warning(f"openai error: {e}")
        return None


async def call_llm(
    client: httpx.AsyncClient,
    model: str,
    prompt: str,
    provider_name: str,
) -> Optional[dict]:
    """Call NVIDIA NIM API with the given model. Returns parsed JSON or None."""
    if not NVIDIA_API_KEY:
        log.error("No NVIDIA_API_KEY set")
        return None

    try:
        resp = await client.post(
            f"{NVIDIA_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {NVIDIA_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "max_tokens": 300,
            },
            timeout=LLM_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()

        # Strip markdown code fences if present
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(
                l for l in lines if not l.strip().startswith("```")
            )

        return json.loads(content)

    except httpx.TimeoutException:
        log.warning(f"{provider_name} timeout ({LLM_TIMEOUT}s)")
        return None
    except httpx.HTTPStatusError as e:
        log.warning(f"{provider_name} HTTP {e.response.status_code}: {e.response.text[:200]}")
        return None
    except json.JSONDecodeError as e:
        log.warning(f"{provider_name} JSON parse error: {e}")
        return None
    except Exception as e:
        log.warning(f"{provider_name} error: {e}")
        return None


def parse_advice(raw: dict, slug: str, ctx_snapshot: dict | None = None) -> Optional[Advice]:
    """Validate and parse LLM output into Advice object.

    ctx_snapshot: the runner context dict captured BEFORE the LLM call.
    Used to stamp context_ts + context prices for staleness detection.
    """
    try:
        # Extract context-snapshot fields for staleness guard
        ctx_ts = 0.0
        ctx_time_left = 0.0
        ctx_up_bid = 0.0
        ctx_dn_bid = 0.0
        if ctx_snapshot:
            ctx_ts = ctx_snapshot.get("_snapshot_ts", time.time())
            # Try new /api/advisor_context format (windows array)
            for w in ctx_snapshot.get("windows", []):
                if w.get("slug") == slug or w.get("bet_side"):
                    ctx_time_left = w.get("time_left", 0)
                    ctx_up_bid = w.get("up_bid", 0)
                    ctx_dn_bid = w.get("dn_bid", 0)
                    break
            else:
                # Fallback: legacy /api/status format (markets array)
                for m in ctx_snapshot.get("markets", []):
                    if m.get("slug") == slug or m.get("betSide"):
                        ctx_time_left = m.get("timeLeft", 0)
                        ctx_up_bid = m.get("upBid", 0)
                        ctx_dn_bid = m.get("dnBid", 0)
                        break

        adv = Advice(
            version=str(raw.get("version", "1")),
            ts=time.time(),
            context_ts=ctx_ts,
            context_time_left=ctx_time_left,
            context_up_bid=ctx_up_bid,
            context_dn_bid=ctx_dn_bid,
            window_slug=slug,
            action=str(raw.get("action", "NO_ACTION")).upper(),
            confidence=float(raw.get("confidence", 0)),
            size_pct=float(raw.get("size_pct", 0)),
            reason_code=str(raw.get("reason_code", ""))[:50],
            reason=str(raw.get("reason", ""))[:200],
            ttl_sec=int(raw.get("ttl_sec", 3)),
            risk_flag=str(raw.get("risk_flag", "NONE")).upper(),
            param_adjustments=raw.get("param_adjustments", {}),  # P5
        )
        if adv.is_valid():
            return adv
        log.warning(f"Invalid advice: {raw}")
        return None
    except Exception as e:
        log.warning(f"Advice parse error: {e}")
        return None


# ── Main Loop ─────────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("PolyBot Advisor Monitor starting")
    log.info(f"  Runner: {RUNNER_URL}")
    _primary = f"GPT-4o ({OPENAI_MODEL})" if OPENAI_API_KEY else f"Kimi ({KIMI_MODEL})"
    _fallback = f"Kimi ({KIMI_MODEL})" if OPENAI_API_KEY else f"OpenClaw ({OPENCLAW_MODEL})"
    log.info(f"  Primary: {_primary}  Fallback: {_fallback}")
    log.info(f"  Poll: {POLL_INTERVAL}s  TTL: {ADVICE_TTL}s  Max RPM: {MAX_RPM}")
    log.info(f"  Dry run: {DRY_RUN_ADVISOR}")
    log.info(f"  Advice file: {ADVICE_FILE}")
    if not OPENAI_API_KEY and not NVIDIA_API_KEY:
        log.error("No API keys set! Set OPENAI_API_KEY or NVIDIA_API_KEY.")
        log.info("Exiting.")
        return
    log.info("=" * 60)

    cb_openai = CircuitBreaker()
    cb_kimi = CircuitBreaker()
    cb_openclaw = CircuitBreaker()
    rate_limiter = RateLimiter(MAX_RPM)
    stats = Stats()
    window_cooldowns: dict[str, float] = {}  # slug -> last call time
    last_advice: Optional[Advice] = None

    async with httpx.AsyncClient(timeout=10.0) as http:
        while True:
            try:
                await asyncio.sleep(POLL_INTERVAL)

                # 1. Poll runner — prefer /api/advisor_context (EV-enriched), fallback to /api/status
                try:
                    resp = await http.get(f"{RUNNER_URL}/api/advisor_context", timeout=3.0)
                    if resp.status_code == 200:
                        ctx = resp.json()
                    else:
                        # Fallback to /api/status (legacy format)
                        resp = await http.get(f"{RUNNER_URL}/api/status", timeout=3.0)
                        ctx = resp.json()
                    ctx["_snapshot_ts"] = time.time()  # stamp when context was captured
                except Exception as e:
                    log.debug(f"Runner poll failed: {e}")
                    continue

                # 2. Check triggers (works with both formats)
                should_call, trigger = should_call_advisor(ctx)
                if not should_call:
                    continue

                # 3. Find the active window slug (support both formats)
                slug = ""
                # New format: windows array
                for w in ctx.get("windows", []):
                    if w.get("bet_side"):
                        slug = w.get("slug", "")
                        break
                # Legacy format: markets array
                if not slug:
                    for m in ctx.get("markets", []):
                        if m.get("betSide"):
                            slug = m.get("slug", "")
                            break
                if not slug:
                    continue

                # 4. Per-window cooldown
                last_call = window_cooldowns.get(slug, 0)
                if time.time() - last_call < PER_WINDOW_COOLDOWN:
                    continue

                # 5. Rate limit
                if not rate_limiter.allow():
                    log.debug("Rate limited")
                    continue

                # 6. Build prompt
                prompt = _build_prompt(ctx, trigger)
                if not prompt:
                    continue

                input_hash = hashlib.md5(prompt.encode()).hexdigest()[:8]
                stats.calls += 1
                rate_limiter.record()
                window_cooldowns[slug] = time.time()

                # 7. Call LLM (GPT-4o primary → Kimi → OpenClaw fallback)
                t0 = time.time()
                raw = None
                provider = "none"

                # Try GPT-4o first (fast, 1-3s)
                if OPENAI_API_KEY and cb_openai.allow_request():
                    raw = await call_openai(http, OPENAI_MODEL, prompt)
                    if raw:
                        cb_openai.record_success()
                        provider = "gpt4o"
                    else:
                        cb_openai.record_failure()
                        stats.timeouts += 1

                # Fallback: Kimi k2.5
                if raw is None and cb_kimi.allow_request():
                    raw = await call_llm(http, KIMI_MODEL, prompt, "kimi")
                    if raw:
                        cb_kimi.record_success()
                        provider = "kimi"
                    else:
                        cb_kimi.record_failure()
                        stats.timeouts += 1

                # Last resort: Kimi k2-instruct
                if raw is None and cb_openclaw.allow_request():
                    raw = await call_llm(http, OPENCLAW_MODEL, prompt, "openclaw")
                    if raw:
                        cb_openclaw.record_success()
                        provider = "openclaw"
                    else:
                        cb_openclaw.record_failure()
                        stats.timeouts += 1

                latency = time.time() - t0
                stats.latency_sum += latency
                stats.latency_count += 1
                stats.provider_used[provider] = stats.provider_used.get(provider, 0) + 1

                if raw is None:
                    log.info(f"No LLM response [hash={input_hash}] ({latency:.1f}s)")
                    # provider is already "none" — already counted on line above
                    continue

                # 8. Parse and validate (pass ctx for staleness guard)
                advice = parse_advice(raw, slug, ctx_snapshot=ctx)
                if not advice:
                    stats.parse_fails += 1
                    log.warning(f"Invalid response from {provider}: {json.dumps(raw)[:200]}")
                    continue

                # 9. Write advice file
                advice_data = advice.to_dict()
                advice_data["_provider"] = provider
                advice_data["_latency_ms"] = round(latency * 1000)
                advice_data["_input_hash"] = input_hash
                advice_data["_trigger"] = trigger
                advice_data["_dry_run"] = DRY_RUN_ADVISOR

                # Context age = time from snapshot to now (includes LLM latency)
                ctx_age = time.time() - advice.context_ts if advice.context_ts > 0 else latency
                advice_data["_context_age_ms"] = round(ctx_age * 1000)

                if DRY_RUN_ADVISOR:
                    # Log-only mode: don't write to file
                    log.info(
                        f"[DRY] {provider} → {advice.action} "
                        f"conf={advice.confidence:.2f} size={advice.size_pct:.2f} "
                        f"risk={advice.risk_flag} | {advice.reason} "
                        f"[{latency*1000:.0f}ms ctx_age={ctx_age:.1f}s]"
                    )
                    stats.ignored += 1
                else:
                    ADVICE_FILE.write_text(json.dumps(advice_data, indent=2))
                    log.info(
                        f"[LIVE] {provider} → {advice.action} "
                        f"conf={advice.confidence:.2f} size={advice.size_pct:.2f} "
                        f"| {advice.reason} [{latency*1000:.0f}ms ctx_age={ctx_age:.1f}s]"
                    )
                    stats.applied += 1

                last_advice = advice

                # 10. Periodic stats
                if stats.calls % 10 == 0:
                    log.info(f"Stats: {stats.to_dict()}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    log.info("Advisor monitor stopped")
    log.info(f"Final stats: {stats.to_dict()}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted")
