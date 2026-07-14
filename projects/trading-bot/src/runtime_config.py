"""Centralized runtime configuration for PolyBot.

Single source of truth for all tunable trading parameters.
Loaded at startup, persisted to .bot_state.json, adjustable via API.
"""

from dataclasses import dataclass, field, asdict


@dataclass
class RuntimeConfig:
    # ── Entry conviction gates ──
    # Engine-level conviction floors (in directional.py _should_enter)
    # Engine typically outputs 0.01-0.07; floors must be within that range
    conviction_floor_up: float = 0.03
    conviction_floor_down: float = 0.04
    # Additional live-only conviction floor (runner.py, after engine gates pass)
    # Time-bucketed: early (<45s), mid (45-120s), late (>=120s)
    live_min_conviction: float = 0.05          # legacy fallback
    live_min_conviction_early: float = 0.04
    live_min_conviction_mid: float = 0.06
    live_min_conviction_late: float = 0.05

    # ── EV gates ──
    # Directional: minimum fav_ev to enter (engine edge is typically 0.003-0.01)
    directional_ev_floor: float = 0.005
    # If ask > 0.55, require higher fav_ev
    high_ask_ev_floor: float = 0.01

    # ── Price chasing gates ──
    # ask 0.55-0.65: require this conviction (time-bucketed)
    mid_price_min_conviction: float = 0.06       # legacy fallback
    mid_price_min_conviction_early: float = 0.05  # first 45s
    mid_price_min_conviction_std: float = 0.06    # 45s+
    # ask > 0.65: require this conviction (still high — expensive fills)
    high_price_min_conviction: float = 0.10

    # ── Price divergence gate ──
    # Minimum q_mid divergence from 0.50 to enter (by time bucket)
    price_div_early: float = 0.00     # first 45s — market still forming, allow entry
    price_div_mid: float = 0.01       # 45-120s
    price_div_late: float = 0.01      # 120s+

    # ── Bet sizing ──
    max_bet_dollars: float = 5.00
    max_bet_pct: float = 0.25         # max fraction of bankroll per bet
    kelly_cap: float = 0.25

    # ── Time gate ──
    time_gate_enabled: bool = True
    time_gate_hours: list = field(default_factory=lambda: [0, 3, 4, 14, 19])

    # ── Wallet confirmation ──
    wallet_confirm_enabled: bool = False

    # ── UP bias ──
    up_bias_auto_flip: bool = False
    up_bias_hard_enabled: bool = False

    # ── CHOP regime gate ──
    chop_conviction_floor: float = 0.03
    chop_edge_floor: float = 0.01
    chop_div_floor: float = 0.04

    # ── Drag gate ──
    max_entry_drag: float = 0.03

    # ── Hedged EV gates ──
    hedged_fav_ev_floor: float = -0.03
    hedged_cev_extreme: float = -0.07
    hedged_fav_cev_pair: tuple = (-0.01, -0.05)  # (fav_ev, combined_ev)

    def to_dict(self) -> dict:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "RuntimeConfig":
        """Load from dict, ignoring unknown keys."""
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid}
        return cls(**filtered)

    def merge_state(self, state: dict) -> None:
        """Update config from persisted state dict (selective keys only)."""
        mapping = {
            "live_min_conviction": "live_min_conviction",
            "max_bet_dollars": "max_bet_dollars",
            "time_gate_enabled": "time_gate_enabled",
            "time_gate_hours": "time_gate_hours",
            "wallet_confirm_enabled": "wallet_confirm_enabled",
            "up_bias_auto_flip": "up_bias_auto_flip",
            "up_bias_hard_enabled": "up_bias_hard_enabled",
        }
        for state_key, attr in mapping.items():
            if state_key in state:
                setattr(self, attr, state[state_key])


def should_enter(signal, book, cfg: RuntimeConfig, is_live: bool) -> tuple[bool, str]:
    """Unified entry gate. Same logic for dry and live, plus one live-only layer.

    Args:
        signal: DirectionalSignal (side, conviction, approach, p_smooth, edge, regime, etc.)
        book: dict with keys: fav_ask, hedge_ask, fav_ev, combined_ev, entry_drag,
              q_mid, time_into, fav_cost
        cfg: RuntimeConfig instance
        is_live: whether this is a live (real money) entry

    Returns:
        (should_enter, reason)
    """
    conv = signal.conviction
    side = signal.side

    # GATE 1: Conviction floor (asymmetric by side)
    floor = cfg.conviction_floor_down if side == "DOWN" else cfg.conviction_floor_up
    if conv < floor:
        return False, f"conviction {conv:.3f} < {side} floor {floor:.2f}"

    # GATE 2: No direction
    if side == "NONE":
        return False, "no directional signal"

    edge = book.get("edge", 0)
    q_mid = book.get("q_mid", 0.50)
    time_into = book.get("time_into", 0)

    # GATE 3: CHOP regime
    if getattr(signal, "regime", "") == "CHOP":
        chop_div = abs(q_mid - 0.50)
        if conv < cfg.chop_conviction_floor and edge < cfg.chop_edge_floor and chop_div < cfg.chop_div_floor:
            return False, "choppy regime, weak signals"

    # GATE 4: Price divergence
    if time_into < 45:
        min_div = cfg.price_div_early
    elif time_into < 120:
        min_div = cfg.price_div_mid
    else:
        min_div = cfg.price_div_late
    price_div = abs(q_mid - 0.50)
    if price_div < min_div and conv < 0.15 and edge < 0.04:
        return False, f"price div {price_div:.3f} < {min_div:.2f}"

    # GATE 5: Drag
    entry_drag = book.get("entry_drag", 0)
    if entry_drag > cfg.max_entry_drag:
        return False, f"drag {entry_drag:.3f} > {cfg.max_entry_drag:.3f}"

    # GATE 6: EV floor
    fav_ev = book.get("fav_ev", 0)
    combined_ev = book.get("combined_ev", 0)
    fav_ask = book.get("fav_ask", 0.50)

    if signal.approach == "DIRECTIONAL":
        ev_floor = cfg.directional_ev_floor
        if fav_ask > 0.55:
            ev_floor = max(ev_floor, cfg.high_ask_ev_floor)
        if fav_ev <= ev_floor:
            return False, f"dir fav_ev {fav_ev:.4f} <= {ev_floor:.4f}"
    else:
        if fav_ev <= cfg.hedged_fav_ev_floor:
            return False, f"hedged fav_ev {fav_ev:.4f} <= {cfg.hedged_fav_ev_floor}"
        if combined_ev <= cfg.hedged_cev_extreme:
            return False, f"hedged cev {combined_ev:.4f} <= {cfg.hedged_cev_extreme}"
        fav_thresh, cev_thresh = cfg.hedged_fav_cev_pair
        if fav_ev <= fav_thresh and combined_ev <= cev_thresh:
            return False, f"hedged fav+cev {fav_ev:.4f}/{combined_ev:.4f}"

    # GATE 7: Price chasing (mid-price gate is time-aware)
    if fav_ask > 0.65:
        if conv < cfg.high_price_min_conviction:
            return False, f"high price ask={fav_ask:.2f} conv={conv:.3f} < {cfg.high_price_min_conviction}"
    elif fav_ask > 0.55:
        mid_floor = cfg.mid_price_min_conviction_early if time_into < 45 else cfg.mid_price_min_conviction_std
        if conv < mid_floor:
            _bucket = "early" if time_into < 45 else "std"
            return False, f"mid price {_bucket} ask={fav_ask:.2f} conv={conv:.3f} < {mid_floor}"

    # GATE 8: Live-only conviction floor (time-bucketed)
    if is_live:
        if time_into < 45:
            live_floor = cfg.live_min_conviction_early
        elif time_into < 120:
            live_floor = cfg.live_min_conviction_mid
        else:
            live_floor = cfg.live_min_conviction_late
        if conv < live_floor:
            _bucket = "early" if time_into < 45 else ("mid" if time_into < 120 else "late")
            return False, f"live conv {_bucket} {conv:.3f} < {live_floor}"

    return True, "passed"
