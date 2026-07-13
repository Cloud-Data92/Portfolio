"""Wallet confirmation signal — monitors a reference wallet as second direction confirmation.

Fail-open: if RPC errors or stale data, entries are NOT blocked.
Sticky flips with persistence to avoid noise.
"""

import os
import time
import aiohttp


class WalletSignal:
    """Monitors a reference wallet position as second direction confirmation.

    Fail-open: if RPC errors or stale data, entries are NOT blocked.
    """

    # Reference wallet to mirror as a confirmation signal (set in .env)
    WALLET = os.getenv("SIGNAL_WALLET", "")
    CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    POLL_INTERVAL = 2   # seconds between RPC calls (was 5, tightened for delta tracking)

    # Flip thresholds (sticky — don't flip on noise)
    MIN_POSITION_SHARES = 2.0    # Ignore positions < 2 shares
    CONFIRM_RATIO = 0.60         # Confirm if wallet >= 60% on engine's side
    FLIP_RATIO = 0.75            # Disagree only if wallet >= 75% on OPPOSITE side
    PERSISTENCE_POLLS = 2        # Must see flip for 2 consecutive polls

    def __init__(self, enabled=False, rpc_url="https://polygon-bor-rpc.publicnode.com"):
        self.enabled = enabled
        self._rpc_url = rpc_url
        self._last_poll = 0
        self._cached_direction = None
        self._cached_up_shares = 0.0
        self._cached_down_shares = 0.0
        self._pending_flip = None
        self._pending_flip_count = 0
        self._poll_errors = 0
        # Delta tracking: detect active position changes between polls
        self._prev_up_shares = 0.0
        self._prev_down_shares = 0.0
        self._delta_up = 0.0      # cumulative share change over rolling window
        self._delta_down = 0.0    # cumulative share change over rolling window
        self._delta_ts = 0.0      # timestamp of last delta computation
        self._delta_history = []  # [(ts, d_up, d_down), ...] rolling 15s window
        self._last_token_ids = ("", "")  # track token changes (new window resets delta)

    async def _get_balances(self, wallet: str, up_token_id: str, down_token_id: str) -> tuple:
        """Query UP + DOWN balances in a single RPC call via balanceOfBatch.

        ERC1155 balanceOfBatch(address[], uint256[]) gets both balances in one call,
        avoiding block mismatch and halving RPC load.
        """
        # balanceOfBatch(address[],uint256[]) selector: 0x4e1273f4
        addr = wallet[2:].lower().zfill(64)
        up_id = hex(int(up_token_id))[2:].zfill(64)
        dn_id = hex(int(down_token_id))[2:].zfill(64)

        # ABI encoding for balanceOfBatch(address[2], uint256[2])
        data = "0x4e1273f4"
        data += "0000000000000000000000000000000000000000000000000000000000000040"  # offset to addresses array
        data += "00000000000000000000000000000000000000000000000000000000000000a0"  # offset to ids array
        data += "0000000000000000000000000000000000000000000000000000000000000002"  # addresses length = 2
        data += addr  # addresses[0]
        data += addr  # addresses[1]
        data += "0000000000000000000000000000000000000000000000000000000000000002"  # ids length = 2
        data += up_id   # ids[0] = up_token_id
        data += dn_id   # ids[1] = down_token_id

        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": self.CTF_CONTRACT, "data": data}, "latest"]
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self._rpc_url, json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                result = await resp.json()
                raw = result.get("result", "0x")
                if len(raw) < 130:  # too short — empty or error
                    return 0.0, 0.0
                # Decode ABI-encoded uint256[] — offset + length + values
                hex_data = raw[2:]  # strip 0x
                offset = int(hex_data[0:64], 16) * 2  # in hex chars
                length = int(hex_data[offset:offset + 64], 16)
                vals = []
                for i in range(length):
                    start = offset + 64 + i * 64
                    vals.append(int(hex_data[start:start + 64], 16) / 1e6)
                up_bal = vals[0] if len(vals) > 0 else 0.0
                dn_bal = vals[1] if len(vals) > 1 else 0.0
                return up_bal, dn_bal

    async def get_direction(self, up_token_id, down_token_id):
        """Returns (direction, confidence, up_shares, down_shares).

        Fail-open on errors — returns cached values, never blocks entries.
        """
        now = time.time()
        if now - self._last_poll < self.POLL_INTERVAL:
            total = max(1, self._cached_up_shares + self._cached_down_shares)
            conf = max(self._cached_up_shares, self._cached_down_shares) / total
            return (self._cached_direction, conf,
                    self._cached_up_shares, self._cached_down_shares)

        try:
            up_bal, down_bal = await self._get_balances(
                self.WALLET, up_token_id, down_token_id,
            )
        except Exception:
            self._poll_errors += 1
            # FAIL-OPEN: return cached (or None), don't block entries
            return (self._cached_direction, 0.0,
                    self._cached_up_shares, self._cached_down_shares)

        self._last_poll = now

        # Delta tracking: rolling 15s window of share changes
        _token_key = (str(up_token_id), str(down_token_id))
        if _token_key != self._last_token_ids:
            # New market window — reset delta baselines
            self._prev_up_shares = up_bal
            self._prev_down_shares = down_bal
            self._delta_up = 0.0
            self._delta_down = 0.0
            self._delta_history = []
            self._last_token_ids = _token_key
        else:
            # Same window — compute per-poll delta and accumulate
            _d_up = up_bal - self._prev_up_shares
            _d_dn = down_bal - self._prev_down_shares
            self._prev_up_shares = up_bal
            self._prev_down_shares = down_bal
            # Add to rolling history
            self._delta_history.append((now, _d_up, _d_dn))
            # Prune entries older than 15s
            _cutoff = now - 15.0
            self._delta_history = [(t, u, d) for t, u, d in self._delta_history
                                   if t >= _cutoff]
            # Cumulative delta = sum over rolling window
            self._delta_up = sum(u for _, u, _ in self._delta_history)
            self._delta_down = sum(d for _, _, d in self._delta_history)
        self._delta_ts = now

        self._cached_up_shares = up_bal
        self._cached_down_shares = down_bal
        total = up_bal + down_bal

        if total < self.MIN_POSITION_SHARES:
            self._cached_direction = None
            self._pending_flip = None
            self._pending_flip_count = 0
            return None, 0.0, up_bal, down_bal

        up_pct = up_bal / total
        down_pct = down_bal / total

        # Determine raw signal from wallet position
        if up_pct >= self.CONFIRM_RATIO:
            raw_dir = "UP"
        elif down_pct >= self.CONFIRM_RATIO:
            raw_dir = "DOWN"
        else:
            return (self._cached_direction, max(up_pct, down_pct),
                    up_bal, down_bal)

        # Persistence check for direction changes
        if raw_dir != self._cached_direction and self._cached_direction is not None:
            if raw_dir == self._pending_flip:
                self._pending_flip_count += 1
            else:
                self._pending_flip = raw_dir
                self._pending_flip_count = 1
            if self._pending_flip_count >= self.PERSISTENCE_POLLS:
                self._cached_direction = raw_dir
                self._pending_flip = None
                self._pending_flip_count = 0
            # else: keep old direction until persistence met
        else:
            self._cached_direction = raw_dir
            self._pending_flip = None
            self._pending_flip_count = 0

        return (self._cached_direction, max(up_pct, down_pct),
                up_bal, down_bal)

    def should_confirm(self, engine_side: str) -> tuple:
        """Check if wallet agrees with engine direction.

        Fail-open: only blocks when wallet CLEARLY disagrees with persistence.
        Returns (confirmed: bool, reason: str).
        """
        total = self._cached_up_shares + self._cached_down_shares
        if total < self.MIN_POSITION_SHARES:
            return True, "no_position"  # Don't block on absence
        if self._cached_direction is None:
            return True, "no_signal"    # Don't block on unclear signal
        if self._cached_direction == engine_side:
            return True, "agree"
        # Wallet disagrees — only block if wallet has strong conviction
        opp_shares = (self._cached_up_shares if self._cached_direction == "UP"
                      else self._cached_down_shares)
        opp_pct = opp_shares / total
        if opp_pct >= self.FLIP_RATIO:
            return False, f"disagree_{self._cached_direction}_{opp_pct:.0%}"
        return True, "weak_disagree"  # Disagrees but not strongly enough — pass
