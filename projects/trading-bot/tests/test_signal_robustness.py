import time
import datetime as dt
from pathlib import Path
import sys
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.runner as runner_mod
from src.directional import DirectionalEngine
from src.polymarket import (
    PolymarketClient,
    _buffered_gas_limit,
    _classify_redeem_failure,
    _ctf_redeem_index_sets,
    _redeem_position_value,
)
from src.runtime_config import RuntimeConfig, should_enter as policy_should_enter
from src.runner import ContinuousRunner, MarketWindow, _gate_metric_key, _metric_slug


def test_metric_slug_sanitizes_free_text():
    assert _metric_slug("prime window, need confirmation") == "prime_window_need_confirmation"
    assert _gate_metric_key("prime window, need confirmation") == "gate_time_prime"
    assert _gate_metric_key("mystery reason!?") == "gate_other_mystery_reason"


def test_redeem_helpers_classify_gas_and_value_fallback():
    assert _redeem_position_value({"payout": 0, "size": "2.75"}) == 2.75
    assert _redeem_position_value({"payout": "1.25", "size": "2.75"}) == 1.25
    assert _classify_redeem_failure("insufficient funds for gas * price + value") == "no_gas"
    assert _classify_redeem_failure("nonce too low") == "nonce"
    assert _classify_redeem_failure("execution reverted") == "revert"
    assert _ctf_redeem_index_sets(0) == [1]
    assert _ctf_redeem_index_sets(1) == [2]
    assert _ctf_redeem_index_sets("x") == [1, 2]
    assert _buffered_gas_limit(220_542, default_gas=200_000) == 264650
    assert _buffered_gas_limit(None, default_gas=200_000) == 200_000


def test_redeem_positions_advances_nonce_after_ctf_revert(monkeypatch):
    import web3

    class FakeTxHash:
        def __init__(self, nonce: int):
            self._nonce = nonce

        def hex(self):
            return f"0x{self._nonce:064x}"

    class FakeAccount:
        address = "0xabc"

        def sign_transaction(self, tx):
            return SimpleNamespace(raw_transaction=tx)

    class FakeAccountAPI:
        def from_key(self, _private_key):
            return FakeAccount()

    class FakeContractFunctions:
        def balanceOf(self, _account, _asset_id):
            return SimpleNamespace(call=lambda: 1)

        def redeemPositions(self, *_args):
            return SimpleNamespace(
                estimate_gas=lambda params: 125_000,
                build_transaction=lambda params: dict(params),
            )

    class FakeContract:
        def __init__(self):
            self.functions = FakeContractFunctions()

    class FakeEth:
        def __init__(self):
            self.account = FakeAccountAPI()
            self.gas_price = 100
            self.sent_nonces = []
            self.receipt_statuses = [0, 1]

        def get_transaction_count(self, _address, _block_identifier=None):
            return 7 + len(self.sent_nonces)

        def contract(self, address=None, abi=None):
            return FakeContract()

        def send_raw_transaction(self, raw_tx):
            nonce = raw_tx["nonce"]
            if nonce in self.sent_nonces:
                raise Exception("nonce too low")
            self.sent_nonces.append(nonce)
            return FakeTxHash(nonce)

        def wait_for_transaction_receipt(self, _tx_hash, timeout=45):
            return SimpleNamespace(status=self.receipt_statuses.pop(0), gasUsed=21000)

    fake_eth = FakeEth()

    class FakeWeb3:
        def __init__(self, provider):
            self.provider = provider
            self.eth = fake_eth

        @staticmethod
        def HTTPProvider(url, request_kwargs=None):
            return (url, request_kwargs)

        @staticmethod
        def to_checksum_address(address):
            return address

        def is_connected(self):
            return True

    monkeypatch.setattr(web3, "Web3", FakeWeb3)

    client = PolymarketClient(private_key="0x" + "1" * 64)
    monkeypatch.setattr(
        client,
        "get_redeemable_positions",
        lambda _wallet: [
            {
                "conditionId": "0x" + "11" * 32,
                "size": "1.5",
                "title": "First market",
                "outcome": "Down",
                "outcomeIndex": 1,
                "asset": "1",
                "negativeRisk": False,
            },
            {
                "conditionId": "0x" + "22" * 32,
                "size": "2.0",
                "title": "Second market",
                "outcome": "Down",
                "outcomeIndex": 1,
                "asset": "2",
                "negativeRisk": False,
            },
        ],
    )

    results = client.redeem_positions("0xabc")

    assert fake_eth.sent_nonces == [7, 8]
    assert [r.status for r in results] == ["failed", "success"]
    assert results[0].failure_class == "revert"
    assert results[1].tx_hash == f"0x{8:064x}"


def test_execute_order_downsizes_buy_after_fok_unfilled(monkeypatch):
    class FakePolyClient:
        def __init__(self):
            self.attempt_amounts = []
            self.last_fill_shares = 0.0
            self.last_fill_cost = 0.0

        def set_allowances(self):
            return True

        def place_market_order(self, token_id, side, amount, tick_size="0.01", neg_risk=True):
            self.attempt_amounts.append(round(float(amount), 2))
            if len(self.attempt_amounts) == 1:
                self.last_fill_shares = 0.0
                self.last_fill_cost = 0.0
                return None
            self.last_fill_shares = 2.82
            self.last_fill_cost = round(float(amount), 2)
            return "oid-2"

    monkeypatch.setattr(ContinuousRunner, "_load_persisted_state", lambda self: None)
    monkeypatch.setattr(runner_mod, "create_poly_client", lambda _config: FakePolyClient())
    monkeypatch.setattr(
        runner_mod.config,
        "resolve_identity",
        lambda: SimpleNamespace(can_self_redeem=True, eoa="0xabc", funder="0xabc"),
    )
    monkeypatch.setattr(runner_mod.time, "sleep", lambda _secs: None)

    runner = ContinuousRunner(dry_run=False, bankroll=10.0, max_bet_dollars=4.0)

    order_id = runner._execute_order(
        token_id="token-1",
        side="BUY",
        shares=3.53,
        price=0.51,
        neg_risk=False,
        max_retries=2,
        spend_dollars=1.80,
    )

    assert order_id == "oid-2"
    assert runner._poly_client.attempt_amounts == [1.8, 1.62]
    assert runner._metrics["buy_fok_downsize_retries"] == 1
    assert runner._metrics["buy_fok_downsize_success"] == 1
    assert runner._last_fill_shares == 2.82


def test_entry_sizing_honors_configured_max_bet_pct(monkeypatch):
    class FakePolyClient:
        pass

    monkeypatch.setattr(ContinuousRunner, "_load_persisted_state", lambda self: None)
    monkeypatch.setattr(runner_mod, "create_poly_client", lambda _config: FakePolyClient())
    monkeypatch.setattr(
        runner_mod.config,
        "resolve_identity",
        lambda: SimpleNamespace(can_self_redeem=True, eoa="0xabc", funder="0xabc"),
    )

    runner = ContinuousRunner(
        dry_run=False,
        bankroll=20.0,
        max_bet_dollars=5.0,
        max_bet_pct=0.25,
    )
    runner.wallet_signal.enabled = False

    sizing = runner._compute_entry_sizing("DIRECTIONAL", side="DOWN", remaining=99.0)

    assert sizing["pct_bet_cap"] == 5.0
    assert sizing["base_bet"] == 5.0
    assert sizing["alloc"] == 5.0
    assert sizing["wallet_mult"] == 1.0
    assert sizing["deploy_pct"] == 1.0
    assert sizing["initial_spend"] == 5.0


def test_directional_approach_uses_no_adds():
    engine = DirectionalEngine(bankroll=20.0, max_bet_dollars=5.0)

    approach, dom_pct, budget, max_adds = engine._decide_approach(
        conviction=0.20,
        side="DOWN",
        bankroll=20.0,
        buy_price=0.45,
        up_ask=0.55,
        down_ask=0.45,
        p_smooth=0.40,
    )

    assert approach == "DIRECTIONAL"
    assert dom_pct == 1.0
    assert budget >= 1.01
    assert max_adds == 0


def test_generated_signal_preserves_runtime_gate_fields(monkeypatch):
    engine = DirectionalEngine(bankroll=20.0, max_bet_dollars=5.0)

    class FixedDatetime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 3, 17, 1, 1, 0, tzinfo=tz)

    monkeypatch.setattr(dt, "datetime", FixedDatetime)

    monkeypatch.setattr(
        engine,
        "_calc_probability",
        lambda *args, **kwargs: {
            "side": "UP",
            "conviction": 0.12,
            "edge": 0.055,
            "q_mid": 0.508,
            "p_smooth": 0.62,
            "regime": "TREND",
            "signals": {"z": 0.4},
        },
    )
    monkeypatch.setattr(engine, "_estimate_probability", lambda *args, **kwargs: ("UP", 0.60, {}))
    monkeypatch.setattr(engine, "_should_enter", lambda *args, **kwargs: (True, "confirmed", 1))

    signal, _ = engine.get_conviction_signal(
        up_price=0.51,
        down_price=0.49,
        up_depth=1000,
        down_depth=1000,
        time_remaining=240,
        interval_secs=300,
        bankroll=20.0,
        up_spread=0.01,
        down_spread=0.01,
        up_best_bid=0.50,
        down_best_bid=0.48,
        up_best_ask=0.52,
        down_best_ask=0.50,
    )

    assert signal is not None
    assert signal.edge == 0.055
    assert signal.regime == "TREND"
    assert signal.p_smooth == 0.62
    assert signal.q_mid == 0.508


def test_runtime_policy_uses_model_edge_from_signal():
    signal = runner_mod.DirectionalSignal(
        side="UP",
        confidence=0.62,
        kelly_fraction=0.1,
        bet_size=2.0,
        expected_value=0.1,
        signals={"z": 0.4},
        reason="test",
        conviction=0.12,
        approach="DIRECTIONAL",
        edge=0.055,
        regime="TREND",
        p_smooth=0.62,
        q_mid=0.508,
    )
    cfg = RuntimeConfig(price_div_mid=0.01)

    should_trade, reason = policy_should_enter(
        signal,
        {
            "fav_ask": 0.52,
            "hedge_ask": 0.50,
            "fav_ev": 0.02,
            "combined_ev": -0.02,
            "entry_drag": 0.02,
            "q_mid": signal.q_mid,
            "edge": signal.edge,
            "time_into": 90,
            "fav_cost": 0.53,
        },
        cfg,
        is_live=True,
    )

    assert should_trade
    assert reason == "passed"


def test_book_event_refreshes_quote_timestamp():
    runner = ContinuousRunner(dry_run=True, bankroll=10.0)
    runner._ws_poly_updates = 0
    window = MarketWindow(
        asset="BTC",
        interval_label="5m",
        interval_secs=300,
        slug="btc-updown-5m-test",
        title="BTC test",
        start_ts=time.time(),
        end_ts=time.time() + 300,
        up_token="up-token",
        down_token="down-token",
    )
    window._last_ws_recv_mono = 1.0
    runner._active_windows[window.slug] = window

    runner._ws_poly_handle_event({
        "event_type": "book",
        "asset_id": "up-token",
        "bids": [{"price": "0.40", "size": "10"}],
        "asks": [{"price": "0.42", "size": "12"}],
    })

    assert window._last_ws_recv_mono > 1.0
    assert window.up_best_bid == 0.40
    assert window.up_best_ask == 0.42


def test_should_enter_uses_per_window_edge_history():
    engine = DirectionalEngine(bankroll=10.0)
    edge_history = [0.08] * 5 + [0.02] * 5

    should_enter, reason, updated_ready = engine._should_enter(
        {
            "side": "UP",
            "conviction": 0.20,
            "edge": 0.02,
            "q_mid": 0.56,
            "regime": "TREND",
            "signals": {"z": 0.08},
            "confidence_w": 0.20,
        },
        time_into=30,
        up_spread=0.01,
        down_spread=0.01,
        up_depth=2500,
        down_depth=2500,
        entry_ready_count=1,
        edge_history=edge_history,
    )

    assert not should_enter
    assert "declining edge slope" in reason
    assert updated_ready == 0
    assert len(edge_history) == 11
    assert not hasattr(engine, "_edge_history")
