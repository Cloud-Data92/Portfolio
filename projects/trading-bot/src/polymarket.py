"""
Polymarket API client for the BTC 15-minute arbitrage bot.
Handles market discovery, price fetching, and order execution.
"""

import time
import json
import httpx
from typing import Optional, Tuple, List
from dataclasses import dataclass, field
from datetime import datetime, timezone


def _redeem_position_value(position: dict) -> float:
    """Best-effort redeemable value from a position payload."""
    for key in ("payout", "size"):
        try:
            val = float(position.get(key, 0) or 0)
        except (TypeError, ValueError):
            val = 0.0
        if val > 0:
            return val
    return 0.0


def _classify_redeem_failure(msg: str) -> str:
    """Normalize redeem failures into stable classes."""
    text = (msg or "").lower()
    if "insufficient funds" in text or "gas * price + value" in text or "out of gas" in text:
        return "no_gas"
    if "nonce" in text:
        return "nonce"
    return "revert"


def _ctf_redeem_index_sets(outcome_index: int) -> list[int]:
    """Redeem only the winning outcome when we know which token we hold."""
    try:
        idx = int(outcome_index)
    except (TypeError, ValueError):
        return [1, 2]
    if idx == 0:
        return [1]
    if idx == 1:
        return [2]
    return [1, 2]


def _buffered_gas_limit(estimated_gas: int | None, default_gas: int, cap: int = 400_000) -> int:
    """Apply a small buffer to gas estimates while keeping a sane ceiling."""
    try:
        est = int(estimated_gas or 0)
    except (TypeError, ValueError):
        est = 0
    if est <= 0:
        return default_gas
    return max(default_gas, min(cap, int(est * 1.2)))


@dataclass
class Market:
    """Represents a BTC Up/Down market."""
    slug: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    end_time: datetime
    
    @property
    def time_remaining(self) -> int:
        """Seconds until market closes."""
        now = datetime.now(timezone.utc)
        delta = self.end_time - now
        return max(0, int(delta.total_seconds()))
    
    @property
    def is_active(self) -> bool:
        """Whether market is still open for trading."""
        return self.time_remaining > 0


@dataclass 
class PriceInfo:
    """Current prices for UP and DOWN."""
    up_ask: float  # Price to buy UP
    down_ask: float  # Price to buy DOWN
    up_bid: float  # Price to sell UP
    down_bid: float  # Price to sell DOWN
    up_depth: float  # Available liquidity on UP ask
    down_depth: float  # Available liquidity on DOWN ask
    timestamp: float
    
    @property
    def total_ask(self) -> float:
        """Combined cost to buy both sides."""
        return self.up_ask + self.down_ask
    
    @property
    def is_stale(self, max_age: float = 30.0) -> bool:
        """Whether price data is too old."""
        return (time.time() - self.timestamp) > max_age


@dataclass
class RedeemResult:
    """Per-condition result from a redeem attempt."""
    condition_id: str
    market_title: str
    contract_path: str       # "CTF", "NegRisk", or "skipped"
    owner_address: str       # Who we redeemed for
    status: str              # "success", "skipped", "failed"
    failure_class: str = ""  # "", "wrong_owner", "no_gas", "nonce", "revert", "already_redeemed", "no_balance"
    tx_hash: str = ""
    gas_used: int = 0
    value: float = 0.0       # USDC.e redeemed

    def to_dict(self) -> dict:
        return {
            "condition_id": self.condition_id,
            "market_title": self.market_title,
            "contract_path": self.contract_path,
            "owner_address": self.owner_address,
            "status": self.status,
            "failure_class": self.failure_class,
            "tx_hash": self.tx_hash,
            "gas_used": self.gas_used,
            "value": self.value,
        }


class PolymarketClient:
    """Client for interacting with Polymarket CLOB API."""
    
    BASE_URL = "https://clob.polymarket.com"
    GAMMA_URL = "https://gamma-api.polymarket.com"
    
    def __init__(self, private_key: str = "", api_key: str = "", 
                 api_secret: str = "", api_passphrase: str = "",
                 signature_type: int = 0, funder: str = ""):
        """Initialize the client."""
        self.private_key = private_key
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.signature_type = signature_type
        self.funder = funder
        
        self._client = httpx.Client(timeout=30.0)
        self._clob_client = None  # Lazy load py-clob-client
        
    def _get_clob_client(self):
        """Lazy load the py-clob-client for authenticated operations."""
        if self._clob_client is None:
            try:
                from py_clob_client.client import ClobClient
                from py_clob_client.clob_types import ApiCreds
                
                creds = ApiCreds(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    api_passphrase=self.api_passphrase,
                )
                
                self._clob_client = ClobClient(
                    host=self.BASE_URL,
                    key=self.private_key,
                    chain_id=137,  # Polygon mainnet
                    signature_type=self.signature_type,
                    funder=self.funder if self.funder else None,
                    creds=creds,
                )
            except ImportError:
                print("py-clob-client not installed. Run: pip install py-clob-client")
                raise
                
        return self._clob_client
    
    def find_active_btc_market(self) -> Optional[Market]:
        """
        Find the currently active BTC 15-minute Up/Down market.
        Returns None if no active market found.
        """
        try:
            # Search for BTC 15-minute markets
            response = self._client.get(
                f"{self.GAMMA_URL}/markets",
                params={
                    "closed": "false",
                    "limit": 50,
                }
            )
            response.raise_for_status()
            markets = response.json()
            
            # Filter for BTC 15-min Up/Down markets
            for market in markets:
                slug = market.get("slug", "").lower()
                question = market.get("question", "").lower()
                
                # Match BTC 15-minute markets
                if ("btc" in slug or "bitcoin" in question) and "15" in slug:
                    if "up" in slug or "down" in slug or "updown" in slug:
                        return self._parse_market(market)
                        
            # Alternative: search by specific pattern
            response = self._client.get(
                f"{self.GAMMA_URL}/markets",
                params={
                    "slug_contains": "btc-updown-15m",
                    "closed": "false",
                }
            )
            if response.status_code == 200:
                markets = response.json()
                if markets:
                    return self._parse_market(markets[0])
                    
            return None
            
        except Exception as e:
            print(f"Error finding market: {e}")
            return None
    
    def _parse_market(self, data: dict) -> Optional[Market]:
        """Parse market data from API response."""
        try:
            # Get token IDs for UP and DOWN outcomes
            tokens = data.get("tokens", [])
            up_token = None
            down_token = None
            
            for token in tokens:
                outcome = token.get("outcome", "").lower()
                if "up" in outcome or "goes up" in outcome:
                    up_token = token.get("token_id")
                elif "down" in outcome or "goes down" in outcome:
                    down_token = token.get("token_id")
            
            if not up_token or not down_token:
                return None
            
            # Parse end time
            end_time_str = data.get("end_date_iso") or data.get("end_time")
            if end_time_str:
                end_time = datetime.fromisoformat(end_time_str.replace("Z", "+00:00"))
            else:
                # Fallback: assume 15 minutes from now
                end_time = datetime.now(timezone.utc)
            
            return Market(
                slug=data.get("slug", ""),
                condition_id=data.get("condition_id", ""),
                up_token_id=up_token,
                down_token_id=down_token,
                end_time=end_time,
            )
            
        except Exception as e:
            print(f"Error parsing market: {e}")
            return None
    
    def get_prices(self, market: Market) -> Optional[PriceInfo]:
        """
        Get current bid/ask prices for a market.
        Uses order book data for accurate execution prices.
        """
        try:
            # Get order books for both tokens
            up_book = self._get_order_book(market.up_token_id)
            down_book = self._get_order_book(market.down_token_id)
            
            if not up_book or not down_book:
                return None
            
            # Best ask = lowest sell price (what we pay to buy)
            # Best bid = highest buy price (what we get to sell)
            up_ask = float(up_book["asks"][0]["price"]) if up_book.get("asks") else 1.0
            up_bid = float(up_book["bids"][0]["price"]) if up_book.get("bids") else 0.0
            down_ask = float(down_book["asks"][0]["price"]) if down_book.get("asks") else 1.0
            down_bid = float(down_book["bids"][0]["price"]) if down_book.get("bids") else 0.0
            
            # Get depth (available size at best ask)
            up_depth = float(up_book["asks"][0]["size"]) if up_book.get("asks") else 0.0
            down_depth = float(down_book["asks"][0]["size"]) if down_book.get("asks") else 0.0
            
            return PriceInfo(
                up_ask=up_ask,
                down_ask=down_ask,
                up_bid=up_bid,
                down_bid=down_bid,
                up_depth=up_depth,
                down_depth=down_depth,
                timestamp=time.time(),
            )
            
        except Exception as e:
            print(f"Error getting prices: {e}")
            return None
    
    def _get_order_book(self, token_id: str) -> Optional[dict]:
        """Fetch order book for a token."""
        try:
            response = self._client.get(
                f"{self.BASE_URL}/book",
                params={"token_id": token_id}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching order book: {e}")
            return None
    
    def get_balance(self) -> float:
        """Get USDC balance on Polymarket via get_balance_allowance().
        Uses AssetType.COLLATERAL to fetch the USDC (collateral) balance.
        Returns balance in dollars (divides raw 6-decimal units by 1e6).
        """
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self._get_clob_client()
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            result = client.get_balance_allowance(params)
            # result is a dict with 'balance' and 'allowance' keys
            # balance is returned in raw 6-decimal USDC.e units (e.g. 16217862 = $16.22)
            if result and isinstance(result, dict):
                raw = float(result.get("balance", 0))
                return raw / 1e6 if raw > 1000 else raw  # Auto-detect raw vs dollar
            return float(result) / 1e6 if result and float(result) > 1000 else (float(result) if result else 0.0)
        except ImportError:
            # Fallback: try the older API if types aren't available
            try:
                client = self._get_clob_client()
                result = client.get_balance_allowance()
                if result and isinstance(result, dict):
                    raw = float(result.get("balance", 0))
                    return raw / 1e6 if raw > 1000 else raw
                return float(result) / 1e6 if result and float(result) > 1000 else (float(result) if result else 0.0)
            except Exception as e2:
                print(f"Error getting balance (fallback): {e2}")
                return 0.0
        except Exception as e:
            print(f"Error getting balance: {e}")
            return 0.0
    
    def place_order(
        self,
        token_id: str,
        side: str,  # "BUY" or "SELL"
        size: float,
        price: float,
        order_type: str = "GTC",  # "GTC", "FOK", "GTD"
        expiration: int = 0,
        tick_size: str = "0.01",  # "0.1", "0.01", "0.001", "0.0001"
        neg_risk: bool = True,  # BTC Up/Down markets use neg_risk
        post_only: bool = False,  # True = maker only (zero fees), rejects if would cross
    ) -> Optional[str]:
        """
        Place a limit order on the Polymarket CLOB.
        Returns order ID if successful, None otherwise.

        Args:
            token_id: ERC1155 token ID for the outcome
            side: "BUY" or "SELL"
            size: Number of shares
            price: Price per share (rounded to tick_size)
            order_type: "GTC", "FOK", or "GTD"
            expiration: Unix timestamp for GTD orders
            tick_size: Market tick size precision
            neg_risk: Whether market uses neg-risk exchange (multi-outcome)
            post_only: If True, order only posts to book (maker). Rejects if it would cross.
        """
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import BUY, SELL

            client = self._get_clob_client()

            # Round price to tick size
            ts = float(tick_size)
            price = round(round(price / ts) * ts, len(tick_size.split(".")[-1]) if "." in tick_size else 2)

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=BUY if side.upper() == "BUY" else SELL,
            )
            if expiration > 0:
                order_args.expiration = str(expiration)

            # Map string to OrderType enum
            ot_map = {"GTC": OrderType.GTC, "FOK": OrderType.FOK, "GTD": OrderType.GTD}
            ot = ot_map.get(order_type.upper(), OrderType.GTC)

            options = PartialCreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            )
            signed_order = client.create_order(order_args, options)
            response = client.post_order(signed_order, ot, post_only=post_only)

            if response and ("orderID" in response or "orderId" in response):
                order_id = response.get("orderID") or response.get("orderId")
                # Extract fill data if available (same as FOK path)
                taking = float(response.get("takingAmount", 0))
                making = float(response.get("makingAmount", 0))
                if taking > 0 or making > 0:
                    print(f"    Limit fill: taking={taking:.4f} making={making:.4f}")
                    self.last_fill_shares = taking if side.upper() == "BUY" else making
                    self.last_fill_cost = making if side.upper() == "BUY" else taking
                return order_id
            print(f"  Order response (no ID): {response}")
            return None

        except Exception as e:
            err_str = str(e).lower()
            print(f"Error placing order: {e}")
            # Re-raise balance/allowance errors so caller can retry
            if "balance" in err_str or "allowance" in err_str:
                raise
            return None

    def place_market_order(
        self,
        token_id: str,
        side: str,
        amount: float,
        tick_size: str = "0.01",
        neg_risk: bool = True,
    ) -> Optional[str]:
        """Place a market order (Fill-or-Kill).

        For BUY: amount is USDC to spend.
        For SELL: amount is shares to sell.

        Returns order_id or None.
        Also sets self.last_fill_shares with actual fill quantity.
        """
        self.last_fill_shares = 0.0
        self.last_fill_cost = 0.0
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import BUY, SELL

            client = self._get_clob_client()

            market_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=BUY if side.upper() == "BUY" else SELL,
            )

            options = PartialCreateOrderOptions(
                tick_size=tick_size,
                neg_risk=neg_risk,
            )
            signed_order = client.create_market_order(market_args, options)
            response = client.post_order(signed_order, OrderType.FOK)

            if response and "orderID" in response:
                order_id = response["orderID"]
                # Extract actual fill amounts from response
                taking = float(response.get("takingAmount", 0))
                making = float(response.get("makingAmount", 0))
                status = response.get("status", "unknown")
                if taking > 0 or making > 0:
                    print(f"    FOK fill: taking={taking:.4f} making={making:.4f} status={status}")
                # For BUY: takingAmount = shares received, makingAmount = USDC spent
                # For SELL: takingAmount = USDC received, makingAmount = shares sold
                self.last_fill_shares = taking if side.upper() == "BUY" else making
                self.last_fill_cost = making if side.upper() == "BUY" else taking
                return order_id
            if response:
                print(f"    FOK response (no orderID): {response}")
            return None

        except Exception as e:
            err_str = str(e).lower()
            print(f"Error placing market order: {e}")
            # Re-raise balance/allowance errors so caller can retry
            if "balance" in err_str or "allowance" in err_str:
                raise
            return None

    def set_allowances(self) -> bool:
        """Approve USDC.e collateral on Polymarket exchange contracts.

        Only COLLATERAL (USDC.e) approval is needed for buying.
        Conditional token (ERC1155) approvals require specific token IDs
        and are handled per-market when selling.
        """
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self._get_clob_client()
            # Approve USDC.e collateral (for buying shares)
            client.update_balance_allowance(
                params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            return True
        except Exception as e:
            print(f"Error setting allowances: {e}")
            return False

    def set_token_allowance(self, token_id: str) -> bool:
        """Approve a specific conditional token for selling on the exchange."""
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self._get_clob_client()
            client.update_balance_allowance(
                params=BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                )
            )
            return True
        except Exception as e:
            print(f"Error setting token allowance: {e}")
            return False

    def get_token_balance(self, token_id: str) -> float:
        """Get actual conditional token balance from CLOB.

        Returns the number of shares we actually hold for this token.
        Critical for selling — we can only sell what we actually have.
        """
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self._get_clob_client()
            # First refresh the CLOB's view of our on-chain balance
            client.update_balance_allowance(
                params=BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                )
            )
            # Now query the actual balance
            result = client.get_balance_allowance(
                params=BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                )
            )
            if result and isinstance(result, dict):
                raw = float(result.get("balance", 0))
                # Conditional token balances are in raw units (6 decimals like USDC)
                bal = raw / 1e6 if raw > 1000 else raw
                return bal
            return 0.0
        except Exception as e:
            print(f"    Token balance query error: {str(e)[:80]}")
            return -1.0  # Return -1 to indicate error, not 0

    def get_clob_balance(self) -> float:
        """Get USDC.e balance available on the Polymarket CLOB exchange.
        Returns balance in dollars (divides raw 6-decimal units by 1e6).
        """
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            client = self._get_clob_client()
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            result = client.get_balance_allowance(params)
            if result and isinstance(result, dict):
                raw = float(result.get("balance", 0))
                allowance = result.get("allowance", "?")
                # API returns raw 6-decimal units (e.g. 16217862 = $16.22)
                bal = raw / 1e6 if raw > 1000 else raw
                # Debug: CLOB balance raw={raw} → ${bal:.4f}
                return bal
            return 0.0
        except Exception as e:
            print(f"Error getting CLOB balance: {e}")
            return 0.0
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order."""
        try:
            client = self._get_clob_client()
            response = client.cancel(order_id)
            return response is not None
        except Exception as e:
            print(f"Error canceling order: {e}")
            return False
    
    def get_order_status(self, order_id: str) -> Optional[dict]:
        """Get current status of an order."""
        try:
            client = self._get_clob_client()
            return client.get_order(order_id)
        except Exception as e:
            print(f"Error getting order status: {e}")
            return None

    def cancel_all_orders(self) -> bool:
        """Cancel all open orders. Safety cleanup on startup or before settlement."""
        try:
            client = self._get_clob_client()
            client.cancel_all()
            return True
        except Exception as e:
            print(f"Error canceling all orders: {e}")
            return False

    def cancel_orders(self, order_ids: list[str]) -> bool:
        """Cancel multiple orders by ID in a single request."""
        if not order_ids:
            return True
        try:
            client = self._get_clob_client()
            client.cancel_orders(order_ids)
            return True
        except Exception as e:
            print(f"Error canceling orders: {e}")
            return False

    def get_open_orders(self, market: str = "", asset_id: str = "") -> list[dict]:
        """List open orders, optionally filtered by market or asset_id."""
        try:
            from py_clob_client.clob_types import OpenOrderParams
            client = self._get_clob_client()
            params = OpenOrderParams()
            if market:
                params.market = market
            if asset_id:
                params.asset_id = asset_id
            return client.get_orders(params) or []
        except Exception as e:
            print(f"Error getting open orders: {e}")
            return []

    def place_orders_batch(
        self,
        orders: list[dict],
        tick_size: str = "0.01",
        neg_risk: bool = True,
    ) -> list[Optional[str]]:
        """Place multiple orders in one API call.

        Each order dict: {token_id, side, size, price, order_type?, post_only?}
        Returns list of order IDs (None for failures).
        """
        if not orders:
            return []
        try:
            from py_clob_client.clob_types import (
                OrderArgs, OrderType, PartialCreateOrderOptions, PostOrdersArgs,
            )
            from py_clob_client.order_builder.constants import BUY, SELL

            client = self._get_clob_client()
            options = PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk)
            ot_map = {"GTC": OrderType.GTC, "FOK": OrderType.FOK, "GTD": OrderType.GTD}

            batch_args = []
            for o in orders:
                ts = float(tick_size)
                price = round(round(o["price"] / ts) * ts,
                              len(tick_size.split(".")[-1]) if "." in tick_size else 2)
                args = OrderArgs(
                    token_id=o["token_id"],
                    price=price,
                    size=o["size"],
                    side=BUY if o["side"].upper() == "BUY" else SELL,
                )
                signed = client.create_order(args, options)
                ot = ot_map.get(o.get("order_type", "GTC").upper(), OrderType.GTC)
                batch_args.append(PostOrdersArgs(
                    order=signed,
                    orderType=ot,
                    postOnly=o.get("post_only", False),
                ))

            response = client.post_orders(batch_args)
            # Response is a list of results per order
            results = []
            if isinstance(response, list):
                for r in response:
                    if isinstance(r, dict):
                        results.append(r.get("orderID") or r.get("orderId"))
                    else:
                        results.append(None)
            else:
                results = [None] * len(orders)
            return results

        except Exception as e:
            print(f"Error placing batch orders: {e}")
            return [None] * len(orders)

    def get_redeemable_positions(self, wallet_address: str) -> list[dict]:
        """Get all redeemable (settled winning) positions from data API."""
        try:
            r = self._client.get(
                f"https://data-api.polymarket.com/positions",
                params={"user": wallet_address.lower(), "sizeThreshold": "0", "redeemable": "true"},
                timeout=10,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            # data API redeemable=true already filters for winning positions
            # curPrice can be 0 or 1 after settlement — the redeemable flag is authoritative
            return [p for p in data if float(p.get("size", 0)) > 0]
        except Exception as e:
            print(f"  Error fetching redeemable positions: {e}")
            return []

    def _ensure_neg_risk_approval(self, w3, account) -> bool:
        """Ensure the NegRisk Adapter is approved as operator on CTF (ERC-1155).

        Required before NegRisk Adapter can pull conditional tokens from our wallet.
        This is a one-time on-chain transaction; subsequent calls are a no-op.
        """
        from web3 import Web3

        CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

        CHECK_ABI = [{
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "operator", "type": "address"},
            ],
            "name": "isApprovedForAll",
            "outputs": [{"name": "", "type": "bool"}],
            "stateMutability": "view",
            "type": "function",
        }]
        APPROVE_ABI = [{
            "inputs": [
                {"name": "operator", "type": "address"},
                {"name": "approved", "type": "bool"},
            ],
            "name": "setApprovalForAll",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function",
        }]

        ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_CONTRACT), abi=CHECK_ABI + APPROVE_ABI)
        adapter_addr = Web3.to_checksum_address(NEG_RISK_ADAPTER)

        # Check if already approved
        try:
            already = ctf.functions.isApprovedForAll(account.address, adapter_addr).call()
            if already:
                return True
        except Exception:
            pass  # If check fails, try to approve anyway

        # Send setApprovalForAll tx
        try:
            nonce = w3.eth.get_transaction_count(account.address)
            gas_price = w3.eth.gas_price
            tx = ctf.functions.setApprovalForAll(adapter_addr, True).build_transaction({
                "from": account.address,
                "nonce": nonce,
                "gas": 100_000,
                "gasPrice": int(gas_price * 1.5),
                "chainId": 137,
            })
            signed = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=45)
            if receipt.status == 1:
                print("  ✓ Approved NegRisk Adapter as operator on CTF (one-time)")
                return True
            else:
                print("  ✗ setApprovalForAll reverted")
                return False
        except Exception as e:
            print(f"  ✗ Approval tx failed: {str(e)[:80]}")
            return False

    def ensure_exchange_approvals(self) -> bool:
        """Ensure BOTH exchange contracts are approved as operators on CTF.

        Required one-time on-chain tx before we can SELL conditional tokens.
        The exchange pulls ERC-1155 tokens from our wallet during sell orders.
        Without this approval, all sell orders fail with 'not enough balance/allowance'.

        Approves:
        - Standard Exchange: 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
        - NegRisk Exchange:  0xC5d563A36AE78145C45a50134d48A1215220f80a
        """
        from web3 import Web3
        from eth_account import Account

        # Connect to Polygon RPC
        rpcs = ["https://polygon-bor-rpc.publicnode.com", "https://polygon.drpc.org"]
        w3 = None
        for rpc in rpcs:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
                if w3.is_connected():
                    break
            except Exception:
                continue
        if not w3 or not w3.is_connected():
            print("  ✗ No RPC connection for exchange approvals")
            return False

        if not self.private_key:
            print("  ✗ No private key for exchange approvals")
            return False
        account = Account.from_key(self.private_key)

        CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        EXCHANGES = [
            ("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", "Standard Exchange"),
            ("0xC5d563A36AE78145C45a50134d48A1215220f80a", "NegRisk Exchange"),
        ]

        CHECK_ABI = [{
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "operator", "type": "address"},
            ],
            "name": "isApprovedForAll",
            "outputs": [{"name": "", "type": "bool"}],
            "stateMutability": "view",
            "type": "function",
        }]
        APPROVE_ABI = [{
            "inputs": [
                {"name": "operator", "type": "address"},
                {"name": "approved", "type": "bool"},
            ],
            "name": "setApprovalForAll",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function",
        }]

        ctf = w3.eth.contract(
            address=Web3.to_checksum_address(CTF_CONTRACT),
            abi=CHECK_ABI + APPROVE_ABI,
        )
        all_ok = True

        for exch_addr, name in EXCHANGES:
            exch = Web3.to_checksum_address(exch_addr)
            try:
                already = ctf.functions.isApprovedForAll(account.address, exch).call()
                if already:
                    print(f"  ✓ {name} already approved for CTF sells")
                    continue
            except Exception:
                pass

            try:
                nonce = w3.eth.get_transaction_count(account.address)
                gas_price = w3.eth.gas_price
                tx = ctf.functions.setApprovalForAll(exch, True).build_transaction({
                    "from": account.address,
                    "nonce": nonce,
                    "gas": 100_000,
                    "gasPrice": int(gas_price * 1.5),
                    "chainId": 137,
                })
                signed = account.sign_transaction(tx)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=45)
                if receipt.status == 1:
                    print(f"  ✓ Approved {name} as CTF operator (one-time)")
                else:
                    print(f"  ✗ {name} approval reverted")
                    all_ok = False
            except Exception as e:
                print(f"  ✗ {name} approval failed: {str(e)[:80]}")
                all_ok = False

        return all_ok

    def split_position(self, condition_id: str, amount_usdc: float) -> dict:
        """Split USDC into equal UP + DOWN outcome tokens via CTF contract.

        This is the inverse of merging/redeeming: it takes USDC and creates
        equal amounts of all outcome tokens. For a binary market (UP/DOWN),
        splitting $10 gives you 10 UP shares + 10 DOWN shares.

        Cost: ~0.02-0.04 MATIC gas (no Polymarket fees).
        Requires USDC.e balance on Polygon (on-chain, not CLOB balance).

        Args:
            condition_id: The market's conditionId (hex string)
            amount_usdc: Amount of USDC.e to split (e.g., 10.0 = $10)

        Returns: {success: bool, up_shares: float, down_shares: float, tx_hash: str, error: str}
        """
        from web3 import Web3

        CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        USDCE_TOKEN = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

        CTF_SPLIT_ABI = [{
            "inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "partition", "type": "uint256[]"},
                {"name": "amount", "type": "uint256"},
            ],
            "name": "splitPosition",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function",
        }]

        ERC20_APPROVE_ABI = [{
            "inputs": [
                {"name": "spender", "type": "address"},
                {"name": "amount", "type": "uint256"},
            ],
            "name": "approve",
            "outputs": [{"name": "", "type": "bool"}],
            "stateMutability": "nonpayable",
            "type": "function",
        }]

        ERC20_ALLOWANCE_ABI = [{
            "inputs": [
                {"name": "owner", "type": "address"},
                {"name": "spender", "type": "address"},
            ],
            "name": "allowance",
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        }]

        result = {"success": False, "up_shares": 0.0, "down_shares": 0.0, "tx_hash": "", "error": ""}

        rpcs = [
            "https://polygon-bor-rpc.publicnode.com",
            "https://polygon.drpc.org",
        ]

        w3 = None
        for rpc in rpcs:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
                if w3.is_connected():
                    break
            except Exception:
                continue
        if not w3 or not w3.is_connected():
            result["error"] = "No RPC connection"
            return result

        account = w3.eth.account.from_key(self.private_key)

        # Convert USDC amount to 6-decimal integer (USDC.e has 6 decimals)
        amount_raw = int(amount_usdc * 1_000_000)
        if amount_raw < 1_000_000:  # Minimum $1
            result["error"] = f"Amount too small: ${amount_usdc:.2f} (min $1)"
            return result

        try:
            # 1. Ensure USDC.e is approved for CTF contract
            usdc_contract = w3.eth.contract(
                address=Web3.to_checksum_address(USDCE_TOKEN),
                abi=ERC20_APPROVE_ABI + ERC20_ALLOWANCE_ABI,
            )
            current_allowance = usdc_contract.functions.allowance(
                account.address, Web3.to_checksum_address(CTF_CONTRACT)
            ).call()

            nonce = w3.eth.get_transaction_count(account.address)
            gas_price = w3.eth.gas_price

            if current_allowance < amount_raw:
                # Approve max uint256 (one-time, saves gas on future splits)
                max_uint = 2**256 - 1
                approve_tx = usdc_contract.functions.approve(
                    Web3.to_checksum_address(CTF_CONTRACT), max_uint
                ).build_transaction({
                    "from": account.address,
                    "nonce": nonce,
                    "gas": 100_000,
                    "gasPrice": int(gas_price * 1.5),
                    "chainId": 137,
                })
                signed = account.sign_transaction(approve_tx)
                tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
                if receipt.status != 1:
                    result["error"] = "USDC.e approval failed"
                    return result
                nonce += 1
                print(f"  ✓ Approved USDC.e for CTF split (one-time)")

            # 2. Split position
            cond_bytes = bytes.fromhex(condition_id[2:]) if condition_id.startswith("0x") else bytes.fromhex(condition_id)
            zero_parent = b"\x00" * 32

            ctf = w3.eth.contract(
                address=Web3.to_checksum_address(CTF_CONTRACT), abi=CTF_SPLIT_ABI
            )

            split_tx = ctf.functions.splitPosition(
                Web3.to_checksum_address(USDCE_TOKEN),
                zero_parent,
                cond_bytes,
                [1, 2],  # partition: indexSets for binary (YES=1, NO=2)
                amount_raw,
            ).build_transaction({
                "from": account.address,
                "nonce": nonce,
                "gas": 300_000,
                "gasPrice": int(gas_price * 1.5),
                "chainId": 137,
            })
            signed = account.sign_transaction(split_tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=45)

            if receipt.status == 1:
                shares = amount_usdc  # 1 USDC = 1 share of each side
                result["success"] = True
                result["up_shares"] = shares
                result["down_shares"] = shares
                result["tx_hash"] = tx_hash.hex()
                print(f"  ✓ SPLIT ${amount_usdc:.2f} → {shares:.1f} UP + {shares:.1f} DOWN shares")
            else:
                result["error"] = "Split transaction reverted"
                print(f"  ✗ Split reverted for ${amount_usdc:.2f}")

        except Exception as e:
            result["error"] = str(e)[:100]
            print(f"  ✗ Split error: {result['error']}")

        return result

    def redeem_positions(self, wallet_address: str, identity=None) -> List[RedeemResult]:
        """Auto-redeem all winning positions on settled markets.

        Strategy: try CTF base contract first (works for most BTC Up/Down
        5-minute markets), then fall back to NegRisk Adapter for true
        neg-risk markets.

        CTF: redeemPositions(collateral, parentCollection, conditionId, [1,2])
        NegRisk: redeemPositions(conditionId, [yesAmount, noAmount])

        Args:
            wallet_address: Address to query for redeemable positions.
            identity: Optional TradingIdentity. If provided and can_self_redeem
                      is False, all positions are skipped with a warning.

        Returns:
            List of RedeemResult — one per condition attempted.
        """
        import time as _time
        from web3 import Web3

        results: List[RedeemResult] = []

        # ── Safety check: can we redeem? ────────────────────────────────
        if identity and not identity.can_self_redeem:
            # Signer (EOA) does not own tokens (funder does). Can't redeem.
            positions = self.get_redeemable_positions(wallet_address)
            for p in positions:
                if float(p.get("size", 0)) > 0:
                    results.append(RedeemResult(
                        condition_id=p.get("conditionId", ""),
                        market_title=p.get("title", "?"),
                        contract_path="skipped",
                        owner_address=identity.funder,
                        status="skipped",
                        failure_class="wrong_owner",
                        value=float(p.get("size", 0)),
                    ))
            if results:
                print(f"  ⚠ Cannot auto-redeem: signer ({identity.eoa[:10]}...) ≠ "
                      f"token owner ({identity.funder[:10]}...). "
                      f"Skipped {len(results)} position(s). Redeem manually via Polymarket UI.")
            return results

        positions = self.get_redeemable_positions(wallet_address)
        if not positions:
            return results

        # Group by conditionId
        by_condition = {}
        for p in positions:
            cid = p.get("conditionId", "")
            if not cid:
                continue
            size = float(p.get("size", 0))
            neg_risk = p.get("negativeRisk", p.get("negRisk", False))
            if isinstance(neg_risk, str):
                neg_risk = neg_risk.lower() == "true"
            if cid not in by_condition:
                by_condition[cid] = {
                    "size": 0.0,
                    "title": p.get("title", ""),
                    "outcome": p.get("outcome", "?"),
                    "outcomeIndex": int(p.get("outcomeIndex", 0)),
                    "asset": p.get("asset", ""),  # ERC1155 tokenId
                    "negative_risk": bool(neg_risk),
                }
            by_condition[cid]["size"] += size
            by_condition[cid]["negative_risk"] = by_condition[cid]["negative_risk"] or bool(neg_risk)

        CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
        USDCE_TOKEN = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

        CTF_REDEEM_ABI = [{
            "inputs": [
                {"name": "collateralToken", "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSets", "type": "uint256[]"},
            ],
            "name": "redeemPositions",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function",
        }]
        NEG_RISK_REDEEM_ABI = [{
            "inputs": [
                {"name": "conditionId", "type": "bytes32"},
                {"name": "amounts", "type": "uint256[]"},
            ],
            "name": "redeemPositions",
            "outputs": [],
            "stateMutability": "nonpayable",
            "type": "function",
        }]
        BALANCE_OF_ABI = [{
            "inputs": [
                {"name": "account", "type": "address"},
                {"name": "id", "type": "uint256"},
            ],
            "name": "balanceOf",
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        }]

        rpcs = [
            "https://polygon-bor-rpc.publicnode.com",
            "https://polygon.drpc.org",
        ]

        w3 = None
        for rpc in rpcs:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
                if w3.is_connected():
                    break
            except Exception:
                continue
        if not w3 or not w3.is_connected():
            for cond_id, info in by_condition.items():
                results.append(RedeemResult(
                    condition_id=cond_id,
                    market_title=info["title"],
                    contract_path="skipped",
                    owner_address=wallet_address,
                    status="failed",
                    failure_class="no_rpc",
                    value=info["size"],
                ))
            return results

        ctf_contract = w3.eth.contract(
            address=Web3.to_checksum_address(CTF_CONTRACT), abi=CTF_REDEEM_ABI + BALANCE_OF_ABI
        )
        neg_risk_contract = w3.eth.contract(
            address=Web3.to_checksum_address(NEG_RISK_ADAPTER), abi=NEG_RISK_REDEEM_ABI
        )
        account = w3.eth.account.from_key(self.private_key)

        # Validate: signer must be the token owner
        if identity and account.address.lower() != identity.funder.lower():
            print(f"  ⚠ ASSERTION FAILED: account.address ({account.address}) != identity.funder ({identity.funder})")
            for cond_id, info in by_condition.items():
                results.append(RedeemResult(
                    condition_id=cond_id,
                    market_title=info["title"],
                    contract_path="skipped",
                    owner_address=identity.funder,
                    status="skipped",
                    failure_class="wrong_owner",
                    value=info["size"],
                ))
            return results

        def _pending_nonce(fallback: int | None = None) -> int:
            try:
                pending = w3.eth.get_transaction_count(account.address, "pending")
            except TypeError:
                pending = w3.eth.get_transaction_count(account.address)
            except Exception:
                if fallback is None:
                    raise
                return fallback
            return pending if fallback is None else max(pending, fallback)

        nonce = _pending_nonce()
        gas_price = w3.eth.gas_price

        for cond_id, info in by_condition.items():
            try:
                cond_bytes = bytes.fromhex(cond_id[2:]) if cond_id.startswith("0x") else bytes.fromhex(cond_id)
                zero_parent = b"\x00" * 32

                # Pre-check: verify on-chain token balance > 0 before burning gas
                asset_id = info.get("asset", "")
                if asset_id:
                    try:
                        on_chain_check = ctf_contract.functions.balanceOf(
                            account.address, int(asset_id)
                        ).call()
                        if on_chain_check == 0:
                            print(f"  ⏭ Skip {info['title'][:40]} — 0 tokens on-chain (already redeemed)")
                            results.append(RedeemResult(
                                condition_id=cond_id,
                                market_title=info["title"],
                                contract_path="skipped",
                                owner_address=account.address,
                                status="skipped",
                                failure_class="already_redeemed",
                                value=0.0,
                            ))
                            continue
                    except Exception:
                        pass  # balanceOf failed, proceed with TX attempt

                # Strategy 1: CTF base contract — works for non-NegRisk markets
                try:
                    submitted_nonce = None
                    index_sets = _ctf_redeem_index_sets(info.get("outcomeIndex", 0))
                    tx_fn = ctf_contract.functions.redeemPositions(
                        Web3.to_checksum_address(USDCE_TOKEN),
                        zero_parent,
                        cond_bytes,
                        index_sets,
                    )
                    gas_limit = 200_000
                    try:
                        gas_limit = _buffered_gas_limit(
                            tx_fn.estimate_gas({"from": account.address}),
                            default_gas=gas_limit,
                        )
                    except Exception:
                        pass
                    tx = tx_fn.build_transaction({
                        "from": account.address,
                        "nonce": nonce,
                        "gas": gas_limit,
                        "gasPrice": int(gas_price * 1.5),
                        "chainId": 137,
                    })
                    signed = account.sign_transaction(tx)
                    submitted_nonce = nonce
                    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=45)
                    nonce = _pending_nonce(submitted_nonce + 1)

                    if receipt.status == 1:
                        print(f"  ✓ Redeemed ${info['size']:.2f} ({info['outcome']}) via CTF · {info['title'][:40]}")
                        results.append(RedeemResult(
                            condition_id=cond_id,
                            market_title=info["title"],
                            contract_path="CTF",
                            owner_address=account.address,
                            status="success",
                            tx_hash=tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash),
                            gas_used=receipt.get("gasUsed", 0) if isinstance(receipt, dict) else getattr(receipt, "gasUsed", 0),
                            value=info["size"],
                        ))
                        _time.sleep(1)
                        continue
                    else:
                        raise Exception("CTF reverted")
                except Exception as ctf_err:
                    ctf_msg = str(ctf_err)[:60]
                    ctf_fc = _classify_redeem_failure(str(ctf_err))
                    if submitted_nonce is not None:
                        try:
                            nonce = _pending_nonce(submitted_nonce + 1)
                        except Exception:
                            nonce = submitted_nonce + 1
                    elif ctf_fc == "nonce":
                        try:
                            nonce = _pending_nonce(nonce)
                        except Exception:
                            pass
                    if not info.get("negative_risk", False) or ctf_fc == "no_gas":
                        results.append(RedeemResult(
                            condition_id=cond_id,
                            market_title=info["title"],
                            contract_path="CTF",
                            owner_address=account.address,
                            status="failed",
                            failure_class=ctf_fc,
                            value=info["size"],
                        ))
                        continue

                # Strategy 2: NegRisk Adapter — for true neg-risk markets
                try:
                    self._ensure_neg_risk_approval(w3, account)
                    nonce = _pending_nonce(nonce)
                    submitted_nonce = None

                    asset_id = info.get("asset", "")
                    if asset_id:
                        on_chain_bal = ctf_contract.functions.balanceOf(
                            account.address, int(asset_id)
                        ).call()
                    else:
                        on_chain_bal = int(info["size"] * 1_000_000)

                    idx = info["outcomeIndex"]
                    if idx == 0:
                        amounts = [on_chain_bal, 0]
                    else:
                        amounts = [0, on_chain_bal]

                    tx = neg_risk_contract.functions.redeemPositions(
                        cond_bytes,
                        amounts,
                    ).build_transaction({
                        "from": account.address,
                        "nonce": nonce,
                        "gas": 400_000,
                        "gasPrice": int(gas_price * 1.5),
                        "chainId": 137,
                    })
                    signed = account.sign_transaction(tx)
                    submitted_nonce = nonce
                    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
                    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=45)
                    nonce = _pending_nonce(submitted_nonce + 1)

                    if receipt.status == 1:
                        print(f"  ✓ Redeemed ${info['size']:.2f} ({info['outcome']}) via NegRisk · {info['title'][:40]}")
                        results.append(RedeemResult(
                            condition_id=cond_id,
                            market_title=info["title"],
                            contract_path="NegRisk",
                            owner_address=account.address,
                            status="success",
                            tx_hash=tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash),
                            gas_used=receipt.get("gasUsed", 0) if isinstance(receipt, dict) else getattr(receipt, "gasUsed", 0),
                            value=info["size"],
                        ))
                    else:
                        print(f"  ✗ Both CTF+NegRisk reverted for {info['title'][:40]}")
                        results.append(RedeemResult(
                            condition_id=cond_id,
                            market_title=info["title"],
                            contract_path="NegRisk",
                            owner_address=account.address,
                            status="failed",
                            failure_class="revert",
                            value=info["size"],
                        ))
                except Exception as neg_err:
                    neg_msg = str(neg_err)[:60]
                    fc = _classify_redeem_failure(neg_msg)
                    print(f"  ✗ Redeem failed: CTF={ctf_msg} NegRisk={neg_msg}")
                    results.append(RedeemResult(
                        condition_id=cond_id,
                        market_title=info["title"],
                        contract_path="NegRisk",
                        owner_address=account.address,
                        status="failed",
                        failure_class=fc,
                        value=info["size"],
                    ))
                    if "nonce" in neg_msg.lower():
                        try:
                            nonce = _pending_nonce(nonce)
                        except Exception:
                            pass

            except Exception as e:
                print(f"  ✗ Redeem error for {info['title'][:35]}: {str(e)[:60]}")
                results.append(RedeemResult(
                    condition_id=cond_id,
                    market_title=info["title"],
                    contract_path="unknown",
                    owner_address=account.address,
                    status="failed",
                    failure_class="revert",
                    value=info["size"],
                ))

            _time.sleep(2)

        return results

    @staticmethod
    def summarize_redeem_results(results: List[RedeemResult]) -> dict:
        """Aggregate RedeemResult list into legacy summary dict.

        Returns: {"redeemed": int, "value": float, "errors": int, "skipped": int, "results": list}
        """
        redeemed = sum(1 for r in results if r.status == "success")
        value = round(sum(r.value for r in results if r.status == "success"), 2)
        errors = sum(1 for r in results if r.status == "failed")
        skipped = sum(1 for r in results if r.status == "skipped")
        return {
            "redeemed": redeemed,
            "value": value,
            "errors": errors,
            "skipped": skipped,
            "results": [r.to_dict() for r in results],
        }

    def close(self):
        """Close HTTP client."""
        self._client.close()


def create_client(config) -> PolymarketClient:
    """Create PolymarketClient from config."""
    return PolymarketClient(
        private_key=config.private_key,
        api_key=config.api_key,
        api_secret=config.api_secret,
        api_passphrase=config.api_passphrase,
        signature_type=config.signature_type,
        funder=config.funder,
    )
