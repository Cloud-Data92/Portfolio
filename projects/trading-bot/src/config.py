"""
Configuration loader for the BTC arbitrage bot.
Loads settings from environment variables / .env file.

Credentials are loaded from ~/.config/polybot/.env (secure, chmod 600).
Non-sensitive overrides can go in the project-root .env file.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Polygon RPC endpoint — polygon-rpc.com disabled their free tier
POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"

# Load SECURE credentials first (private key, API keys)
secure_env = Path.home() / ".config" / "polybot" / ".env"
if secure_env.exists():
    load_dotenv(secure_env)

# Then load project-root .env for non-sensitive overrides
project_env = Path(__file__).parent.parent / ".env"
load_dotenv(project_env, override=False)  # Don't override secure values


@dataclass
class TradingIdentity:
    """Resolved wallet identity — who signs, who owns tokens, who can redeem.

    This model makes the ownership relationships explicit so the rest of the
    codebase never has to guess.

    Fields:
        eoa:              Account.from_key(private_key).address — always available
        funder:           Who the CLOB treats as maker/token owner.
                          = eoa when POLYMARKET_FUNDER is unset.
        display_proxy:    POLYMARKET_PROXY_ADDRESS — UI-only balance display (Polymarket web wallet)
        signature_type:   0=EOA, 1=Magic.link, 2=Gnosis Safe
        can_self_redeem:  True when eoa == funder (signer owns tokens and can redeem them)
    """
    eoa: str
    funder: str
    display_proxy: str
    signature_type: int
    can_self_redeem: bool

    def to_dict(self) -> dict:
        """Serialize for API responses (safe — no secrets)."""
        return {
            "eoa": self.eoa,
            "funder": self.funder,
            "display_proxy": self.display_proxy,
            "signature_type": self.signature_type,
            "can_self_redeem": self.can_self_redeem,
        }


@dataclass
class Config:
    """Bot configuration loaded from environment variables."""
    
    # Polymarket credentials
    private_key: str
    api_key: str
    api_secret: str
    api_passphrase: str
    signature_type: int
    funder: str
    host: str
    
    # Trading settings
    target_pair_cost: float
    order_size: int
    dry_run: bool
    min_time_remaining: int
    scan_interval: float
    
    # Alerts
    telegram_token: str
    telegram_chat_id: str
    discord_webhook: str
    
    # Dashboard
    dashboard_enabled: bool
    dashboard_host: str
    dashboard_port: int

    # Feeds
    btc_price_source: str

    # History
    alert_history_enabled: bool

    # Logging
    log_level: str
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        
        private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        if not private_key:
            print("WARNING: POLYMARKET_PRIVATE_KEY not set!")
        
        return cls(
            # Polymarket credentials
            private_key=private_key,
            api_key=os.getenv("POLYMARKET_API_KEY", ""),
            api_secret=os.getenv("POLYMARKET_API_SECRET", ""),
            api_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE", ""),
            signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0")),
            funder=os.getenv("POLYMARKET_FUNDER", ""),
            host=os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com"),
            
            # Trading settings
            target_pair_cost=float(os.getenv("TARGET_PAIR_COST", "0.97")),
            order_size=int(os.getenv("ORDER_SIZE", "5")),
            dry_run=os.getenv("DRY_RUN", "true").lower() == "true",
            min_time_remaining=int(os.getenv("MIN_TIME_REMAINING", "120")),
            scan_interval=float(os.getenv("SCAN_INTERVAL", "1")),
            
            # Alerts
            telegram_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            discord_webhook=os.getenv("DISCORD_WEBHOOK_URL", ""),
            
            # Dashboard
            dashboard_enabled=os.getenv("DASHBOARD_ENABLED", "true").lower() == "true",
            dashboard_host=os.getenv("DASHBOARD_HOST", "127.0.0.1"),
            dashboard_port=int(os.getenv("DASHBOARD_PORT", "8080")),

            # Feeds
            btc_price_source=os.getenv("BTC_PRICE_SOURCE", "binance"),

            # History
            alert_history_enabled=os.getenv("ALERT_HISTORY_ENABLED", "true").lower() == "true",

            # Logging
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
    
    def validate(self) -> list[str]:
        """Validate configuration and return list of errors."""
        errors = []
        
        if not self.private_key:
            errors.append("POLYMARKET_PRIVATE_KEY is required")
        elif not self.private_key.startswith("0x"):
            errors.append("POLYMARKET_PRIVATE_KEY must start with 0x")
            
        if self.target_pair_cost >= 1.0:
            errors.append("TARGET_PAIR_COST must be less than 1.0")
        if self.target_pair_cost < 0.9:
            errors.append("TARGET_PAIR_COST seems too low (< 0.9)")
            
        if self.order_size < 5:
            errors.append("ORDER_SIZE must be at least 5")
            
        if self.signature_type not in [0, 1, 2]:
            errors.append("POLYMARKET_SIGNATURE_TYPE must be 0, 1, or 2")
            
        if self.signature_type == 1 and not self.funder:
            errors.append("POLYMARKET_FUNDER required for Magic.link wallets (signature_type=1)")
            
        return errors
    
    def get_wallet_address(self) -> str:
        """Derive public wallet address from private key (safe to display)."""
        if not self.private_key:
            return "(no key)"
        try:
            from eth_account import Account
            return Account.from_key(self.private_key).address
        except Exception:
            return "(derivation failed)"

    def resolve_identity(self) -> TradingIdentity:
        """Resolve the full wallet identity from configuration.

        Determines: who signs transactions (EOA), who owns tokens (funder),
        and whether the signer can redeem tokens directly.
        """
        eoa = self.get_wallet_address()
        funder = self.funder if self.funder else eoa
        proxy = os.getenv("POLYMARKET_PROXY_ADDRESS", "").strip()
        can_self_redeem = (eoa.lower() == funder.lower())

        return TradingIdentity(
            eoa=eoa,
            funder=funder,
            display_proxy=proxy,
            signature_type=self.signature_type,
            can_self_redeem=can_self_redeem,
        )

    def get_onchain_balance(self, address: str = "") -> float:
        """Fetch USDC balance directly from Polygon RPC.

        Checks both native USDC and USDC.e (bridged). Polymarket CLOB uses
        USDC.e (0x2791...) but user may hold native USDC (0x3c49...).
        Returns whichever is higher, so dashboard shows useful balance.

        Args:
            address: Wallet to query. Defaults to EOA from private key.
        """
        addr = address or self.get_wallet_address()
        if not addr or addr.startswith("("):
            return 0.0
        try:
            import httpx
            data_suffix = "0x70a08231000000000000000000000000" + addr[2:].lower()

            # Native USDC on Polygon
            native = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
            # USDC.e (bridged) — used by Polymarket CLOB exchange
            bridged = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

            native_bal = 0.0
            bridged_bal = 0.0
            for token, label in [(native, "native"), (bridged, "bridged")]:
                try:
                    resp = httpx.post(POLYGON_RPC, json={
                        "jsonrpc": "2.0", "method": "eth_call",
                        "params": [{"to": token, "data": data_suffix}, "latest"],
                        "id": 1,
                    }, timeout=10.0)
                    bal = int(resp.json().get("result", "0x0"), 16) / 1e6
                    if label == "native":
                        native_bal = bal
                    else:
                        bridged_bal = bal
                except Exception:
                    pass

            # Return the higher balance (user may have either)
            return max(native_bal, bridged_bal)
        except Exception:
            return 0.0

    def get_usdce_balance(self, address: str = "") -> float:
        """Fetch USDC.e (bridged) balance — the token Polymarket CLOB uses.

        Args:
            address: Wallet to query. Defaults to EOA from private key.
        """
        addr = address or self.get_wallet_address()
        if not addr or addr.startswith("("):
            return 0.0
        try:
            import httpx
            usdce = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            resp = httpx.post(POLYGON_RPC, json={
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": usdce, "data": "0x70a08231000000000000000000000000" + addr[2:].lower()}, "latest"],
                "id": 1,
            }, timeout=10.0)
            return int(resp.json().get("result", "0x0"), 16) / 1e6
        except Exception:
            return 0.0

    def get_native_usdc_balance(self, address: str = "") -> float:
        """Fetch native USDC balance (0x3c49...) — NOT used by Polymarket CLOB.

        Args:
            address: Wallet to query. Defaults to EOA from private key.
        """
        addr = address or self.get_wallet_address()
        if not addr or addr.startswith("("):
            return 0.0
        try:
            import httpx
            native = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
            resp = httpx.post(POLYGON_RPC, json={
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": native, "data": "0x70a08231000000000000000000000000" + addr[2:].lower()}, "latest"],
                "id": 1,
            }, timeout=10.0)
            return int(resp.json().get("result", "0x0"), 16) / 1e6
        except Exception:
            return 0.0

    def get_pol_balance(self, address: str = "") -> float:
        """Fetch native POL (MATIC) balance from Polygon RPC.

        Args:
            address: Wallet to query. Defaults to EOA from private key.
        """
        addr = address or self.get_wallet_address()
        if not addr or addr.startswith("("):
            return 0.0
        try:
            import httpx
            resp = httpx.post(POLYGON_RPC, json={
                "jsonrpc": "2.0", "method": "eth_getBalance",
                "params": [addr, "latest"],
                "id": 1,
            }, timeout=10.0)
            return int(resp.json().get("result", "0x0"), 16) / 1e18
        except Exception:
            return 0.0

    def get_proxy_usdce_balance(self) -> float:
        """Fetch USDC.e balance of the Polymarket proxy wallet.

        The proxy address (from POLYMARKET_PROXY_ADDRESS env) is the smart-contract
        wallet that Polymarket's web UI uses. It holds the real CLOB funds and may
        differ from the bot's EOA address.
        """
        proxy = os.getenv("POLYMARKET_PROXY_ADDRESS", "").strip()
        if not proxy or len(proxy) != 42:
            return 0.0
        try:
            import httpx
            usdce = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            resp = httpx.post(POLYGON_RPC, json={
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": usdce, "data": "0x70a08231000000000000000000000000" + proxy[2:].lower()}, "latest"],
                "id": 1,
            }, timeout=10.0)
            return int(resp.json().get("result", "0x0"), 16) / 1e6
        except Exception:
            return 0.0

    def get_all_balances(self, address: str = "", gas_address: str = "") -> dict:
        """Fetch owner balances plus the signer gas balance.

        Args:
            address: Token owner / trading wallet to query. Defaults to EOA.
            gas_address: Address that pays Polygon gas. Defaults to ``address``.
        """
        addr = address or self.get_wallet_address()
        gas_addr = gas_address or addr
        proxy_bal = self.get_proxy_usdce_balance()
        return {
            "usdc": self.get_onchain_balance(addr),          # max(native, bridged)
            "usdce": self.get_usdce_balance(addr),           # USDC.e specifically (CLOB uses this)
            "nativeUsdc": self.get_native_usdc_balance(addr), # Native USDC (0x3c49...)
            "proxyUsdce": proxy_bal,                          # Polymarket proxy wallet USDC.e
            "pol": self.get_pol_balance(gas_addr),
            "address": addr,
            "gasAddress": gas_addr,
            "proxyAddress": os.getenv("POLYMARKET_PROXY_ADDRESS", ""),
        }

    def print_summary(self):
        """Print configuration summary. NEVER prints secrets."""
        print("\n" + "=" * 60)
        print("CONFIGURATION")
        print("=" * 60)

        addr = self.get_wallet_address()
        print(f"Wallet:           {addr}")
        print(f"Private Key:      {'[OK] Loaded from ~/.config/polybot/.env' if self.private_key else '[--] Not set'}")
        print(f"Signature Type:   {self.signature_type} ({'EOA' if self.signature_type == 0 else 'Magic.link' if self.signature_type == 1 else 'Gnosis Safe'})")
        print(f"API Credentials:  {'[OK] Set' if self.api_key and self.api_secret and self.api_passphrase else '[--] Incomplete'}")
        print(f"Target Cost:      ${self.target_pair_cost:.3f} (need {(1 - self.target_pair_cost) * 100:.1f}% edge)")
        print(f"Order Size:       {self.order_size} shares")
        print(f"Mode:             {'DRY RUN (simulation)' if self.dry_run else 'LIVE TRADING'}")
        print(f"Min Time Left:    {self.min_time_remaining}s")
        print(f"Telegram:         {'[OK]' if self.telegram_token and self.telegram_chat_id else '[--]'}")
        print(f"Discord:          {'[OK]' if self.discord_webhook else '[--]'}")
        print(f"Credentials:      ~/.config/polybot/.env")
        print("=" * 60 + "\n")


# Global config instance
config = Config.from_env()
