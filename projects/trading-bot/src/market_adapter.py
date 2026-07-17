"""
Abstract market adapter interface for extensible exchange support.

The adapter pattern decouples the bot logic from any specific market/exchange.
To add a new market source (e.g., Kalshi), create a new class that inherits
from MarketAdapter and register it with adapter_registry.

Usage:
    from src.market_adapter import MarketAdapter, adapter_registry

    class KalshiAdapter(MarketAdapter):
        label = "Kalshi"
        description = "CFTC-regulated event contracts"
        ...

    adapter_registry["kalshi"] = KalshiAdapter
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data shapes (adapter-agnostic)
# ---------------------------------------------------------------------------

@dataclass
class MarketInfo:
    """A tradeable binary-outcome market."""
    slug: str                        # Unique identifier (e.g., "will-btc-hit-100k-2025-02-08")
    question: str                    # Human-readable question
    end_date: str                    # ISO-8601 end date
    source: str                      # Adapter id (e.g., "polymarket", "kalshi")
    condition_id: str = ""           # Internal market/condition identifier
    token_up: str = ""               # Token id for YES/UP outcome
    token_down: str = ""             # Token id for NO/DOWN outcome
    image: str = ""                  # Thumbnail URL
    active: bool = True
    extra: dict = field(default_factory=dict)


@dataclass
class PriceQuote:
    """Best-ask prices for a binary pair."""
    market_slug: str
    up_ask: float                    # Best ask price for YES/UP
    down_ask: float                  # Best ask price for NO/DOWN
    total_ask: float                 # up_ask + down_ask
    up_depth: int = 0               # Available shares at best ask
    down_depth: int = 0
    timestamp: float = 0.0
    extra: dict = field(default_factory=dict)


@dataclass
class OrderResult:
    """Result from placing an order."""
    success: bool
    order_id: str = ""
    filled_price: float = 0.0
    filled_size: int = 0
    message: str = ""
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class MarketAdapter(ABC):
    """Base class every market source must implement."""

    label: str = "Unknown"
    description: str = ""

    @abstractmethod
    def find_active_markets(self) -> list[MarketInfo]:
        """Return currently tradeable binary-outcome markets."""
        ...

    @abstractmethod
    def get_prices(self, market: MarketInfo) -> Optional[PriceQuote]:
        """Fetch best-ask prices for a market."""
        ...

    @abstractmethod
    def place_order(self, market: MarketInfo, outcome: str, size: int,
                    price: float) -> OrderResult:
        """Place a limit order. outcome is 'up' or 'down'."""
        ...

    @abstractmethod
    def get_balance(self) -> float:
        """Return available balance in USD."""
        ...

    def get_order_status(self, order_id: str) -> dict:
        """Check the status of a placed order. Optional override."""
        return {"order_id": order_id, "status": "unknown"}

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order. Optional override."""
        return False


# ---------------------------------------------------------------------------
# Polymarket adapter (wraps existing PolymarketClient)
# ---------------------------------------------------------------------------

class PolymarketAdapter(MarketAdapter):
    """Adapter wrapping the existing PolymarketClient."""

    label = "Polymarket"
    description = "Binary outcome prediction markets on Polygon"

    def __init__(self, client=None):
        self._client = client

    @property
    def client(self):
        if self._client is None:
            from .polymarket import PolymarketClient
            self._client = PolymarketClient()
        return self._client

    def find_active_markets(self) -> list[MarketInfo]:
        raw = self.client.find_btc_markets()
        markets = []
        for m in raw:
            tokens = m.get("tokens", [])
            token_up = ""
            token_down = ""
            for t in tokens:
                if t.get("outcome", "").lower() in ("yes", "up"):
                    token_up = t.get("token_id", "")
                elif t.get("outcome", "").lower() in ("no", "down"):
                    token_down = t.get("token_id", "")
            markets.append(MarketInfo(
                slug=m.get("slug", m.get("condition_id", "")),
                question=m.get("question", ""),
                end_date=m.get("end_date_iso", ""),
                source="polymarket",
                condition_id=m.get("condition_id", ""),
                token_up=token_up,
                token_down=token_down,
                image=m.get("image", ""),
                active=m.get("active", True),
                extra=m,
            ))
        return markets

    def get_prices(self, market: MarketInfo) -> Optional[PriceQuote]:
        book = self.client.get_order_book(market.token_up)
        if not book:
            return None
        up_ask = float(book.get("asks", [{}])[0].get("price", 0)) if book.get("asks") else 0.0
        up_depth = int(float(book.get("asks", [{}])[0].get("size", 0))) if book.get("asks") else 0

        book_down = self.client.get_order_book(market.token_down)
        down_ask = float(book_down.get("asks", [{}])[0].get("price", 0)) if book_down and book_down.get("asks") else 0.0
        down_depth = int(float(book_down.get("asks", [{}])[0].get("size", 0))) if book_down and book_down.get("asks") else 0

        import time
        return PriceQuote(
            market_slug=market.slug,
            up_ask=up_ask,
            down_ask=down_ask,
            total_ask=up_ask + down_ask,
            up_depth=up_depth,
            down_depth=down_depth,
            timestamp=time.time(),
        )

    def place_order(self, market: MarketInfo, outcome: str, size: int,
                    price: float) -> OrderResult:
        token_id = market.token_up if outcome == "up" else market.token_down
        try:
            result = self.client.create_and_place_order(
                token_id=token_id,
                price=price,
                size=size,
            )
            oid = ""
            if isinstance(result, dict):
                oid = result.get("orderID", result.get("id", ""))
            return OrderResult(success=True, order_id=oid, filled_price=price, filled_size=size)
        except Exception as e:
            return OrderResult(success=False, message=str(e))

    def get_balance(self) -> float:
        return self.client.get_balance()


# ---------------------------------------------------------------------------
# Adapter registry — import this to discover available adapters
# ---------------------------------------------------------------------------

adapter_registry: dict[str, type[MarketAdapter]] = {
    "polymarket": PolymarketAdapter,
}
