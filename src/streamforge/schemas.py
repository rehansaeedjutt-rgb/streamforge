"""Pydantic models for pipeline messages."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Side(StrEnum):
    """Trade aggressor side."""

    BUY = "BUY"
    SELL = "SELL"


class TradeEvent(BaseModel):
    """A single trade as it flows through the pipeline.

    This is the message format on the ``crypto.trades.raw`` topic.
    Anything that lands on Kafka must be JSON-serializable, so we keep
    Decimal -> str on the wire and restore on the consumer side.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    trade_id: int
    price: str  # Decimal as string on the wire
    quantity: str
    side: Side
    timestamp: datetime

    @property
    def price_decimal(self) -> Decimal:
        return Decimal(self.price)

    @property
    def quantity_decimal(self) -> Decimal:
        return Decimal(self.quantity)

    @property
    def notional(self) -> Decimal:
        """price * quantity, exact."""
        return self.price_decimal * self.quantity_decimal

    def to_wire(self) -> dict[str, str | int]:
        """Serialize to a Kafka-friendly dict."""
        return {
            "symbol": self.symbol,
            "trade_id": self.trade_id,
            "price": self.price,
            "quantity": self.quantity,
            "side": self.side.value,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_wire(cls, raw: dict[str, str | int]) -> TradeEvent:
        """Deserialize from a Kafka message."""
        return cls(
            symbol=str(raw["symbol"]),
            trade_id=int(raw["trade_id"]),
            price=str(raw["price"]),
            quantity=str(raw["quantity"]),
            side=Side(str(raw["side"])),
            timestamp=datetime.fromisoformat(str(raw["timestamp"])),
        )


class WindowedAggregate(BaseModel):
    """Aggregated stats for a symbol over a time window.

    Emitted once per window per symbol. Consumed by the sink.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    window_start: datetime
    window_end: datetime
    trade_count: int = Field(..., ge=0)
    buy_volume: str  # Decimal as string
    sell_volume: str
    vwap: str  # Volume-weighted average price
    high: str
    low: str
    last_price: str

    @property
    def vwap_decimal(self) -> Decimal:
        return Decimal(self.vwap)
