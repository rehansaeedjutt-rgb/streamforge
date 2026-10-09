"""Tests for streamforge.schemas."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from streamforge.schemas import Side, TradeEvent, WindowedAggregate

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _trade() -> TradeEvent:
    return TradeEvent(
        symbol="BTCUSDT",
        trade_id=12345,
        price="50000.00",
        quantity="0.001",
        side=Side.BUY,
        timestamp=_TS,
    )


def test_trade_notional_exact() -> None:
    t = _trade()
    assert t.notional == Decimal("50.00000")


def test_trade_roundtrip_wire() -> None:
    original = _trade()
    restored = TradeEvent.from_wire(original.to_wire())
    assert restored.symbol == original.symbol
    assert restored.trade_id == original.trade_id
    assert restored.price == original.price
    assert restored.quantity == original.quantity
    assert restored.side == original.side
    assert restored.timestamp == original.timestamp


def test_trade_immutable() -> None:
    t = _trade()
    with pytest.raises(ValidationError):
        t.symbol = "ETHUSDT"  # type: ignore[misc]


def test_side_enum_values() -> None:
    assert Side.BUY.value == "BUY"
    assert Side.SELL.value == "SELL"


def test_aggregate_vwap_decimal() -> None:
    agg = WindowedAggregate(
        symbol="BTCUSDT",
        window_start=_TS,
        window_end=_TS,
        trade_count=10,
        buy_volume="5",
        sell_volume="5",
        vwap="50123.45",
        high="50500",
        low="49500",
        last_price="50200",
    )
    assert agg.vwap_decimal == Decimal("50123.45")
