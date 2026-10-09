"""Tests for the windowed aggregator — no Kafka, no network."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from streamforge.consumer.aggregator import WindowedAggregator
from streamforge.schemas import Side, TradeEvent

_BASE = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _trade(symbol: str, price: str, qty: str, side: Side, offset_s: int = 0) -> TradeEvent:
    return TradeEvent(
        symbol=symbol,
        trade_id=offset_s + 1,
        price=price,
        quantity=qty,
        side=side,
        timestamp=_BASE + timedelta(seconds=offset_s),
    )


def test_aggregator_rejects_zero_window() -> None:
    import pytest

    with pytest.raises(ValueError):
        WindowedAggregator(0)


def test_single_trade_does_not_emit() -> None:
    agg = WindowedAggregator(window_seconds=10)
    closed = agg.add(_trade("BTCUSDT", "50000", "1", Side.BUY))
    assert closed == []


def test_window_closes_when_time_advances() -> None:
    agg = WindowedAggregator(window_seconds=10)
    agg.add(_trade("BTCUSDT", "50000", "1", Side.BUY, offset_s=0))
    # A trade 15 seconds later is in the next 10s window, so the first closes.
    closed = agg.add(_trade("BTCUSDT", "50100", "1", Side.BUY, offset_s=15))
    assert len(closed) == 1
    assert closed[0].symbol == "BTCUSDT"
    assert closed[0].trade_count == 1


def test_aggregate_sums_volumes_by_side() -> None:
    agg = WindowedAggregator(window_seconds=60)
    agg.add(_trade("BTCUSDT", "50000", "2", Side.BUY, offset_s=0))
    agg.add(_trade("BTCUSDT", "50000", "3", Side.SELL, offset_s=1))
    closed = agg.flush()
    assert len(closed) == 1
    assert Decimal(closed[0].buy_volume) == Decimal("2")
    assert Decimal(closed[0].sell_volume) == Decimal("3")


def test_aggregate_high_low_tracking() -> None:
    agg = WindowedAggregator(window_seconds=60)
    agg.add(_trade("BTCUSDT", "50000", "1", Side.BUY, offset_s=0))
    agg.add(_trade("BTCUSDT", "51000", "1", Side.BUY, offset_s=1))
    agg.add(_trade("BTCUSDT", "49000", "1", Side.BUY, offset_s=2))
    closed = agg.flush()
    assert Decimal(closed[0].high) == Decimal("51000")
    assert Decimal(closed[0].low) == Decimal("49000")


def test_aggregate_vwap_is_volume_weighted() -> None:
    # (100*2 + 200*1) / 3 = 400/3 = 133.33...
    agg = WindowedAggregator(window_seconds=60)
    agg.add(_trade("BTCUSDT", "100", "2", Side.BUY, offset_s=0))
    agg.add(_trade("BTCUSDT", "200", "1", Side.BUY, offset_s=1))
    closed = agg.flush()
    expected = (Decimal(100) * 2 + Decimal(200) * 1) / Decimal(3)
    assert Decimal(closed[0].vwap) == expected


def test_flush_clears_state() -> None:
    agg = WindowedAggregator(window_seconds=60)
    agg.add(_trade("BTCUSDT", "50000", "1", Side.BUY))
    first = agg.flush()
    second = agg.flush()
    assert len(first) == 1
    assert second == []


def test_multiple_symbols_tracked_independently() -> None:
    agg = WindowedAggregator(window_seconds=60)
    agg.add(_trade("BTCUSDT", "50000", "1", Side.BUY, offset_s=0))
    agg.add(_trade("ETHUSDT", "3000", "10", Side.BUY, offset_s=1))
    closed = agg.flush()
    assert len(closed) == 2
    symbols = {c.symbol for c in closed}
    assert symbols == {"BTCUSDT", "ETHUSDT"}
