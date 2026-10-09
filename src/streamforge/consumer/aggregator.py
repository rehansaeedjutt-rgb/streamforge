"""Tumbling-window aggregator.

Reads trades from Kafka, groups them by symbol into fixed-size time
windows, and emits a WindowedAggregate per window per symbol when the
window closes.

Design: this is a *tumbling* window (non-overlapping). For sliding
windows I would keep multiple buckets alive; not needed yet.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import structlog
from kafka import KafkaConsumer

from streamforge.config import get_settings
from streamforge.schemas import Side, TradeEvent, WindowedAggregate

logger = structlog.get_logger(__name__)


@dataclass
class _Bucket:
    """In-flight aggregation state for one symbol in one window."""

    symbol: str
    window_start: datetime
    window_end: datetime
    trade_count: int = 0
    buy_volume: Decimal = field(default_factory=lambda: Decimal(0))
    sell_volume: Decimal = field(default_factory=lambda: Decimal(0))
    high: Decimal | None = None
    low: Decimal | None = None
    last_price: Decimal | None = None
    # Sum of price * qty for VWAP. Tracking both lets us avoid a second pass.
    notional_sum: Decimal = field(default_factory=lambda: Decimal(0))
    volume_sum: Decimal = field(default_factory=lambda: Decimal(0))

    def add(self, trade: TradeEvent) -> None:
        self.trade_count += 1
        qty = trade.quantity_decimal
        price = trade.price_decimal

        if trade.side is Side.BUY:
            self.buy_volume += qty
        else:
            self.sell_volume += qty

        if self.high is None or price > self.high:
            self.high = price
        if self.low is None or price < self.low:
            self.low = price

        self.last_price = price
        self.notional_sum += price * qty
        self.volume_sum += qty

    def to_aggregate(self) -> WindowedAggregate:
        vwap = self.notional_sum / self.volume_sum if self.volume_sum > 0 else Decimal(0)
        return WindowedAggregate(
            symbol=self.symbol,
            window_start=self.window_start,
            window_end=self.window_end,
            trade_count=self.trade_count,
            buy_volume=str(self.buy_volume),
            sell_volume=str(self.sell_volume),
            vwap=str(vwap),
            high=str(self.high if self.high is not None else Decimal(0)),
            low=str(self.low if self.low is not None else Decimal(0)),
            last_price=str(self.last_price if self.last_price is not None else Decimal(0)),
        )


class WindowedAggregator:
    """Accumulates trades into tumbling windows and yields closed ones.

    Time is driven by trade timestamps, not wall clock. This makes the
    aggregator testable without sleeping.
    """

    def __init__(self, window_seconds: int) -> None:
        if window_seconds <= 0:
            msg = "window_seconds must be positive"
            raise ValueError(msg)
        self._window = timedelta(seconds=window_seconds)
        self._current: dict[str, _Bucket] = {}

    def _window_bounds(self, ts: datetime) -> tuple[datetime, datetime]:
        """Align the window to wall-clock boundaries so windows are stable."""
        epoch_seconds = int(ts.timestamp())
        window_seconds = int(self._window.total_seconds())
        aligned = (epoch_seconds // window_seconds) * window_seconds
        start = datetime.fromtimestamp(aligned, tz=UTC)
        return start, start + self._window

    def add(self, trade: TradeEvent) -> list[WindowedAggregate]:
        """Feed a trade. Returns any aggregates that closed as a result."""
        start, end = self._window_bounds(trade.timestamp)
        closed: list[WindowedAggregate] = []

        # Anything in a different window has now closed — flush it.
        stale_symbols = [sym for sym, b in self._current.items() if b.window_start != start]
        for sym in stale_symbols:
            stale_bucket = self._current.pop(sym)
            closed.append(stale_bucket.to_aggregate())

        existing = self._current.get(trade.symbol)
        if existing is None or existing.window_start != start:
            new_bucket = _Bucket(
                symbol=trade.symbol,
                window_start=start,
                window_end=end,
            )
            self._current[trade.symbol] = new_bucket
            existing = new_bucket

        existing.add(trade)
        return closed

    def flush(self) -> list[WindowedAggregate]:
        """Close all open windows. Call this at shutdown so nothing is lost."""
        out: list[WindowedAggregate] = []
        for bucket in self._current.values():
            out.append(bucket.to_aggregate())
        self._current.clear()
        return out


def consume_and_aggregate(
    max_messages: int = 500,
    timeout_ms: int = 10_000,
) -> list[WindowedAggregate]:
    """Read from Kafka, aggregate, and return closed windows.

    Returns after ``max_messages`` have been consumed or the consumer
    idles for ``timeout_ms``. Suitable for a CLI demo run — a long-lived
    service would loop forever instead.
    """
    settings = get_settings()
    consumer = KafkaConsumer(
        settings.kafka_topic_trades,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_ms,
    )

    aggregator = WindowedAggregator(settings.aggregate_window_seconds)
    closed: list[WindowedAggregate] = []
    count = 0

    logger.info("consumer.start", topic=settings.kafka_topic_trades, max_messages=max_messages)

    try:
        for msg in consumer:
            try:
                trade = TradeEvent.from_wire(msg.value)
            except (KeyError, ValueError) as exc:
                logger.warning("consumer.bad_message", error=str(exc))
                continue

            closed.extend(aggregator.add(trade))
            count += 1
            if count >= max_messages:
                break
    finally:
        closed.extend(aggregator.flush())
        consumer.close()

    logger.info("consumer.done", consumed=count, aggregates_emitted=len(closed))
    return closed
