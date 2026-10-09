"""Binance WebSocket -> Kafka producer.

Reuses the WebSocket pattern from tickstream but instead of writing to
SQLite directly, publishes each trade to Kafka. That way multiple
downstream consumers can subscribe without contending for the same
WebSocket connection.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import aclosing
from datetime import UTC, datetime
from typing import Any

import structlog
import websockets
from kafka import KafkaProducer

from streamforge.config import get_settings
from streamforge.schemas import Side, TradeEvent

logger = structlog.get_logger(__name__)


class BinanceKafkaProducer:
    """Publishes Binance trades to a Kafka topic.

    Uses the sync KafkaProducer from kafka-python because its async story
    is still rough. Sending is fast — microseconds for a local broker —
    so blocking the event loop briefly is acceptable here. If this ever
    becomes a bottleneck, switch to aiokafka.
    """

    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None) -> None:
        settings = get_settings()
        self._servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self._topic = topic or settings.kafka_topic_trades
        self._producer: KafkaProducer | None = None

    def _ensure_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self._servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # durability matters more than latency here
                retries=3,
                linger_ms=10,
            )
        return self._producer

    def publish(self, trade: TradeEvent) -> None:
        """Publish a single trade. Blocks briefly until the send is queued."""
        producer = self._ensure_producer()
        # Symbol as key → all trades for the same pair land on the same
        # partition, so ordering is preserved per symbol.
        producer.send(self._topic, key=trade.symbol, value=trade.to_wire())

    def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close(timeout=5)
            self._producer = None


def _parse_binance_message(raw: str | bytes) -> TradeEvent | None:
    """Convert a combined-stream message to a TradeEvent, or None on skip."""
    try:
        payload: dict[str, Any] = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("producer.invalid_json", raw=str(raw)[:200])
        return None

    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    try:
        is_buyer_maker = bool(data["m"])
        return TradeEvent(
            symbol=str(data["s"]),
            trade_id=int(data["t"]),
            price=str(data["p"]),
            quantity=str(data["q"]),
            side=Side.SELL if is_buyer_maker else Side.BUY,
            timestamp=datetime.fromtimestamp(int(data["T"]) / 1000, tz=UTC),
        )
    except (KeyError, ValueError, ArithmeticError) as exc:
        logger.warning("producer.parse_failed", error=str(exc))
        return None


async def run_producer(
    symbols: list[str],
    duration_seconds: int = 30,
) -> int:
    """Stream Binance trades into Kafka for ``duration_seconds``.

    Returns the number of trades successfully published.
    """
    settings = get_settings()
    normalized = [s.lower() for s in symbols]
    streams = "/".join(f"{s}@trade" for s in normalized)
    base = settings.binance_ws_base_url.rstrip("/")
    if base.endswith("/ws"):
        base = base[:-3]
    url = f"{base}/stream?streams={streams}"

    producer = BinanceKafkaProducer()
    published = 0
    stop_at = asyncio.get_event_loop().time() + duration_seconds

    logger.info("producer.connecting", url=url, duration_seconds=duration_seconds)

    try:
        async with aclosing(_ws_stream(url)) as stream:
            async for raw in stream:
                trade = _parse_binance_message(raw)
                if trade is None:
                    continue
                try:
                    producer.publish(trade)
                    published += 1
                except Exception as exc:
                    logger.error("producer.publish_failed", error=str(exc))
                    continue

                if published % 100 == 0:
                    logger.info("producer.progress", published=published)

                if asyncio.get_event_loop().time() >= stop_at:
                    break
    except KeyboardInterrupt:
        logger.info("producer.interrupted", published=published)
    finally:
        producer.flush()
        producer.close()
        logger.info("producer.done", published=published)

    return published


async def _ws_stream(url: str):  # type: ignore[no-untyped-def]
    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        logger.info("producer.connected")
        async for raw in ws:
            yield raw
