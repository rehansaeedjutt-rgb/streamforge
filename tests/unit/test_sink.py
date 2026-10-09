"""Tests for the SQLite aggregate sink."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from streamforge.schemas import WindowedAggregate
from streamforge.sink.sqlite_sink import SQLiteAggregateSink

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _agg(symbol: str = "BTCUSDT", start: datetime = _TS) -> WindowedAggregate:
    return WindowedAggregate(
        symbol=symbol,
        window_start=start,
        window_end=start,
        trade_count=5,
        buy_volume="1.5",
        sell_volume="2.5",
        vwap="50000.00",
        high="50100",
        low="49900",
        last_price="50050",
    )


@pytest.mark.asyncio
async def test_initialize_creates_table(tmp_path: Path) -> None:
    sink = SQLiteAggregateSink(tmp_path / "t.db")
    await sink.initialize()
    try:
        assert await sink.count() == 0
    finally:
        await sink.close()


@pytest.mark.asyncio
async def test_write_inserts_row(tmp_path: Path) -> None:
    sink = SQLiteAggregateSink(tmp_path / "t.db")
    await sink.initialize()
    try:
        inserted = await sink.write(_agg())
        assert inserted is True
        assert await sink.count() == 1
    finally:
        await sink.close()


@pytest.mark.asyncio
async def test_write_idempotent_on_replay(tmp_path: Path) -> None:
    sink = SQLiteAggregateSink(tmp_path / "t.db")
    await sink.initialize()
    try:
        first = await sink.write(_agg())
        second = await sink.write(_agg())
        assert first is True
        assert second is False
        assert await sink.count() == 1
    finally:
        await sink.close()


@pytest.mark.asyncio
async def test_write_before_initialize_raises(tmp_path: Path) -> None:
    sink = SQLiteAggregateSink(tmp_path / "t.db")
    with pytest.raises(RuntimeError, match="initialize"):
        await sink.write(_agg())


@pytest.mark.asyncio
async def test_count_before_initialize_raises(tmp_path: Path) -> None:
    sink = SQLiteAggregateSink(tmp_path / "t.db")
    with pytest.raises(RuntimeError, match="initialize"):
        await sink.count()
