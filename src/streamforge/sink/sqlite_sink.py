"""SQLite persistence for windowed aggregates.

Same approach as tickstream: plain aiosqlite, no ORM, exact Decimal
values stored as TEXT. Composite primary key keeps the sink idempotent
if we replay a window.
"""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import structlog

from streamforge.schemas import WindowedAggregate

logger = structlog.get_logger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS aggregates (
    symbol TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    trade_count INTEGER NOT NULL,
    buy_volume TEXT NOT NULL,
    sell_volume TEXT NOT NULL,
    vwap TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    last_price TEXT NOT NULL,
    PRIMARY KEY (symbol, window_start)
);
CREATE INDEX IF NOT EXISTS idx_aggregates_window
    ON aggregates (window_start DESC);
"""


class SQLiteAggregateSink:
    """Writes WindowedAggregate rows to SQLite. Idempotent on replay."""

    def __init__(self, db_path: str | Path = "streamforge.db") -> None:
        self._db_path = str(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        if self._conn is not None:
            return
        self._conn = await aiosqlite.connect(self._db_path)
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()
        logger.info("sink.initialized", path=self._db_path)

    async def write(self, aggregate: WindowedAggregate) -> bool:
        """Insert a single aggregate. Returns True if newly inserted."""
        if self._conn is None:
            msg = "initialize() must be called before write()"
            raise RuntimeError(msg)
        cursor = await self._conn.execute(
            "INSERT OR IGNORE INTO aggregates "
            "(symbol, window_start, window_end, trade_count, buy_volume, "
            " sell_volume, vwap, high, low, last_price) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                aggregate.symbol,
                aggregate.window_start.isoformat(),
                aggregate.window_end.isoformat(),
                aggregate.trade_count,
                aggregate.buy_volume,
                aggregate.sell_volume,
                aggregate.vwap,
                aggregate.high,
                aggregate.low,
                aggregate.last_price,
            ),
        )
        inserted = cursor.rowcount if cursor.rowcount is not None else 0
        await self._conn.commit()
        return inserted > 0

    async def count(self) -> int:
        if self._conn is None:
            msg = "initialize() must be called before count()"
            raise RuntimeError(msg)
        async with self._conn.execute("SELECT COUNT(*) FROM aggregates") as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
