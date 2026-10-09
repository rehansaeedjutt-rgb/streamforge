"""Command-line entry point for streamforge."""

from __future__ import annotations

import argparse
import asyncio
import sys

from streamforge import __version__
from streamforge.config import get_settings
from streamforge.consumer.aggregator import consume_and_aggregate
from streamforge.producer.binance_producer import run_producer
from streamforge.sink.sqlite_sink import SQLiteAggregateSink


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="streamforge",
        description="Real-time crypto data pipeline: Kafka + aggregation + storage.",
    )
    parser.add_argument("--version", action="version", version=f"streamforge {__version__}")

    sub = parser.add_subparsers(dest="command", required=True)

    produce = sub.add_parser("produce", help="Stream Binance trades into Kafka.")
    produce.add_argument("--symbols", default="btcusdt,ethusdt")
    produce.add_argument("--duration", type=int, default=15, help="Seconds to run.")

    consume = sub.add_parser("consume", help="Read Kafka, aggregate, and store.")
    consume.add_argument("--max-messages", type=int, default=200)
    consume.add_argument("--timeout-ms", type=int, default=8000)

    return parser


def _run_produce(args: argparse.Namespace) -> int:
    symbols = [s.strip().lower() for s in args.symbols.split(",") if s.strip()]
    published = asyncio.run(run_producer(symbols, duration_seconds=args.duration))
    print(f"\nPublished {published} trades to Kafka.")
    print("Now run: python -m streamforge consume")
    return 0


def _run_consume(args: argparse.Namespace) -> int:
    settings = get_settings()
    aggregates = consume_and_aggregate(
        max_messages=args.max_messages,
        timeout_ms=args.timeout_ms,
    )

    if not aggregates:
        print("\nNo aggregates emitted. Is the producer running? Is Kafka up?")
        return 1

    async def write_all() -> int:
        sink = SQLiteAggregateSink(settings.sqlite_db_path)
        await sink.initialize()
        written = 0
        try:
            for agg in aggregates:
                inserted = await sink.write(agg)
                if inserted:
                    written += 1
            total = await sink.count()
        finally:
            await sink.close()
        return total

    total = asyncio.run(write_all())

    print(f"\nReceived {len(aggregates)} aggregates from the pipeline.")
    print(f"Wrote new aggregates to {settings.sqlite_db_path}")
    print(f"Total rows in DB: {total}")
    print()
    print("Sample (first 3):")
    for agg in aggregates[:3]:
        print(
            f"  {agg.symbol:<8} {agg.window_start.isoformat()} "
            f"trades={agg.trade_count} vwap={agg.vwap}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "produce":
        return _run_produce(args)
    if args.command == "consume":
        return _run_consume(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
