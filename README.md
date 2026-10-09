# streamforge

> Real-time crypto data pipeline: Binance WebSocket → Kafka → windowed aggregation → SQLite.

[![CI](https://github.com/rehansaeedjutt-rgb/streamforge/actions/workflows/ci.yml/badge.svg)](https://github.com/rehansaeedjutt-rgb/streamforge/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)

## What this is

`streamforge` is a production-style streaming pipeline for crypto market data.
It ingests trades from Binance over WebSocket, publishes them to Kafka,
aggregates them into tumbling time windows, and persists the aggregates to SQLite.

Unlike `tickstream` (which writes trades directly to SQLite in a single process),
`streamforge` separates the producer and consumer so multiple downstream services
can consume the same stream without contending for the WebSocket connection.

## Architecture

    Binance WS
        |
        v
    [ Producer ]  -- publishes TradeEvent (JSON) -->
        |
        v
    Kafka topic: crypto.trades.raw
        |
        v
    [ Consumer / Aggregator ]  -- tumbling windows -->
        |
        v
    SQLite: aggregates table

## Why I built this

Real-time data systems get hard once you split producer and consumer. Every
retail "bot" I have seen does everything in one file, and the day you want
a second consumer (dashboard, alert system, backtester), you have to tear
it apart.

Kafka is the standard answer for that problem in production. I wanted to
build the smallest honest version of it, using public data, and document
the failure modes I hit.

## What works today

- **Producer** — Binance combined-stream WebSocket → Kafka topic.
  Async ingestion with proper context-manager shutdown (same pattern as tickstream).
- **Aggregator** — Tumbling windows aligned to wall-clock boundaries.
  VWAP, high, low, buy/sell volume, trade count per window per symbol.
- **Sink** — Idempotent SQLite writes. Composite primary key on (symbol, window_start).
- **CLI** — `python -m streamforge produce` and `python -m streamforge consume`.
- **18 tests** covering schemas, aggregator logic, and sink behaviour.
- **GitHub Actions CI** on Python 3.12, 3.13, 3.14.

## What is not here yet

- **No Flink job.** The aggregator is plain Python running in a consumer loop.
  Flink 1.20 is installed on my machine and on the roadmap, but I am not going
  to pretend I have integrated it when I have not.
- **No producer tests.** The producer needs a live Kafka broker to test
  meaningfully. Mocking Kafka hides the bugs I care about (serialization,
  broker failover, backpressure).
- **No exactly-once semantics.** The sink is idempotent, so replays are safe,
  but duplicate processing is possible. Kafka transactions would fix this.
- **No schema registry.** Message format is a Pydantic model serialized to JSON.
  Fine for a single team, not fine across many.

## How to run

### Prerequisites

- Python 3.12+
- Kafka broker on `localhost:9092`
- Binance public WebSocket (no API key required)

### Setup

    git clone https://github.com/rehansaeedjutt-rgb/streamforge.git
    cd streamforge
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1  # Windows
    pip install -e ".[dev]"

### Start Kafka (using the local Kafka install)

    # Terminal 1 — start Kafka (Windows)
    C:\dev\apache\kafka\bin\windows\zookeeper-server-start.bat C:\dev\apache\kafka\config\zookeeper.properties
    # Terminal 2 — start Kafka broker
    C:\dev\apache\kafka\bin\windows\kafka-server-start.bat C:\dev\apache\kafka\config\server.properties

### Run the pipeline

    # Terminal 3 — publish trades for 15 seconds
    python -m streamforge produce --symbols btcusdt,ethusdt --duration 15

    # Terminal 4 — read, aggregate, and store
    python -m streamforge consume --max-messages 200

## Design notes (the interesting parts)

**Tumbling windows, not sliding.** Non-overlapping windows aligned to
wall-clock boundaries. Simpler to reason about, and the sink key is stable.
Sliding windows would require keeping multiple buckets per symbol alive —
easy to add later, not needed yet.

**Time driven by trade timestamps, not wall clock.** The aggregator advances
on the timestamp of each incoming trade. This is not how you want it in
production (a stalled feed would never close windows), but it makes the
aggregator testable without sleeping. Production would use a watermark.

**Idempotent sink.** The `(symbol, window_start)` primary key + `INSERT OR
IGNORE` means replaying a window does not double-count. This matters the
moment you introduce any kind of at-least-once delivery.

**Decimal as TEXT on the wire.** Same reasoning as every other project I
have written: floats are for physics, not for money. Kafka messages carry
price and quantity as strings, parsed back to `Decimal` on the consumer side.

**Symbol as partition key.** All trades for the same pair land on the same
Kafka partition, so ordering is preserved per symbol. Cross-symbol ordering
is not guaranteed, and does not need to be.

## Testing

    pytest --cov=src/streamforge --cov-report=term-missing

Current coverage: 48% overall. The gap is entirely `__main__.py` (CLI) and
`binance_producer.py` (needs live Kafka). The aggregation and storage logic
is tested at 78% and 93% respectively.

## Roadmap

- [ ] Kafka integration test using testcontainers
- [ ] Flink job that replaces the Python consumer loop
- [ ] Producer backpressure handling
- [ ] Schema registry integration
- [ ] Multi-symbol correlation windows
- [ ] DuckDB alternative sink for analytics

## Disclaimer

Educational project. Not audited. Not financial advice.

## License

MIT — see [LICENSE](LICENSE).

## Author

**Muhammad Rehan Saeed** - [GitHub](https://github.com/rehansaeedjutt-rgb)