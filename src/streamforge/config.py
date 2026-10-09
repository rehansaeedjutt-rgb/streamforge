"""Configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for streamforge.

    All values overridable via env vars prefixed with ``STREAMFORGE_``.
    Defaults assume Kafka and Flink are running on localhost — which is
    what the docker-compose file in this repo sets up.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="STREAMFORGE_",
        case_sensitive=False,
        extra="ignore",
    )

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_trades: str = "crypto.trades.raw"
    kafka_topic_aggregates: str = "crypto.aggregates"
    kafka_consumer_group: str = "streamforge"

    # Binance upstream
    binance_ws_base_url: str = "wss://stream.binance.com:9443/ws"
    symbols: list[str] = Field(default_factory=lambda: ["btcusdt", "ethusdt"])

    # Storage
    sqlite_db_path: str = "streamforge.db"

    # Aggregation window — in seconds.
    # Smaller = more real-time, more DB writes. 5s is a reasonable default.
    aggregate_window_seconds: int = Field(default=5, gt=0, le=300)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
