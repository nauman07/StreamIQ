import os
import json
import time
import logging
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SYMBOLS         = os.getenv("SYMBOLS", "AAPL,GOOGL,MSFT,TSLA,NVDA").split(",")
FETCH_INTERVAL  = int(os.getenv("FETCH_INTERVAL", "30"))
RAW_TOPIC       = "market.raw"

# Try multiple data fetch strategies
STRATEGIES = [
    {"period": "5d",  "interval": "1d"},   # daily — always works, even weekends
    {"period": "1mo", "interval": "1d"},   # monthly daily — fallback
]


def make_producer(retries=10, delay=5) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready, retrying in {delay}s... ({attempt+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def fetch_symbol(symbol: str) -> dict | None:
    """Try multiple strategies to get data for a symbol."""
    for strategy in STRATEGIES:
        try:
            ticker = yf.Ticker(symbol)
            hist   = ticker.history(**strategy)
            if not hist.empty:
                latest = hist.iloc[-1]
                return {
                    "symbol":    symbol,
                    "timestamp": hist.index[-1].isoformat(),
                    "open":      round(float(latest["Open"]),  4),
                    "high":      round(float(latest["High"]),  4),
                    "low":       round(float(latest["Low"]),   4),
                    "close":     round(float(latest["Close"]), 4),
                    "volume":    int(latest["Volume"]),
                }
        except Exception as e:
            log.warning(f"{symbol} strategy {strategy} failed: {e}")
            continue
    return None


def fetch_ticks(symbols: list) -> list:
    ticks = []
    for symbol in symbols:
        tick = fetch_symbol(symbol)
        if tick:
            ticks.append(tick)
            log.info(f"{symbol}: close={tick['close']} vol={tick['volume']}")
        else:
            log.warning(f"{symbol}: all strategies failed, skipping")
    return ticks


def main():
    producer = make_producer()
    log.info(f"Tracking: {SYMBOLS} | publish every {FETCH_INTERVAL}s")

    while True:
        ticks = fetch_ticks(SYMBOLS)
        for tick in ticks:
            producer.send(topic=RAW_TOPIC, key=tick["symbol"], value=tick)
        producer.flush()
        log.info(f"Published {len(ticks)} ticks → sleeping {FETCH_INTERVAL}s")
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()