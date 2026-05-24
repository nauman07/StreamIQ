import os
import json
import time
import logging
import psycopg2
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from collections import defaultdict
from indicators import process_tick

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CONSUMER] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC       = "market.raw"
GROUP_ID        = "market-processor"

# Keep rolling window of last 100 ticks per symbol for indicator calculation
HISTORY_WINDOW  = 100
symbol_history  = defaultdict(lambda: pd.DataFrame(columns=["ts", "close", "volume"]))


def get_db_conn(retries=10, delay=5):
    """Connect to Postgres with retry."""
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                dbname=os.getenv("POSTGRES_DB", "market_db"),
                user=os.getenv("POSTGRES_USER", "market_user"),
                password=os.getenv("POSTGRES_PASSWORD", "market_pass"),
            )
            log.info("Connected to PostgreSQL")
            return conn
        except Exception as e:
            log.warning(f"Postgres not ready ({e}), retrying in {delay}s... ({attempt+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres")


def make_consumer(retries=15, delay=5) -> KafkaConsumer:
    """Connect to Kafka with retry."""
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                RAW_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                consumer_timeout_ms=1000,
            )
            log.info(f"Subscribed to {RAW_TOPIC}")
            return consumer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready, retrying in {delay}s... ({attempt+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def save_raw(cursor, tick: dict):
    """Insert raw tick into market_raw."""
    cursor.execute("""
        INSERT INTO market_raw (symbol, ts, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        tick["symbol"], tick["timestamp"],
        tick["open"], tick["high"], tick["low"],
        tick["close"], tick["volume"],
    ))


def save_processed(cursor, processed: dict):
    """Insert processed tick into market_processed."""
    cursor.execute("""
        INSERT INTO market_processed
            (symbol, ts, close, volume, sma_20, sma_50, rsi_14, volume_spike, signal)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        processed["symbol"], processed["ts"],
        processed["close"],  processed["volume"],
        processed["sma_20"], processed["sma_50"],
        processed["rsi_14"], processed["volume_spike"],
        processed["signal"],
    ))


def update_history(symbol: str, tick: dict):
    """Add tick to rolling in-memory history, keep last HISTORY_WINDOW rows."""
    global symbol_history
    new_row = pd.DataFrame([{
        "ts":     tick["timestamp"],
        "close":  tick["close"],
        "volume": tick["volume"],
    }])
    symbol_history[symbol] = pd.concat(
        [symbol_history[symbol], new_row], ignore_index=True
    ).tail(HISTORY_WINDOW)


def main():
    conn     = get_db_conn()
    consumer = make_consumer()
    log.info("Consumer running — waiting for messages...")

    while True:
        try:
            for message in consumer:
                tick   = message.value
                symbol = tick["symbol"]

                try:
                    # 1. Update in-memory history
                    update_history(symbol, tick)

                    # 2. Compute indicators
                    processed = process_tick(symbol, symbol_history[symbol])

                    # 3. Save both raw and processed to Postgres
                    with conn.cursor() as cur:
                        save_raw(cur, tick)
                        save_processed(cur, processed)
                    conn.commit()

                    log.info(
                        f"{symbol} | close={processed['close']} "
                        f"RSI={processed['rsi_14']} signal={processed['signal']}"
                    )

                except Exception as e:
                    conn.rollback()
                    log.error(f"Error processing {symbol}: {e}")

        except Exception as e:
            log.error(f"Consumer loop error: {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()
