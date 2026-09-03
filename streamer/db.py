import sys
import time
import uuid
import clickhouse_connect
from config import (
    BATCH_INTERVAL_SEC,
    BATCH_SIZE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    logger,
)

ch_client = None
data_buffer = []
last_flush_time = time.time()


def init_clickhouse():
    global ch_client
    logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")

    for _ in range(12):
        try:
            ch_client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
            )
            break
        except Exception as e:
            logger.warning(
                f"Could not connect to ClickHouse, retrying in 5 seconds... Error: {e}"
            )
            time.sleep(5)

    if ch_client is None:
        logger.error("Failed to connect to ClickHouse. Exiting.")
        sys.exit(1)

    logger.info("Connected to ClickHouse successfully.")
    ch_client.command("CREATE DATABASE IF NOT EXISTS raw")

    ch_client.command("""
        CREATE TABLE IF NOT EXISTS raw.trades (
            trade_id String,
            symbol String,
            price Float64,
            volume Float64,
            timestamp Int64,
            conditions Array(String),
            ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp)
        """)

    ch_client.command("""
        CREATE TABLE IF NOT EXISTS raw.price_targets (
            symbol String,
            target_high Float64,
            target_low Float64,
            target_mean Float64,
            target_median Float64,
            number_of_analyst Int32,
            last_updated String,
            ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (symbol, last_updated)
        """)

    ch_client.command("""
        CREATE TABLE IF NOT EXISTS raw.recommendations (
            symbol String,
            period String,
            strong_buy Int32,
            buy Int32,
            hold Int32,
            sell Int32,
            strong_sell Int32,
            ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (symbol, period)
        """)


def flush_buffer():
    global last_flush_time
    if not data_buffer:
        last_flush_time = time.time()
        return

    rows_to_insert = list(data_buffer)
    data_buffer.clear()
    last_flush_time = time.time()

    logger.info(f"Flushing {len(rows_to_insert)} records to ClickHouse raw.trades...")
    try:
        ch_client.insert(
            "raw.trades",
            rows_to_insert,
            column_names=[
                "trade_id",
                "symbol",
                "price",
                "volume",
                "timestamp",
                "conditions",
            ],
        )
        logger.info(f"Successfully inserted {len(rows_to_insert)} records.")
    except Exception as e:
        logger.error(f"Error inserting records to ClickHouse: {e}")
        data_buffer.extend(rows_to_insert)


def add_to_buffer(symbol, price, volume, timestamp, conditions):
    try:
        trade_id = str(uuid.uuid4())
        row = (
            trade_id,
            str(symbol),
            float(price),
            float(volume),
            int(timestamp),
            [str(c) for c in conditions] if conditions else [],
        )
        data_buffer.append(row)

        if len(data_buffer) >= BATCH_SIZE:
            flush_buffer()
    except Exception as e:
        logger.error(f"Error processing row: {e}")


def check_and_flush():
    if time.time() - last_flush_time >= BATCH_INTERVAL_SEC:
        flush_buffer()
