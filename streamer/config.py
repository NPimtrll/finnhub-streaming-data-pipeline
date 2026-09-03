import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("streamer")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "raw")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
SYMBOLS = os.getenv("SYMBOLS", "AAPL,MSFT,TSLA,BINANCE:BTCUSDT,BINANCE:ETHUSDT").split(
    ","
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_INTERVAL_SEC = float(os.getenv("BATCH_INTERVAL_SEC", "2.0"))
