import os
import sys
import time
import json
import random
import signal
import logging
import websocket
import clickhouse_connect
from threading import Thread

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load configurations
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "raw")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
SYMBOLS = os.getenv("SYMBOLS", "AAPL,MSFT,TSLA,BINANCE:BTCUSDT,BINANCE:ETHUSDT").split(",")

# Buffer settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_INTERVAL_SEC = float(os.getenv("BATCH_INTERVAL_SEC", "2.0"))

# State variables
data_buffer = []
last_flush_time = time.time()
running = True

# ClickHouse Client
ch_client = None

def init_clickhouse():
    global ch_client
    logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    
    # Retry loop to allow ClickHouse to start up
    for i in range(12):
        try:
            ch_client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD
            )
            break
        except Exception as e:
            logger.warning(f"Could not connect to ClickHouse, retrying in 5 seconds... Error: {e}")
            time.sleep(5)
            
    if ch_client is None:
        logger.error("Failed to connect to ClickHouse after multiple retries. Exiting.")
        sys.exit(1)

    logger.info("Connected to ClickHouse successfully.")
    
    # Initialize DB and Raw table
    ch_client.command("CREATE DATABASE IF NOT EXISTS raw")
    ch_client.command(
        """
        CREATE TABLE IF NOT EXISTS raw.trades (
            symbol String,
            price Float64,
            volume Float64,
            timestamp Int64,
            conditions Array(String),
            ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp)
        """
    )
    logger.info("Database and raw table initialized.")

def flush_buffer():
    global data_buffer, last_flush_time
    if not data_buffer:
        last_flush_time = time.time()
        return

    # Snapshot and clear buffer
    rows_to_insert = list(data_buffer)
    data_buffer.clear()
    last_flush_time = time.time()

    logger.info(f"Flushing {len(rows_to_insert)} records to ClickHouse raw.trades...")
    try:
        ch_client.insert(
            "raw.trades",
            rows_to_insert,
            column_names=["symbol", "price", "volume", "timestamp", "conditions"]
        )
        logger.info(f"Successfully inserted {len(rows_to_insert)} records.")
    except Exception as e:
        logger.error(f"Error inserting records to ClickHouse: {e}")
        # Put records back at the front of the queue to avoid loss
        data_buffer.extend(rows_to_insert)

def add_to_buffer(symbol, price, volume, timestamp, conditions):
    # Ensure types are correct
    try:
        row = (
            str(symbol),
            float(price),
            float(volume),
            int(timestamp),
            [str(c) for c in conditions] if conditions else []
        )
        data_buffer.append(row)
        
        # Flush if batch size limit reached
        if len(data_buffer) >= BATCH_SIZE:
            flush_buffer()
    except Exception as e:
        logger.error(f"Error processing row: {e}")

def signal_handler(signum, frame):
    global running
    logger.info("Received termination signal. Shutting down gracefully...")
    running = False

# Register signals for clean exit
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- MOCK SIMULATED MODE ---
def run_mock_streamer():
    logger.info("No FINNHUB_API_KEY provided. Running in MOCK MODE.")
    
    # Initialize base prices for mock symbols
    prices = {
        "AAPL": 175.50,
        "MSFT": 355.20,
        "TSLA": 230.10,
        "BINANCE:BTCUSDT": 65200.00,
        "BINANCE:ETHUSDT": 3450.00
    }
    
    # Standard fallback base prices for any other custom symbols
    for sym in SYMBOLS:
        if sym not in prices:
            prices[sym] = 100.0

    while running:
        # Generate 1 to 5 random trades
        num_trades = random.randint(1, 5)
        for _ in range(num_trades):
            symbol = random.choice(list(prices.keys()))
            
            # Simple random walk for prices
            volatility = 0.0005 if "BINANCE" not in symbol else 0.001
            change_percent = random.normalvariate(0, volatility)
            prices[symbol] *= (1 + change_percent)
            
            price = round(prices[symbol], 2 if "BINANCE" not in symbol else 4)
            volume = round(random.uniform(0.001, 10.0), 6) if "BINANCE" in symbol else random.randint(1, 500)
            timestamp = int(time.time() * 1000)
            
            # Occasionally add some trade conditions
            conditions = []
            if random.random() < 0.1:
                conditions = [random.choice(["Regular", "DerivativelyPriced", "IntermarketSweep"])]
                
            add_to_buffer(symbol, price, volume, timestamp, conditions)
            
        # Check if batch timeout exceeded
        if time.time() - last_flush_time >= BATCH_INTERVAL_SEC:
            flush_buffer()
            
        # Small sleep between event generation loops
        time.sleep(random.uniform(0.1, 0.5))

    # Final flush on exit
    flush_buffer()
    logger.info("Mock streamer stopped.")

# --- WEB SOCKET MODE ---
def on_message(ws, message):
    try:
        msg = json.loads(message)
        msg_type = msg.get("type")
        
        if msg_type == "trade":
            data = msg.get("data", [])
            for trade in data:
                # API format: s=symbol, p=price, v=volume, t=timestamp, c=conditions list
                symbol = trade.get("s")
                price = trade.get("p")
                volume = trade.get("v")
                timestamp = trade.get("t")
                conditions = trade.get("c", [])
                
                if symbol and price is not None and volume is not None and timestamp:
                    add_to_buffer(symbol, price, volume, timestamp, conditions)
                    
        elif msg_type == "ping":
            # Finnhub occasionally sends ping, keep connection alive
            pass
        else:
            logger.debug(f"Received non-trade message: {msg}")
            
    except Exception as e:
        logger.error(f"Error handling WebSocket message: {e}")

def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    logger.info(f"WebSocket Connection Closed: {close_status_code} - {close_msg}")

def on_open(ws):
    logger.info("WebSocket Connection Opened. Subscribing to symbols...")
    for symbol in SYMBOLS:
        sub_msg = {"type": "subscribe", "symbol": symbol.strip()}
        ws.send(json.dumps(sub_msg))
        logger.info(f"Subscribed to {symbol.strip()}")

def run_websocket_streamer():
    logger.info("FINNHUB_API_KEY provided. Running in WEB SOCKET MODE.")
    
    # Thread to periodically flush buffer
    def flush_loop():
        while running:
            if time.time() - last_flush_time >= BATCH_INTERVAL_SEC:
                flush_buffer()
            time.sleep(0.5)
            
    flush_thread = Thread(target=flush_loop, daemon=True)
    flush_thread.start()
    
    websocket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"
    
    while running:
        try:
            ws = websocket.WebSocketApp(
                websocket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            # Run websocket. Will block until closed or error
            ws.run_forever()
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
            
    # Final flush on exit
    flush_buffer()
    logger.info("Websocket streamer stopped.")

# --- MAIN ---
if __name__ == "__main__":
    init_clickhouse()
    
    if FINNHUB_API_KEY:
        run_websocket_streamer()
    else:
        run_mock_streamer()
