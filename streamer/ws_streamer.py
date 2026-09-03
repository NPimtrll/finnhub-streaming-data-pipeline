import json
from threading import Thread
import time
from analyst import fetch_and_store_analyst_data
from config import FINNHUB_API_KEY, SYMBOLS, logger
import db
import websocket


def on_message(ws, message):
    try:
        msg = json.loads(message)
        if msg.get("type") == "trade":
            for trade in msg.get("data", []):
                symbol = trade.get("s")
                price = trade.get("p")
                volume = trade.get("v")
                timestamp = trade.get("t")
                conditions = trade.get("c", [])

                if symbol and price is not None and volume is not None and timestamp:
                    db.add_to_buffer(symbol, price, volume, timestamp, conditions)
    except Exception as e:
        logger.error(f"Error handling WebSocket message: {e}")


def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.info(f"WebSocket Closed: {close_status_code} - {close_msg}")


def on_open(ws):
    logger.info("WebSocket Connected. Subscribing...")
    for symbol in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol.strip()}))


def run_websocket_streamer(is_running_func):
    logger.info("Running in WEBSOCKET MODE.")
    fetch_and_store_analyst_data()

    def flush_loop():
        while is_running_func():
            db.check_and_flush()
            time.sleep(0.5)

    Thread(target=flush_loop, daemon=True).start()
    websocket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

    while is_running_func():
        try:
            ws = websocket.WebSocketApp(
                websocket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever()
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}. Reconnecting in 5s...")
            time.sleep(5)

    db.flush_buffer()
    logger.info("Websocket streamer stopped.")
