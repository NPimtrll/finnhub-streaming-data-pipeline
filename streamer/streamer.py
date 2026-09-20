import signal
import sys
from config import FINNHUB_API_KEY, logger
from db import init_clickhouse
from ws_streamer import run_websocket_streamer

running = True


def signal_handler(signum, frame):
    global running
    logger.info("Shutting down gracefully...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    if not FINNHUB_API_KEY:
        logger.error("FINNHUB_API_KEY is not set. A valid Finnhub API key is required.")
        sys.exit(1)

    init_clickhouse()
    run_websocket_streamer(lambda: running)
