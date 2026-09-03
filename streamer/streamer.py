import signal
from config import FINNHUB_API_KEY, logger
from db import init_clickhouse
from mock_streamer import run_mock_streamer
from ws_streamer import run_websocket_streamer

running = True


def signal_handler(signum, frame):
    global running
    logger.info("Shutting down gracefully...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    init_clickhouse()

    if FINNHUB_API_KEY:
        run_websocket_streamer(lambda: running)
    else:
        run_mock_streamer(lambda: running)
