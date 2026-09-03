import random
import time
from analyst import generate_mock_analyst_data
from config import SYMBOLS, logger
import db


def run_mock_streamer(is_running_func):
    logger.info("Running in MOCK MODE.")
    generate_mock_analyst_data()

    prices = {
        "AAPL": 175.50,
        "MSFT": 355.20,
        "TSLA": 230.10,
        "BINANCE:BTCUSDT": 65200.00,
        "BINANCE:ETHUSDT": 3450.00,
    }

    for sym in SYMBOLS:
        if sym not in prices:
            prices[sym] = 100.0

    while is_running_func():
        num_trades = random.randint(1, 5)
        for _ in range(num_trades):
            symbol = random.choice(list(prices.keys()))
            volatility = 0.0005 if "BINANCE" not in symbol else 0.001
            prices[symbol] *= 1 + random.normalvariate(0, volatility)

            price = round(prices[symbol], 2 if "BINANCE" not in symbol else 4)
            volume = (
                round(random.uniform(0.001, 10.0), 6)
                if "BINANCE" in symbol
                else random.randint(1, 500)
            )
            timestamp = int(time.time() * 1000)

            conditions = (
                [random.choice(["Regular", "DerivativelyPriced", "IntermarketSweep"])]
                if random.random() < 0.1
                else []
            )

            db.add_to_buffer(symbol, price, volume, timestamp, conditions)

        db.check_and_flush()
        time.sleep(random.uniform(0.1, 0.5))

    db.flush_buffer()
    logger.info("Mock streamer stopped.")
