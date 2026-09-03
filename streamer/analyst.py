from datetime import datetime
import random
import time
from config import FINNHUB_API_KEY, SYMBOLS, logger
import db
import requests


def fetch_and_store_analyst_data():
    logger.info("Fetching analyst recommendations and price targets...")
    for sym in SYMBOLS:
        if ":" in sym:
            continue

        try:
            target_url = f"https://finnhub.io/api/v1/stock/price-target?symbol={sym}&token={FINNHUB_API_KEY}"
            resp = requests.get(target_url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("targetMean"):
                    db.ch_client.insert(
                        "raw.price_targets",
                        [(
                            str(data["symbol"]),
                            float(data["targetHigh"]),
                            float(data["targetLow"]),
                            float(data["targetMean"]),
                            float(data["targetMedian"]),
                            int(data["numberOfAnalyst"]),
                            str(data["lastUpdated"]),
                        )],
                        column_names=["symbol", "target_high", "target_low", "target_mean", "target_median", "number_of_analyst", "last_updated"],
                    )
            elif resp.status_code == 429:
                time.sleep(5)

            rec_url = f"https://finnhub.io/api/v1/stock/recommendation?symbol={sym}&token={FINNHUB_API_KEY}"
            resp = requests.get(rec_url, timeout=10)
            if resp.status_code == 200:
                trends = resp.json()
                if isinstance(trends, list) and trends:
                    rows = [
                        (
                            str(t["symbol"]),
                            str(t["period"]),
                            int(t["strongBuy"]),
                            int(t["buy"]),
                            int(t["hold"]),
                            int(t["sell"]),
                            int(t["strongSell"]),
                        )
                        for t in trends[:3]
                    ]
                    db.ch_client.insert(
                        "raw.recommendations",
                        rows,
                        column_names=["symbol", "period", "strong_buy", "buy", "hold", "sell", "strong_sell"],
                    )
            elif resp.status_code == 429:
                time.sleep(5)

            time.sleep(1)
        except Exception as e:
            logger.error(f"Error fetching analyst data for {sym}: {e}")


def generate_mock_analyst_data():
    current_date = datetime.now().strftime("%Y-%m-%d")
    periods = ["2026-08-01", "2026-07-01", "2026-06-01"]

    targets = {
        "AAPL": (240.0, 160.0, 208.5, 207.0, 38),
        "MSFT": (460.0, 340.0, 412.0, 410.0, 45),
        "TSLA": (320.0, 140.0, 245.5, 248.0, 29),
        "BINANCE:BTCUSDT": (95000.0, 58000.0, 81000.0, 80000.0, 15),
        "BINANCE:ETHUSDT": (5200.0, 2800.0, 4200.0, 4150.0, 12),
    }

    recs = {
        "AAPL": (18, 12, 6, 1, 0),
        "MSFT": (25, 14, 3, 0, 0),
        "TSLA": (8, 11, 8, 2, 1),
        "BINANCE:BTCUSDT": (11, 3, 1, 0, 0),
        "BINANCE:ETHUSDT": (8, 4, 0, 0, 0),
    }

    try:
        pt_rows = [
            (sym, high, low, mean, median, analysts, current_date)
            for sym, (high, low, mean, median, analysts) in targets.items()
        ]
        db.ch_client.insert(
            "raw.price_targets",
            pt_rows,
            column_names=["symbol", "target_high", "target_low", "target_mean", "target_median", "number_of_analyst", "last_updated"],
        )

        rec_rows = []
        for sym, (sb, b, h, s, ss) in recs.items():
            for p in periods:
                noise = random.randint(-2, 2)
                rec_rows.append((
                    sym,
                    p,
                    max(0, sb + noise),
                    max(0, b - noise),
                    max(0, h),
                    max(0, s),
                    max(0, ss),
                ))
        db.ch_client.insert(
            "raw.recommendations",
            rec_rows,
            column_names=["symbol", "period", "strong_buy", "buy", "hold", "sell", "strong_sell"],
        )
    except Exception as e:
        logger.error(f"Error generating mock analyst data: {e}")
