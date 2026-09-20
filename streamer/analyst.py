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
                        [
                            (
                                str(data["symbol"]),
                                float(data["targetHigh"]),
                                float(data["targetLow"]),
                                float(data["targetMean"]),
                                float(data["targetMedian"]),
                                int(data["numberOfAnalyst"]),
                                str(data["lastUpdated"]),
                            )
                        ],
                        column_names=[
                            "symbol",
                            "target_high",
                            "target_low",
                            "target_mean",
                            "target_median",
                            "number_of_analyst",
                            "last_updated",
                        ],
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
                        column_names=[
                            "symbol",
                            "period",
                            "strong_buy",
                            "buy",
                            "hold",
                            "sell",
                            "strong_sell",
                        ],
                    )
            elif resp.status_code == 429:
                time.sleep(5)

            time.sleep(1)
        except Exception as e:
            logger.error(f"Error fetching analyst data for {sym}: {e}")
