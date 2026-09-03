import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "streamer"))

import db


def setup_function():
    db.data_buffer.clear()
    db.ch_client = MagicMock()


def test_add_to_buffer_single_record():
    db.add_to_buffer(
        symbol="AAPL",
        price=180.50,
        volume=10,
        timestamp=1710000000000,
        conditions=["Regular"],
    )

    assert len(db.data_buffer) == 1

    trade_id, symbol, price, volume, timestamp, conditions = db.data_buffer[0]
    assert isinstance(trade_id, str)
    assert symbol == "AAPL"
    assert price == 180.50
    assert volume == 10.0
    assert timestamp == 1710000000000
    assert conditions == ["Regular"]


def test_flush_buffer_clears_queue_and_inserts():
    db.data_buffer = [
        ("id-1", "AAPL", 180.50, 10.0, 1710000000000, ["Regular"]),
        ("id-2", "MSFT", 400.00, 5.0, 1710000000000, []),
    ]

    db.flush_buffer()

    assert len(db.data_buffer) == 0
    db.ch_client.insert.assert_called_once()
    assert db.ch_client.insert.call_args[0][0] == "raw.trades"


def test_flush_buffer_empty_does_nothing():
    db.data_buffer = []
    db.flush_buffer()

    db.ch_client.insert.assert_not_called()


def test_flush_buffer_recovers_data_on_failure():
    sample_row = ("id-1", "AAPL", 180.50, 10.0, 1710000000000, [])
    db.data_buffer = [sample_row]

    db.ch_client.insert.side_effect = Exception("ClickHouse connection lost")

    db.flush_buffer()

    assert len(db.data_buffer) == 1
    assert db.data_buffer[0] == sample_row
