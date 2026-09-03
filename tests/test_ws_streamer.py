import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "streamer"))

import ws_streamer


@patch("db.add_to_buffer")
def test_on_message_handles_valid_trade_event(mock_add_to_buffer):
    sample_payload = json.dumps(
        {
            "type": "trade",
            "data": [
                {
                    "s": "AAPL",
                    "p": 178.25,
                    "v": 100,
                    "t": 1710000000000,
                    "c": ["1"],
                }
            ],
        }
    )

    ws_streamer.on_message(None, sample_payload)

    mock_add_to_buffer.assert_called_once_with(
        "AAPL", 178.25, 100, 1710000000000, ["1"]
    )


@patch("db.add_to_buffer")
def test_on_message_ignores_ping_or_non_trade(mock_add_to_buffer):
    ping_payload = json.dumps({"type": "ping"})
    ws_streamer.on_message(None, ping_payload)

    mock_add_to_buffer.assert_not_called()


@patch("db.add_to_buffer")
def test_on_message_handles_malformed_json_gracefully(mock_add_to_buffer):
    broken_payload = "NOT_A_JSON"
    ws_streamer.on_message(None, broken_payload)

    mock_add_to_buffer.assert_not_called()
