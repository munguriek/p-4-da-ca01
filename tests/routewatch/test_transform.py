import json

import pytest

from airdeall.routewatch.transform import InvalidRISMessage, decode_update


@pytest.fixture
def announcement_message():
    return {
        "type": "ris_message",
        "data": {
            "id": "37.49.232.7-01a1f9c20000000e",
            "timestamp": 1795191603.2,
            "host": "rrc21.ripe.net",
            "peer": "37.49.232.7",
            "peer_asn": "8218",
            "type": "UPDATE",
            "med": 0,
            "origin": "IGP",
            "path": [8218, 37100, 327708, 37133, 36909, 36909, 36909],
            "community": [[8218, 102], [8218, 20000], [8218, 20110]],
            "announcements": [
                {
                    "next_hop": "37.49.232.7",
                    "prefixes": ["41.220.128.0/20"],
                }
            ],
            "withdrawals": [],
        },
    }


def test_decodes_a_captured_update_without_changing_its_data(
    announcement_message,
) -> None:
    payload = json.dumps(announcement_message, indent=2).encode("utf-8")

    assert decode_update(payload) == announcement_message["data"]


def test_accepts_a_withdrawal_without_announcement_attributes(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    for name in ("announcements", "path", "origin", "community", "med"):
        data.pop(name)
    data["withdrawals"] = ["41.220.128.0/20"]
    payload = json.dumps(announcement_message).encode("utf-8")

    assert decode_update(payload) == data


def test_ignores_keepalive_messages(announcement_message) -> None:
    common = ("id", "timestamp", "host", "peer", "peer_asn")
    data = {name: announcement_message["data"][name] for name in common}
    data["type"] = "KEEPALIVE"
    payload = json.dumps({"type": "ris_message", "data": data}).encode("utf-8")

    assert decode_update(payload) is None


def test_ignores_pong_messages() -> None:
    assert decode_update(b'{"type":"pong","data":null}') is None


@pytest.mark.parametrize(
    "payload",
    [
        b"\xff",
        b'{"type":',
        b"[]",
        b"{}",
        b'{"type":[]}',
        b'{"type":"unknown","data":{}}',
        b'{"type":"ris_message"}',
        b'{"type":"ris_message","data":[]}',
        b'{"type":"ris_message","data":{}}',
        b'{"type":"ris_message","data":{"type":[]}}',
        b'{"type":"ris_message","data":{"type":"UNKNOWN"}}',
    ],
)
def test_rejects_invalid_envelopes(payload: bytes) -> None:
    with pytest.raises(InvalidRISMessage):
        decode_update(payload)


@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), float("-inf")],
    ids=["NaN", "Infinity", "-Infinity"],
)
def test_rejects_nonstandard_json_numbers(announcement_message, value) -> None:
    announcement_message["data"]["timestamp"] = value
    payload = json.dumps(announcement_message).encode("utf-8")

    with pytest.raises(InvalidRISMessage, match="Non-finite"):
        decode_update(payload)


def test_reports_ris_source_errors() -> None:
    payload = b'{"type":"ris_error","data":{"message":"source unavailable"}}'

    with pytest.raises(InvalidRISMessage, match="source reported"):
        decode_update(payload)
