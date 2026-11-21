import json
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from airdeall.routewatch.transform import (
    InvalidRISMessage,
    PrefixAction,
    UpdateMetadata,
    decode_update,
    expand_prefix_actions,
    normalize_metadata,
)


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


def test_normalizes_metadata_from_the_capture(announcement_message) -> None:
    data = announcement_message["data"]
    original = deepcopy(data)

    assert normalize_metadata(data) == UpdateMetadata(
        source_id="37.49.232.7-01a1f9c20000000e",
        event_ts=datetime(2026, 11, 20, 16, 20, 3, 200000, tzinfo=UTC),
        collector_host="rrc21.ripe.net",
        peer_ip="37.49.232.7",
        peer_asn=8218,
    )
    assert data == original


def test_normalizes_padding_case_and_ipv6(announcement_message) -> None:
    data = announcement_message["data"]
    original_id = data["id"]
    data.update(
        {
            "id": f" {original_id} ",
            "host": " RRC21.RIPE.NET ",
            "peer": " 2001:07f8:0054:0005::0007 ",
            "peer_asn": " 8218 ",
            "timestamp": 0,
        }
    )

    assert normalize_metadata(data) == UpdateMetadata(
        source_id=original_id,
        event_ts=datetime(1970, 1, 1, tzinfo=UTC),
        collector_host="rrc21.ripe.net",
        peer_ip="2001:7f8:54:5::7",
        peer_asn=8218,
    )


@pytest.mark.parametrize("value", [0, 8218, 64512, 4_294_967_295])
def test_accepts_integer_asns_in_the_encoding_range(
    announcement_message, value
) -> None:
    data = announcement_message["data"]
    data["peer_asn"] = value

    metadata = normalize_metadata(data)

    assert metadata.peer_asn == value
    assert type(metadata.peer_asn) is int


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", None),
        ("id", " "),
        ("id", "bad\x00id"),
        ("host", 42),
        ("peer", "not-an-ip"),
        ("peer_asn", None),
        ("peer_asn", True),
        ("peer_asn", 8218.5),
        ("peer_asn", "not-an-asn"),
        ("peer_asn", -1),
        ("peer_asn", 2**32),
        pytest.param("peer_asn", "9" * 5000, id="overlong-asn"),
        ("timestamp", None),
        ("timestamp", "1795191603.2"),
        ("timestamp", True),
        ("timestamp", float("nan")),
        ("timestamp", float("inf")),
        ("timestamp", -1),
        ("timestamp", 10**100),
    ],
)
def test_rejects_invalid_metadata(announcement_message, field, value) -> None:
    data = announcement_message["data"]
    data[field] = value

    with pytest.raises(InvalidRISMessage, match=field):
        normalize_metadata(data)


def test_expands_the_captured_announcement(announcement_message) -> None:
    data = announcement_message["data"]
    original = deepcopy(data)

    assert expand_prefix_actions(data) == [
        PrefixAction(
            action_index=0,
            action_type="ANNOUNCE",
            prefix="41.220.128.0/20",
            ip_version=4,
            prefix_length=20,
            next_hops=("37.49.232.7",),
            prefix_was_normalized=False,
        )
    ]
    assert data == original


def test_expands_multiple_groups_and_withdrawals(announcement_message) -> None:
    data = announcement_message["data"]
    data["withdrawals"] = ["192.0.2.0/24", "2001:db8::/32"]
    data["announcements"] = [
        {
            "next_hop": "37.49.232.7",
            "prefixes": ["41.220.128.0/20", "41.220.144.0/20"],
        },
        {
            "next_hop": "2001:7f8:54:5::7",
            "prefixes": ["2804:1070::/32"],
        },
    ]

    actions = expand_prefix_actions(data)

    assert [(item.action_index, item.action_type, item.prefix) for item in actions] == [
        (0, "WITHDRAW", "192.0.2.0/24"),
        (1, "WITHDRAW", "2001:db8::/32"),
        (2, "ANNOUNCE", "41.220.128.0/20"),
        (3, "ANNOUNCE", "41.220.144.0/20"),
        (4, "ANNOUNCE", "2804:1070::/32"),
    ]
    assert [item.next_hops for item in actions] == [
        (),
        (),
        ("37.49.232.7",),
        ("37.49.232.7",),
        ("2001:7f8:54:5::7",),
    ]


@pytest.mark.parametrize(
    ("prefix", "version", "length"),
    [("2804:1070::/32", 6, 32), ("41.220.128.0/20", 4, 20)],
)
def test_handles_ipv6_next_hops_without_inferring_prefix_family(
    announcement_message, prefix, version, length
) -> None:
    data = announcement_message["data"]
    data["announcements"] = [
        {
            "next_hop": " 2001:07F8:0054:0005::0007, fe80::224:dc00:5ce:67c4 ",
            "prefixes": [prefix],
        }
    ]

    assert expand_prefix_actions(data) == [
        PrefixAction(
            action_index=0,
            action_type="ANNOUNCE",
            prefix=prefix,
            ip_version=version,
            prefix_length=length,
            next_hops=("2001:7f8:54:5::7", "fe80::224:dc00:5ce:67c4"),
            prefix_was_normalized=False,
        )
    ]


@pytest.mark.parametrize(
    ("value", "prefix", "version", "length", "changed"),
    [
        (" 41.220.135.7/20 ", "41.220.128.0/20", 4, 20, True),
        (" 2A09:B880:0000::/29 ", "2a09:b880::/29", 6, 29, True),
        ("0.0.0.0/0", "0.0.0.0/0", 4, 0, False),
        ("::/0", "::/0", 6, 0, False),
    ],
)
def test_canonicalizes_prefixes_and_records_changes(
    announcement_message, value, prefix, version, length, changed
) -> None:
    data = announcement_message["data"]
    group = data["announcements"][0]
    group["prefixes"] = [value]
    if version == 6:
        group["next_hop"] = "2001:7f8:54:5::7"

    action = expand_prefix_actions(data)[0]

    assert action.prefix == prefix
    assert action.ip_version == version
    assert action.prefix_length == length
    assert action.prefix_was_normalized is changed


@pytest.mark.parametrize("fields", [{}, {"announcements": [], "withdrawals": []}])
def test_empty_action_fields_produce_no_rows(announcement_message, fields) -> None:
    data = announcement_message["data"]
    data.pop("announcements")
    data.pop("withdrawals")
    data.update(fields)

    assert expand_prefix_actions(data) == []


def test_preserves_repeated_prefix_occurrences(announcement_message) -> None:
    data = announcement_message["data"]
    data["announcements"][0]["prefixes"] *= 2

    actions = expand_prefix_actions(data)

    assert [item.action_index for item in actions] == [0, 1]
    assert [item.prefix for item in actions] == [
        "41.220.128.0/20",
        "41.220.128.0/20",
    ]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("announcements", None),
        ("announcements", {}),
        ("withdrawals", None),
        ("withdrawals", "192.0.2.0/24"),
        ("announcements", [None]),
        ("announcements", [{}]),
        (
            "announcements",
            [{"next_hop": "37.49.232.7", "prefixes": "41.220.128.0/20"}],
        ),
        ("announcements", [{"prefixes": ["41.220.128.0/20"]}]),
    ],
)
def test_rejects_invalid_action_structures(announcement_message, field, value) -> None:
    data = announcement_message["data"]
    data[field] = value

    with pytest.raises(InvalidRISMessage):
        expand_prefix_actions(data)


@pytest.mark.parametrize("field", ["announcements", "withdrawals"])
@pytest.mark.parametrize(
    "value",
    [
        None,
        12,
        "not-a-network",
        "41.220.128.0",
        "41.220.128.0/33",
        "2804:1070::/129",
        "fe80::%eth0/64",
    ],
)
def test_rejects_a_bad_prefix_even_after_a_valid_one(
    announcement_message, field, value
) -> None:
    data = announcement_message["data"]
    prefixes = ["41.220.128.0/20", value]
    if field == "announcements":
        data["announcements"][0]["prefixes"] = prefixes
    else:
        data["withdrawals"] = prefixes

    with pytest.raises(InvalidRISMessage):
        expand_prefix_actions(data)


@pytest.mark.parametrize("value", [None, 37, "not-an-ip", "37.49.232.7,", " , "])
def test_rejects_invalid_next_hops(announcement_message, value) -> None:
    data = announcement_message["data"]
    data["announcements"][0]["next_hop"] = value

    with pytest.raises(InvalidRISMessage):
        expand_prefix_actions(data)
