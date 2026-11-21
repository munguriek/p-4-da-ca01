import json
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from airdeall.routewatch.transform import (
    ASPathFeatures,
    CleanEvent,
    CommunityFeatures,
    InvalidRISMessage,
    PrefixAction,
    TransformResult,
    UpdateMetadata,
    decode_update,
    expand_prefix_actions,
    normalize_as_path,
    normalize_communities,
    normalize_metadata,
    normalize_origin,
    transform_message,
)


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


def test_derives_features_from_the_captured_path(announcement_message) -> None:
    data = announcement_message["data"]
    original = deepcopy(data)

    assert normalize_as_path(data) == ASPathFeatures(
        as_path=(8218, 37100, 327708, 37133, 36909, 36909, 36909),
        as_path_length=7,
        unique_as_count=5,
        prepending_count=2,
        origin_asn=36909,
        has_as_set=False,
        has_private_asn=False,
        has_nonconsecutive_repeat=False,
    )
    assert data == original


def test_separated_repeats_are_not_counted_as_prepends(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["path"] = [8218, 37100, 8218]

    features = normalize_as_path(data)

    assert features.prepending_count == 0
    assert features.has_nonconsecutive_repeat is True
    assert features.unique_as_count == 2


def test_canonicalizes_as_sets_and_keeps_sequence_runs(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["path"] = [" 8218 ", 8218, ["37100", 3356, 37100], 8218, 8218]

    assert normalize_as_path(data) == ASPathFeatures(
        as_path=(8218, 8218, (3356, 37100), 8218, 8218),
        as_path_length=5,
        unique_as_count=3,
        prepending_count=2,
        origin_asn=8218,
        has_as_set=True,
        has_private_asn=False,
        has_nonconsecutive_repeat=None,
    )


def test_terminal_as_set_has_no_single_ordered_origin(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["path"] = [8218, [4_200_000_000, 3356]]

    assert normalize_as_path(data) == ASPathFeatures(
        as_path=(8218, (3356, 4_200_000_000)),
        as_path_length=2,
        unique_as_count=3,
        prepending_count=0,
        origin_asn=None,
        has_as_set=True,
        has_private_asn=True,
        has_nonconsecutive_repeat=None,
    )


def test_missing_path_remains_missing(announcement_message) -> None:
    data = announcement_message["data"]
    data.pop("path")

    assert normalize_as_path(data) is None


def test_present_empty_path_has_zero_counts(announcement_message) -> None:
    data = announcement_message["data"]
    data["path"] = []

    assert normalize_as_path(data) == ASPathFeatures(
        as_path=(),
        as_path_length=0,
        unique_as_count=0,
        prepending_count=0,
        origin_asn=None,
        has_as_set=False,
        has_private_asn=False,
        has_nonconsecutive_repeat=False,
    )


@pytest.mark.parametrize(
    ("asn", "private"),
    [
        (0, False),
        (64_511, False),
        (64_512, True),
        (65_534, True),
        (65_535, False),
        (4_199_999_999, False),
        (4_200_000_000, True),
        (4_294_967_294, True),
        (4_294_967_295, False),
    ],
)
def test_private_asn_boundaries(announcement_message, asn, private) -> None:
    data = announcement_message["data"]
    data["path"] = [asn]

    features = normalize_as_path(data)

    assert features.has_private_asn is private
    assert features.origin_asn == asn


@pytest.mark.parametrize(
    "value",
    [
        None,
        "8218 37100",
        8218,
        {},
        [True],
        [8218.5],
        [None],
        [-1],
        [2**32],
        ["not-an-asn"],
        [[]],
        [[[8218]]],
        [[3356, True]],
        [8218, {}],
        pytest.param(["9" * 5000], id="overlong-path-asn"),
    ],
)
def test_rejects_malformed_as_paths(announcement_message, value) -> None:
    data = announcement_message["data"]
    data["path"] = value

    with pytest.raises(InvalidRISMessage, match="path"):
        normalize_as_path(data)


def test_normalizes_captured_origin_and_communities(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    original = deepcopy(data)

    assert normalize_origin(data) == "IGP"
    assert normalize_communities(data) == CommunityFeatures(
        communities=((8218, 102), (8218, 20000), (8218, 20110)),
        community_count=3,
        duplicate_community_count=0,
        has_blackhole_community=False,
    )
    assert data == original


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("IGP", "IGP"),
        (" igp ", "IGP"),
        ("egp", "EGP"),
        (" Incomplete ", "INCOMPLETE"),
    ],
)
def test_normalizes_origin_categories(announcement_message, value, expected) -> None:
    data = announcement_message["data"]
    data["origin"] = value

    assert normalize_origin(data) == expected


def test_missing_optional_attributes_remain_missing(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data.pop("origin")
    data.pop("community")

    assert normalize_origin(data) is None
    assert normalize_communities(data) is None


def test_explicitly_empty_communities_have_known_zero_counts(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["community"] = []

    assert normalize_communities(data) == CommunityFeatures(
        communities=(),
        community_count=0,
        duplicate_community_count=0,
        has_blackhole_community=False,
    )


def test_canonicalizes_communities_and_counts_removed_duplicates(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["community"] = [
        [8218, 20110],
        ["8218", "102"],
        [8218, 102],
        [65535, 666],
        [65535, 666],
    ]
    original = deepcopy(data)

    assert normalize_communities(data) == CommunityFeatures(
        communities=((8218, 102), (8218, 20110), (65535, 666)),
        community_count=3,
        duplicate_community_count=2,
        has_blackhole_community=True,
    )
    assert data == original


@pytest.mark.parametrize(
    ("values", "blackhole"),
    [
        ([[65535, 666]], True),
        ([[65535, 667]], False),
        ([[8218, 666]], False),
        ([[0, 0], [65535, 65535]], False),
    ],
)
def test_checks_the_exact_blackhole_community(
    announcement_message, values, blackhole
) -> None:
    data = announcement_message["data"]
    data["community"] = values

    features = normalize_communities(data)

    assert features is not None
    assert features.has_blackhole_community is blackhole


@pytest.mark.parametrize("value", [None, "", "?", 0, True, [], "IGP\x00"])
def test_rejects_invalid_origin(announcement_message, value) -> None:
    data = announcement_message["data"]
    data["origin"] = value

    with pytest.raises(InvalidRISMessage, match="origin"):
        normalize_origin(data)


@pytest.mark.parametrize(
    "value",
    [
        None,
        "8218:102",
        {},
        [None],
        [[8218]],
        [[8218, 102, 7]],
        [[8218, -1]],
        [[65536, 102]],
        [[8218, 65536]],
        [[True, 102]],
        [[8218, 1.5]],
        [["not-a-number", 102]],
        [[327708, 102]],
        pytest.param([["9" * 5000, 102]], id="overlong-community-word"),
    ],
)
def test_rejects_invalid_standard_communities(announcement_message, value) -> None:
    data = announcement_message["data"]
    data["community"] = value

    with pytest.raises(InvalidRISMessage, match="community"):
        normalize_communities(data)


def _encode_message(message) -> bytes:
    return json.dumps(message).encode("utf-8")


def test_builds_a_clean_event_from_the_capture(announcement_message) -> None:
    result = transform_message(_encode_message(announcement_message))

    assert result.status == "cleaned"
    assert len(result.events) == 1
    event = result.events[0]

    assert isinstance(event, CleanEvent)
    assert event.metadata.source_id == "37.49.232.7-01a1f9c20000000e"
    assert event.metadata.peer_asn == 8218
    assert event.action.action_index == 0
    assert event.action.action_type == "ANNOUNCE"
    assert event.action.prefix == "41.220.128.0/20"
    assert event.origin == "IGP"
    assert event.path_features is not None
    assert event.path_features.origin_asn == 36909
    assert event.path_features.prepending_count == 2
    assert event.community_features is not None
    assert event.community_features.community_count == 3


def test_mixed_update_keeps_withdrawal_attributes_empty(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["withdrawals"] = ["41.220.128.0/20"]
    data["announcements"][0]["prefixes"].append("41.220.144.0/20")

    result = transform_message(_encode_message(announcement_message))

    assert result.status == "cleaned"
    assert [
        (event.action.action_index, event.action.action_type, event.action.prefix)
        for event in result.events
    ] == [
        (0, "WITHDRAW", "41.220.128.0/20"),
        (1, "ANNOUNCE", "41.220.128.0/20"),
        (2, "ANNOUNCE", "41.220.144.0/20"),
    ]

    withdrawal = result.events[0]
    assert withdrawal.action.next_hops == ()
    assert withdrawal.path_features is None
    assert withdrawal.origin is None
    assert withdrawal.community_features is None

    for announcement in result.events[1:]:
        assert announcement.action.next_hops == ("37.49.232.7",)
        assert announcement.path_features is not None
        assert announcement.path_features.prepending_count == 2
        assert announcement.origin == "IGP"
        assert announcement.community_features is not None
        assert announcement.community_features.community_count == 3


@pytest.mark.parametrize("attributes_present", [False, True])
def test_withdrawals_do_not_require_announcement_attributes(
    announcement_message, attributes_present
) -> None:
    data = announcement_message["data"]
    data["announcements"] = []
    data["withdrawals"] = ["41.220.128.0/20"]
    for field in ("path", "origin", "community"):
        if attributes_present:
            data[field] = None
        else:
            data.pop(field)

    result = transform_message(_encode_message(announcement_message))

    assert result.status == "cleaned"
    assert len(result.events) == 1
    event = result.events[0]
    assert event.action.action_type == "WITHDRAW"
    assert event.path_features is None
    assert event.origin is None
    assert event.community_features is None


def test_missing_announcement_attributes_remain_unknown(
    announcement_message,
) -> None:
    for field in ("path", "origin", "community"):
        announcement_message["data"].pop(field)

    result = transform_message(_encode_message(announcement_message))

    assert result.status == "cleaned"
    event = result.events[0]
    assert event.action.action_type == "ANNOUNCE"
    assert event.path_features is None
    assert event.origin is None
    assert event.community_features is None


def test_explicitly_empty_announcement_attributes_keep_zero_counts(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["path"] = []
    data["community"] = []

    result = transform_message(_encode_message(announcement_message))

    event = result.events[0]
    assert event.path_features is not None
    assert event.path_features.as_path_length == 0
    assert event.path_features.origin_asn is None
    assert event.community_features is not None
    assert event.community_features.community_count == 0


@pytest.mark.parametrize(
    "payload",
    [
        b'{"type":"pong"}',
        b'{"type":"ris_message","data":{"type":"KEEPALIVE"}}',
    ],
)
def test_reports_control_messages_as_ignored(payload: bytes) -> None:
    assert transform_message(payload) == TransformResult(status="ignored", events=())


@pytest.mark.parametrize("fields_present", [False, True])
def test_reports_updates_without_prefix_actions_as_empty(
    announcement_message, fields_present
) -> None:
    data = announcement_message["data"]
    for field in ("announcements", "withdrawals"):
        if fields_present:
            data[field] = []
        else:
            data.pop(field)

    assert transform_message(_encode_message(announcement_message)) == (
        TransformResult(status="empty", events=())
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [("path", None), ("origin", "?"), ("community", [[65536, 1]])],
)
def test_rejects_bad_attributes_in_a_mixed_update(
    announcement_message, field, value
) -> None:
    data = announcement_message["data"]
    data["withdrawals"] = ["41.220.128.0/20"]
    data[field] = value

    with pytest.raises(InvalidRISMessage, match=field):
        transform_message(_encode_message(announcement_message))


def test_empty_updates_still_require_valid_metadata(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["announcements"] = []
    data["withdrawals"] = []
    data.pop("peer")

    with pytest.raises(InvalidRISMessage, match="peer"):
        transform_message(_encode_message(announcement_message))


def test_bad_later_prefix_rejects_the_whole_transformation(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["announcements"][0]["prefixes"].append("not-a-prefix")

    with pytest.raises(InvalidRISMessage, match="prefix"):
        transform_message(_encode_message(announcement_message))


def test_rebuild_is_deterministic_and_keeps_prefix_occurrences(
    announcement_message,
) -> None:
    data = announcement_message["data"]
    data["announcements"][0]["prefixes"] *= 2
    payload = _encode_message(announcement_message)

    first = transform_message(payload)
    second = transform_message(payload)

    assert first == second
    assert first.status == "cleaned"
    assert [event.action.action_index for event in first.events] == [0, 1]
    assert [event.action.prefix for event in first.events] == [
        "41.220.128.0/20",
        "41.220.128.0/20",
    ]


@pytest.mark.parametrize(
    "payload",
    [
        b"{",
        b'{"type":"ris_error","data":{"message":"bad request"}}',
    ],
)
def test_transformation_propagates_invalid_payloads(payload: bytes) -> None:
    with pytest.raises(InvalidRISMessage):
        transform_message(payload)
