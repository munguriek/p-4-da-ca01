import json
from dataclasses import dataclass
from datetime import UTC, datetime
from ipaddress import ip_address, ip_network
from typing import Any, Literal, NoReturn


class InvalidRISMessage(ValueError):
    """A captured message cannot be accepted by the UPDATE decoder."""


def _reject_constant(value: str) -> NoReturn:
    raise InvalidRISMessage(f"Non-finite JSON number: {value}")


def decode_update(payload: bytes) -> dict[str, Any] | None:
    """Decode an UPDATE envelope; field-level cleaning comes later."""
    try:
        message = json.loads(
            payload.decode("utf-8"),
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidRISMessage("Payload must be UTF-8 JSON") from error

    if not isinstance(message, dict):
        raise InvalidRISMessage("RIS envelope must be an object")

    kind = message.get("type")
    if kind in ("pong", "ris_subscribe_ok", "ris_rrc_list"):
        return None
    if kind == "ris_error":
        raise InvalidRISMessage("RIS source reported an error")
    if kind != "ris_message":
        raise InvalidRISMessage("Unsupported RIS envelope type")

    data = message.get("data")
    if not isinstance(data, dict):
        raise InvalidRISMessage("RIS data must be an object")

    subtype = data.get("type")
    if subtype == "UPDATE":
        return data
    if subtype in ("KEEPALIVE", "OPEN", "NOTIFICATION", "RIS_PEER_STATE"):
        return None

    raise InvalidRISMessage("Unsupported BGP message type")


@dataclass(frozen=True)
class UpdateMetadata:
    source_id: str
    event_ts: datetime
    collector_host: str
    peer_ip: str
    peer_asn: int


def _required_text(update: dict[str, Any], name: str) -> str:
    value = update.get(name)
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise InvalidRISMessage(f"{name} must be nonempty text without NUL characters")
    return value.strip()


# def _normalize_asn(value: Any, name: str) -> int:
#     if isinstance(value, bool) or not isinstance(value, (str, int)):
#         raise InvalidRISMessage(f"{name} must be numeric text or an integer")

#     try:
#         asn = int(value)
#     except ValueError as error:
#         raise InvalidRISMessage(f"{name} cannot be converted to an integer") from error


#     if not 0 <= asn <= 4_294_967_295:
#         raise InvalidRISMessage(f"{name} is outside the unsigned 32-bit range")
#     return asn
def _normalize_uint(value: Any, name: str, *, bits: int) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise InvalidRISMessage(f"{name} must be numeric text or an integer")

    try:
        number = int(value)
    except ValueError as error:
        raise InvalidRISMessage(f"{name} cannot be converted to an integer") from error

    if not 0 <= number < 2**bits:
        raise InvalidRISMessage(f"{name} is outside the unsigned {bits}-bit range")
    return number


def _normalize_asn(value: Any, name: str) -> int:
    return _normalize_uint(value, name, bits=32)


def normalize_metadata(update: dict[str, Any]) -> UpdateMetadata:
    """Normalize shared metadata from an already decoded UPDATE."""
    source_id = _required_text(update, "id")
    collector_host = _required_text(update, "host").lower()
    peer_text = _required_text(update, "peer")

    try:
        peer_ip = str(ip_address(peer_text))
    except ValueError as error:
        raise InvalidRISMessage("peer must be a valid IP address") from error

    raw_asn = update.get("peer_asn")
    if isinstance(raw_asn, bool) or not isinstance(raw_asn, (str, int)):
        raise InvalidRISMessage("peer_asn must be numeric text or an integer")

    peer_asn = _normalize_asn(update.get("peer_asn"), "peer_asn")

    timestamp = update.get("timestamp")
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, (int, float))
        or timestamp < 0
    ):
        raise InvalidRISMessage("timestamp must be a nonnegative number")

    try:
        event_ts = datetime.fromtimestamp(timestamp, tz=UTC)
    except (ValueError, OverflowError, OSError) as error:
        raise InvalidRISMessage("timestamp cannot be represented in UTC") from error

    return UpdateMetadata(
        source_id=source_id,
        event_ts=event_ts,
        collector_host=collector_host,
        peer_ip=peer_ip,
        peer_asn=peer_asn,
    )


@dataclass(frozen=True)
class PrefixAction:
    action_index: int
    action_type: Literal["ANNOUNCE", "WITHDRAW"]
    prefix: str
    ip_version: int
    prefix_length: int
    next_hops: tuple[str, ...]
    prefix_was_normalized: bool


def _make_prefix_action(
    index: int,
    action_type: Literal["ANNOUNCE", "WITHDRAW"],
    value: Any,
    next_hops: tuple[str, ...],
) -> PrefixAction:
    if not isinstance(value, str) or "/" not in value or "%" in value:
        raise InvalidRISMessage("prefix must be CIDR text without a scope ID")

    try:
        network = ip_network(value.strip(), strict=False)
    except ValueError as error:
        raise InvalidRISMessage("prefix must be a valid CIDR network") from error

    prefix = str(network)
    return PrefixAction(
        action_index=index,
        action_type=action_type,
        prefix=prefix,
        ip_version=network.version,
        prefix_length=network.prefixlen,
        next_hops=next_hops,
        prefix_was_normalized=value != prefix,
    )


def _normalize_next_hops(group: dict[str, Any]) -> tuple[str, ...]:
    text = _required_text(group, "next_hop")
    try:
        return tuple(str(ip_address(part.strip())) for part in text.split(","))
    except ValueError as error:
        raise InvalidRISMessage("next_hop must contain valid IP addresses") from error


def expand_prefix_actions(update: dict[str, Any]) -> list[PrefixAction]:
    """Preserve occurrences; emit withdrawals before announcements."""
    withdrawals = update.get("withdrawals", [])
    announcements = update.get("announcements", [])
    if not isinstance(withdrawals, list):
        raise InvalidRISMessage("withdrawals must be a list")
    if not isinstance(announcements, list):
        raise InvalidRISMessage("announcements must be a list")

    actions: list[PrefixAction] = []

    for value in withdrawals:
        actions.append(_make_prefix_action(len(actions), "WITHDRAW", value, ()))

    for group in announcements:
        if not isinstance(group, dict):
            raise InvalidRISMessage("announcements must contain objects")

        prefixes = group.get("prefixes")
        if not isinstance(prefixes, list):
            raise InvalidRISMessage("announcement prefixes must be a list")
        next_hops = _normalize_next_hops(group)

        for value in prefixes:
            actions.append(
                _make_prefix_action(len(actions), "ANNOUNCE", value, next_hops)
            )

    return actions


@dataclass(frozen=True)
class ASPathFeatures:
    as_path: tuple[int | tuple[int, ...], ...]
    as_path_length: int
    unique_as_count: int
    prepending_count: int
    origin_asn: int | None
    has_as_set: bool
    has_private_asn: bool
    has_nonconsecutive_repeat: bool | None


def normalize_as_path(update: dict[str, Any]) -> ASPathFeatures | None:
    """Normalize the reported path and derive structural features."""
    if "path" not in update:
        return None

    values = update["path"]
    if not isinstance(values, list):
        raise InvalidRISMessage("path must be a list when present")

    normalized: list[int | tuple[int, ...]] = []
    all_asns: set[int] = set()
    collapsed: list[int] = []
    previous: int | None = None
    prepending_count = 0
    has_as_set = False

    for value in values:
        if isinstance(value, list):
            if not value:
                raise InvalidRISMessage("AS_SET in path must be nonempty")

            members = tuple(sorted({_normalize_asn(item, "path") for item in value}))
            normalized.append(members)
            all_asns.update(members)
            has_as_set = True
            previous = None
        else:
            asn = _normalize_asn(value, "path")
            normalized.append(asn)
            all_asns.add(asn)
            if asn == previous:
                prepending_count += 1
            else:
                collapsed.append(asn)
            previous = asn

    last = normalized[-1] if normalized else None
    origin_asn = last if isinstance(last, int) else None
    repeated = None if has_as_set else len(collapsed) != len(set(collapsed))

    return ASPathFeatures(
        as_path=tuple(normalized),
        as_path_length=len(normalized),
        unique_as_count=len(all_asns),
        prepending_count=prepending_count,
        origin_asn=origin_asn,
        has_as_set=has_as_set,
        has_private_asn=any(
            64_512 <= asn <= 65_534 or 4_200_000_000 <= asn <= 4_294_967_294
            for asn in all_asns
        ),
        has_nonconsecutive_repeat=repeated,
    )


@dataclass(frozen=True)
class CommunityFeatures:
    communities: tuple[tuple[int, int], ...]
    community_count: int
    duplicate_community_count: int
    has_blackhole_community: bool


def normalize_origin(update: dict[str, Any]) -> str | None:
    if "origin" not in update:
        return None

    origin = _required_text(update, "origin").upper()
    if origin not in {"IGP", "EGP", "INCOMPLETE"}:
        raise InvalidRISMessage("origin must be IGP, EGP or INCOMPLETE")
    return origin


def normalize_communities(update: dict[str, Any]) -> CommunityFeatures | None:
    """Canonicalize standard communities and report removed duplicates."""
    if "community" not in update:
        return None

    values = update["community"]
    if not isinstance(values, list):
        raise InvalidRISMessage("community must be a list when present")

    unique: set[tuple[int, int]] = set()
    for pair in values:
        if not isinstance(pair, list) or len(pair) != 2:
            raise InvalidRISMessage("community entries must be pairs")

        high = _normalize_uint(pair[0], "community", bits=16)
        low = _normalize_uint(pair[1], "community", bits=16)
        unique.add((high, low))

    communities = tuple(sorted(unique))
    return CommunityFeatures(
        communities=communities,
        community_count=len(communities),
        duplicate_community_count=len(values) - len(communities),
        has_blackhole_community=(65_535, 666) in unique,
    )


@dataclass(frozen=True)
class CleanEvent:
    metadata: UpdateMetadata
    action: PrefixAction
    path_features: ASPathFeatures | None
    origin: str | None
    community_features: CommunityFeatures | None


@dataclass(frozen=True)
class TransformResult:
    status: Literal["cleaned", "empty", "ignored"]
    events: tuple[CleanEvent, ...]


def transform_message(payload: bytes) -> TransformResult:
    """Build all prefix events, or raise without returning a partial result."""
    update = decode_update(payload)
    if update is None:
        return TransformResult(status="ignored", events=())

    metadata = normalize_metadata(update)
    actions = expand_prefix_actions(update)
    if not actions:
        return TransformResult(status="empty", events=())

    path_features: ASPathFeatures | None = None
    origin: str | None = None
    community_features: CommunityFeatures | None = None

    if any(action.action_type == "ANNOUNCE" for action in actions):
        path_features = normalize_as_path(update)
        origin = normalize_origin(update)
        community_features = normalize_communities(update)

    events: list[CleanEvent] = []
    for action in actions:
        is_announcement = action.action_type == "ANNOUNCE"
        events.append(
            CleanEvent(
                metadata=metadata,
                action=action,
                path_features=path_features if is_announcement else None,
                origin=origin if is_announcement else None,
                community_features=(community_features if is_announcement else None),
            )
        )

    return TransformResult(status="cleaned", events=tuple(events))
