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

    try:
        peer_asn = int(raw_asn)
    except ValueError as error:
        raise InvalidRISMessage("peer_asn cannot be converted to an integer") from error

    if not 0 <= peer_asn <= 4_294_967_295:
        raise InvalidRISMessage("peer_asn is outside the unsigned 32-bit range")

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
