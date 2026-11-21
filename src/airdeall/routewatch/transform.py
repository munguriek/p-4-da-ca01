import json
from typing import Any, NoReturn


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
