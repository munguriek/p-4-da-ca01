import json
from collections.abc import Iterator

import httpx

from .stream import iter_messages

RIS_URL = "https://ris-live.ripe.net/v1/stream/?format=json&client=airdeall-routewatch"


def iter_ris_messages(
    client: httpx.Client,
    *,
    collector: str = "rrc21",
) -> Iterator[bytes]:
    """Join original message payloads from RIS Live."""
    headers = {
        "User-Agent": "AirdeallRouteWatch/0.1",
        "X-RIS-Subscribe": json.dumps(
            {
                "host": collector,
                "type": "UPDATE",
                "socketOptions": {"includeRaw": False},
            }
        ),
    }

    with client.stream("GET", RIS_URL, headers=headers) as response:
        response.raise_for_status()
        yield from iter_messages(response.iter_bytes())
