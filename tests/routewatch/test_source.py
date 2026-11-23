import json
from contextlib import closing

import httpx
import pytest

from routewatch.source import iter_ris_messages


def test_streams_original_bytes_using_the_selected_collector():
    payload = b'{"type":"ris_message","data":{"type":"UPDATE","id":"event-1"}}  '

    class ChunkStream(httpx.SyncByteStream):
        def __iter__(self):
            yield payload[:20]
            yield payload[20:] + b"\n"
            pytest.fail("Reader consumed beyond the requested message")

    def handler(request):
        assert request.method == "GET"
        assert request.url.host == "ris-live.ripe.net"
        assert request.url.params["format"] == "json"

        subscription = json.loads(request.headers["X-RIS-Subscribe"])
        assert subscription == {
            "host": "rrc00",
            "type": "UPDATE",
            "socketOptions": {"includeRaw": False},
        }

        return httpx.Response(200, stream=ChunkStream())

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as client,
        closing(iter_ris_messages(client, collector="rrc00")) as messages,
    ):
        assert next(messages) == payload


def test_raises_for_an_unsuccessful_http_response():
    def handler(request):
        return httpx.Response(503, text="Temporarily unavailable")

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        list(iter_ris_messages(client))
