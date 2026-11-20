from airdeall.routewatch.stream import iter_messages


def test_reassembles_a_message_split_across_http_chunks():
    payload = (
        b'{"type":"ris_message","data":{"type":"UPDATE",'
        b'"id":"event-1","path":[64500,64496]}}'
    )
    chunks = [
        payload[:25],
        payload[25:] + b"\n",
    ]

    assert list(iter_messages(chunks)) == [payload]
