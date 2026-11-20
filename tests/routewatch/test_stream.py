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


def test_yields_two_messages_from_one_chunk():
    first = b'{"id":"event-1"}'
    second = b'{"id":"event-2"}'

    chunks = [first + b"\n" + second + b"\n"]

    assert list(iter_messages(chunks)) == [first, second]

def test_removes_crlf_without_changing_payload_whitespace():
    payload = b'{"id":"event-1"}  '

    chunks = [payload + b"\r\n"]

    assert list(iter_messages(chunks)) == [payload]
