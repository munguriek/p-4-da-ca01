import pytest

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


def test_ignores_empty_lines():
    payload = b'{"id":"event-1"}'

    chunks = [
        b"\n\r\n",
        payload + b"\n",
        b"\n\r\n",
    ]

    assert list(iter_messages(chunks)) == [payload]


def test_rejects_an_incomplete_message_when_stream_ends():
    chunks = [b'{"id":"event-1"']

    with pytest.raises(ValueError, match="incomplete message"):
        list(iter_messages(chunks))


def test_rejects_a_completed_message_larger_than_the_limit():
    chunks = [b"12345\n"]

    with pytest.raises(ValueError, match="exceeds"):
        list(iter_messages(chunks, max_message_bytes=4))


def test_rejects_an_oversized_unfinished_message_immediately():
    def chunks():
        yield b"123"
        yield b"45"
        pytest.fail("Reader requested more data after exceeding the limit")

    with pytest.raises(ValueError, match="exceeds"):
        list(iter_messages(chunks(), max_message_bytes=4))


def test_accepts_the_limit_with_a_split_crlf_delimiter():
    chunks = [
        b"1234\r",
        b"\n12\n",
    ]

    assert list(iter_messages(chunks, max_message_bytes=4)) == [
        b"1234",
        b"12",
    ]
