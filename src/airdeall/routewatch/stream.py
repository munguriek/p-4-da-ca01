from collections.abc import Iterable, Iterator


def iter_messages(chunks: Iterable[bytes]) -> Iterator[bytes]:
    """Join incoming byte chunks into complete messages"""
    buffer = b""

    for chunk in chunks:
        buffer += chunk
        while b"\n" in buffer:
            message, buffer = buffer.split(b"\n", 1)
            message = message.removesuffix(b"\r")
            if message:
                yield message
