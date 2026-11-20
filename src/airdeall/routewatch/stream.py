from collections.abc import Iterable, Iterator


def iter_messages(
    chunks: Iterable[bytes], *, max_message_bytes: int = 8_000_000
) -> Iterator[bytes]:
    """Join incoming byte chunks into complete messages"""
    buffer = b""

    for chunk in chunks:
        buffer += chunk
        while b"\n" in buffer:
            message, buffer = buffer.split(b"\n", 1)
            message = message.removesuffix(b"\r")
            if len(message) > max_message_bytes:
                raise ValueError("Message exceeds maximum size")
            if message:
                yield message
        if len(buffer.removesuffix(b"\r")) > max_message_bytes:
            raise ValueError("Message exceeds maximum size")
    if buffer:
        raise ValueError("Stream ended with an incomplete message")
