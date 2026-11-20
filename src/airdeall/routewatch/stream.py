from collections.abc import Iterable, Iterator


def iter_messages(chunks: Iterable[bytes]) -> Iterator[bytes]:
    """Join incoming byte chunks into complete messages"""
    raise NotImplementedError