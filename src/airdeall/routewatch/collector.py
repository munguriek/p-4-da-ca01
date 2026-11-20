import logging
from contextlib import closing
from datetime import UTC, datetime
from time import sleep
from uuid import uuid4

import httpx

from .capture import capture_message
from .publisher import KafkaPublisher
from .source import iter_ris_messages
from .stream import IncompleteMessageError

logger = logging.getLogger(__name__)


def collect_messages(
    client: httpx.Client,
    publisher: KafkaPublisher,
    *,
    collector: str = "rrc21",
    max_messages: int = 0,
) -> int:
    """Collect across connections; zero means no message limit."""
    if max_messages < 0:
        raise ValueError("max_messages must be zero or positive")

    total = 0
    backoff = 1.0

    try:
        while max_messages == 0 or total < max_messages:
            run_id = uuid4()
            reason = "end of stream"
            try:
                with closing(
                    iter_ris_messages(client, collector=collector)
                ) as messages:
                    for payload in messages:
                        record = capture_message(
                            payload,
                            run_id=run_id,
                            received_at=datetime.now(UTC),
                        )
                        publisher.publish(record)
                        total += 1
                        backoff = 1.0

                        if max_messages and total >= max_messages:
                            break
            except (
                httpx.NetworkError,
                httpx.TimeoutException,
                httpx.RemoteProtocolError,
                httpx.ProxyError,
                IncompleteMessageError,
            ) as error:
                reason = type(error).__name__
            if max_messages and total >= max_messages:
                break
            publisher.flush(timeout=35)
            logger.warning(
                "RIS connection ended (%s); reconnecting in %.1f seconds; run_id=%s",
                reason,
                backoff,
                run_id,
            )
            sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    finally:
        publisher.flush(timeout=35)

    return total
