import logging
from contextlib import closing
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import sleep
from uuid import uuid4

import httpx

from .capture import capture_message
from .publisher import KafkaPublisher
from .source import iter_ris_messages
from .stream import IncompleteMessageError

logger = logging.getLogger(__name__)


def _retry_after_seconds(
    response: httpx.Response,
    *,
    now: datetime,
) -> float:
    value = response.headers.get("Retry-After")
    if value is None:
        return 0.0

    try:
        return float(max(0, int(value)))
    except (ValueError, OverflowError):
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0

    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)

    return max(0.0, (when - now).total_seconds())


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
            retry_after = 0.0
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
            except httpx.HTTPStatusError as error:
                status = error.response.status_code
                if status not in {408, 429, 500, 502, 503, 504}:
                    raise

                reason = f"http_{status}"
                retry_after = _retry_after_seconds(
                    error.response,
                    now=datetime.now(UTC),
                )
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
            delay = max(backoff, retry_after)
            publisher.flush(timeout=35)
            logger.warning(
                "RIS connection ended (%s); reconnecting in %.1f seconds; run_id=%s",
                reason,
                delay,
                run_id,
            )
            sleep(delay)
            backoff = min(backoff * 2, 30.0)
    finally:
        publisher.flush(timeout=35)

    return total
