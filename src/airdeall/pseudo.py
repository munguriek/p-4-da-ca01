"""Pseudonyms for GitLab users, applied before anything reaches Kafka.

Pure functions: the key is an argument, and nothing reads the clock,
the environment or the network.
"""

import hashlib
import hmac
import re

# An address ends in a dot and two or more letters, so foo@1.2.3 is not one.
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}")
# An @ that no letter, digit or dot comes before, then a username or a group path.
MENTION = re.compile(r"(?<![A-Za-z0-9.])@([A-Za-z0-9_.\-/]+)")


def pseudonym(key: bytes, username: str) -> str:
    """u_ and the first 16 hex characters of HMAC-SHA256 of the lower-cased name"""
    digest = hmac.new(key, username.lower().encode(), hashlib.sha256).hexdigest()
    return "u_" + digest[:16]


def scrub_text(key: bytes, text: str | None) -> str | None:
    """Replace email addresses and @-mentions of people; keep group paths"""
    if not text:
        return text

    def replace(mention: re.Match[str]) -> str:
        name = mention.group(1).rstrip(".")  # a username never ends with a dot
        if not name or "/" in name:  # nothing left, or a group path
            return mention.group(0)
        full_stops = mention.group(1)[len(name) :]
        return "@" + pseudonym(key, name) + full_stops

    return MENTION.sub(replace, EMAIL.sub("[email]", text))


def scrub_user(key: bytes, user: dict) -> dict:
    """A user becomes its pseudonym alone: no id, name, avatar or profile URL"""
    return {"pseudonym": pseudonym(key, user["username"])}


def _scrub_users(key: bytes, value):
    """A copy of value with every user in it, at any depth, made a pseudonym"""
    if isinstance(value, dict):
        if "username" in value:
            return scrub_user(key, value)
        return {name: _scrub_users(key, item) for name, item in value.items()}
    if isinstance(value, list):
        return [_scrub_users(key, item) for item in value]
    return value


def scrub_issue(key: bytes, issue: dict) -> dict:
    """A copy of an issue with its users and the mentions in its text pseudonymised"""
    scrubbed = _scrub_users(key, issue)
    for name in ("title", "description"):
        if name in scrubbed:
            scrubbed[name] = scrub_text(key, scrubbed[name])
    return scrubbed


def scrub_event(key: bytes, event: dict) -> dict:
    """A copy of a label or state event with its user pseudonymised"""
    scrubbed = _scrub_users(key, event)
    label = scrubbed.get("label")
    if label and label.get("description"):
        label["description"] = scrub_text(key, label["description"])
    return scrubbed
