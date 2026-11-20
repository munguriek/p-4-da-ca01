"""GitLab.com's REST API, for the production tracker poller"""

import logging
from datetime import date, timedelta
from typing import Protocol

import httpx

log = logging.getLogger(__name__)

RENEW_WARNING = timedelta(days=14)


class GitLabAuthError(Exception):
    """The token is unusable or wider than read_api. Never retried"""


class GitLabGetter(Protocol):
    """The one method check_token needs from the client"""

    def get(self, path: str) -> httpx.Response: ...


def check_token(client: GitLabGetter, today: date) -> date:
    """Refuse a token that is inactive, revoked, undated or wider than read_api

    Only the expiry date is logged: the response also names the user and the
    addresses that last used the token, so it is neither logged nor stored.
    """
    details = client.get("/personal_access_tokens/self").json()
    usable = details.get("active") and not details.get("revoked")
    if not usable or details.get("scopes") != ["read_api"]:
        raise GitLabAuthError("the GitLab token is unusable or wider than read_api")
    if not details.get("expires_at"):
        raise GitLabAuthError("the GitLab token has no expiry date")
    expires = date.fromisoformat(details["expires_at"])
    log.info("GitLab token expires on %s", expires)
    if expires - today < RENEW_WARNING:
        log.warning("GitLab token expires in %d days: renew it", (expires - today).days)
    return expires
