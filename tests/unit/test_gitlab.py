from datetime import date

import httpx
import pytest

from airdeall.gitlab import GitLabAuthError, check_token

TODAY = date(2026, 11, 20)


class FakeClient:
    """Answers every GET with the token description it was given"""

    def __init__(self, details: dict):
        self.details = details
        self.paths: list[str] = []

    def get(self, path: str) -> httpx.Response:
        self.paths.append(path)
        return httpx.Response(200, json=self.details)


def token(**changes) -> dict:
    """A usable read_api token's description, with any field changed"""
    usable = {
        "active": True,
        "revoked": False,
        "scopes": ["read_api"],
        "expires_at": "2026-12-31",
    }
    return usable | changes


def test_read_api_token_gives_its_expiry_date():
    client = FakeClient(token())

    assert check_token(client, TODAY) == date(2026, 12, 31)
    assert client.paths == ["/personal_access_tokens/self"]


@pytest.mark.parametrize(
    "scopes", [["api", "read_api"], ["api"], ["read_api", "read_user"]]
)
def test_token_wider_than_read_api_is_refused(scopes):
    with pytest.raises(GitLabAuthError):
        check_token(FakeClient(token(scopes=scopes)), TODAY)


@pytest.mark.parametrize(
    "changes",
    [{"active": False}, {"revoked": True}, {"expires_at": None}],
    ids=["inactive", "revoked", "no-expiry"],
)
def test_unusable_token_is_refused(changes):
    with pytest.raises(GitLabAuthError):
        check_token(FakeClient(token(**changes)), TODAY)


def test_token_expiring_within_14_days_is_warned(caplog):
    check_token(FakeClient(token(expires_at="2026-11-30")), TODAY)

    assert "renew" in caplog.text
