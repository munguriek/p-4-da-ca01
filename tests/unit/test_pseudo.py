from airdeall.pseudo import pseudonym, scrub_event, scrub_issue, scrub_text

KEY = bytes(32)  # fake keys
OTHER_KEY = bytes([1]) * 32
NAMES = ("alice", "bob", "carol", "dave")  # invented people only


def user(username: str) -> dict:
    """An invented GitLab user, shaped as the API returns one."""
    return {
        "id": 1,
        "username": username,
        "name": username.title(),
        "state": "active",
        "avatar_url": f"https://example.com/{username}.png",
        "web_url": f"https://gitlab.example.com/{username}",
    }


ISSUE = {
    "id": 101,
    "iid": 7,
    "title": "Restart the primary, asked by @alice",
    "description": "cc @bob and @gitlab-com/gl-infra/sre",
    "author": user("alice"),
    "assignee": user("bob"),
    "assignees": [user("bob"), user("carol")],
    "closed_by": user("dave"),
    "labels": ["change::in-progress", "C2"],
    "created_at": "2026-11-01T10:00:00.000Z",
    "updated_at": "2026-11-02T10:00:00.000Z",
}


def test_pseudonym_is_stable_and_keyed():
    first = pseudonym(KEY, "alice")

    assert first == pseudonym(KEY, "alice")
    assert first != pseudonym(OTHER_KEY, "alice")
    assert first.startswith("u_") and len(first) == 18


def test_pseudonym_ignores_case():
    assert pseudonym(KEY, "Alice") == pseudonym(KEY, "alice")


def test_scrub_text_replaces_mentions_not_emails():
    text = "ping @alice, mail alice@example.com, cc @gitlab-com/gl-infra/sre"

    assert scrub_text(KEY, text) == (
        f"ping @{pseudonym(KEY, 'alice')}, mail [email], cc @gitlab-com/gl-infra/sre"
    )


def test_scrub_text_keeps_the_full_stop():
    assert scrub_text(KEY, "ask @bob.") == f"ask @{pseudonym(KEY, 'bob')}."


def test_scrub_text_keeps_dots_inside_a_username():
    expected = f"ask @{pseudonym(KEY, 'alice.smith')}."

    assert scrub_text(KEY, "ask @alice.smith.") == expected


def test_scrub_text_leaves_versions_and_digests_alone():
    text = "pin foo@1.2.3 and image@sha256:0123abcd"

    assert scrub_text(KEY, text) == text


def test_scrub_issue_leaves_no_user_fields():
    scrubbed = scrub_issue(KEY, ISSUE)

    for field in ("author", "assignee", "closed_by"):
        assert set(scrubbed[field]) == {"pseudonym"}
    assert all(set(person) == {"pseudonym"} for person in scrubbed["assignees"])
    assert not any(name in str(scrubbed).lower() for name in NAMES)
    assert scrubbed["labels"] == ISSUE["labels"]
    assert (scrubbed["created_at"], scrubbed["updated_at"]) == (
        ISSUE["created_at"],
        ISSUE["updated_at"],
    )
    assert ISSUE["author"]["username"] == "alice"  # the original is not used


def test_scrub_event_user_is_a_pseudonym():
    closed = {"id": 9, "user": user("alice"), "state": "closed"}
    nobody = {"id": 10, "user": None, "state": "reopened"}

    assert scrub_event(KEY, closed)["user"] == {"pseudonym": pseudonym(KEY, "alice")}
    assert scrub_event(KEY, nobody)["user"] is None


def test_scrub_event_scrubs_label_description():
    label = {
        "id": 3,
        "name": "change::in-progress",
        "color": "#ed9121",
        "description": "Ask @alice first",
    }
    event = {"id": 11, "user": user("bob"), "label": label, "action": "add"}

    scrubbed = scrub_event(KEY, event)["label"]

    assert scrubbed["description"] == f"Ask @{pseudonym(KEY, 'alice')} first"
    assert (scrubbed["name"], scrubbed["color"]) == (label["name"], label["color"])
