"""fold_message: reactions attach to the first link in a post, not every link."""

from __future__ import annotations

from concerto.board import LinkEntry, fold_message


def _reactions(name: str, users: list[str]) -> list[dict[str, object]]:
    return [{"name": name, "users": users}]


def test_only_first_link_gets_reaction_counts() -> None:
    entries: dict[str, LinkEntry] = {}
    fold_message(
        entries,
        "1",
        "first https://a/1 second https://b/2",
        _reactions("thumbsup", ["U1", "U2"]),
    )
    assert entries["https://a/1"].going == 2
    # The second link is tracked but gets no counts.
    assert "https://b/2" in entries
    assert entries["https://b/2"].going == 0


def test_link_gets_counts_from_the_post_it_leads() -> None:
    entries: dict[str, LinkEntry] = {}
    # https://b/2 trails here (no counts)...
    fold_message(entries, "1", "https://a/1 https://b/2", _reactions("ticket", ["U1"]))
    # ...but leads its own post, so it picks up those counts.
    fold_message(entries, "2", "https://b/2", _reactions("ticket", ["U2", "U3"]))
    assert entries["https://a/1"].going == 1
    assert entries["https://b/2"].going == 2
