"""Tests for aggregate_status_counts precedence and dedup."""

from __future__ import annotations

from concerto.board import aggregate_status_counts


def test_precedence_and_dedup() -> None:
    # +1 outranks ?, which outranks pray; each user counted once.
    reactions = [
        {"name": "thumbsup", "users": ["U1", "U2"]},
        {"name": "ticket", "users": ["U2"]},  # U2 already counted as going
        {"name": "eyes", "users": ["U2", "U3"]},  # U2 has a ticket -> not undecided
        {"name": "pray", "users": ["U3", "U4"]},  # U3 is undecided -> not looking
    ]
    assert aggregate_status_counts(reactions) == (2, 1, 1)


def test_ignored_and_malformed_contribute_nothing() -> None:
    assert aggregate_status_counts([{"name": "heart", "users": ["U1"]}]) == (0, 0, 0)
    assert aggregate_status_counts(None) == (0, 0, 0)
    assert aggregate_status_counts([{"name": "pray"}, "junk"]) == (0, 0, 0)
