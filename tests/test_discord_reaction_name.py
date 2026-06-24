"""Tests for mapping Discord emoji to the core's neutral reaction names."""

from __future__ import annotations

from types import SimpleNamespace

from concerto.discord_bot import _reaction_name


def test_unicode_strings_map_to_shortcodes() -> None:
    # Raw unicode string (how discord.py delivers unicode reactions).
    assert _reaction_name("\N{THUMBS UP SIGN}") == "thumbsup"
    assert _reaction_name("\N{PERSON WITH FOLDED HANDS}") == "pray"
    # Unknown unicode passes through unchanged (won't match a tracked set).
    assert _reaction_name("\N{PILE OF POO}") == "\N{PILE OF POO}"


def test_emoji_objects() -> None:
    # Unicode PartialEmoji: no id, name is the raw char. SimpleNamespace
    # duck-types the emoji; _reaction_name only touches .id / .name.
    unicode_emoji = SimpleNamespace(id=None, name="\N{EYES}")
    assert _reaction_name(unicode_emoji) == "eyes"  # type: ignore[arg-type]

    # Custom server emoji: has an id, name used as-is (lets a server define
    # its own e.g. :ticket:).
    custom = SimpleNamespace(id=123, name="ticket")
    assert _reaction_name(custom) == "ticket"  # type: ignore[arg-type]
