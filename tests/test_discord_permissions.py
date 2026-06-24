"""Tests for the !concerto command permission guard (_can_manage)."""

from __future__ import annotations

from unittest.mock import MagicMock

import discord

from concerto.discord_bot import _can_manage


def _message(*, is_member: bool, manage_channels: bool) -> discord.Message:
    # spec= sets __class__ so the isinstance() checks in _can_manage hold.
    author = MagicMock(spec=discord.Member if is_member else discord.User)
    perms = MagicMock()
    perms.manage_channels = manage_channels
    channel = MagicMock(spec=discord.TextChannel)
    channel.permissions_for.return_value = perms
    message = MagicMock(spec=discord.Message)
    message.author = author
    message.channel = channel
    return message


def test_member_with_manage_channels_is_allowed() -> None:
    assert _can_manage(_message(is_member=True, manage_channels=True)) is True


def test_member_without_manage_channels_is_denied() -> None:
    assert _can_manage(_message(is_member=True, manage_channels=False)) is False


def test_non_member_author_is_denied() -> None:
    # e.g. a webhook or plain User, never a guild Member.
    assert _can_manage(_message(is_member=False, manage_channels=True)) is False
