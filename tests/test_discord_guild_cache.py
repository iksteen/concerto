"""Discord's render path (message_url, origin prefix) reads only the cache."""

from __future__ import annotations

from unittest.mock import MagicMock

from concerto.discord_bot import DiscordBotService, _guild_of


def _service() -> DiscordBotService:
    svc = DiscordBotService(
        name="mydiscord",
        token="x",
        session=MagicMock(),
        repository=MagicMock(),
        db=MagicMock(),
    )
    # A live gateway lookup while rendering is exactly what we're avoiding.
    svc._client = MagicMock()
    svc._client.get_channel.side_effect = AssertionError("live lookup at render")
    return svc


def test_guild_of_extracts_id_and_name() -> None:
    channel = MagicMock()
    channel.guild.id = 123
    channel.guild.name = "My Server"
    assert _guild_of(channel) == ("123", "My Server")
    no_guild = MagicMock()
    no_guild.guild = None
    assert _guild_of(no_guild) == (None, None)
    assert _guild_of(None) == (None, None)


def test_render_path_uses_cache_not_gateway() -> None:
    svc = _service()
    svc._guild_ids["42"] = "999"
    svc._guild_names["42"] = "My Server"

    assert svc.message_url("42", "1700000000") == (
        "https://discord.com/channels/999/42/1700000000"
    )
    assert svc._origin_prefix("42") == "My Server"
    # Uncached channel: fall back, still no gateway call.
    assert svc.message_url("77", "1700000000") is None
    assert svc._origin_prefix("77") == "mydiscord"
    assert not svc._client.get_channel.called
