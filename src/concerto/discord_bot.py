"""Discord platform layer: a discord.py gateway client on the agnostic core.

Mirrors ``slack_bot``: translates Discord events into the neutral
`BoardService` ingestion calls and supplies the two hooks.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from typing import TYPE_CHECKING, Any

import aiohttp
import aiosqlite
import discord
from fastapi import FastAPI

from concerto import concert_scraper
from concerto.board import (
    PLUS_ONE_REACTIONS,
    PRAY_REACTIONS,
    QUESTION_REACTIONS,
    WEB_API_TIMEOUT_SECONDS,
    BoardRepository,
    BoardService,
    LinkEntry,
    fold_message,
    register_board_routes,
    required_env,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger("concerto")

TRACKED_REACTIONS = PLUS_ONE_REACTIONS | QUESTION_REACTIONS | PRAY_REACTIONS

# Discord delivers unicode reactions as the raw character; map the ones we
# track onto the neutral shortcodes the core uses. Custom (server) emoji keep
# their own name, so a server can define e.g. a custom ``:ticket:``.
_UNICODE_TO_NAME = {
    "\N{THUMBS UP SIGN}": "thumbsup",
    "\N{TICKET}": "ticket",
    "\N{BLACK QUESTION MARK ORNAMENT}": "question",
    "\N{WHITE QUESTION MARK ORNAMENT}": "grey_question",
    "\N{EYES}": "eyes",
    "\N{PERSON WITH FOLDED HANDS}": "pray",
}


class DiscordBotService(BoardService):
    def __init__(
        self,
        token: str,
        session: aiohttp.ClientSession,
        repository: BoardRepository,
        command: str = "!concerto",
    ) -> None:
        super().__init__(session, repository)
        self._token = token
        self._command = command

        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.guild_reactions = True
        intents.message_content = True  # privileged: enable in the dev portal
        self._client = discord.Client(intents=intents)
        self._register_handlers()

    # --- core hooks ---

    def is_supported_channel(self, channel_id: str) -> bool:
        # Discord channel ids are numeric snowflakes; DMs are filtered in the
        # event handlers (where the guild is known), not here.
        return channel_id.isdigit()

    def message_url(self, channel_id: str, source_message_ts: str | None) -> str | None:
        if not source_message_ts or not channel_id.isdigit():
            return None
        channel = self._client.get_channel(int(channel_id))
        guild = getattr(channel, "guild", None)
        if guild is None:
            return None
        return (
            f"https://discord.com/channels/{guild.id}/{channel_id}/{source_message_ts}"
        )

    # --- gateway lifecycle ---

    async def run(self) -> None:
        await self._client.start(self._token)

    async def close(self) -> None:
        await self._client.close()

    def _register_handlers(self) -> None:
        client = self._client

        @client.event
        async def on_ready() -> None:
            logger.info("Connected to Discord as %s", client.user)

        @client.event
        async def on_message(message: discord.Message) -> None:
            await self._on_message(message)

        @client.event
        async def on_raw_reaction_add(
            payload: discord.RawReactionActionEvent,
        ) -> None:
            await self._on_reaction(payload)

        @client.event
        async def on_raw_reaction_remove(
            payload: discord.RawReactionActionEvent,
        ) -> None:
            await self._on_reaction(payload)

    # --- event handlers ---

    async def _on_message(self, message: discord.Message) -> None:
        if message.author.bot or message.guild is None:
            return

        content = message.content.strip().lower()
        if content in {f"{self._command} rebuild", f"{self._command} rescan"}:
            await self._rebuild(message.channel)
            return

        await self.apply_message(str(message.channel.id), message.id, message.content)

    async def _on_reaction(self, payload: discord.RawReactionActionEvent) -> None:
        if payload.guild_id is None:
            return
        # Skip a message fetch for reactions we don't track.
        if _reaction_name(payload.emoji) not in TRACKED_REACTIONS:
            return

        channel = self._client.get_channel(payload.channel_id)
        # ponytail: text channels only; add Thread/VoiceChannel here if links
        # start showing up in those.
        if not isinstance(channel, discord.TextChannel):
            return
        try:
            message = await channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.Forbidden):
            return

        # Re-parse the whole message's reactions, never the single delta.
        reactions = await _normalize_reactions(message)
        await self.apply_reactions(
            str(channel.id), message.id, message.content, reactions
        )

    async def _rebuild(self, channel: discord.abc.MessageableChannel) -> None:
        entries: dict[str, LinkEntry] = {}
        async for message in channel.history(limit=None):
            reactions = await _normalize_reactions(message)
            fold_message(entries, message.id, message.content, reactions)
        await self.replace_board(str(channel.id), entries)


def _reaction_name(emoji: discord.PartialEmoji | discord.Emoji | str) -> str:
    """Map a Discord emoji to the core's neutral reaction name."""
    if isinstance(emoji, str):
        return _UNICODE_TO_NAME.get(emoji, emoji)
    # Emoji / PartialEmoji: unicode ones have no id; custom keep their name.
    name = emoji.name or ""
    if emoji.id is None:
        return _UNICODE_TO_NAME.get(name, name)
    return name


async def _normalize_reactions(message: discord.Message) -> list[dict[str, Any]]:
    """Build the core's neutral reaction shape.

    Fetches users only for the reactions we track (avoids pulling user lists
    for noise).
    """
    result: list[dict[str, Any]] = []
    for reaction in message.reactions:
        name = _reaction_name(reaction.emoji)
        if name not in TRACKED_REACTIONS:
            continue
        users = [str(user.id) async for user in reaction.users()]
        result.append({"name": name, "users": users})
    return result


def create_app() -> FastAPI:
    token = required_env("DISCORD_BOT_TOKEN")
    database_path = os.getenv("CONCERTO_DB_PATH", "./concerto.db")
    command = os.getenv("CONCERTO_DISCORD_COMMAND", "!concerto").strip()

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=WEB_API_TIMEOUT_SECONDS)
        async with (
            aiohttp.ClientSession(
                timeout=timeout, max_field_size=concert_scraper.MAX_HEADER_BYTES
            ) as session,
            aiosqlite.connect(database_path) as db,
        ):
            repository = BoardRepository(db)
            await repository.init()
            service = DiscordBotService(
                token=token,
                session=session,
                repository=repository,
                command=command,
            )
            client_task = asyncio.create_task(service.run())
            try:
                yield {"service": service}
            finally:
                await service.close()
                client_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await client_task

    app = FastAPI(lifespan=lifespan)
    register_board_routes(app)
    return app
