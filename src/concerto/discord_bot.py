"""Discord platform layer: a discord.py gateway client on the agnostic core.

Mirrors ``slack_bot``: translates Discord events into the neutral
`BoardService` ingestion calls and supplies the two hooks.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

import discord

from concerto.board import (
    PLUS_ONE_REACTIONS,
    PRAY_REACTIONS,
    QUESTION_REACTIONS,
    BoardRepository,
    BoardService,
    LinkEntry,
    fold_message,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine

    import aiohttp
    import aiosqlite

logger = logging.getLogger("concerto")

TRACKED_REACTIONS = PLUS_ONE_REACTIONS | QUESTION_REACTIONS | PRAY_REACTIONS

# Discord delivers unicode reactions as the raw character; map the ones we
# track onto the neutral shortcodes the core uses. Custom (server) emoji keep
# their own name, so a server can define e.g. a custom ``:ticket:``.
# Command feedback: reacted onto the command message (the bot never posts text).
_WORKING = "\N{HOURGLASS WITH FLOWING SAND}"
_DONE = "\N{WHITE HEAVY CHECK MARK}"
_REFUSED = "\N{CROSS MARK}"
_DENIED = "\N{NO ENTRY SIGN}"  # command refused: caller lacks permission

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
        name: str,
        token: str,
        session: aiohttp.ClientSession,
        repository: BoardRepository,
        db: aiosqlite.Connection,
        command: str = "!concerto",
    ) -> None:
        super().__init__(name, session, repository)
        self._token = token
        self._command = command
        self._db = db
        # Channel ids opted in via `!concerto track`; empty = track nothing.
        # Discord ids are globally-unique snowflakes, so this set is safe across
        # every server the bot is in without scoping by guild.
        self._tracked: set[str] = set()
        # channel_id -> guild id / name, cached so the render path (message_url,
        # origin label) never does a live gateway lookup.
        self._guild_ids: dict[str, str] = {}
        self._guild_names: dict[str, str] = {}
        # Background rebuilds (kicked off by `track`); kept referenced so the
        # loop doesn't GC them mid-run, cancelled on shutdown.
        self._tasks: set[asyncio.Task[None]] = set()

        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.guild_reactions = True
        intents.message_content = True  # privileged: enable in the dev portal
        self._client = discord.Client(intents=intents)
        self._register_handlers()

    # --- core hooks ---

    def is_supported_channel(self, channel_id: str) -> bool:
        # Opt-in: only channels explicitly tracked via `!concerto track`.
        return channel_id in self._tracked

    def message_url(self, channel_id: str, source_message_ts: str | None) -> str | None:
        guild_id = self._guild_ids.get(channel_id)
        if not source_message_ts or not guild_id:
            return None
        return (
            f"https://discord.com/channels/{guild_id}/{channel_id}/{source_message_ts}"
        )

    async def fetch_channel_name(self, channel_id: str) -> str | None:
        # Only used at startup/on ingestion to populate the cache, not at render.
        if not channel_id.isdigit():
            return None
        return _channel_name(self._client.get_channel(int(channel_id)))

    def _origin_prefix(self, channel_id: str) -> str:
        # Show the cached Discord server (guild) name, not the connector name.
        return self._guild_names.get(channel_id) or self._connector_id

    # --- gateway lifecycle ---

    async def setup(self) -> None:
        """Create the tracked-channels table and load it into memory."""
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS discord_tracked_channels ("
            "channel_id TEXT PRIMARY KEY, guild_id TEXT, guild_name TEXT)"
        )
        # Migrate tables created before guild_name existed.
        async with self._db.execute(
            "PRAGMA table_info(discord_tracked_channels)"
        ) as cursor:
            columns = {str(row[1]) async for row in cursor if len(row) > 1}
        if "guild_name" not in columns:
            await self._db.execute(
                "ALTER TABLE discord_tracked_channels ADD COLUMN guild_name TEXT"
            )
        await self._db.commit()
        async with self._db.execute(
            "SELECT channel_id, guild_id, guild_name FROM discord_tracked_channels"
        ) as cursor:
            async for row in cursor:
                channel_id = str(row[0])
                self._tracked.add(channel_id)
                if row[1]:
                    self._guild_ids[channel_id] = str(row[1])
                if row[2]:
                    self._guild_names[channel_id] = str(row[2])
        await self.load_channel_names()

    async def run(self) -> None:
        await self._client.start(self._token)

    async def close(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        await self._client.close()

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("Background task failed: %s", exc, exc_info=exc)

    def _register_handlers(self) -> None:
        client = self._client

        @client.event
        async def on_ready() -> None:
            logger.info("Connected to Discord as %s", client.user)
            # The gateway cache is populated now (unlike at setup()), so channel
            # and guild names resolve; warm the caches the render path reads.
            await self.refresh_channel_names()
            for channel_id in list(self._tracked):
                if channel_id.isdigit():
                    await self._remember_guild(client.get_channel(int(channel_id)))

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

        # Commands work in any channel (you must be able to `track` an
        # untracked one); the gate below only guards passive link ingestion.
        command = self._match_command(message.content.strip().lower())
        if command is not None:
            # Privileged: only members who can manage the channel may run them.
            if not _can_manage(message):
                await message.add_reaction(_DENIED)
                return
            if command == "track":
                await self._track(message)
            elif command == "untrack":
                await self._untrack(message)
            else:
                await self._rebuild(message)
            return

        if not self.is_supported_channel(str(message.channel.id)):
            return
        await self.set_channel_name(
            str(message.channel.id), _channel_name(message.channel)
        )
        await self._remember_guild(message.channel)
        await self.apply_message(str(message.channel.id), message.id, message.content)

    def _match_command(self, content: str) -> str | None:
        return {
            f"{self._command} track": "track",
            f"{self._command} untrack": "untrack",
            f"{self._command} rebuild": "rebuild",
            f"{self._command} rescan": "rebuild",
        }.get(content)

    async def _on_reaction(self, payload: discord.RawReactionActionEvent) -> None:
        if payload.guild_id is None:
            return
        if not self.is_supported_channel(str(payload.channel_id)):
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
        await self.set_channel_name(str(channel.id), _channel_name(channel))
        await self._remember_guild(channel)
        await self.apply_reactions(
            str(channel.id), message.id, message.content, reactions
        )

    async def _track(self, message: discord.Message) -> None:
        channel_id = str(message.channel.id)
        self._tracked.add(channel_id)
        # Persist the tracked row and cache the guild (id for links, name for
        # the origin label) in one upsert.
        await self._remember_guild(message.channel)
        # Backfill links posted before tracking, in the background; _rebuild
        # handles the ⏳→✅ feedback on this command message.
        self._spawn(self._rebuild(message))

    async def _remember_guild(self, channel: object) -> None:
        channel_id = str(getattr(channel, "id", ""))
        guild_id, guild_name = _guild_of(channel)
        if not channel_id or not guild_id:
            return
        if (
            self._guild_ids.get(channel_id) == guild_id
            and self._guild_names.get(channel_id) == guild_name
        ):
            return
        # Share the board lock so this commit can't flush a board save's
        # half-done transaction on the same connection.
        async with self._lock:
            self._guild_ids[channel_id] = guild_id
            if guild_name:
                self._guild_names[channel_id] = guild_name
            await self._db.execute(
                "INSERT INTO discord_tracked_channels"
                "(channel_id, guild_id, guild_name) VALUES(?, ?, ?) "
                "ON CONFLICT(channel_id) DO UPDATE SET guild_id = excluded.guild_id, "
                "guild_name = COALESCE(excluded.guild_name, guild_name)",
                (channel_id, guild_id, guild_name),
            )
            await self._db.commit()

    async def _untrack(self, message: discord.Message) -> None:
        channel_id = str(message.channel.id)
        async with self._lock:
            await self._db.execute(
                "DELETE FROM discord_tracked_channels WHERE channel_id = ?",
                (channel_id,),
            )
            await self._db.commit()
        self._tracked.discard(channel_id)
        self._guild_ids.pop(channel_id, None)
        self._guild_names.pop(channel_id, None)
        # Drop the channel's tracked links so the board doesn't go stale
        # (replace_board takes the lock itself, so it's outside the block above).
        await self.replace_board(channel_id, {})
        await message.add_reaction(_DONE)

    async def _rebuild(self, message: discord.Message) -> None:
        channel = message.channel
        if not self.is_supported_channel(str(channel.id)):
            await message.add_reaction(_REFUSED)
            return
        await message.add_reaction(_WORKING)
        await self.set_channel_name(str(channel.id), _channel_name(channel))
        await self._remember_guild(channel)
        entries: dict[str, LinkEntry] = {}
        async for historic in channel.history(limit=None):
            reactions = await _normalize_reactions(historic)
            fold_message(entries, historic.id, historic.content, reactions)
        await self.replace_board(str(channel.id), entries)
        await self._clear_working(message)
        await message.add_reaction(_DONE)

    async def _clear_working(self, message: discord.Message) -> None:
        user = self._client.user
        if user is None:
            return
        with contextlib.suppress(discord.HTTPException):
            await message.remove_reaction(_WORKING, user)


def _channel_name(channel: object) -> str | None:
    name = getattr(channel, "name", None)
    return f"#{name}" if isinstance(name, str) and name else None


def _guild_of(channel: object) -> tuple[str | None, str | None]:
    guild = getattr(channel, "guild", None)
    guild_id = getattr(guild, "id", None)
    name = getattr(guild, "name", None)
    return (
        str(guild_id) if guild_id is not None else None,
        name if isinstance(name, str) and name else None,
    )


def _can_manage(message: discord.Message) -> bool:
    """Whether the command's author may run the privileged !concerto commands.

    Requires Manage Channels on the command's channel — administrators and the
    guild owner have it implicitly, and per-channel moderator overrides count.
    """
    author = message.author
    channel = message.channel
    if not isinstance(author, discord.Member) or not isinstance(
        channel, discord.abc.GuildChannel
    ):
        return False
    return channel.permissions_for(author).manage_channels


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
