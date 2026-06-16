from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs

import aiohttp
import aiosqlite
from fastapi import FastAPI, HTTPException, Request, Response

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Coroutine

logger = logging.getLogger("concerto")

_background_tasks: set[asyncio.Task[Any]] = set()


def _spawn(coro: Coroutine[Any, Any, None]) -> None:
    # Keep a reference so the task is not garbage-collected mid-flight.
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


SLACK_API_BASE = "https://slack.com/api"
SUMMARY_MARKER = "[concerto-link-board]"
PLUS_ONE_REACTIONS = {"+1", "thumbsup"}
QUESTION_REACTIONS = {"question", "grey_question"}
PRAY_REACTIONS = {"pray"}


@dataclass
class LinkEntry:
    posters: set[str] = field(default_factory=set)
    ticket_holders: set[str] = field(default_factory=set)
    interested: set[str] = field(default_factory=set)
    ticketswap_wanted: set[str] = field(default_factory=set)
    source_message_ts: str | None = None


@dataclass
class ChannelBoard:
    pin_ts: str | None = None
    links: dict[str, LinkEntry] = field(default_factory=dict)


class SlackApiError(RuntimeError):
    pass


class BoardRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def init(self) -> None:
        await self._db.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS board_state (
                channel_id TEXT PRIMARY KEY,
                pin_ts TEXT
            );

            CREATE TABLE IF NOT EXISTS links (
                channel_id TEXT NOT NULL,
                url TEXT NOT NULL,
                source_message_ts TEXT,
                PRIMARY KEY (channel_id, url)
            );

            CREATE TABLE IF NOT EXISTS link_posters (
                channel_id TEXT NOT NULL,
                url TEXT NOT NULL,
                user_id TEXT NOT NULL,
                PRIMARY KEY (channel_id, url, user_id)
            );

            CREATE TABLE IF NOT EXISTS link_statuses (
                channel_id TEXT NOT NULL,
                url TEXT NOT NULL,
                user_id TEXT NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (channel_id, url, user_id),
                CHECK (status IN ('ticket_holder', 'interested', 'ticketswap_wanted'))
            );
            """
        )
        await self._ensure_links_source_message_ts_column()
        await self._db.commit()

    async def _ensure_links_source_message_ts_column(self) -> None:
        async with self._db.execute("PRAGMA table_info(links)") as cursor:
            columns = {str(row[1]) async for row in cursor if len(row) > 1}
        if "source_message_ts" in columns:
            return
        await self._db.execute("ALTER TABLE links ADD COLUMN source_message_ts TEXT")

    async def load_board(self, channel_id: str) -> ChannelBoard:
        board = ChannelBoard()

        async with self._db.execute(
            "SELECT pin_ts FROM board_state WHERE channel_id = ?",
            (channel_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                board.pin_ts = row[0]

        async with self._db.execute(
            "SELECT url, source_message_ts FROM links WHERE channel_id = ?",
            (channel_id,),
        ) as cursor:
            async for row in cursor:
                board.links[str(row[0])] = LinkEntry(
                    source_message_ts=_normalize_ts(row[1])
                )

        await self._load_memberships(channel_id, board.links, "link_posters", "posters")
        await self._load_statuses(channel_id, board.links)

        return board

    async def _load_memberships(
        self,
        channel_id: str,
        links: dict[str, LinkEntry],
        table_name: str,
        member_attr: str,
    ) -> None:
        # table_name is an internal constant, never user input.
        query = f"SELECT url, user_id FROM {table_name} WHERE channel_id = ?"  # noqa: S608
        async with self._db.execute(query, (channel_id,)) as cursor:
            async for row in cursor:
                url = str(row[0])
                user_id = str(row[1])
                entry = links.setdefault(url, LinkEntry())
                group = getattr(entry, member_attr)
                if isinstance(group, set):
                    group.add(user_id)

    async def _load_statuses(
        self, channel_id: str, links: dict[str, LinkEntry]
    ) -> None:
        async with self._db.execute(
            "SELECT url, user_id, status FROM link_statuses WHERE channel_id = ?",
            (channel_id,),
        ) as cursor:
            async for row in cursor:
                url = str(row[0])
                user_id = str(row[1])
                status = str(row[2])
                entry = links.setdefault(url, LinkEntry())
                if status == "ticket_holder":
                    entry.ticket_holders.add(user_id)
                elif status == "interested":
                    entry.interested.add(user_id)
                elif status == "ticketswap_wanted":
                    entry.ticketswap_wanted.add(user_id)

    async def save_board(self, channel_id: str, board: ChannelBoard) -> None:
        await self._db.execute(
            "INSERT INTO board_state(channel_id, pin_ts) VALUES(?, ?) "
            "ON CONFLICT(channel_id) DO UPDATE SET pin_ts = excluded.pin_ts",
            (channel_id, board.pin_ts),
        )

        await self._db.execute("DELETE FROM links WHERE channel_id = ?", (channel_id,))
        await self._db.execute(
            "DELETE FROM link_posters WHERE channel_id = ?", (channel_id,)
        )
        await self._db.execute(
            "DELETE FROM link_statuses WHERE channel_id = ?", (channel_id,)
        )

        for url, entry in board.links.items():
            await self._db.execute(
                "INSERT INTO links(channel_id, url, source_message_ts) VALUES(?, ?, ?)",
                (channel_id, url, _normalize_ts(entry.source_message_ts)),
            )
            await self._insert_memberships(
                channel_id, url, "link_posters", entry.posters
            )
            await self._insert_status_memberships(
                channel_id, url, "ticket_holder", entry.ticket_holders
            )
            await self._insert_status_memberships(
                channel_id, url, "interested", entry.interested
            )
            await self._insert_status_memberships(
                channel_id,
                url,
                "ticketswap_wanted",
                entry.ticketswap_wanted,
            )

        await self._db.commit()

    async def _insert_memberships(
        self,
        channel_id: str,
        url: str,
        table_name: str,
        user_ids: set[str],
    ) -> None:
        if not user_ids:
            return

        query = f"INSERT INTO {table_name}(channel_id, url, user_id) VALUES(?, ?, ?)"
        await self._db.executemany(
            query,
            [(channel_id, url, user_id) for user_id in sorted(user_ids)],
        )

    async def _insert_status_memberships(
        self,
        channel_id: str,
        url: str,
        status: str,
        user_ids: set[str],
    ) -> None:
        if not user_ids:
            return

        await self._db.executemany(
            "INSERT INTO link_statuses(channel_id, url, user_id, status) VALUES(?, ?, ?, ?)",
            [(channel_id, url, user_id, status) for user_id in sorted(user_ids)],
        )


class SlackBotService:
    def __init__(
        self,
        bot_token: str,
        session: aiohttp.ClientSession,
        repository: BoardRepository,
    ) -> None:
        self._bot_token = bot_token
        self._session = session
        self._repository = repository
        self._boards: dict[str, ChannelBoard] = {}
        self._bot_user_id: str | None = None
        self._workspace_url: str | None = None
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        auth_info = await self._api_call("auth.test", {})
        user_id = str(auth_info.get("user_id", ""))
        if not user_id:
            msg = "Slack auth.test did not return user_id"
            raise SlackApiError(msg)
        self._bot_user_id = user_id
        self._workspace_url = _normalize_workspace_url(auth_info.get("url"))

    async def handle_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("type", ""))

        if event_type == "message":
            await self._handle_message_event(event)
            return

        if event_type == "member_joined_channel":
            await self._handle_member_joined_channel_event(event)
            return

        if event_type == "reaction_added":
            await self._handle_reaction_event(event, added=True)
            return

        if event_type == "reaction_removed":
            await self._handle_reaction_event(event, added=False)

    async def _handle_member_joined_channel_event(self, event: dict[str, Any]) -> None:
        if not self._bot_user_id:
            return

        if str(event.get("user", "")) != self._bot_user_id:
            return

        channel_id = str(event.get("channel", ""))
        if not _is_supported_channel(channel_id):
            return

        history_entries = await self._collect_history_link_entries(channel_id)
        if not history_entries:
            return

        async with self._lock:
            board = await self._get_board_locked(channel_id)
            changed = False

            for link, scanned_entry in history_entries.items():
                entry = board.links.setdefault(link, LinkEntry())
                if _merge_link_entry(entry, scanned_entry):
                    changed = True

            if not changed:
                return

            await self._persist_locked(channel_id, board)
            await self._update_summary_message_locked(channel_id, board)

    async def _handle_message_event(self, event: dict[str, Any]) -> None:
        if event.get("subtype"):
            return
        channel_id = str(event.get("channel", ""))
        if not _is_supported_channel(channel_id):
            return

        text = str(event.get("text", ""))
        user_id = str(event.get("user", ""))
        message_ts = _normalize_ts(event.get("ts"))
        links = _extract_links(text)
        if not links:
            return

        async with self._lock:
            board = await self._get_board_locked(channel_id)
            for link in links:
                entry = board.links.setdefault(link, LinkEntry())
                if user_id:
                    entry.posters.add(user_id)
                _set_earliest_source_message_ts(entry, message_ts)
            await self._persist_locked(channel_id, board)
            await self._update_summary_message_locked(channel_id, board)

    async def _rebuild_board_from_history(self, channel_id: str) -> None:
        history_entries = await self._collect_history_link_entries(channel_id)
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            board.links = history_entries
            await self._persist_locked(channel_id, board)
            await self._update_summary_message_locked(channel_id, board)

    async def handle_rebuild_command(self, channel_id: str) -> None:
        if not _is_supported_channel(channel_id):
            return
        await self._rebuild_board_from_history(channel_id)

    async def _handle_reaction_event(  # noqa: PLR0911
        self, event: dict[str, Any], *, added: bool
    ) -> None:
        reaction = str(event.get("reaction", ""))
        if (
            reaction not in PLUS_ONE_REACTIONS
            and reaction not in QUESTION_REACTIONS
            and reaction not in PRAY_REACTIONS
        ):
            return

        item = event.get("item")
        if not isinstance(item, dict):
            return
        if str(item.get("type", "")) != "message":
            return

        channel_id = str(item.get("channel", ""))
        if not _is_supported_channel(channel_id):
            return

        message_ts = str(item.get("ts", ""))
        if not message_ts:
            return

        user_id = str(event.get("user", ""))
        if not user_id:
            return

        text = await self._get_message_text(channel_id, message_ts)
        if not text:
            return

        links = _extract_links(text)
        if not links:
            return

        async with self._lock:
            board = await self._get_board_locked(channel_id)
            for link in links:
                entry = board.links.setdefault(link, LinkEntry())
                _set_earliest_source_message_ts(entry, message_ts)
                _apply_status_reaction(entry, reaction, user_id, added=added)
            await self._persist_locked(channel_id, board)
            await self._update_summary_message_locked(channel_id, board)

    async def _get_board_locked(self, channel_id: str) -> ChannelBoard:
        board = self._boards.get(channel_id)
        if board is not None:
            return board

        board = await self._repository.load_board(channel_id)
        self._boards[channel_id] = board
        return board

    async def _update_summary_message_locked(
        self, channel_id: str, board: ChannelBoard
    ) -> None:
        pin_ts = await self._ensure_summary_pin_locked(channel_id, board)
        message = self._render_summary_message(channel_id, board)
        try:
            await self._api_call(
                "chat.update",
                {
                    "channel": channel_id,
                    "ts": pin_ts,
                    "text": message,
                },
            )
        except SlackApiError as exc:
            if "message_not_found" not in str(exc):
                raise
            logger.warning(
                "Summary pin %s not found in channel %s; recreating pinned summary message",
                pin_ts,
                channel_id,
            )
            board.pin_ts = None
            await self._persist_locked(channel_id, board)
            recreated_pin_ts = await self._ensure_summary_pin_locked(channel_id, board)
            await self._api_call(
                "chat.update",
                {
                    "channel": channel_id,
                    "ts": recreated_pin_ts,
                    "text": message,
                },
            )

    async def _ensure_summary_pin_locked(
        self, channel_id: str, board: ChannelBoard
    ) -> str:
        if board.pin_ts:
            return board.pin_ts

        try:
            pins_response = await self._api_call("pins.list", {"channel": channel_id})
            items = pins_response.get("items", [])
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    message = item.get("message")
                    if not isinstance(message, dict):
                        continue
                    text = str(message.get("text", ""))
                    ts = str(message.get("ts", ""))
                    if SUMMARY_MARKER in text and ts:
                        board.pin_ts = ts
                        await self._persist_locked(channel_id, board)
                        return ts
        except SlackApiError as exc:
            if "invalid_arguments" not in str(exc):
                raise
            logger.warning(
                "pins.list returned invalid_arguments for channel %s; posting new summary pin",
                channel_id,
            )

        create_response = await self._api_call(
            "chat.postMessage",
            {
                "channel": channel_id,
                "text": self._render_summary_message(channel_id, board),
            },
        )
        message_ts = str(create_response.get("ts", ""))
        if not message_ts:
            msg = "Slack did not return a message timestamp for summary post"
            raise SlackApiError(msg)

        await self._api_call(
            "pins.add",
            {
                "channel": channel_id,
                "timestamp": message_ts,
            },
        )
        board.pin_ts = message_ts
        await self._persist_locked(channel_id, board)
        return message_ts

    async def _persist_locked(self, channel_id: str, board: ChannelBoard) -> None:
        await self._repository.save_board(channel_id, board)

    async def _get_message_text(self, channel: str, message_ts: str) -> str:
        history_response = await self._api_call(
            "conversations.history",
            {
                "channel": channel,
                "inclusive": True,
                "latest": message_ts,
                "limit": 1,
                "oldest": message_ts,
            },
        )

        messages = history_response.get("messages", [])
        if not isinstance(messages, list) or not messages:
            return ""

        message = messages[0]
        if not isinstance(message, dict):
            return ""

        return str(message.get("text", ""))

    async def _collect_history_link_entries(  # noqa: PLR0912
        self, channel_id: str
    ) -> dict[str, LinkEntry]:
        links_data: dict[str, LinkEntry] = {}
        cursor: str | None = None

        while True:
            payload: dict[str, Any] = {
                "channel": channel_id,
                "limit": 200,
            }
            if cursor:
                payload["cursor"] = cursor

            history_response = await self._api_call("conversations.history", payload)
            messages = history_response.get("messages", [])
            if isinstance(messages, list):
                for message in messages:
                    if not isinstance(message, dict):
                        continue
                    text = str(message.get("text", ""))
                    links = _extract_links(text)
                    if not links:
                        continue

                    user_id = str(message.get("user", ""))
                    for link in links:
                        entry = links_data.setdefault(link, LinkEntry())
                        if user_id:
                            entry.posters.add(user_id)
                        _set_earliest_source_message_ts(entry, message.get("ts"))

                    reactions = message.get("reactions")
                    if not isinstance(reactions, list):
                        continue

                    for reaction_obj in reactions:
                        if not isinstance(reaction_obj, dict):
                            continue
                        reaction = str(reaction_obj.get("name", ""))
                        users = reaction_obj.get("users")
                        if not isinstance(users, list):
                            continue
                        for raw_user_id in users:
                            reacting_user_id = str(raw_user_id)
                            if not reacting_user_id:
                                continue
                            for link in links:
                                _apply_status_reaction(
                                    links_data[link],
                                    reaction,
                                    reacting_user_id,
                                    added=True,
                                )

            metadata = history_response.get("response_metadata")
            if not isinstance(metadata, dict):
                break

            next_cursor = str(metadata.get("next_cursor", ""))
            if not next_cursor:
                break
            cursor = next_cursor

        return links_data

    async def _api_call(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._bot_token}",
            "Content-Type": "application/json; charset=utf-8",
        }

        async with self._session.post(
            f"{SLACK_API_BASE}/{method}",
            headers=headers,
            data=json.dumps(payload),
        ) as response:
            body = await response.json(content_type=None)

        if not isinstance(body, dict):
            msg = f"Slack API returned invalid response for {method}"
            raise SlackApiError(msg)

        if not body.get("ok", False):
            error_code = str(body.get("error", "unknown_error"))
            msg = f"Slack API {method} failed: {error_code}"
            raise SlackApiError(msg)

        return body

    def _render_summary_message(self, channel_id: str, board: ChannelBoard) -> str:
        lines = [
            SUMMARY_MARKER,
            "*Concert Link Board*",
            "Add a message with a concert link to track it here.",
            (
                "React with `:+1:` when you have a ticket, `:question:` when interested, "
                "or `:pray:` when looking on TicketSwap."
            ),
            "",
        ]

        if not board.links:
            lines.append("No links tracked yet.")
            return "\n".join(lines)

        for index, link in enumerate(sorted(board.links), start=1):
            entry = board.links[link]
            ticket_mentions = _format_mentions(entry.ticket_holders)
            interest_mentions = _format_mentions(entry.interested)
            ticketswap_mentions = _format_mentions(entry.ticketswap_wanted)
            if entry.source_message_ts:
                permalink = _build_slack_message_permalink(
                    workspace_url=self._workspace_url,
                    channel_id=channel_id,
                    message_ts=entry.source_message_ts,
                )
                lines.append(f"{index}. <{link}|{link}> <{permalink}|:link:>")
            else:
                lines.append(f"{index}. <{link}|{link}>")
            lines.append(
                "   "
                f"tickets: {ticket_mentions} / "
                f"interested: {interest_mentions} / "
                f"ticketswap wanted: {ticketswap_mentions}"
            )

        return "\n".join(lines)


def create_app() -> FastAPI:
    bot_token = _required_env("SLACK_BOT_TOKEN")
    signing_secret = _required_env("SLACK_SIGNING_SECRET")
    database_path = os.getenv("CONCERTO_DB_PATH", "./concerto.db")

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=20)
        async with (
            aiohttp.ClientSession(timeout=timeout) as session,
            aiosqlite.connect(database_path) as db,
        ):
            repository = BoardRepository(db)
            await repository.init()
            service = SlackBotService(
                bot_token=bot_token,
                session=session,
                repository=repository,
            )
            await service.initialize()
            yield {"service": service, "signing_secret": signing_secret}

    app = FastAPI(lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/slack/events", response_model=None)
    async def slack_events(request: Request) -> Response | dict[str, bool]:
        signing_secret_from_state = str(request.state.signing_secret)
        payload_bytes = await request.body()

        if not _is_valid_signature(request, payload_bytes, signing_secret_from_state):
            raise HTTPException(status_code=401, detail="invalid Slack signature")

        payload_raw = await request.json()
        if not isinstance(payload_raw, dict):
            raise HTTPException(status_code=400, detail="invalid request payload")

        if payload_raw.get("type") == "url_verification":
            challenge = str(payload_raw.get("challenge", ""))
            return Response(content=challenge, media_type="text/plain")

        if payload_raw.get("type") != "event_callback":
            return {"ok": True}

        event = payload_raw.get("event")
        if not isinstance(event, dict):
            return {"ok": True}

        service = request.state.service
        if not isinstance(service, SlackBotService):
            raise HTTPException(status_code=500, detail="service not initialized")

        _spawn(_run_event(service, event))
        return {"ok": True}

    @app.post("/slack/commands", response_model=None)
    async def slack_commands(request: Request) -> dict[str, str]:
        signing_secret_from_state = str(request.state.signing_secret)
        payload_bytes = await request.body()

        if not _is_valid_signature(request, payload_bytes, signing_secret_from_state):
            raise HTTPException(status_code=401, detail="invalid Slack signature")

        payload = parse_qs(payload_bytes.decode("utf-8"), keep_blank_values=True)
        command = payload.get("command", [""])[0].strip()
        text = payload.get("text", [""])[0].strip().lower()
        channel_id = payload.get("channel_id", [""])[0].strip()

        if command != "/concerto":
            return {
                "response_type": "ephemeral",
                "text": "Unsupported command. Use /concerto rebuild",
            }

        if text not in {"rebuild", "rescan"}:
            return {
                "response_type": "ephemeral",
                "text": "Usage: /concerto rebuild",
            }

        if not _is_supported_channel(channel_id):
            return {
                "response_type": "ephemeral",
                "text": "This command only works in public/private channels.",
            }

        service = request.state.service
        if not isinstance(service, SlackBotService):
            raise HTTPException(status_code=500, detail="service not initialized")

        _spawn(_run_rebuild_command(service, channel_id))
        return {
            "response_type": "ephemeral",
            "text": "Rebuilding pinned summary from channel history. I will update the pin when done.",
        }

    return app


async def _run_event(service: SlackBotService, event: dict[str, Any]) -> None:
    try:
        await service.handle_event(event)
    except SlackApiError:
        logger.exception("Slack API call failed while handling event")
    except Exception:
        logger.exception("Unexpected error while handling Slack event")


async def _run_rebuild_command(service: SlackBotService, channel_id: str) -> None:
    try:
        await service.handle_rebuild_command(channel_id)
    except SlackApiError:
        logger.exception(
            "Slack API call failed while rebuilding channel %s", channel_id
        )
    except Exception:
        logger.exception("Unexpected error while rebuilding channel %s", channel_id)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        msg = f"Missing required environment variable: {name}"
        raise RuntimeError(msg)
    return value


def _is_valid_signature(request: Request, body: bytes, signing_secret: str) -> bool:
    timestamp = request.headers.get("x-slack-request-timestamp")
    signature = request.headers.get("x-slack-signature")
    if not timestamp or not signature:
        return False

    try:
        request_ts = int(timestamp)
    except ValueError:
        return False

    if abs(time.time() - request_ts) > 60 * 5:
        return False

    basestring = f"v0:{timestamp}:{body.decode('utf-8')}"
    computed = hmac.new(
        signing_secret.encode("utf-8"),
        basestring.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    expected = f"v0={computed}"
    return hmac.compare_digest(expected, signature)


def _extract_links(text: str) -> list[str]:
    if not text:
        return []

    links = [
        match.strip()
        for match in re.findall(r"<((?:https?://)[^>|]+)(?:\|[^>]+)?>", text)
    ]
    links += [
        match.rstrip('.,!?:;)"]') for match in re.findall(r"https?://[^\s<>]+", text)
    ]

    return list(dict.fromkeys(links))


def _format_mentions(user_ids: set[str]) -> str:
    if not user_ids:
        return "-"
    mentions = [f"<@{user_id}>" for user_id in sorted(user_ids)]
    return ", ".join(mentions)


def _update_membership(group: set[str], user_id: str, *, added: bool) -> None:
    if added:
        group.add(user_id)
        return

    group.discard(user_id)


def _apply_status_reaction(
    entry: LinkEntry, reaction: str, user_id: str, *, added: bool
) -> None:
    if reaction in PLUS_ONE_REACTIONS:
        _update_membership(entry.ticket_holders, user_id, added=added)
        if added:
            entry.interested.discard(user_id)
            entry.ticketswap_wanted.discard(user_id)
        return

    if reaction in QUESTION_REACTIONS:
        if user_id in entry.ticket_holders:
            return
        _update_membership(entry.interested, user_id, added=added)
        if added:
            entry.ticketswap_wanted.discard(user_id)
        return

    if reaction in PRAY_REACTIONS:
        if user_id in entry.ticket_holders:
            return
        _update_membership(entry.ticketswap_wanted, user_id, added=added)
        if added:
            entry.interested.discard(user_id)


def _merge_link_entry(target: LinkEntry, source: LinkEntry) -> bool:
    before = (
        len(target.posters),
        len(target.ticket_holders),
        len(target.interested),
        len(target.ticketswap_wanted),
        target.source_message_ts,
    )
    target.posters.update(source.posters)
    target.ticket_holders.update(source.ticket_holders)
    target.interested.update(source.interested)
    target.ticketswap_wanted.update(source.ticketswap_wanted)
    _set_earliest_source_message_ts(target, source.source_message_ts)
    after = (
        len(target.posters),
        len(target.ticket_holders),
        len(target.interested),
        len(target.ticketswap_wanted),
        target.source_message_ts,
    )
    return before != after


def _is_supported_channel(channel_id: str) -> bool:
    return channel_id.startswith(("C", "G"))


def _normalize_ts(value: object) -> str | None:
    if value is None:
        return None
    ts = str(value).strip()
    if not ts:
        return None
    return ts


def _set_earliest_source_message_ts(entry: LinkEntry, candidate_ts: object) -> None:
    normalized = _normalize_ts(candidate_ts)
    if not normalized:
        return
    if not entry.source_message_ts:
        entry.source_message_ts = normalized
        return
    if _ts_key(normalized) < _ts_key(entry.source_message_ts):
        entry.source_message_ts = normalized


def _ts_key(ts: str) -> tuple[int, int]:
    if "." in ts:
        seconds_part, _, micros_part = ts.partition(".")
        try:
            return (int(seconds_part), int((micros_part + "000000")[:6]))
        except ValueError:
            pass
    try:
        numeric = int(ts)
    except ValueError:
        return (0, 0)
    return (numeric, 0)


def _normalize_workspace_url(value: object) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return raw.rstrip("/")


def _build_slack_message_permalink(
    workspace_url: str | None, channel_id: str, message_ts: str
) -> str:
    ts_raw = _normalize_ts(message_ts)
    if not ts_raw:
        return "https://slack.com"
    ts_compact = ts_raw.replace(".", "")
    if not ts_compact:
        return "https://slack.com"
    base_url = workspace_url or "https://slack.com"
    return f"{base_url}/archives/{channel_id}/p{ts_compact}"
