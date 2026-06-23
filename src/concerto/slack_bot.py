from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import re
from dataclasses import dataclass, field
from html import escape
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import aiohttp
import aiosqlite
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from concerto import concert_scraper

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
WEB_API_TIMEOUT_SECONDS = 20
SOCKET_HEARTBEAT_SECONDS = 30
SOCKET_RECONNECT_DELAY_SECONDS = 1
SSE_KEEPALIVE_SECONDS = 15
DAYS_PER_WEEK = 7
DAYS_PER_MONTH = 31
PLUS_ONE_REACTIONS = {"+1", "thumbsup", "ticket"}
QUESTION_REACTIONS = {"question", "grey_question", "eyes"}
PRAY_REACTIONS = {"pray"}

# Links on these domains (and their subdomains) are never tracked.
IGNORED_LINK_DOMAINS = (
    "slack.com",
    "nrc.nl",
    "youtube.com",
    "youtu.be",
    "spotify.com",
    "infrapuin.nl",
)


@dataclass
class LinkEntry:
    posters: set[str] = field(default_factory=set)
    # Aggregate reaction counts only — we never store who reacted (privacy).
    going: int = 0  # have a ticket
    undecided: int = 0  # interested, no ticket yet
    looking: int = 0  # looking for a ticket on TicketSwap
    source_message_ts: str | None = None
    band: str | None = None
    event_date: str | None = None
    event_end_date: str | None = None
    venue: str | None = None
    expired: bool = False

    @property
    def has_metadata(self) -> bool:
        return bool(self.band or self.event_date or self.venue)

    @property
    def is_resolved(self) -> bool:
        # Either we have metadata, or the event is gone — no need to re-scrape.
        return self.has_metadata or self.expired


@dataclass
class ChannelBoard:
    links: dict[str, LinkEntry] = field(default_factory=dict)


@dataclass
class EventView:
    """An immutable snapshot of a tracked link for the overview page."""

    url: str
    band: str | None
    venue: str | None
    expired: bool
    message_url: str | None
    date: dt.date | None
    end_date: dt.date | None  # end of a multi-day run, else None
    going: int  # have a ticket
    undecided: int  # interested, no ticket yet
    looking: int  # looking for a ticket on TicketSwap


class SlackApiError(RuntimeError):
    pass


class BoardRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def init(self) -> None:
        await self._db.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS links (
                channel_id TEXT NOT NULL,
                url TEXT NOT NULL,
                source_message_ts TEXT,
                band TEXT,
                event_date TEXT,
                event_end_date TEXT,
                venue TEXT,
                expired TEXT,
                PRIMARY KEY (channel_id, url)
            );

            CREATE TABLE IF NOT EXISTS link_posters (
                channel_id TEXT NOT NULL,
                url TEXT NOT NULL,
                user_id TEXT NOT NULL,
                PRIMARY KEY (channel_id, url, user_id)
            );

            -- We no longer store who reacted, only aggregate counts on `links`.
            -- Run a channel rebuild to repopulate the counts.
            DROP TABLE IF EXISTS link_statuses;
            """
        )
        await self._ensure_links_columns()
        await self._db.commit()

    async def _ensure_links_columns(self) -> None:
        async with self._db.execute("PRAGMA table_info(links)") as cursor:
            existing = {str(row[1]) async for row in cursor if len(row) > 1}
        for column in (
            "source_message_ts",
            "band",
            "event_date",
            "event_end_date",
            "venue",
            "expired",
        ):
            if column not in existing:
                await self._db.execute(f"ALTER TABLE links ADD COLUMN {column} TEXT")
        for column in ("going", "undecided", "looking"):
            if column not in existing:
                await self._db.execute(
                    f"ALTER TABLE links ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                )

    async def load_board(self, channel_id: str) -> ChannelBoard:
        board = ChannelBoard()

        async with self._db.execute(
            "SELECT url, source_message_ts, band, event_date, venue, expired, "
            "event_end_date, going, undecided, looking FROM links WHERE channel_id = ?",
            (channel_id,),
        ) as cursor:
            async for row in cursor:
                board.links[str(row[0])] = LinkEntry(
                    source_message_ts=_normalize_ts(row[1]),
                    band=_opt_str(row[2]),
                    event_date=_opt_str(row[3]),
                    venue=_opt_str(row[4]),
                    expired=bool(_opt_str(row[5])),
                    event_end_date=_opt_str(row[6]),
                    going=int(row[7] or 0),
                    undecided=int(row[8] or 0),
                    looking=int(row[9] or 0),
                )

        await self._load_memberships(channel_id, board.links, "link_posters", "posters")

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

    async def save_board(self, channel_id: str, board: ChannelBoard) -> None:
        await self._db.execute("DELETE FROM links WHERE channel_id = ?", (channel_id,))
        await self._db.execute(
            "DELETE FROM link_posters WHERE channel_id = ?", (channel_id,)
        )

        for url, entry in board.links.items():
            await self._db.execute(
                "INSERT INTO links"
                "(channel_id, url, source_message_ts, band, event_date, venue, "
                "expired, event_end_date, going, undecided, looking) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    channel_id,
                    url,
                    _normalize_ts(entry.source_message_ts),
                    entry.band,
                    entry.event_date,
                    entry.venue,
                    "1" if entry.expired else None,
                    entry.event_end_date,
                    entry.going,
                    entry.undecided,
                    entry.looking,
                ),
            )
            await self._insert_memberships(
                channel_id, url, "link_posters", entry.posters
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


class SlackBotService:
    def __init__(
        self,
        bot_token: str,
        app_token: str,
        session: aiohttp.ClientSession,
        repository: BoardRepository,
        command: str = "/concerto",
    ) -> None:
        self._bot_token = bot_token
        self._app_token = app_token
        self._session = session
        self._repository = repository
        self._command = command
        self._boards: dict[str, ChannelBoard] = {}
        self._bot_user_id: str | None = None
        self._workspace_url: str | None = None
        self._metadata_tried: set[str] = set()
        self._lock = asyncio.Lock()
        self._subscribers: dict[str, set[asyncio.Queue[None]]] = {}

    async def initialize(self) -> None:
        auth_info = await self._api_call("auth.test", {})
        user_id = str(auth_info.get("user_id", ""))
        if not user_id:
            msg = "Slack auth.test did not return user_id"
            raise SlackApiError(msg)
        self._bot_user_id = user_id
        self._workspace_url = _normalize_workspace_url(auth_info.get("url"))

    async def event_views(self, channel_id: str) -> list[EventView]:
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            return [
                EventView(
                    url=url,
                    band=entry.band,
                    venue=entry.venue,
                    expired=entry.expired,
                    message_url=_message_permalink(
                        self._workspace_url, channel_id, entry.source_message_ts
                    ),
                    date=_parse_iso_date(entry.event_date),
                    end_date=_parse_iso_date(entry.event_end_date),
                    going=entry.going,
                    undecided=entry.undecided,
                    looking=entry.looking,
                )
                for url, entry in board.links.items()
            ]

    async def handle_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("type", ""))

        if event_type == "message":
            await self._handle_message_event(event)
            return

        if event_type == "member_joined_channel":
            await self._handle_member_joined_channel_event(event)
            return

        if event_type in {"reaction_added", "reaction_removed"}:
            await self._handle_reaction_event(event)

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

            if changed:
                await self._persist_locked(channel_id, board)

        await self._enrich_links(channel_id, list(history_entries))

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

        await self._enrich_links(channel_id, links)

    async def _rebuild_board_from_history(self, channel_id: str) -> None:
        history_entries = await self._collect_history_link_entries(channel_id)
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            board.links = history_entries
            await self._persist_locked(channel_id, board)

        await self._enrich_links(channel_id, list(history_entries))

    async def handle_rebuild_command(self, channel_id: str) -> None:
        if not _is_supported_channel(channel_id):
            return
        await self._rebuild_board_from_history(channel_id)

    async def _handle_reaction_event(  # noqa: PLR0911
        self, event: dict[str, Any]
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

        # Re-parse the whole message's reactions instead of tracking the delta,
        # so we only ever keep aggregate counts and never store who reacted.
        message = await self._get_message(channel_id, message_ts)
        if message is None:
            return

        links = _extract_links(str(message.get("text", "")))
        if not links:
            return

        counts = _aggregate_status_counts(message.get("reactions"))

        async with self._lock:
            board = await self._get_board_locked(channel_id)
            for link in links:
                entry = board.links.setdefault(link, LinkEntry())
                _set_earliest_source_message_ts(entry, message_ts)
                # ponytail: a URL reposted across messages shows the counts of
                # whichever post was last reacted on; reactions cluster on one.
                entry.going, entry.undecided, entry.looking = counts
            await self._persist_locked(channel_id, board)

        await self._enrich_links(channel_id, links)

    async def _scrape_metadata(self, url: str) -> concert_scraper.ConcertInfo | None:
        try:
            info = await concert_scraper.scrape(url, self._session)
        except (concert_scraper.ScrapeError, aiohttp.ClientError, TimeoutError) as exc:
            # Other HTTP errors (5xx, etc.) and network blips are expected and
            # unactionable; log concisely without a traceback.
            logger.warning("Could not scrape %s: %s", url, exc)
            return None
        logger.debug(
            "Scraped %s -> band=%r date=%s venue=%r expired=%s",
            url,
            info.band,
            info.date,
            info.venue,
            info.expired,
        )
        return info

    async def _enrich_links(self, channel_id: str, urls: list[str]) -> None:
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            pending = [
                url
                for url in dict.fromkeys(urls)
                if url not in self._metadata_tried
                and not (url in board.links and board.links[url].is_resolved)
            ]
            self._metadata_tried.update(pending)

        if not pending:
            return

        logger.debug(
            "Enriching %d link(s) in %s: %s", len(pending), channel_id, pending
        )
        scraped = {
            url: info
            for url in pending
            if (info := await self._scrape_metadata(url)) is not None
        }
        if not scraped:
            return

        async with self._lock:
            board = await self._get_board_locked(channel_id)
            changed = False
            for url, info in scraped.items():
                entry = board.links.get(url)
                if entry is not None and _apply_metadata(entry, info):
                    changed = True
            if changed:
                await self._persist_locked(channel_id, board)

    async def _get_board_locked(self, channel_id: str) -> ChannelBoard:
        board = self._boards.get(channel_id)
        if board is not None:
            return board

        board = await self._repository.load_board(channel_id)
        self._boards[channel_id] = board
        return board

    async def _persist_locked(self, channel_id: str, board: ChannelBoard) -> None:
        await self._repository.save_board(channel_id, board)
        self._notify(channel_id)

    def subscribe(self, channel_id: str) -> asyncio.Queue[None]:
        # maxsize=1 coalesces a burst of updates into a single pending reload.
        queue: asyncio.Queue[None] = asyncio.Queue(maxsize=1)
        self._subscribers.setdefault(channel_id, set()).add(queue)
        return queue

    def unsubscribe(self, channel_id: str, queue: asyncio.Queue[None]) -> None:
        subscribers = self._subscribers.get(channel_id)
        if subscribers is None:
            return
        subscribers.discard(queue)
        if not subscribers:
            del self._subscribers[channel_id]

    def _notify(self, channel_id: str) -> None:
        for queue in self._subscribers.get(channel_id, ()):
            if queue.empty():
                queue.put_nowait(None)

    async def _get_message(self, channel: str, message_ts: str) -> dict[str, Any] | None:
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
            return None

        message = messages[0]
        return message if isinstance(message, dict) else None

    async def _collect_history_link_entries(
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
                    going, undecided, looking = _aggregate_status_counts(
                        message.get("reactions")
                    )
                    for link in links:
                        entry = links_data.setdefault(link, LinkEntry())
                        if user_id:
                            entry.posters.add(user_id)
                        _set_earliest_source_message_ts(entry, message.get("ts"))
                        # ponytail: same URL across posts -> keep the highest
                        # count per status; reactions normally sit on one post.
                        entry.going = max(entry.going, going)
                        entry.undecided = max(entry.undecided, undecided)
                        entry.looking = max(entry.looking, looking)

            metadata = history_response.get("response_metadata")
            if not isinstance(metadata, dict):
                break

            next_cursor = str(metadata.get("next_cursor", ""))
            if not next_cursor:
                break
            cursor = next_cursor

        return links_data

    async def _api_call(
        self, method: str, payload: dict[str, Any], *, token: str | None = None
    ) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {token or self._bot_token}",
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

    async def _open_socket_url(self) -> str:
        response = await self._api_call(
            "apps.connections.open", {}, token=self._app_token
        )
        url = str(response.get("url", ""))
        if not url:
            msg = "Slack apps.connections.open did not return a url"
            raise SlackApiError(msg)
        return url

    async def run_socket_mode(self) -> None:
        # The websocket is long-lived, so it gets its own session without a
        # total timeout; heartbeat pings detect a dead connection.
        ws_timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)
        async with aiohttp.ClientSession(timeout=ws_timeout) as ws_session:
            while True:
                try:
                    url = await self._open_socket_url()
                    async with ws_session.ws_connect(
                        url, heartbeat=SOCKET_HEARTBEAT_SECONDS
                    ) as ws:
                        logger.info("Connected to Slack Socket Mode")
                        await self._consume_socket(ws)
                except (SlackApiError, aiohttp.ClientError, TimeoutError) as exc:
                    logger.warning("Socket Mode connection lost: %s", exc)
                await asyncio.sleep(SOCKET_RECONNECT_DELAY_SECONDS)

    async def _consume_socket(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for message in ws:
            if message.type is not aiohttp.WSMsgType.TEXT:
                continue
            try:
                data = json.loads(message.data)
            except (json.JSONDecodeError, ValueError):
                logger.warning("Ignoring malformed Socket Mode frame")
                continue
            if isinstance(data, dict) and not await self._dispatch_socket_message(
                ws, data
            ):
                return

    async def _dispatch_socket_message(
        self, ws: aiohttp.ClientWebSocketResponse, data: dict[str, Any]
    ) -> bool:
        # Returns False when Slack asks us to reconnect.
        message_type = str(data.get("type", ""))
        if message_type == "disconnect":
            logger.info("Slack requested reconnect: %s", data.get("reason"))
            return False
        if message_type not in {"events_api", "slash_commands"}:
            return True

        envelope_id = data.get("envelope_id")
        payload = data.get("payload")
        if not isinstance(envelope_id, str) or not isinstance(payload, dict):
            return True

        if message_type == "events_api":
            await ws.send_json({"envelope_id": envelope_id})
            event = payload.get("event")
            if isinstance(event, dict):
                logger.debug(
                    "Event received: type=%s subtype=%s channel=%s user=%s",
                    event.get("type"),
                    event.get("subtype"),
                    event.get("channel"),
                    event.get("user"),
                )
                _spawn(_run_event(self, event))
        else:
            logger.debug(
                "Slash command received: command=%s text=%r channel=%s",
                payload.get("command"),
                payload.get("text"),
                payload.get("channel_id"),
            )
            await ws.send_json(
                {"envelope_id": envelope_id, "payload": self._command_response(payload)}
            )
        return True

    def _command_response(self, payload: dict[str, Any]) -> dict[str, str]:
        command = str(payload.get("command", "")).strip()
        text = str(payload.get("text", "")).strip().lower()
        channel_id = str(payload.get("channel_id", "")).strip()

        if command != self._command:
            return {
                "response_type": "ephemeral",
                "text": f"Unsupported command. Use {self._command} rebuild",
            }
        if text not in {"rebuild", "rescan"}:
            return {
                "response_type": "ephemeral",
                "text": f"Usage: {self._command} rebuild",
            }
        if not _is_supported_channel(channel_id):
            return {
                "response_type": "ephemeral",
                "text": "This command only works in public/private channels.",
            }

        _spawn(_run_rebuild_command(self, channel_id))
        return {
            "response_type": "ephemeral",
            "text": "Rescanning channel history for links.",
        }


def create_app() -> FastAPI:
    bot_token = _required_env("SLACK_BOT_TOKEN")
    app_token = _required_env("SLACK_APP_TOKEN")
    database_path = os.getenv("CONCERTO_DB_PATH", "./concerto.db")
    command = _slash_command()

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
            service = SlackBotService(
                bot_token=bot_token,
                app_token=app_token,
                session=session,
                repository=repository,
                command=command,
            )
            await service.initialize()
            # Slack is handled over Socket Mode, running alongside the HTTP app.
            socket_task = asyncio.create_task(service.run_socket_mode())
            try:
                yield {"service": service}
            finally:
                socket_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await socket_task

    app = FastAPI(lifespan=lifespan)

    @app.get("/")
    async def index() -> dict[str, str]:
        return {"message": "Hello world"}

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/board/{channel_id}", response_class=HTMLResponse)
    async def board(channel_id: str, request: Request) -> str:
        service = request.state.service
        if not isinstance(service, SlackBotService):
            raise HTTPException(status_code=500, detail="service not initialized")
        if not _is_supported_channel(channel_id):
            raise HTTPException(status_code=404, detail="unknown channel")
        views = await service.event_views(channel_id)
        return _render_overview(channel_id, views)

    @app.get("/board/{channel_id}/events")
    async def board_events(channel_id: str, request: Request) -> StreamingResponse:
        service = request.state.service
        if not isinstance(service, SlackBotService):
            raise HTTPException(status_code=500, detail="service not initialized")
        if not _is_supported_channel(channel_id):
            raise HTTPException(status_code=404, detail="unknown channel")

        queue = service.subscribe(channel_id)

        async def stream() -> AsyncIterator[str]:
            try:
                while True:
                    try:
                        await asyncio.wait_for(
                            queue.get(), timeout=SSE_KEEPALIVE_SECONDS
                        )
                        yield "data: update\n\n"
                    except TimeoutError:
                        yield ": keepalive\n\n"
            finally:
                service.unsubscribe(channel_id, queue)

        return StreamingResponse(stream(), media_type="text/event-stream")

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


def _slash_command() -> str:
    # Slack delivers the command with a leading slash (e.g. "/concerto-dev"),
    # so normalize the configured value to match regardless of how it's written.
    command = os.getenv("CONCERTO_SLASH_COMMAND", "/concerto").strip()
    return command if command.startswith("/") else f"/{command}"


def _extract_links(text: str) -> list[str]:
    if not text:
        return []

    # Slack wraps URLs as <url> or <url|label>; capture just the url.
    links = [
        match.strip()
        for match in re.findall(r"<((?:https?://)[^>|]+)(?:\|[^>]+)?>", text)
    ]
    # Bare URLs (not Slack-wrapped). Exclude "|" so a wrapped <url|label> is
    # not re-captured here as "url|label".
    links += [
        match.rstrip('.,!?:;)"]') for match in re.findall(r"https?://[^\s<>|]+", text)
    ]

    return [url for url in dict.fromkeys(links) if not _is_ignored_url(url)]


def _is_ignored_url(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    return any(
        host == domain or host.endswith("." + domain) for domain in IGNORED_LINK_DOMAINS
    )


def _aggregate_status_counts(reactions: object) -> tuple[int, int, int]:
    """Collapse a message's `reactions` into (going, undecided, looking).

    Each user is counted once. A user holding a ticket (+1) outranks interest
    (?), which outranks looking (pray) — so the categories never double-count.
    User ids are used only transiently here; only the counts are returned.
    """
    ticket: set[str] = set()
    interested: set[str] = set()
    looking: set[str] = set()
    if isinstance(reactions, list):
        for reaction_obj in reactions:
            if not isinstance(reaction_obj, dict):
                continue
            name = str(reaction_obj.get("name", ""))
            users = reaction_obj.get("users")
            if not isinstance(users, list):
                continue
            ids = {str(u) for u in users if str(u)}
            if name in PLUS_ONE_REACTIONS:
                ticket |= ids
            elif name in QUESTION_REACTIONS:
                interested |= ids
            elif name in PRAY_REACTIONS:
                looking |= ids
    interested -= ticket
    looking -= ticket | interested
    return len(ticket), len(interested), len(looking)


def _merge_link_entry(target: LinkEntry, source: LinkEntry) -> bool:
    before = (
        len(target.posters),
        target.going,
        target.undecided,
        target.looking,
        target.source_message_ts,
    )
    target.posters.update(source.posters)
    target.going = max(target.going, source.going)
    target.undecided = max(target.undecided, source.undecided)
    target.looking = max(target.looking, source.looking)
    _set_earliest_source_message_ts(target, source.source_message_ts)
    after = (
        len(target.posters),
        target.going,
        target.undecided,
        target.looking,
        target.source_message_ts,
    )
    return before != after


def _is_supported_channel(channel_id: str) -> bool:
    return channel_id.startswith(("C", "G"))


def _apply_metadata(entry: LinkEntry, info: concert_scraper.ConcertInfo) -> bool:
    before = (
        entry.band,
        entry.event_date,
        entry.event_end_date,
        entry.venue,
        entry.expired,
    )
    if info.band:
        entry.band = info.band
    if info.date:
        entry.event_date = info.date.isoformat()
    if info.end_date:
        entry.event_end_date = info.end_date.isoformat()
    if info.venue:
        entry.venue = info.venue
    if info.expired:
        entry.expired = True
    return (
        entry.band,
        entry.event_date,
        entry.event_end_date,
        entry.venue,
        entry.expired,
    ) != before


def _opt_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_iso_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


def _normalize_workspace_url(value: object) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().rstrip("/")
    return raw or None


def _message_permalink(
    workspace_url: str | None, channel_id: str, message_ts: str | None
) -> str | None:
    ts = _normalize_ts(message_ts)
    if not workspace_url or not ts:
        return None
    return f"{workspace_url}/archives/{channel_id}/p{ts.replace('.', '')}"


_OVERVIEW_CSS = """
:root {
  --bg: #0b0d12; --card: #161a23; --line: #232838;
  --text: #e8eaf0; --muted: #8a90a2; --accent: #ff5c7c;
}
* { box-sizing: border-box; }
body {
  margin: 0; min-height: 100vh; color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: radial-gradient(1100px 600px at 50% -10%, #1b2133, var(--bg));
}
.top { text-align: center; padding: 56px 20px 8px; }
.top h1 {
  margin: 0; font-size: 2.4rem; font-weight: 800; letter-spacing: -0.02em;
  background: linear-gradient(90deg, #ff5c7c, #a78bfa);
  -webkit-background-clip: text; background-clip: text; color: transparent;
}
.sub { color: var(--muted); margin: 10px 0 0; font-size: 0.95rem; }
main { max-width: 760px; margin: 0 auto; padding: 24px 16px 72px; }
.section {
  color: var(--muted); font-size: 0.74rem; font-weight: 700;
  text-transform: uppercase; letter-spacing: 0.09em; margin: 28px 6px 12px;
}
.events { display: flex; flex-direction: column; gap: 12px; }
.card {
  display: flex; gap: 16px; align-items: center; padding: 14px 16px;
  background: var(--card); border: 1px solid var(--line); border-radius: 14px;
  transition: transform .12s ease, border-color .12s ease;
}
.card:hover { transform: translateY(-2px); border-color: var(--accent); }
.date {
  flex: 0 0 62px; text-align: center; display: flex; flex-direction: column;
  line-height: 1.1; background: #0f131b; border-radius: 10px; padding: 8px 6px;
}
.date .dow { font-size: 0.68rem; color: var(--muted); text-transform: uppercase; }
.date .dom { font-size: 1.5rem; font-weight: 800; }
.date .moy { font-size: 0.68rem; color: var(--muted); }
.date.tba .dom { font-size: 0.95rem; color: var(--accent); padding: 6px 0; }
.meta { flex: 1 1 auto; min-width: 0; }
.band { font-size: 1.14rem; font-weight: 650; word-break: break-word; }
.venue { color: var(--muted); font-size: 0.92rem; margin: 2px 0 9px; }
.run { color: var(--accent); font-size: 0.82rem; margin: -4px 0 9px; }
.status { display: flex; gap: 12px; margin: 0 0 9px; }
.stat {
  font-size: 0.92rem; font-variant-numeric: tabular-nums;
  cursor: default; user-select: none;
}
.links { display: flex; gap: 8px; flex-wrap: wrap; }
.link {
  font-size: 0.8rem; text-decoration: none; color: var(--text);
  background: #0f131b; border: 1px solid var(--line);
  padding: 4px 11px; border-radius: 999px;
}
.link:hover { border-color: var(--accent); color: var(--accent); }
.empty { text-align: center; color: var(--muted); padding: 64px 0; }
"""


def _fallback_name(url: str) -> str:
    return escape(urlsplit(url).hostname or url)


def _render_date_badge(date: dt.date | None) -> str:
    if date is None:
        return '<div class="date tba"><span class="dom">?</span></div>'
    return (
        '<div class="date">'
        f'<span class="dow">{date:%a}</span>'
        f'<span class="dom">{date:%d}</span>'
        f'<span class="moy">{date:%b %Y}</span>'
        "</div>"
    )


def _render_status(view: EventView) -> str:
    # (emoji, count, hover label); zero-count statuses are omitted.
    stats = [
        ("\N{TICKET}", view.going, "have a ticket"),
        ("\N{EYES}", view.undecided, "interested"),
        ("\N{PERSON WITH FOLDED HANDS}", view.looking, "looking for a ticket"),
    ]
    pills = [
        f'<span class="stat" title="{label}">{emoji} {count}</span>'
        for emoji, count, label in stats
        if count
    ]
    return f'<div class="status">{"".join(pills)}</div>' if pills else ""


def _is_upcoming(view: EventView, today: dt.date) -> bool:
    if view.expired:
        return False
    # End of the run if it's a multi-day event, else the single date.
    effective = view.end_date or view.date
    return effective is None or effective >= today


def _render_run(view: EventView) -> str:
    # Show the closing date for a multi-day run; the badge shows the opening.
    end = view.end_date
    if end is None or (view.date is not None and end <= view.date):
        return ""
    return f'<div class="run">through {end:%-d %b %Y}</div>'


def _render_event_card(view: EventView) -> str:
    name = escape(view.band) if view.band else _fallback_name(view.url)
    venue = escape(view.venue) if view.venue else "&mdash;"
    links = [
        f'<a class="link" href="{escape(view.url)}" '
        'target="_blank" rel="noopener">Event &#8599;</a>'
    ]
    if view.message_url:
        links.append(
            f'<a class="link" href="{escape(view.message_url)}" '
            'target="_blank" rel="noopener">Slack &#8599;</a>'
        )
    return (
        '<article class="card">'
        f"{_render_date_badge(view.date)}"
        '<div class="meta">'
        f'<div class="band">{name}</div>'
        f'<div class="venue">{venue}</div>'
        f"{_render_run(view)}"
        f"{_render_status(view)}"
        f'<div class="links">{" ".join(links)}</div>'
        "</div>"
        "</article>"
    )


def _render_section(title: str, views: list[EventView]) -> str:
    cards = "\n".join(_render_event_card(view) for view in views)
    return (
        f'<div class="section">{escape(title)}</div><div class="events">{cards}</div>'
    )


def _render_overview(channel_id: str, views: list[EventView]) -> str:
    today = dt.datetime.now(tz=dt.UTC).date()
    # A multi-day run stays relevant until its end date, not its opening date,
    # so it isn't hidden while performances are still happening.
    upcoming = [view for view in views if _is_upcoming(view, today)]
    undated = [view for view in upcoming if view.date is None]
    dated = sorted(
        (view for view in upcoming if view.date is not None),
        key=lambda view: view.date or dt.date.min,
    )

    week: list[EventView] = []
    month: list[EventView] = []
    later: list[EventView] = []
    for view in dated:
        if view.date is None:
            continue
        days = (view.date - today).days
        if days <= DAYS_PER_WEEK:
            week.append(view)
        elif days < DAYS_PER_MONTH:
            month.append(view)
        else:
            later.append(view)

    groups = [
        ("Date unknown", undated),
        ("This week", week),
        ("This month", month),
        ("Upcoming", later),
    ]
    sections = "".join(
        _render_section(title, group) for title, group in groups if group
    )
    body = sections or '<div class="empty">No upcoming events tracked yet.</div>'

    plural = "" if len(upcoming) == 1 else "s"
    return (
        "<!doctype html>"
        '<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        "<title>Upcoming concerts</title>"
        f"<style>{_OVERVIEW_CSS}</style></head><body>"
        '<header class="top"><h1>Upcoming concerts</h1>'
        f'<p class="sub">{len(upcoming)} event{plural} &middot; '
        f"{escape(channel_id)}</p></header>"
        f"<main>{body}</main>"
        # Live-reload when the board changes; EventSource auto-reconnects on drop.
        "<script>new EventSource(location.pathname+'/events')"
        ".onmessage=()=>location.reload()</script>"
        "</body></html>"
    )


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
