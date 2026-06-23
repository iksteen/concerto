"""Slack platform layer: Socket Mode transport on top of the agnostic core.

Translates Slack events into the neutral `BoardService` ingestion calls and
supplies the Slack-specific channel check and message permalink.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from typing import TYPE_CHECKING, Any

import aiohttp
import aiosqlite
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
    from collections.abc import AsyncIterator, Coroutine

logger = logging.getLogger("concerto")

SLACK_API_BASE = "https://slack.com/api"
SOCKET_HEARTBEAT_SECONDS = 30
SOCKET_RECONNECT_DELAY_SECONDS = 1

_background_tasks: set[asyncio.Task[Any]] = set()


def _spawn(coro: Coroutine[Any, Any, None]) -> None:
    # Keep a reference so the task is not garbage-collected mid-flight.
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


class SlackApiError(RuntimeError):
    pass


class SlackBotService(BoardService):
    def __init__(
        self,
        bot_token: str,
        app_token: str,
        session: aiohttp.ClientSession,
        repository: BoardRepository,
        command: str = "/concerto",
    ) -> None:
        super().__init__(session, repository)
        self._bot_token = bot_token
        self._app_token = app_token
        self._command = command
        self._bot_user_id: str | None = None
        self._workspace_url: str | None = None

    async def initialize(self) -> None:
        auth_info = await self._api_call("auth.test", {})
        user_id = str(auth_info.get("user_id", ""))
        if not user_id:
            msg = "Slack auth.test did not return user_id"
            raise SlackApiError(msg)
        self._bot_user_id = user_id
        self._workspace_url = _normalize_workspace_url(auth_info.get("url"))

    # --- core hooks ---

    def is_supported_channel(self, channel_id: str) -> bool:
        # Only public/private channels (ids start C/G); ignore DMs etc.
        return channel_id.startswith(("C", "G"))

    def message_url(self, channel_id: str, source_message_ts: str | None) -> str | None:
        if not self._workspace_url or not source_message_ts:
            return None
        ts = source_message_ts.replace(".", "")
        return f"{self._workspace_url}/archives/{channel_id}/p{ts}"

    # --- event dispatch ---

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

    async def _handle_message_event(self, event: dict[str, Any]) -> None:
        if event.get("subtype"):
            return
        channel_id = str(event.get("channel", ""))
        if not self.is_supported_channel(channel_id):
            return
        await self.apply_message(
            channel_id, event.get("ts"), str(event.get("text", ""))
        )

    async def _handle_member_joined_channel_event(self, event: dict[str, Any]) -> None:
        if not self._bot_user_id:
            return
        if str(event.get("user", "")) != self._bot_user_id:
            return
        channel_id = str(event.get("channel", ""))
        if not self.is_supported_channel(channel_id):
            return
        entries = await self._collect_history_link_entries(channel_id)
        await self.merge_entries(channel_id, entries)

    async def _handle_reaction_event(self, event: dict[str, Any]) -> None:
        reaction = str(event.get("reaction", ""))
        if (
            reaction not in PLUS_ONE_REACTIONS
            and reaction not in QUESTION_REACTIONS
            and reaction not in PRAY_REACTIONS
        ):
            return

        item = event.get("item")
        if not isinstance(item, dict) or str(item.get("type", "")) != "message":
            return

        channel_id = str(item.get("channel", ""))
        message_ts = str(item.get("ts", ""))
        if not self.is_supported_channel(channel_id) or not message_ts:
            return

        # Re-parse the whole message's reactions rather than the single delta,
        # so we only ever keep aggregate counts and never store who reacted.
        message = await self._get_message(channel_id, message_ts)
        if message is None:
            return
        await self.apply_reactions(
            channel_id,
            message_ts,
            str(message.get("text", "")),
            message.get("reactions"),
        )

    async def handle_rebuild_command(self, channel_id: str) -> None:
        if not self.is_supported_channel(channel_id):
            return
        entries = await self._collect_history_link_entries(channel_id)
        await self.replace_board(channel_id, entries)

    # --- Slack reads ---

    async def _get_message(
        self, channel: str, message_ts: str
    ) -> dict[str, Any] | None:
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
            payload: dict[str, Any] = {"channel": channel_id, "limit": 200}
            if cursor:
                payload["cursor"] = cursor

            history_response = await self._api_call("conversations.history", payload)
            messages = history_response.get("messages", [])
            if isinstance(messages, list):
                for message in messages:
                    if not isinstance(message, dict):
                        continue
                    # Slack's `reactions` is already the neutral shape that
                    # fold_message expects: [{"name", "users": [...]}, ...].
                    fold_message(
                        links_data,
                        message.get("ts"),
                        str(message.get("text", "")),
                        message.get("reactions"),
                    )

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

    # --- Socket Mode transport ---

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
        if not self.is_supported_channel(channel_id):
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
    bot_token = required_env("SLACK_BOT_TOKEN")
    app_token = required_env("SLACK_APP_TOKEN")
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
    register_board_routes(app)
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


def _slash_command() -> str:
    # Slack delivers the command with a leading slash (e.g. "/concerto-dev"),
    # so normalize the configured value to match regardless of how it's written.
    command = os.getenv("CONCERTO_SLASH_COMMAND", "/concerto").strip()
    return command if command.startswith("/") else f"/{command}"


def _normalize_workspace_url(value: object) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().rstrip("/")
    return raw or None
