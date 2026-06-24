from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import tomllib
from typing import TYPE_CHECKING, Any

import aiohttp
import aiosqlite
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

from concerto import concert_scraper
from concerto.board import (
    WEB_API_TIMEOUT_SECONDS,
    BoardRepository,
    BoardService,
    register_board_routes,
)
from concerto.discord_bot import DiscordBotService
from concerto.slack_bot import SlackBotService

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger("concerto")

DEFAULT_CONFIG_PATH = "./concerto.toml"
DEFAULT_DB_PATH = "./concerto.db"


def _load_config() -> dict[str, Any]:
    path = os.getenv("CONCERTO_CONFIG", DEFAULT_CONFIG_PATH)
    try:
        with open(path, "rb") as handle:  # noqa: PTH123
            return tomllib.load(handle)
    except FileNotFoundError as exc:
        msg = f"Config file not found: {path} (set CONCERTO_CONFIG)"
        raise RuntimeError(msg) from exc
    except tomllib.TOMLDecodeError as exc:
        msg = f"Invalid TOML in {path}: {exc}"
        raise RuntimeError(msg) from exc


def _validate_connectors(config: dict[str, Any]) -> list[dict[str, Any]]:
    connectors = config.get("connector")
    if not isinstance(connectors, list) or not connectors:
        msg = "Config must define at least one [[connector]]"
        raise RuntimeError(msg)

    seen: set[str] = set()
    for spec in connectors:
        if not isinstance(spec, dict):
            msg = "Each [[connector]] must be a table"
            raise RuntimeError(msg)  # noqa: TRY004
        name = spec.get("name")
        if not isinstance(name, str) or not name:
            msg = "Each connector needs a non-empty 'name'"
            raise RuntimeError(msg)
        if "/" in name:
            msg = f"Connector name must not contain '/': {name!r}"
            raise RuntimeError(msg)
        if name in seen:
            msg = f"Duplicate connector name: {name!r}"
            raise RuntimeError(msg)
        seen.add(name)

        ctype = spec.get("type")
        required = {"slack": ("bot_token", "app_token"), "discord": ("token",)}.get(
            ctype if isinstance(ctype, str) else ""
        )
        if required is None:
            msg = f"Connector {name!r} has unknown type {ctype!r} (slack/discord)"
            raise RuntimeError(msg)
        missing = [key for key in required if not spec.get(key)]
        if missing:
            msg = f"Connector {name!r} ({ctype}) missing: {', '.join(missing)}"
            raise RuntimeError(msg)

    return connectors


async def _build_connector(
    spec: dict[str, Any],
    session: aiohttp.ClientSession,
    repository: BoardRepository,
    db: aiosqlite.Connection,
) -> BoardService:
    name = str(spec["name"])
    if spec["type"] == "slack":
        slack = SlackBotService(
            name=name,
            bot_token=str(spec["bot_token"]),
            app_token=str(spec["app_token"]),
            session=session,
            repository=repository,
            command=str(spec.get("command", "/concerto")),
        )
        await slack.initialize()
        return slack

    discord_service = DiscordBotService(
        name=name,
        token=str(spec["token"]),
        session=session,
        repository=repository,
        db=db,
        command=str(spec.get("command", "!concerto")),
    )
    await discord_service.setup()
    return discord_service


def _create_app(config: dict[str, Any]) -> FastAPI:
    connectors = _validate_connectors(config)
    server = config.get("server") or {}
    db_path = str(
        server.get("db_path") or os.getenv("CONCERTO_DB_PATH", DEFAULT_DB_PATH)
    )

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=WEB_API_TIMEOUT_SECONDS)
        async with (
            aiohttp.ClientSession(
                timeout=timeout, max_field_size=concert_scraper.MAX_HEADER_BYTES
            ) as session,
            contextlib.AsyncExitStack() as stack,
        ):
            services: dict[str, BoardService] = {}
            tasks: list[asyncio.Task[None]] = []
            for spec in connectors:
                # Each connector keeps its own connection so its writes are
                # transactionally isolated; rows are namespaced by connector.
                db = await stack.enter_async_context(aiosqlite.connect(db_path))
                repository = BoardRepository(db)
                await repository.init()
                service = await _build_connector(spec, session, repository, db)
                services[service.connector_id] = service
                logger.info(
                    "Started connector %s (%s)", service.connector_id, spec["type"]
                )
                tasks.append(asyncio.create_task(service.run()))
            try:
                yield {"services": services}
            finally:
                for service in services.values():
                    with contextlib.suppress(Exception):
                        await service.close()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    app = FastAPI(lifespan=lifespan)
    register_board_routes(app)
    return app


def main() -> None:
    load_dotenv()
    config = _load_config()
    server = config.get("server") or {}

    logging.basicConfig(level=logging.INFO)
    level = str(server.get("log_level") or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.getLogger("concerto").setLevel(level)

    host = str(server.get("host") or os.getenv("HOST", "127.0.0.1"))
    port = int(str(server.get("port") or os.getenv("PORT", "8000")))
    uvicorn.run(_create_app(config), host=host, port=port)


if __name__ == "__main__":
    main()
