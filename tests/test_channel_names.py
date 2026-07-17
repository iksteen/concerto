"""Origin labels carry the tracked channel name, persisted across restarts."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import aiohttp
import aiosqlite

from concerto.board import BoardRepository, BoardService, LinkEntry


class _Svc(BoardService):
    def is_supported_channel(self, channel_id: str) -> bool:  # noqa: ARG002
        return True


class _RenamingSvc(_Svc):
    """A connector whose channels are all currently named ``#renamed``."""

    async def fetch_channel_name(self, channel_id: str) -> str | None:  # noqa: ARG002
        return "#renamed"


class _ServerSvc(_Svc):
    """A connector that labels origins with a server name (like Discord)."""

    def _origin_prefix(self, channel_id: str) -> str:  # noqa: ARG002
        return "My Server"


async def _run_prefix() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "t.db")
        db = await aiosqlite.connect(path)
        try:
            repo = BoardRepository(db)
            await repo.init()
            async with aiohttp.ClientSession() as session:
                svc = _ServerSvc("discord", session, repo)
                await svc.replace_board(
                    "42", {"https://x/1": LinkEntry(going=1, band="A")}
                )
                await svc.set_channel_name("42", "#general")
                views = await svc.event_views("42")
                assert views[0].origins[0].label == "My Server · #general"
        finally:
            await db.close()


def test_origin_prefix_replaces_connector_in_label() -> None:
    asyncio.run(_run_prefix())


async def _run() -> None:
    dbs: list[aiosqlite.Connection] = []

    async def service(session: aiohttp.ClientSession, path: str) -> _Svc:
        db = await aiosqlite.connect(path)
        dbs.append(db)
        repo = BoardRepository(db)
        await repo.init()
        svc = _Svc("work", session, repo)
        await svc.load_channel_names()
        return svc

    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "t.db")
        async with aiohttp.ClientSession() as session:
            try:
                svc = await service(session, path)
                await svc.replace_board(
                    "C1", {"https://x/1": LinkEntry(going=1, band="A")}
                )
                # No name yet -> label is just the connector.
                views = await svc.event_views("C1")
                assert views[0].origins[0].label == "work"

                await svc.set_channel_name("C1", "#gigs")
                views = await svc.event_views("C1")
                assert views[0].origins[0].label == "work · #gigs"

                # A fresh instance loads the persisted name from storage.
                svc2 = await service(session, path)
                views2 = await svc2.event_views("C1")
                assert views2[0].origins[0].label == "work · #gigs"

                # A startup refresh re-fetches current names for boarded
                # channels, picking up a rename that happened while down.
                db = await aiosqlite.connect(path)
                dbs.append(db)
                repo = BoardRepository(db)
                await repo.init()
                svc3 = _RenamingSvc("work", session, repo)
                await svc3.load_channel_names()
                await svc3.refresh_channel_names()
                views3 = await svc3.event_views("C1")
                assert views3[0].origins[0].label == "work · #renamed"
            finally:
                for db in dbs:
                    await db.close()


def test_origin_label_tracks_and_persists_channel_name() -> None:
    asyncio.run(_run())
