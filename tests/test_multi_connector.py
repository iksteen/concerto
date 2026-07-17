"""Tests for multi-connector hosting: storage namespacing + config validation."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import aiohttp
import aiosqlite

from concerto.__main__ import _validate_connectors
from concerto.board import BoardRepository, BoardService, LinkEntry


class _Svc(BoardService):
    def is_supported_channel(self, channel_id: str) -> bool:  # noqa: ARG002
        return True


async def _run_namespacing() -> None:
    dbs: list[aiosqlite.Connection] = []

    async def service(session: aiohttp.ClientSession, path: str, name: str) -> _Svc:
        db = await aiosqlite.connect(path)
        dbs.append(db)
        repo = BoardRepository(db)
        await repo.init()
        return _Svc(name, session, repo)

    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "t.db")
        async with aiohttp.ClientSession() as session:
            try:
                # Two connectors, SAME channel id "C1", different links. band
                # set = already resolved, so no scraping/network happens.
                a = await service(session, path, "ws-a")
                await a.replace_board(
                    "C1", {"https://x/1": LinkEntry(going=2, band="A")}
                )
                b = await service(session, path, "ws-b")
                await b.replace_board(
                    "C1", {"https://x/2": LinkEntry(going=5, band="B")}
                )

                # Fresh instances read from disk — proves on-disk isolation.
                a2 = await service(session, path, "ws-a")
                b2 = await service(session, path, "ws-b")
                va = await a2.event_views("C1")
                vb = await b2.event_views("C1")
            finally:
                for db in dbs:
                    await db.close()

    assert [v.url for v in va] == ["https://x/1"], va
    assert va[0].origins[0].going == 2
    assert va[0].band == "A"
    assert [v.url for v in vb] == ["https://x/2"], vb
    assert vb[0].origins[0].going == 5
    assert vb[0].band == "B"


def test_storage_is_namespaced_per_connector() -> None:
    asyncio.run(_run_namespacing())


def test_validate_connectors_accepts_a_good_config() -> None:
    good = {
        "connector": [
            {"type": "slack", "name": "work", "bot_token": "b", "app_token": "a"},
            {"type": "discord", "name": "main", "token": "t"},
        ]
    }
    assert len(_validate_connectors(good)) == 2


def test_validate_connectors_rejects_bad_configs() -> None:
    bad: list[dict[str, Any]] = [
        {},  # no connectors
        {"connector": []},  # empty
        {
            "connector": [{"type": "slack", "name": "x", "bot_token": "b"}]
        },  # no app_token
        {"connector": [{"type": "discord", "name": "x"}]},  # no token
        {"connector": [{"type": "irc", "name": "x", "token": "t"}]},  # unknown type
        {
            "connector": [{"type": "discord", "name": "a/b", "token": "t"}]
        },  # slash in name
        {
            "connector": [
                {"type": "discord", "name": "dup", "token": "t"},
                {"type": "discord", "name": "dup", "token": "t"},
            ]
        },  # duplicate name
    ]
    for cfg in bad:
        try:
            _validate_connectors(cfg)
        except RuntimeError:
            continue
        msg = f"expected RuntimeError for {cfg!r}"
        raise AssertionError(msg)
