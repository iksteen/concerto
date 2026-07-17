"""Tests for the SSE stream's shutdown handling and update/keepalive output."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from concerto.board import BoardService, _sse_stream


def test_stream_stops_promptly_when_shutdown_is_set() -> None:
    asyncio.run(_run_shutdown())


async def _run_shutdown() -> None:
    shutdown = asyncio.Event()
    shutdown.set()  # already shutting down before the stream starts
    queue: asyncio.Queue[None] = asyncio.Queue()
    service = MagicMock(spec=BoardService)

    chunks = [
        chunk
        async for chunk in _sse_stream([(service, "C1", queue)], shutdown=shutdown)
    ]

    # No keepalive/update emitted; it broke out immediately and cleaned up.
    assert chunks == []
    service.unsubscribe.assert_called_once_with("C1", queue)


def test_stream_emits_update_when_notified() -> None:
    asyncio.run(_run_update())


async def _run_update() -> None:
    shutdown = asyncio.Event()  # not set
    queue: asyncio.Queue[None] = asyncio.Queue()
    queue.put_nowait(None)  # a pending board update
    service = MagicMock(spec=BoardService)

    stream = _sse_stream([(service, "C1", queue)], shutdown=shutdown)
    chunk = await stream.__anext__()
    assert chunk == "data: update\n\n"
    await stream.aclose()
    service.unsubscribe.assert_called_once_with("C1", queue)
