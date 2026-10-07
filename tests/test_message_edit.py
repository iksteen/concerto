"""Edited/deleted messages drop the links they no longer carry."""

from __future__ import annotations

import asyncio

from concerto.board import BoardRepository, BoardService, ChannelBoard, LinkEntry


class _Repo(BoardRepository):
    def __init__(self) -> None:  # no db
        self.saved: dict[str, dict[str, LinkEntry]] = {}

    async def load_board(self, channel_id: str) -> ChannelBoard:  # noqa: ARG002
        return ChannelBoard()

    async def save_board(self, channel_id: str, board: ChannelBoard) -> None:
        self.saved[channel_id] = dict(board.links)


class _Service(BoardService):
    def is_supported_channel(self, channel_id: str) -> bool:  # noqa: ARG002
        return True

    async def _enrich_links(self, channel_id: str, urls: list[str]) -> None:  # noqa: ARG002
        return


async def _exercise() -> None:
    service = _Service("c", None, _Repo())  # type: ignore[arg-type]
    a = "https://example.com/a"
    b = "https://example.com/b"

    await service.apply_message("chan", 100, f"{a} {b}")
    links = (await service._get_board_locked("chan")).links
    assert set(links) == {a, b}

    # Edit: b removed from the message.
    await service.apply_message("chan", 100, a)
    assert set(links) == {a}

    # A link first seen in another message survives this one's delete.
    await service.apply_message("chan", 200, f"{a} {b}")
    await service.apply_message("chan", 200, "")
    assert set(links) == {a}

    # Delete of the originating message drops it.
    await service.apply_message("chan", 100, "")
    assert not links


def test_edit_and_delete_update_the_board() -> None:
    asyncio.run(_exercise())
