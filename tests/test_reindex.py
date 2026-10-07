"""A reindex re-scrapes a message's links and replaces their metadata."""

from __future__ import annotations

import asyncio
import datetime as dt

from concerto.board import BoardRepository, BoardService, ChannelBoard
from concerto.concert_scraper import ConcertInfo

URL = "https://example.com/event"


class _Repo(BoardRepository):
    def __init__(self) -> None:  # no db
        pass

    async def load_board(self, channel_id: str) -> ChannelBoard:  # noqa: ARG002
        return ChannelBoard()

    async def save_board(self, channel_id: str, board: ChannelBoard) -> None:
        pass


class _Service(BoardService):
    def __init__(self) -> None:
        super().__init__("c", None, _Repo())  # type: ignore[arg-type]
        self.results: list[ConcertInfo | None] = []
        self.scraped = 0

    def is_supported_channel(self, channel_id: str) -> bool:  # noqa: ARG002
        return True

    async def _scrape_metadata(self, url: str) -> ConcertInfo | None:  # noqa: ARG002
        self.scraped += 1
        return self.results.pop(0)


async def _exercise() -> None:
    service = _Service()
    # A bad scrape: a non-event page's title stored as the band.
    service.results.append(ConcertInfo(url=URL, band="Venue | Home", venue="Venue"))
    await service.apply_message("chan", 100, URL)
    entry = (await service._get_board_locked("chan")).links[URL]
    assert entry.band == "Venue | Home"

    # Having metadata, the link isn't scraped again on its own.
    await service.apply_message("chan", 100, URL)
    assert service.scraped == 1

    # A reindex scrapes again and replaces the metadata outright.
    service.results.append(
        ConcertInfo(url=URL, band="Band", date=dt.date(2026, 10, 31))
    )
    await service.reindex_message("chan", 100, URL)
    assert service.scraped == 2
    assert (entry.band, entry.event_date, entry.venue) == ("Band", "2026-10-31", None)

    # A failed scrape keeps what was stored.
    service.results.append(None)
    await service.reindex_message("chan", 100, URL)
    assert service.scraped == 3
    assert entry.band == "Band"


def test_reindex_replaces_metadata() -> None:
    asyncio.run(_exercise())
