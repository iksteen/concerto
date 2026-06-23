"""Platform-agnostic core: storage, board logic, and the web overview.

Knows nothing about Slack or Discord. A platform layer (e.g. ``slack_bot``)
translates its own events into the neutral ``BoardService`` ingestion calls and
supplies the two hooks (`is_supported_channel`, `message_url`).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import re
from dataclasses import dataclass, field
from html import escape
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import aiohttp
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from concerto import concert_scraper

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import aiosqlite

logger = logging.getLogger("concerto")

WEB_API_TIMEOUT_SECONDS = 20
SSE_KEEPALIVE_SECONDS = 15
DAYS_PER_WEEK = 7
DAYS_PER_MONTH = 31

# Reaction names that map to each status. These are platform-neutral shortcodes;
# a platform whose reactions use other names must translate to these.
PLUS_ONE_REACTIONS = {"+1", "thumbsup", "ticket"}
QUESTION_REACTIONS = {"question", "grey_question", "eyes"}
PRAY_REACTIONS = {"pray"}

# Links on these domains (and their subdomains) are never tracked.
IGNORED_LINK_DOMAINS = (
    "slack.com",
    "discord.com",
    "nrc.nl",
    "youtube.com",
    "youtu.be",
    "spotify.com",
    "infrapuin.nl",
)


@dataclass
class LinkEntry:
    # Aggregate reaction counts only — we never store who posted or reacted.
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

            -- We no longer store who posted or reacted, only aggregate counts
            -- on `links`. Run a channel rebuild to repopulate the counts.
            DROP TABLE IF EXISTS link_statuses;
            DROP TABLE IF EXISTS link_posters;
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

        return board

    async def save_board(self, channel_id: str, board: ChannelBoard) -> None:
        await self._db.execute("DELETE FROM links WHERE channel_id = ?", (channel_id,))

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

        await self._db.commit()


class BoardService:
    """Owns the board cache, persistence, scraping, and SSE subscribers.

    Platforms drive it through the neutral ingestion methods (`apply_message`,
    `apply_reactions`, `replace_board`, `merge_entries`) and override the two
    hooks below.
    """

    def __init__(
        self, session: aiohttp.ClientSession, repository: BoardRepository
    ) -> None:
        self._session = session
        self._repository = repository
        self._boards: dict[str, ChannelBoard] = {}
        self._metadata_tried: set[str] = set()
        self._lock = asyncio.Lock()
        self._subscribers: dict[str, set[asyncio.Queue[None]]] = {}

    # --- platform hooks (overridden by subclasses) ---

    def is_supported_channel(self, channel_id: str) -> bool:  # noqa: ARG002
        return True

    def message_url(self, channel_id: str, source_message_ts: str | None) -> str | None:  # noqa: ARG002
        return None

    # --- read side ---

    async def event_views(self, channel_id: str) -> list[EventView]:
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            return [
                EventView(
                    url=url,
                    band=entry.band,
                    venue=entry.venue,
                    expired=entry.expired,
                    message_url=self.message_url(channel_id, entry.source_message_ts),
                    date=_parse_iso_date(entry.event_date),
                    end_date=_parse_iso_date(entry.event_end_date),
                    going=entry.going,
                    undecided=entry.undecided,
                    looking=entry.looking,
                )
                for url, entry in board.links.items()
            ]

    # --- neutral ingestion API ---

    async def apply_message(
        self, channel_id: str, message_id: object, text: str
    ) -> None:
        """Track links in a posted message, from this post onward."""
        links = extract_links(text)
        if not links:
            return
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            for link in links:
                entry = board.links.setdefault(link, LinkEntry())
                _set_earliest_source_message_ts(entry, message_id)
            await self._persist_locked(channel_id, board)
        await self._enrich_links(channel_id, links)

    async def apply_reactions(
        self, channel_id: str, message_id: object, text: str, reactions: object
    ) -> None:
        """Re-parse a message's full reaction set into aggregate counts."""
        links = extract_links(text)
        if not links:
            return
        counts = aggregate_status_counts(reactions)
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            for link in links:
                entry = board.links.setdefault(link, LinkEntry())
                _set_earliest_source_message_ts(entry, message_id)
                # ponytail: a URL reposted across messages shows the counts of
                # whichever post was last reacted on; reactions cluster on one.
                entry.going, entry.undecided, entry.looking = counts
            await self._persist_locked(channel_id, board)
        await self._enrich_links(channel_id, links)

    async def replace_board(
        self, channel_id: str, entries: dict[str, LinkEntry]
    ) -> None:
        """Replace the whole channel board (a full history rebuild)."""
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            board.links = entries
            await self._persist_locked(channel_id, board)
        await self._enrich_links(channel_id, list(entries))

    async def merge_entries(
        self, channel_id: str, entries: dict[str, LinkEntry]
    ) -> None:
        """Merge scanned history into the existing board (e.g. on join)."""
        if not entries:
            return
        async with self._lock:
            board = await self._get_board_locked(channel_id)
            changed = False
            for link, scanned_entry in entries.items():
                entry = board.links.setdefault(link, LinkEntry())
                if _merge_link_entry(entry, scanned_entry):
                    changed = True
            if changed:
                await self._persist_locked(channel_id, board)
        await self._enrich_links(channel_id, list(entries))

    # --- metadata enrichment ---

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

    # --- board cache + persistence ---

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

    # --- SSE subscribers ---

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


def fold_message(
    entries: dict[str, LinkEntry], message_id: object, text: str, reactions: object
) -> None:
    """Fold one scanned message into a history ``entries`` accumulator.

    Used by a platform's history scan; `reactions` is the neutral shape
    ``[{"name": str, "users": [str, ...]}, ...]``.
    """
    links = extract_links(text)
    if not links:
        return
    going, undecided, looking = aggregate_status_counts(reactions)
    for link in links:
        entry = entries.setdefault(link, LinkEntry())
        _set_earliest_source_message_ts(entry, message_id)
        # ponytail: same URL across posts -> keep the highest count per status;
        # reactions normally sit on one post.
        entry.going = max(entry.going, going)
        entry.undecided = max(entry.undecided, undecided)
        entry.looking = max(entry.looking, looking)


def extract_links(text: str) -> list[str]:
    if not text:
        return []

    # Slack wraps URLs as <url> or <url|label>; capture just the url. (Discord
    # sends bare URLs, so this branch is simply inert there.)
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


def aggregate_status_counts(reactions: object) -> tuple[int, int, int]:
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
        target.going,
        target.undecided,
        target.looking,
        target.source_message_ts,
    )
    target.going = max(target.going, source.going)
    target.undecided = max(target.undecided, source.undecided)
    target.looking = max(target.looking, source.looking)
    _set_earliest_source_message_ts(target, source.source_message_ts)
    after = (
        target.going,
        target.undecided,
        target.looking,
        target.source_message_ts,
    )
    return before != after


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
    # Works for Slack float timestamps ("1700000000.000100") and for monotonic
    # integer ids like Discord snowflakes (both order earliest-first).
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


def register_board_routes(app: FastAPI) -> None:
    """Add the platform-agnostic web routes to ``app``.

    Routes read the active :class:`BoardService` from ``request.state.service``,
    which the platform's lifespan must populate.
    """

    @app.get("/")
    async def index() -> dict[str, str]:
        return {"message": "Hello world"}

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/board/{channel_id}", response_class=HTMLResponse)
    async def board(channel_id: str, request: Request) -> str:
        service = _service_from_request(request)
        if not service.is_supported_channel(channel_id):
            raise HTTPException(status_code=404, detail="unknown channel")
        views = await service.event_views(channel_id)
        return render_overview(channel_id, views)

    @app.get("/board/{channel_id}/events")
    async def board_events(channel_id: str, request: Request) -> StreamingResponse:
        service = _service_from_request(request)
        if not service.is_supported_channel(channel_id):
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


def _service_from_request(request: Request) -> BoardService:
    service = getattr(request.state, "service", None)
    if not isinstance(service, BoardService):
        raise HTTPException(status_code=500, detail="service not initialized")
    return service


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
            'target="_blank" rel="noopener">Open &#8599;</a>'
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


def render_overview(channel_id: str, views: list[EventView]) -> str:
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
