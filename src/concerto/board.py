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
from dataclasses import dataclass, field, replace
from html import escape
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import aiohttp
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from concerto import concert_scraper

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    import aiosqlite

logger = logging.getLogger("concerto")

WEB_API_TIMEOUT_SECONDS = 20
SSE_KEEPALIVE_SECONDS = 15
DAYS_PER_WEEK = 7
DAYS_PER_MONTH = 31

# Set when the process is shutting down so open SSE streams stop their otherwise
# endless keepalive loop — otherwise uvicorn's graceful drain waits forever for
# them and Ctrl-C appears to hang.
_shutdown_requested = asyncio.Event()


def request_shutdown() -> None:
    """Signal open SSE streams to finish (called from the signal handler)."""
    _shutdown_requested.set()


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
class Origin:
    """One channel that tracks an event: its message link and interest counts."""

    label: str  # e.g. "main · #gigs" or "My Server · #gigs" — origin of the event
    message_url: str | None
    going: int  # have a ticket
    undecided: int  # interested, no ticket yet
    looking: int  # looking for a ticket on TicketSwap


@dataclass
class EventView:
    """An immutable snapshot of a tracked link for the overview page."""

    url: str
    band: str | None
    venue: str | None
    expired: bool
    date: dt.date | None
    end_date: dt.date | None  # end of a multi-day run, else None
    # One entry per channel tracking this event; combined boards have several.
    origins: list[Origin]


class BoardRepository:
    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def init(self) -> None:
        await self._db.executescript(
            """
            PRAGMA journal_mode=WAL;
            -- Multiple connectors keep their own connection to this file; wait
            -- briefly rather than erroring when another holds the write lock.
            PRAGMA busy_timeout=5000;

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

            -- Human channel names for origin labels, keyed by the same
            -- connector/channel_id namespace as `links`.
            CREATE TABLE IF NOT EXISTS channel_names (
                channel_id TEXT PRIMARY KEY,
                name TEXT NOT NULL
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

    async def load_channel_names(self) -> dict[str, str]:
        async with self._db.execute(
            "SELECT channel_id, name FROM channel_names"
        ) as cursor:
            return {str(row[0]): str(row[1]) async for row in cursor}

    async def load_channel_ids(self) -> set[str]:
        """Namespaced ids of every channel that has a board (i.e. tracked links)."""
        async with self._db.execute("SELECT DISTINCT channel_id FROM links") as cursor:
            return {str(row[0]) async for row in cursor}

    async def save_channel_name(self, key: str, name: str) -> None:
        await self._db.execute(
            "INSERT INTO channel_names(channel_id, name) VALUES(?, ?) "
            "ON CONFLICT(channel_id) DO UPDATE SET name = excluded.name",
            (key, name),
        )
        await self._db.commit()


class BoardService:
    """Owns the board cache, persistence, scraping, and SSE subscribers.

    Platforms drive it through the neutral ingestion methods (`apply_message`,
    `apply_reactions`, `replace_board`, `merge_entries`) and override the two
    hooks below.
    """

    def __init__(
        self,
        connector_id: str,
        session: aiohttp.ClientSession,
        repository: BoardRepository,
    ) -> None:
        self._connector_id = connector_id
        self._session = session
        self._repository = repository
        self._boards: dict[str, ChannelBoard] = {}
        # channel_id -> human display name, for origin labels.
        self._channel_names: dict[str, str] = {}
        self._metadata_tried: set[str] = set()
        self._lock = asyncio.Lock()
        self._subscribers: dict[str, set[asyncio.Queue[None]]] = {}

    @property
    def connector_id(self) -> str:
        return self._connector_id

    def _board_key(self, channel_id: str) -> str:
        # Persisted channel rows are namespaced by connector so multiple
        # connectors (e.g. two Slack workspaces) sharing one database can't
        # collide on the same channel id. The in-memory cache stays keyed by
        # the raw channel id since each service only sees its own channels.
        return f"{self._connector_id}/{channel_id}"

    # --- channel names (for origin labels) ---

    async def load_channel_names(self) -> None:
        """Warm the channel-name cache from storage (call once at startup)."""
        prefix = f"{self._connector_id}/"
        rows = await self._repository.load_channel_names()
        self._channel_names = {
            key.removeprefix(prefix): name
            for key, name in rows.items()
            if key.startswith(prefix)
        }

    async def set_channel_name(self, channel_id: str, name: str | None) -> None:
        """Track a channel's display name; connectors call this on ingestion."""
        if not name or self._channel_names.get(channel_id) == name:
            return
        # Share the board lock so this commit can't flush a board save's
        # half-done transaction on the same connection.
        async with self._lock:
            self._channel_names[channel_id] = name
            await self._repository.save_channel_name(self._board_key(channel_id), name)

    async def refresh_channel_names(self) -> None:
        """Re-fetch the current name of every channel with a board (at startup).

        Picks up channels renamed while we were down, and names for channels
        whose board predates name tracking.
        """
        prefix = f"{self._connector_id}/"
        keys = await self._repository.load_channel_ids()
        for channel_id in (
            k.removeprefix(prefix) for k in keys if k.startswith(prefix)
        ):
            await self.set_channel_name(
                channel_id, await self.fetch_channel_name(channel_id)
            )

    async def fetch_channel_name(self, channel_id: str) -> str | None:  # noqa: ARG002
        """Resolve a channel's current display name; overridden per connector."""
        return None

    def _origin_label(self, channel_id: str) -> str:
        prefix = self._origin_prefix(channel_id)
        name = self._channel_names.get(channel_id)
        return f"{prefix} \N{MIDDLE DOT} {name}" if name else prefix

    def _origin_prefix(self, channel_id: str) -> str:  # noqa: ARG002
        """Label shown before the channel name; overridable per connector."""
        return self._connector_id

    # --- platform hooks (overridden by subclasses) ---

    def is_supported_channel(self, channel_id: str) -> bool:  # noqa: ARG002
        return True

    def message_url(self, channel_id: str, source_message_ts: str | None) -> str | None:  # noqa: ARG002
        return None

    # --- connector lifecycle (overridden by subclasses) ---

    async def run(self) -> None:
        """Run the connector's gateway until its task is cancelled."""
        raise NotImplementedError

    async def close(self) -> None:
        """Gracefully stop the connector; called on shutdown before cancel."""

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
                    date=_parse_iso_date(entry.event_date),
                    end_date=_parse_iso_date(entry.event_end_date),
                    origins=[
                        Origin(
                            label=self._origin_label(channel_id),
                            message_url=self.message_url(
                                channel_id, entry.source_message_ts
                            ),
                            going=entry.going,
                            undecided=entry.undecided,
                            looking=entry.looking,
                        )
                    ],
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
        # Persist each event as it resolves so a crash mid-batch keeps progress.
        for url in pending:
            info = await self._scrape_metadata(url)
            if info is None:
                continue
            async with self._lock:
                board = await self._get_board_locked(channel_id)
                entry = board.links.get(url)
                if entry is not None and _apply_metadata(entry, info):
                    await self._persist_locked(channel_id, board)

    # --- board cache + persistence ---

    async def _get_board_locked(self, channel_id: str) -> ChannelBoard:
        board = self._boards.get(channel_id)
        if board is not None:
            return board

        board = await self._repository.load_board(self._board_key(channel_id))
        self._boards[channel_id] = board
        return board

    async def _persist_locked(self, channel_id: str, board: ChannelBoard) -> None:
        await self._repository.save_board(self._board_key(channel_id), board)
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

    Routes resolve the connector's :class:`BoardService` from the
    ``request.state.services`` registry, which the lifespan must populate.
    """

    @app.get("/")
    async def index() -> dict[str, str]:
        return {"message": "Hello world"}

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/board/{connector}/{channel_id}", response_class=HTMLResponse)
    async def board(connector: str, channel_id: str, request: Request) -> str:
        service = _service_from_request(request, connector)
        if not service.is_supported_channel(channel_id):
            raise HTTPException(status_code=404, detail="unknown channel")
        views = await service.event_views(channel_id)
        return render_overview(f"{connector}/{channel_id}", views)

    @app.get("/board/{connector}/{channel_id}/events")
    async def board_events(
        connector: str, channel_id: str, request: Request
    ) -> StreamingResponse:
        service = _service_from_request(request, connector)
        if not service.is_supported_channel(channel_id):
            raise HTTPException(status_code=404, detail="unknown channel")

        return StreamingResponse(
            _sse_stream([(service, channel_id, service.subscribe(channel_id))]),
            media_type="text/event-stream",
        )

    @app.get("/combined/{name}", response_class=HTMLResponse)
    async def combined(name: str, request: Request) -> str:
        sources = _combined_from_request(request, name)
        services = _services_from_request(request)
        views = await _combined_event_views(services, sources)
        return render_overview(name, views)

    @app.get("/combined/{name}/events")
    async def combined_events(name: str, request: Request) -> StreamingResponse:
        sources = _combined_from_request(request, name)
        services = _services_from_request(request)
        subs = [
            (service, channel, service.subscribe(channel))
            for connector, channel in sources
            if (service := services.get(connector)) is not None
        ]
        return StreamingResponse(_sse_stream(subs), media_type="text/event-stream")


Subscription = tuple["BoardService", str, "asyncio.Queue[None]"]


async def _sse_stream(
    subscriptions: list[Subscription],
    *,
    shutdown: asyncio.Event = _shutdown_requested,
) -> AsyncGenerator[str, None]:
    """Emit an SSE ``update`` whenever any subscribed board changes."""
    try:
        while True:
            getters = [
                asyncio.ensure_future(queue.get()) for _, _, queue in subscriptions
            ]
            stop = asyncio.ensure_future(shutdown.wait())
            try:
                done, _ = await asyncio.wait(
                    {*getters, stop},
                    timeout=SSE_KEEPALIVE_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                # A pending get() leaves its queued item in place for the next
                # iteration; cancelling a finished task is a harmless no-op.
                for getter in getters:
                    getter.cancel()
                stop.cancel()
            if stop in done:
                break
            yield (
                "data: update\n\n"
                if any(g in done for g in getters)
                else ": keepalive\n\n"
            )
    finally:
        for service, channel, queue in subscriptions:
            service.unsubscribe(channel, queue)


def _services_from_request(request: Request) -> dict[str, BoardService]:
    services = getattr(request.state, "services", None)
    if not isinstance(services, dict):
        raise HTTPException(status_code=500, detail="services not initialized")
    return services


def _service_from_request(request: Request, connector: str) -> BoardService:
    service = _services_from_request(request).get(connector)
    if not isinstance(service, BoardService):
        raise HTTPException(status_code=404, detail="unknown connector")
    return service


def _combined_from_request(request: Request, name: str) -> list[tuple[str, str]]:
    combined = getattr(request.state, "combined", None)
    if not isinstance(combined, dict):
        raise HTTPException(status_code=500, detail="combined boards not initialized")
    sources: list[tuple[str, str]] | None = combined.get(name)
    if sources is None:
        raise HTTPException(status_code=404, detail="unknown board")
    return sources


async def _combined_event_views(
    services: dict[str, BoardService], sources: list[tuple[str, str]]
) -> list[EventView]:
    collected: list[EventView] = []
    for connector, channel in sources:
        service = services.get(connector)
        if service is not None:
            collected.extend(await service.event_views(channel))
    return merge_event_views(collected)


def merge_event_views(views: list[EventView]) -> list[EventView]:
    """Fold views from several boards into one list, deduped by URL.

    The same event tracked in multiple channels is shown once: metadata is taken
    from the first board that resolved it, and it's only treated as expired when
    every board agrees. Each channel keeps its own origin row (message link plus
    that channel's interest counts) rather than being summed away.
    """
    merged: dict[str, EventView] = {}
    for view in views:
        existing = merged.get(view.url)
        if existing is None:
            merged[view.url] = replace(view, origins=list(view.origins))
            continue
        existing.band = existing.band or view.band
        existing.venue = existing.venue or view.venue
        existing.date = existing.date or view.date
        existing.end_date = existing.end_date or view.end_date
        existing.expired = existing.expired and view.expired
        existing.origins.extend(view.origins)
    return list(merged.values())


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
.band {
  display: inline-block; font-size: 1.14rem; font-weight: 650;
  word-break: break-word; text-decoration: none; color: var(--text);
}
.band:hover { color: var(--accent); }
.venue { color: var(--muted); font-size: 0.92rem; margin: 2px 0 9px; }
.run { color: var(--accent); font-size: 0.82rem; margin: -4px 0 9px; }
.origins { display: flex; flex-direction: column; gap: 7px; }
.origin { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
.status { display: flex; gap: 12px; }
.stat {
  font-size: 0.92rem; font-variant-numeric: tabular-nums;
  cursor: default; user-select: none;
}
.origin-name { font-size: 0.8rem; color: var(--muted); }
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


def _render_status(going: int, undecided: int, looking: int) -> str:
    # (emoji, count, hover label); zero-count statuses are omitted.
    stats = [
        ("\N{TICKET}", going, "have a ticket"),
        ("\N{EYES}", undecided, "interested"),
        ("\N{PERSON WITH FOLDED HANDS}", looking, "looking for a ticket"),
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


def _render_origin(origin: Origin) -> str:
    label = escape(origin.label)
    if origin.message_url:
        head = (
            f'<a class="link" href="{escape(origin.message_url)}" '
            f'target="_blank" rel="noopener">{label} &#8599;</a>'
        )
    else:
        head = f'<span class="origin-name">{label}</span>'
    pills = _render_status(origin.going, origin.undecided, origin.looking)
    return f'<div class="origin">{head}{pills}</div>'


def _render_event_card(view: EventView) -> str:
    name = escape(view.band) if view.band else _fallback_name(view.url)
    venue = escape(view.venue) if view.venue else "&mdash;"
    title = (
        f'<a class="band" href="{escape(view.url)}" '
        f'target="_blank" rel="noopener">{name} &#8599;</a>'
    )
    origins = "".join(_render_origin(origin) for origin in view.origins)
    return (
        '<article class="card">'
        f"{_render_date_badge(view.date)}"
        '<div class="meta">'
        f"{title}"
        f'<div class="venue">{venue}</div>'
        f"{_render_run(view)}"
        f'<div class="origins">{origins}</div>'
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
    year: list[EventView] = []
    later: list[EventView] = []
    for view in dated:
        if view.date is None:
            continue
        days = (view.date - today).days
        if days <= DAYS_PER_WEEK:
            week.append(view)
        elif days < DAYS_PER_MONTH:
            month.append(view)
        elif view.date.year == today.year:
            year.append(view)
        else:
            later.append(view)

    groups = [
        ("Date unknown", undated),
        ("This week", week),
        ("This month", month),
        ("This year", year),
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
