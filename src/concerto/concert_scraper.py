"""Scrape concert pages for band name, date, and venue.

Each supported venue has a dedicated parser keyed by domain. Sites that publish
schema.org ``Event``/``MusicEvent`` JSON-LD (or useful OpenGraph tags) are also
handled by a generic fallback, so unknown venues often work without a parser.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import re
import sys
from dataclasses import dataclass
from html import unescape
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import aiohttp

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 concerto-scraper"
)
REQUEST_TIMEOUT_SECONDS = 20
# Some sites (e.g. Ticketmaster) send headers larger than aiohttp's 8 KiB
# default, which otherwise fails the request.
MAX_HEADER_BYTES = 32768
_DAYS_IN_MONTH = 31

_MONTHS: dict[str, int] = {
    "januari": 1,
    "january": 1,
    "jan": 1,
    "februari": 2,
    "february": 2,
    "feb": 2,
    "maart": 3,
    "march": 3,
    "mrt": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "mei": 5,
    "may": 5,
    "juni": 6,
    "june": 6,
    "jun": 6,
    "juli": 7,
    "july": 7,
    "jul": 7,
    "augustus": 8,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sept": 9,
    "sep": 9,
    "oktober": 10,
    "october": 10,
    "okt": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}

_ISO_DATE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_ORDINAL = re.compile(r"\b(\d{1,2})(?:st|nd|rd|th)\b", re.IGNORECASE)
# "9 April 2026", "23 november 2026", "22 of September 2026"
_DAY_MONTH_YEAR = re.compile(
    r"\b(\d{1,2})\s+(?:of\s+)?([A-Za-z]+)\s+(\d{4})\b",
    re.IGNORECASE,
)
# "April 9, 2026"
_MONTH_DAY_YEAR = re.compile(
    r"\b([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})\b",
    re.IGNORECASE,
)


@dataclass
class ConcertInfo:
    url: str
    band: str | None = None
    date: dt.date | None = None
    venue: str | None = None
    raw_date: str | None = None
    # The page is gone (404/410) or redirected to a listing page, which means
    # the event has been removed from the site and is in the past.
    expired: bool = False

    @property
    def is_complete(self) -> bool:
        return bool(self.band and self.date and self.venue)


class ScrapeError(RuntimeError):
    pass


# --------------------------------------------------------------------------- #
# Date parsing
# --------------------------------------------------------------------------- #
def _safe_date(year: int, month: int, day: int) -> dt.date | None:
    if not 1 <= day <= _DAYS_IN_MONTH:
        return None
    try:
        return dt.date(year, month, day)
    except ValueError:
        return None


def parse_date(text: str | None) -> tuple[dt.date | None, str | None]:
    """Return ``(date, matched_text)`` for the first date found in ``text``."""
    if not text:
        return None, None

    iso = _ISO_DATE.search(text)
    if iso:
        parsed = _safe_date(int(iso[1]), int(iso[2]), int(iso[3]))
        if parsed:
            return parsed, iso[0]

    normalized = _ORDINAL.sub(r"\1", text)

    for match in _DAY_MONTH_YEAR.finditer(normalized):
        month = _MONTHS.get(match[2].lower())
        if month is None:
            continue
        parsed = _safe_date(int(match[3]), month, int(match[1]))
        if parsed:
            return parsed, match[0]

    for match in _MONTH_DAY_YEAR.finditer(normalized):
        month = _MONTHS.get(match[1].lower())
        if month is None:
            continue
        parsed = _safe_date(int(match[3]), month, int(match[2]))
        if parsed:
            return parsed, match[0]

    return None, None


# Many WordPress venues encode the date in the URL slug, e.g.
# "henge-16-jul-2026" or "adrian-vandenberg-13-06-26".
_SLUG_DAY_MONTH_YEAR = re.compile(r"(\d{1,2})-([a-z]{3,})-(\d{4})$", re.IGNORECASE)
_SLUG_NUMERIC = re.compile(r"(\d{1,2})-(\d{1,2})-(\d{2,4})$")
_CENTURY = 2000
_YEAR_CUTOFF = 100


def date_from_slug(url: str) -> tuple[dt.date | None, str | None]:
    slug = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]

    named = _SLUG_DAY_MONTH_YEAR.search(slug)
    if named:
        month = _MONTHS.get(named[2].lower())
        if month is not None:
            parsed = _safe_date(int(named[3]), month, int(named[1]))
            if parsed:
                return parsed, named[0]

    numeric = _SLUG_NUMERIC.search(slug)
    if numeric:
        year = int(numeric[3])
        if year < _YEAR_CUTOFF:
            year += _CENTURY
        parsed = _safe_date(year, int(numeric[2]), int(numeric[1]))
        if parsed:
            return parsed, numeric[0]

    return None, None


# --------------------------------------------------------------------------- #
# HTML helpers
# --------------------------------------------------------------------------- #
def _meta_content(html: str, key: str) -> str | None:
    """Read a ``<meta>`` content value, tolerating either attribute order."""
    attr = r'(?:property|name)=["\']' + re.escape(key) + r'["\']'
    content = r'content=["\'](.*?)["\']'
    for pattern in (attr + r"[^>]*?" + content, content + r"[^>]*?" + attr):
        match = re.search(r"<meta[^>]*?" + pattern, html, re.IGNORECASE | re.DOTALL)
        if match:
            return unescape(match[1]).strip()
    return None


def _title(html: str) -> str | None:
    match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    return unescape(match[1]).strip() if match else None


def _strip_tags(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", value)).strip()


# Status badges venues prepend to event titles, e.g. "UITVERKOCHT | Crypta"
# or "Verplaatst naar Stage 1: Kylesa".
_STATUS_PREFIXES = (
    "uitverkocht",
    "sold out",
    "soldout",
    "afgelast",
    "geannuleerd",
    "cancelled",
    "canceled",
    "verplaatst",
    "verzet",
    "moved",
    "nieuwe datum",
    "new date",
    "extra show",
    "extra concert",
    "extra",
)


def _strip_status_prefix(name: str) -> str:
    cleaned = name.strip()
    for separator in ("|", ":"):
        head, found, tail = cleaned.partition(separator)
        head = head.strip()
        if (
            found
            and tail.strip()
            and any(head.lower().startswith(prefix) for prefix in _STATUS_PREFIXES)
        ):
            return _strip_status_prefix(tail)
    return cleaned


_TRAILING_COMMA = re.compile(r",(\s*[}\]])")


def _load_json_ld(block: str) -> object | None:
    # Some sites emit invalid JSON-LD with trailing commas; retry repaired.
    for candidate in (block, _TRAILING_COMMA.sub(r"\1", block)):
        try:
            parsed: object = json.loads(candidate, strict=False)
        except (json.JSONDecodeError, ValueError):
            continue
        return parsed
    return None


def _json_ld_objects(html: str) -> Iterable[dict[str, object]]:
    blocks = re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    for block in blocks:
        data = _load_json_ld(block)
        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            graph = obj.get("@graph")
            if isinstance(graph, list):
                for node in graph:
                    if isinstance(node, dict):
                        yield node
            else:
                yield obj


def _is_event(obj: dict[str, object]) -> bool:
    raw_type = obj.get("@type", "")
    types = raw_type if isinstance(raw_type, list) else [raw_type]
    return any("Event" in str(value) for value in types)


# --------------------------------------------------------------------------- #
# Generic fallback parsers
# --------------------------------------------------------------------------- #
def _event_to_info(obj: dict[str, object], url: str) -> ConcertInfo:
    info = ConcertInfo(url=url)
    name = obj.get("name")
    if isinstance(name, str) and name.strip():
        info.band = _strip_status_prefix(unescape(name))
    start = obj.get("startDate") or obj.get("date")
    if isinstance(start, str):
        info.date, info.raw_date = parse_date(start)
    location = obj.get("location")
    if isinstance(location, list) and location:
        location = location[0]
    if isinstance(location, dict):
        venue = location.get("name")
        if isinstance(venue, str) and venue.strip():
            info.venue = venue.strip()
    return info


def parse_json_ld(html: str, url: str) -> ConcertInfo:
    events = [
        _event_to_info(obj, url) for obj in _json_ld_objects(html) if _is_event(obj)
    ]
    if not events:
        return ConcertInfo(url=url)
    # Pages may list several events: related/upcoming shows, or an incomplete
    # teaser alongside the real event. Prefer complete events, and within those
    # the one whose name appears in og:title.
    pool = [event for event in events if event.is_complete] or events
    target = (_meta_content(html, "og:title") or "").lower()
    if target:
        for event in pool:
            if event.band and event.band.lower() in target:
                return event
    return pool[0]


def _fill_gaps(info: ConcertInfo, html: str, url: str) -> None:
    if info.band is None:
        title = _meta_content(html, "og:title") or _title(html)
        if title:
            info.band = _strip_status_prefix(title)
    if info.date is None:
        info.date, info.raw_date = parse_date(_meta_content(html, "og:description"))
    if info.date is None:
        info.date, info.raw_date = date_from_slug(url)
    if info.date is None:
        info.date, info.raw_date = parse_date(_strip_tags(html))


# --------------------------------------------------------------------------- #
# Site-specific parsers
# --------------------------------------------------------------------------- #
def parse_tivoli(html: str, url: str) -> ConcertInfo:
    return parse_json_ld(html, url)


def parse_paradiso(html: str, url: str) -> ConcertInfo:
    info = ConcertInfo(url=url, venue="Paradiso")
    title = _meta_content(html, "og:title")
    if title:
        info.band = title.split("|")[0].strip()
    info.date, info.raw_date = parse_date(_meta_content(html, "og:description"))
    if info.date is None:
        info.date, info.raw_date = parse_date(_strip_tags(html))
    return info


def parse_afas(html: str, url: str) -> ConcertInfo:
    info = ConcertInfo(url=url, venue="AFAS Live")
    title = _meta_content(html, "og:title") or _title(html) or ""
    info.date, info.raw_date = parse_date(title)
    # "<band> op <date> in AFAS Live - AFAS Live"
    band = re.split(r"\s+(?:op|on)\s+\d", title, maxsplit=1)[0].strip()
    if band:
        info.band = band
    return info


def parse_melkweg(html: str, url: str) -> ConcertInfo:
    info = ConcertInfo(url=url, venue="Melkweg")
    match = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        return info
    try:
        data = json.loads(match[1])
    except (json.JSONDecodeError, ValueError):
        return info
    attributes = (
        data.get("props", {})
        .get("pageProps", {})
        .get("pageData", {})
        .get("attributes", {})
    )
    if not isinstance(attributes, dict):
        return info
    title = attributes.get("title")
    if isinstance(title, str) and title.strip():
        info.band = title.strip()
    starttime = attributes.get("starttime")
    if isinstance(starttime, str):
        info.date, info.raw_date = parse_date(starttime)
    return info


def parse_ziggo(html: str, url: str) -> ConcertInfo:
    info = ConcertInfo(url=url, venue="Ziggo Dome")
    title = _title(html) or ""
    # "Ziggo Dome - <band>"
    if " - " in title:
        info.band = title.split(" - ", 1)[1].strip()
    # The event blurb is delivered in an escaped React stream; decode it and
    # read the date from the lead paragraph next to the band name.
    decoded = html.encode("utf-8", "replace").decode("unicode_escape", "replace")
    haystack = decoded
    if info.band:
        index = decoded.find(info.band)
        if index != -1:
            haystack = decoded[index:]
    info.date, info.raw_date = parse_date(haystack)
    return info


def _json_ld_with_venue(venue: str) -> Callable[[str, str], ConcertInfo]:
    """JSON-LD parser that forces the venue name.

    Some sites have complete JSON-LD but report the hall as ``location.name``
    (e.g. "Grolsch Zaal", "Concertzaal") rather than the building.
    """

    def parser(html: str, url: str) -> ConcertInfo:
        info = parse_json_ld(html, url)
        info.venue = venue
        return info

    return parser


def _meta_parser(venue: str) -> Callable[[str, str], ConcertInfo]:
    """Parser for simple sites without Event JSON-LD.

    Reads the band from ``og:title`` (dropping a trailing venue name); the date
    is left to :func:`_fill_gaps`, which tries ``og:description`` then the slug.
    """
    bare = venue.removeprefix("De ").strip()
    suffix = re.compile(
        r"\s*[-|]\s*(?:de\s+)?" + re.escape(bare) + r"\s*$",
        re.IGNORECASE,
    )

    def parser(html: str, url: str) -> ConcertInfo:
        info = ConcertInfo(url=url, venue=venue)
        title = _meta_content(html, "og:title") or _title(html) or ""
        band = _strip_status_prefix(suffix.sub("", title))
        if band:
            info.band = band
        return info

    return parser


def parse_p60(html: str, url: str) -> ConcertInfo:
    info = ConcertInfo(url=url, venue="P60")
    title = _meta_content(html, "og:title") or _title(html) or ""
    band = re.sub(r"\s*-\s*Poppodium P60\s*$", "", title).strip()
    if band:
        info.band = band
    # Festival days share one description ("4 en 5 september"); the per-day
    # date card holds the right day, but without a year.
    card = re.search(r'datum-car.*?elementor-shortcode">([^<]+)<', html, re.DOTALL)
    year = re.search(r"\b(20\d{2})\b", _meta_content(html, "og:description") or "")
    if card and year:
        info.date, info.raw_date = parse_date(f"{card[1].strip()} {year[1]}")
    return info


def parse_patronaat(html: str, url: str) -> ConcertInfo:
    info = ConcertInfo(url=url, venue="Patronaat")
    match = re.search(
        r'class="[^"]*event__info-bar--star-date[^"]*"[^>]*>([^<]+)<',
        html,
    )
    if match:
        info.date, info.raw_date = parse_date(match[1])
    title = _meta_content(html, "og:title") or _title(html) or ""
    band = _strip_status_prefix(re.sub(r"\s*-\s*Patronaat\s*$", "", title))
    if band:
        info.band = band
    return info


PARSERS: dict[str, Callable[[str, str], ConcertInfo]] = {
    "tivolivredenburg.nl": parse_tivoli,
    "paradiso.nl": parse_paradiso,
    "ziggodome.nl": parse_ziggo,
    "melkweg.nl": parse_melkweg,
    "afaslive.nl": parse_afas,
    "bibelot.net": parse_json_ld,
    "dehelling.nl": parse_json_ld,
    "dedoelen.nl": parse_json_ld,
    "ticketmaster.nl": parse_json_ld,
    "paard.nl": _json_ld_with_venue("Paard"),
    "amare.nl": _json_ld_with_venue("Amare"),
    "013.nl": _json_ld_with_venue("013"),
    "musicon.nl": _json_ld_with_venue("Musicon"),
    "effenaar.nl": _json_ld_with_venue("Effenaar"),
    "patronaat.nl": parse_patronaat,
    "p60.nl": parse_p60,
    "nobel.nl": _meta_parser("Nobel"),
    "vorstin.nl": _meta_parser("De Vorstin"),
    "mainstage.nl": _meta_parser("Mainstage"),
}


def _parser_for(url: str) -> Callable[[str, str], ConcertInfo] | None:
    host = (urlsplit(url).hostname or "").removeprefix("www.")
    for domain, parser in PARSERS.items():
        if host == domain or host.endswith("." + domain):
            return parser
    return None


def parse(html: str, url: str) -> ConcertInfo:
    """Parse a concert page into a :class:`ConcertInfo`."""
    parser = _parser_for(url)
    info = parser(html, url) if parser else parse_json_ld(html, url)
    if not info.is_complete:
        _fill_gaps(info, html, url)
    return info


# --------------------------------------------------------------------------- #
# Fetching
# --------------------------------------------------------------------------- #
# Statuses that mean the event is gone (removed/past), not a transient error.
# 401 is included because some sites (e.g. Ticketmaster) return it for removed
# event pages, which are inaccessible even in a browser.
GONE_STATUS = {401, 404, 410}
HTTP_ERROR_STATUS = 400


def _redirected_to_ancestor(requested: str, final: str) -> bool:
    """Report whether a redirect landed on an ancestor path (e.g. a listing).

    Removed events typically redirect from /agenda/<slug> to /agenda/, while
    benign redirects (http->https, trailing slash, www) keep the same path.
    """
    requested_path = urlsplit(requested).path.rstrip("/")
    final_path = urlsplit(final).path.rstrip("/")
    return requested_path != final_path and requested_path.startswith(final_path + "/")


async def _fetch(session: aiohttp.ClientSession, url: str) -> str | None:
    """Return the page HTML, or None if the event is gone (expired)."""
    async with session.get(url, headers={"User-Agent": USER_AGENT}) as response:
        if response.status in GONE_STATUS:
            return None
        if response.status >= HTTP_ERROR_STATUS:
            msg = f"GET {url} returned HTTP {response.status}"
            raise ScrapeError(msg)
        if _redirected_to_ancestor(url, str(response.url)):
            return None
        return await response.text()


async def scrape(url: str, session: aiohttp.ClientSession | None = None) -> ConcertInfo:
    if session is None:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(
            timeout=timeout, max_field_size=MAX_HEADER_BYTES
        ) as owned_session:
            return await scrape(url, owned_session)
    html = await _fetch(session, url)
    if html is None:
        return ConcertInfo(url=url, expired=True)
    return parse(html, url)


async def scrape_many(urls: Iterable[str]) -> list[ConcertInfo | ScrapeError]:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(
        timeout=timeout, max_field_size=MAX_HEADER_BYTES
    ) as session:

        async def run(url: str) -> ConcertInfo | ScrapeError:
            try:
                return await scrape(url, session)
            except (ScrapeError, aiohttp.ClientError, TimeoutError) as exc:
                return exc if isinstance(exc, ScrapeError) else ScrapeError(str(exc))

        return await asyncio.gather(*(run(url) for url in urls))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _format(result: ConcertInfo | ScrapeError) -> str:
    if isinstance(result, ScrapeError):
        return f"ERROR: {result}"
    if result.expired:
        return f"EXPIRED  (event removed)\n    {result.url}"
    date_text = result.date.isoformat() if result.date else "?"
    return (
        f"{date_text}  {result.band or '?'}  @ {result.venue or '?'}\n    {result.url}"
    )


async def _main(urls: list[str]) -> int:
    if not urls:
        sys.stdout.write(
            "usage: python -m concerto.concert_scraper <url> [<url> ...]\n"
        )
        return 2
    results = await scrape_many(urls)
    for result in results:
        sys.stdout.write(_format(result) + "\n")
    return 0 if all(not isinstance(r, ScrapeError) for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main(sys.argv[1:])))
