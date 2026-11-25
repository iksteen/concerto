"""Down The Rabbit Hole leads with the year ("2027: 2 t/m 4 juli, ...")."""

from __future__ import annotations

import datetime as dt

from concerto.concert_scraper import parse, parse_down_the_rabbit_hole

_HOME = """
<meta property="og:title" content="Down The Rabbit Hole" />
<meta property="og:description"
 content="Down The Rabbit Hole 2027: 2 t/m 4 juli, Groene Heuvels, Beuningen">
"""

_TICKETS = """
<meta property="og:title" content="Tickets - Down The Rabbit Hole" />
<meta property="og:description" content="Kaartverkoop Down The Rabbit Hole 2027">
"""


def test_leading_year_range() -> None:
    info = parse_down_the_rabbit_hole(_HOME, "https://downtherabbithole.nl/")
    assert info.band == "Down The Rabbit Hole"
    assert info.venue == "Groene Heuvels, Beuningen"
    assert info.date == dt.date(2027, 7, 2)
    assert info.end_date == dt.date(2027, 7, 4)


def test_subpage_without_dates_is_not_an_event() -> None:
    # The tickets page text mentions "4 januari 2027" (ticket transfers); that
    # must not become the festival date.
    info = parse(
        _TICKETS + "<p>vanaf maandag 4 januari 2027 via Ticket Transfer</p>",
        "https://downtherabbithole.nl/tickets",
    )
    assert info.band is None
    assert info.date is None
    assert info.venue is None
