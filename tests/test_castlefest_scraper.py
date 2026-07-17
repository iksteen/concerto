"""Castlefest prints one trailing year for a multi-day range ("30 juli - 2 augustus 2026")."""

from __future__ import annotations

import datetime as dt

from concerto.concert_scraper import parse_castlefest

_HTML = """
<meta property="og:title" content="Castlefest | Where fantasy becomes your reality">
<div>Sluiten 30 juli - 2 augustus 2026 Castlefest</div>
"""


def test_shared_year_range() -> None:
    info = parse_castlefest(_HTML, "https://castlefest.nl/nl")
    assert info.band == "Castlefest"
    assert info.venue == "Keukenhof, Lisse"
    assert info.date == dt.date(2026, 7, 30)
    assert info.end_date == dt.date(2026, 8, 2)
