"""Ekko omits the year on same-year events and prints a 2-digit year otherwise."""

from __future__ import annotations

import datetime as dt

from concerto.concert_scraper import parse_ekko


def _page(title: str, datum: str) -> str:
    return f"""
<meta property="og:title" content="{title}" />
<div>Datum</div>
                <div class="text-normal-mobile sm:text-normal">
                    {datum}
                </div>
"""


def test_yearless_card_is_current_year() -> None:
    info = parse_ekko(_page("EINDBAAS &#8212; EKKO", "za 5 sep"), "https://ekko.nl/e/")
    assert info.band == "EINDBAAS"
    assert info.venue == "EKKO"
    assert info.date == dt.date(dt.datetime.now(tz=dt.UTC).year, 9, 5)


def test_two_digit_year_is_that_year() -> None:
    info = parse_ekko(_page("Gilla Band &#8212; EKKO", "ma 25 jan 27"), "https://x/")
    assert info.band == "Gilla Band"
    assert info.date == dt.date(2027, 1, 25)
