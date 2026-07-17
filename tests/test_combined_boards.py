"""Combined boards: merge duplicate URLs across channels + config validation."""

from __future__ import annotations

import datetime as dt
from typing import Any

from concerto.__main__ import _validate_combined
from concerto.board import EventView, Origin, merge_event_views


def _origin(label: str, url: str | None = None, **counts: int) -> Origin:
    base = {"going": 0, "undecided": 0, "looking": 0}
    base.update(counts)
    return Origin(label=label, message_url=url, **base)


def _view(url: str, origins: list[Origin] | None = None, **kw: Any) -> EventView:
    base: dict[str, Any] = {
        "url": url,
        "band": None,
        "venue": None,
        "expired": False,
        "date": None,
        "end_date": None,
        "origins": list(origins or []),
    }
    base.update(kw)
    return EventView(**base)


def test_duplicate_url_keeps_per_origin_rows_and_coalesces_metadata() -> None:
    merged = merge_event_views(
        [
            _view(
                "https://x/1",
                band="A",
                date=dt.date(2026, 8, 1),
                origins=[_origin("work", "https://slack/1", going=2)],
            ),
            _view(
                "https://x/1",
                venue="V",
                origins=[_origin("main", "https://discord/1", going=3, undecided=1)],
            ),
            _view("https://x/2", origins=[_origin("work", looking=1)]),
        ]
    )
    by_url = {v.url: v for v in merged}
    assert len(merged) == 2
    one = by_url["https://x/1"]
    assert one.band == "A"  # first non-None wins
    assert one.venue == "V"  # filled from the second source
    assert one.date == dt.date(2026, 8, 1)
    # Each channel keeps its own row (label, link, counts) — not summed away.
    assert [(o.label, o.message_url, o.going) for o in one.origins] == [
        ("work", "https://slack/1", 2),
        ("main", "https://discord/1", 3),
    ]


def test_expired_only_when_all_sources_agree() -> None:
    merged = merge_event_views(
        [_view("https://x/1", expired=True), _view("https://x/1", expired=False)]
    )
    assert merged[0].expired is False


def test_validate_combined_accepts_good_config() -> None:
    cfg = {"combined": [{"name": "all", "sources": ["work/C1", "main/42"]}]}
    result = _validate_combined(cfg, {"work", "main"})
    assert result == {"all": [("work", "C1"), ("main", "42")]}


def test_validate_combined_rejects_bad_configs() -> None:
    names = {"work"}
    bad: list[dict[str, Any]] = [
        {"combined": [{"name": "x", "sources": ["nope/C1"]}]},  # unknown connector
        {"combined": [{"name": "a/b", "sources": ["work/C1"]}]},  # slash in name
        {"combined": [{"name": "x", "sources": []}]},  # empty sources
        {"combined": [{"name": "x", "sources": ["work"]}]},  # missing channel
        {"combined": [{"name": "x", "sources": ["work/"]}]},  # empty channel
        {
            "combined": [
                {"name": "dup", "sources": ["work/C1"]},
                {"name": "dup", "sources": ["work/C2"]},
            ]
        },  # duplicate name
    ]
    for cfg in bad:
        try:
            _validate_combined(cfg, names)
        except RuntimeError:
            continue
        msg = f"expected RuntimeError for {cfg!r}"
        raise AssertionError(msg)


def test_validate_combined_absent_is_empty() -> None:
    assert _validate_combined({}, {"work"}) == {}
