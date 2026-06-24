---
name: add-venue
description: Add a new venue parser to concert_scraper.py. Use when the user wants to support scraping a new concert venue/website, or when a venue's band/date/venue comes back wrong or empty. Walks the parser ladder (try generic JSON-LD first, add code only when it fails) and registers the domain in PARSERS.
---

# Add a venue to the concert scraper

All parsing lives in `src/concerto/concert_scraper.py`. A venue = one entry in
the `PARSERS` dict keyed by domain. **Most venues need no new code** — the
generic JSON-LD / OpenGraph fallback already handles them. Climb this ladder
and stop at the first rung that produces a correct `band`, `date`, and `venue`.

## Step 1 — fetch a real event URL and see what the fallback gives

Get a concrete event page URL from the user (not the venue's listing/agenda
page). Then just run the scraper — it already falls back to JSON-LD + OpenGraph
for unknown domains:

```bash
uv run python -m concerto.concert_scraper "<event-url>"
```

Output line is `DATE  BAND  @ VENUE`. Judge each field:
- All three correct → **add nothing.** The fallback works; if the user only
  wanted it to work, you're done. Only register a parser to force the venue
  name (Step 3) or if a parser is otherwise needed.
- `VENUE` is a hall name ("Grolsch Zaal") or `?` but band/date are right →
  Step 3 (`_json_ld_with_venue`).
- `BAND`/`DATE` wrong or `?` → inspect the HTML (Step 2), then Step 4/5.

## Step 2 — inspect the page to pick a strategy

```bash
curl -sL -A "Mozilla/5.0 concerto-scraper" "<event-url>" -o /tmp/venue.html
grep -o 'application/ld+json' /tmp/venue.html | head        # has JSON-LD?
grep -oiE '<meta[^>]*og:(title|description)[^>]*>' /tmp/venue.html | head
grep -o '__NEXT_DATA__\|__NUXT__' /tmp/venue.html | head     # JS-framework blob?
```

What you find maps to a rung:
- **Event JSON-LD present** → Step 3.
- **No JSON-LD but good `og:title`/`og:description`** → Step 4 (`_meta_parser`).
- **Data only in a `__NEXT_DATA__` / React stream / odd HTML class** → Step 5
  (custom parser, like `parse_melkweg`, `parse_ziggo`, `parse_patronaat`).

## Step 3 — JSON-LD venues (most common)

```python
"newvenue.nl": parse_json_ld,                     # JSON-LD venue name is correct
"newvenue.nl": _json_ld_with_venue("New Venue"),  # force the building name
```

Use `_json_ld_with_venue` whenever `location.name` is a sub-hall rather than the
venue people know.

## Step 4 — simple OG sites (no JSON-LD)

`_meta_parser("New Venue")` reads the band from `og:title`, strips a trailing
" - New Venue" / " | New Venue" suffix, and leaves the date to `_fill_gaps`
(which tries `og:description`, then the URL slug, then a `<time>` tag). It
handles a leading "De " in the venue name automatically.

```python
"newvenue.nl": _meta_parser("New Venue"),
```

If the date still comes back empty, the slug or `og:description` format may be
unusual — check whether `date_from_slug` / `parse_date` cover it before writing
a custom parser. Add a month spelling to `_MONTHS` if a date is in an
unrecognized language/abbreviation.

## Step 5 — custom parser (last resort)

Only when the data isn't in JSON-LD or clean OG tags. Write `parse_<venue>(html, url)
-> ConcertInfo`, set `venue=` directly, and reuse the helpers — don't reinvent
them:
- `_meta_content(html, "og:title")`, `_title(html)` — read tags tolerant of attribute order
- `parse_date(text)` → `(date, raw_date)`; assign both to `info.date, info.raw_date`
- `_strip_status_prefix(name)` — drop "UITVERKOCHT |", "Verplaatst naar ...:" badges
- `_strip_tags(html)` — last-ditch date search over visible text
- For `__NEXT_DATA__`/React blobs, copy the JSON-extraction pattern from
  `parse_melkweg` / `parse_ziggo`.

Keep it minimal: set what you can, let `parse()` call `_fill_gaps` for the rest.

## Step 6 — register, test, lint

1. Add the domain (bare, no `www.`) to `PARSERS`. Subdomains match automatically.
2. Re-run the CLI on the event URL **and** a second event from the same venue —
   one event can pass by luck. Confirm band/date/venue.
3. Also confirm a removed event still reports `EXPIRED` (the scraper detects
   404/410/401 and redirect-to-listing on its own — don't special-case it).
4. Lint + type-check (no test suite in this repo):

```bash
uv run pre-commit run -a    # mypy --strict + ruff, must pass
```

## Lessons baked in
- Prefer no parser. The fallback covers many Dutch venues already.
- Parsers return only what they're sure of; `parse()` fills the gaps. Don't
  duplicate slug/OG/time-tag logic inside a parser.
- Title fields carry status badges and venue suffixes — strip them.
- Never raise for a missing field; raising is only for fetch failures
  (`ScrapeError`). A half-filled `ConcertInfo` is fine.
