# Agent Instructions

## Project Defaults
- Language: Python
- Packaging and environment management: `uv`
- Concurrency model: prefer `asyncio` over threading
- Preferred HTTP framework: `fastapi`
- Preferred HTTP client: `aiohttp`

## Quality Checks
- Run linting and type checking with:
  - `uv run pre-commit run -a`
- Or individually: `uv run mypy src` (strict), `uv run ruff check src`, `uv run ruff format src`.
- `select = ["ALL"]` in ruff with a curated ignore list (see `pyproject.toml`); mypy runs `--strict`. New code must pass both.
- There is no test suite. (Note: `.pre-commit-config.yaml` says `poetry run mypy`, but this project uses `uv`.)

## Running

```bash
uv run python -m concerto   # FastAPI + uvicorn, defaults to 127.0.0.1:8000
```

`.env` is loaded automatically. Required: `SLACK_BOT_TOKEN`, `SLACK_SIGNING_SECRET`. Optional: `HOST`, `PORT`, `CONCERTO_DB_PATH` (default `./concerto.db`). See `README.md` for the full behavior spec and required Slack app scopes/event subscriptions.

## What this is

A Slack bot that tracks concert links posted in channels, storing each link with per-user ticket status in SQLite. Reactions on link messages classify each user's status for that link: `:+1:`/`:thumbsup:` = has a ticket, `:question:`/`:grey_question:` = interested, `:pray:` = looking on TicketSwap. The bot only scrapes and stores; it does not post or pin any Slack messages.

`src/concerto/concert_scraper.py` is a standalone module/CLI that extracts band, date, and venue from concert pages across many Dutch venues (`python -m concerto.concert_scraper <url> ...`). It has no Slack dependency.

## Architecture

The Slack bot lives in `src/concerto/slack_bot.py`; the other files are thin entry points:
- `__init__.py` is empty — importing the package (or the scraper submodule) has no side effects.
- `__main__.py` calls `load_dotenv()` + `create_app()` inside `main()` and runs the app under uvicorn (`HOST`/`PORT`), so the Slack env is only needed to run the server.

Three layers inside `slack_bot.py`:

1. **FastAPI routes** (`create_app`): `/slack/events`, `/slack/commands`, `/healthz`. Every Slack request is HMAC-verified against `SLACK_SIGNING_SECRET` (`_is_valid_signature`, 5-minute timestamp window) before processing. Slack needs a fast reply, so event/command work is dispatched to a background task via `_spawn` (which keeps a reference so the task is not GC'd) and the route returns `{"ok": True}` immediately.

2. **`SlackBotService`**: in-memory state + Slack API orchestration. Caches a `ChannelBoard` per channel in `self._boards` and serializes all mutations behind one `asyncio.Lock` — methods suffixed `_locked` assume the lock is held. Slack calls go through `_api_call` (raises `SlackApiError` on `ok: false`); only read calls (`auth.test`, `conversations.history`) are used.

3. **`BoardRepository`**: SQLite persistence via `aiosqlite` (WAL mode), spread across three tables (`links`, `link_posters`, `link_statuses`). `save_board` is a full delete-and-reinsert of a channel's rows in one transaction.

### Domain model
- `LinkEntry`: per-URL membership sets — `posters`, `ticket_holders`, `interested`, `ticketswap_wanted` — plus `source_message_ts` and scraped metadata (`band`, `event_date`, `venue`).
- `ChannelBoard`: `dict[url, LinkEntry]`.

### Status rules (`_apply_status_reaction`)
Statuses are mutually exclusive and ticket-holder wins: adding `:+1:` clears `interested`/`ticketswap_wanted`; `:question:`/`:pray:` are ignored for users who already hold a ticket. Keep this precedence intact when changing reaction handling.

### Metadata enrichment (`_enrich_links`)
After links are persisted, `_enrich_links` runs the concert scraper (`concert_scraper.scrape`, reusing the bot's `aiohttp` session) **outside the lock** — never scrape while holding `self._lock`. It scrapes a URL at most once per process (`self._metadata_tried`) and skips links that already have metadata; results are merged back and persisted under the lock. Scrape failures are logged and ignored — enrichment must never break link tracking.

### Data flows
- **Message with links** → add poster, set earliest `source_message_ts`, persist, then enrich.
- **Reaction add/remove** → fetch reacted message text, re-extract links, apply status, persist, then enrich.
- **Bot joins channel** (`member_joined_channel` for the bot's own user) → scan full history, *merge* into the board, then enrich.
- **`/concerto rebuild`** (also accepts `rescan`) → scan full history, *replace* the board, then enrich.

History scans (`_collect_history_link_entries`) paginate `conversations.history` and read `reactions[].users` directly.

## Conventions
- Only public/private channels are supported — guard channel-scoped work with `_is_supported_channel` (ids starting `C`/`G`).
- Slack timestamps are strings; compare with `_ts_key`, not lexically, and use `_set_earliest_source_message_ts` to track the earliest posting.
