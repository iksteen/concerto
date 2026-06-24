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

`.env` is loaded automatically. Required: `SLACK_BOT_TOKEN` (`xoxb-`, Web API) and `SLACK_APP_TOKEN` (`xapp-`, Socket Mode). Optional: `HOST`, `PORT`, `CONCERTO_DB_PATH` (default `./concerto.db`). See `README.md` for the full behavior spec and required Slack app scopes/event subscriptions.

## What this is

A Slack bot that tracks concert links posted in channels, storing each link with aggregate reaction counts in SQLite. Reactions on link messages classify status: `:+1:`/`:thumbsup:`/`:ticket:` = has a ticket, `:question:`/`:grey_question:`/`:eyes:` = interested, `:pray:` = looking on TicketSwap — but only the per-status counts are kept, never who reacted. The bot only scrapes and stores; it does not post or pin any Slack messages.

`src/concerto/concert_scraper.py` is a standalone module/CLI that extracts band, date, and venue from concert pages across many Dutch venues (`python -m concerto.concert_scraper <url> ...`). It has no Slack dependency.

## Architecture

The code splits into a **platform-agnostic core** (`src/concerto/board.py`) and **platform layers** — `src/concerto/slack_bot.py` and `src/concerto/discord_bot.py` — that both subclass `BoardService` and reuse the same core. The core knows nothing about either chat platform. Entry points are thin:
- `__init__.py` is empty — importing the package (or the scraper submodule) has no side effects.
- `__main__.py` picks the platform from `CONCERTO_PLATFORM` (`slack` default, or `discord`), lazily imports that module's `create_app` (so only the selected platform's module is loaded), and runs it under uvicorn (`HOST`/`PORT`). Both platforms' deps (`discord-py` included) are always installed — no optional extra.

**Core — `board.py`:**

1. **`BoardService`**: owns the in-memory state and all platform-neutral logic — the per-channel `ChannelBoard` cache (`self._boards`), the single `asyncio.Lock` serializing mutations (methods suffixed `_locked` assume it's held), metadata enrichment, and SSE subscribers. Platforms drive it through four neutral ingestion methods — `apply_message`, `apply_reactions`, `replace_board`, `merge_entries` — and override two hooks: `is_supported_channel` and `message_url`. `event_views` builds `EventView` snapshots for the web layer.
2. **`BoardRepository`**: SQLite persistence via `aiosqlite` (WAL mode), a single `links` table. `save_board` is a full delete-and-reinsert of a channel's rows in one transaction.
3. **Web routes** (`register_board_routes`): `/` (placeholder "Hello world"), `/healthz`, and `GET /board/{channel_id}` — a styled overview (`service.event_views` → `EventView` list → `render_overview`, which hides expired/past events and groups the rest into Date unknown / This week / This month / Upcoming) plus its `/events` SSE stream. Reads the active service from `request.state.service`.

**Discord layer — `discord_bot.py`:** `DiscordBotService(BoardService)` wraps a `discord.py` gateway client. `on_message` → `apply_message`; `on_raw_reaction_add/remove` fetch the message and `apply_reactions`; a `!concerto rebuild` text command scans `channel.history` and `replace_board`. `_reaction_name` maps Discord unicode emoji onto the core's neutral shortcodes (custom server emoji keep their own name); `_normalize_reactions` fetches reaction users only for tracked emoji. `message_url` builds a `discord.com/channels/...` link via the cached guild. There is **no** join-time backfill (unlike Slack) — the rebuild command covers it.

**Slack layer — `slack_bot.py`:** `SlackBotService(BoardService)` adds Socket Mode transport + Slack API orchestration. `run_socket_mode` calls `apps.connections.open` (app-level token) for a `wss://` URL, connects on a dedicated session (no total timeout, heartbeat pings), and reconnects on drop/`disconnect`. `_dispatch_socket_message` acks each envelope by `envelope_id` and dispatches `events_api`/`slash_commands`; heavy work runs in a background task via `_spawn` (keeps a reference so the task isn't GC'd). Slack calls go through `_api_call` (raises `SlackApiError` on `ok: false`, optional `token=` override); only read calls (`auth.test`, `conversations.history`, `apps.connections.open`) are used. `create_app` opens the db + `aiohttp` session in the FastAPI lifespan, builds the service, exposes it via `request.state.service`, registers the core routes, and runs `run_socket_mode()` as a background task (cancelled on shutdown).

> **No data migrations.** Schema migrations are fine (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN`, `DROP TABLE IF EXISTS` in `init`), but never write code that reshapes existing *rows* — we end up dragging it along forever. The board is fully reconstructable from Slack history, so to fix data just run a rebuild (`/concerto rebuild`). New columns get a sane default (e.g. counts default `0`) and are repopulated on the next rebuild.

### Domain model
- `LinkEntry`: per-URL aggregate reaction counts — `going` (has a ticket), `undecided` (interested), `looking` (TicketSwap) — `source_message_ts`, scraped metadata (`band`, `event_date`, `venue`), and `expired` (page gone/redirected to a listing → event is in the past). We store **only counts**, never who posted or reacted (privacy).
- `ChannelBoard`: `dict[url, LinkEntry]`.

### Status rules (`aggregate_status_counts`, in `board.py`)
Reactions are re-parsed from the whole message into counts; each user counted once with ticket-holder winning: `:+1:` outranks `:question:`/`:eyes:`, which outranks `:pray:`. The reaction sets are platform-neutral shortcodes (`PLUS_ONE_REACTIONS` etc.); a platform whose emoji use other names must translate to these. Input is the neutral shape `[{"name": str, "users": [str, ...]}, ...]`. Keep this precedence intact when changing reaction handling.

### Metadata enrichment (`BoardService._enrich_links`)
After links are persisted, `_enrich_links` runs the concert scraper (`concert_scraper.scrape`, reusing the bot's `aiohttp` session) **outside the lock** — never scrape while holding `self._lock`. It scrapes a URL at most once per process (`self._metadata_tried`) and skips links already resolved (`is_resolved` = has metadata or is expired); results are merged back and persisted under the lock. A scrape returns `expired=True` when the page is gone (404/410/401) or redirects to an ancestor path (event removed); scrape failures are logged and ignored — enrichment must never break link tracking.

### Data flows (Slack events → neutral ingestion)
- **Message with links** → `apply_message`: set earliest `source_message_ts`, persist, then enrich.
- **Reaction add/remove** → fetch the reacted message, then `apply_reactions`: re-extract links, re-parse *all* its reactions into counts (not the single delta), persist, then enrich.
- **Bot joins channel** (`member_joined_channel` for the bot's own user) → scan full history into entries, then `merge_entries`, then enrich.
- **`/concerto rebuild`** (also accepts `rescan`) → scan full history into entries, then `replace_board`, then enrich.

Slack history scans (`_collect_history_link_entries`) paginate `conversations.history` and fold each message into the entries dict via `board.fold_message`; Slack's `reactions[]` already matches the neutral shape.

## Conventions
- Only public/private channels are supported — guard channel-scoped work with `_is_supported_channel` (ids starting `C`/`G`).
- Slack timestamps are strings; compare with `_ts_key`, not lexically, and use `_set_earliest_source_message_ts` to track the earliest posting.
