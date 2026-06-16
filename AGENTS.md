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

A Slack bot that tracks concert links posted in channels and maintains one pinned summary message ("link board") per channel. Reactions on link messages classify each user's status for that link: `:+1:`/`:thumbsup:` = has a ticket, `:question:`/`:grey_question:` = interested, `:pray:` = looking on TicketSwap.

## Architecture

Everything lives in `src/concerto/slack_bot.py` (~900 lines); the other files are thin entry points:
- `__init__.py` loads `.env` and calls `create_app()`, exposing the FastAPI `app`.
- `__main__.py` runs that app under uvicorn (`HOST`/`PORT`).

Three layers inside `slack_bot.py`:

1. **FastAPI routes** (`create_app`): `/slack/events`, `/slack/commands`, `/healthz`. Every Slack request is HMAC-verified against `SLACK_SIGNING_SECRET` (`_is_valid_signature`, 5-minute timestamp window) before processing. Slack needs a fast reply, so event/command work is dispatched to a background `asyncio.create_task` and the route returns `{"ok": True}` immediately.

2. **`SlackBotService`**: in-memory state + Slack API orchestration. Caches a `ChannelBoard` per channel in `self._boards` and serializes all mutations behind one `asyncio.Lock` — methods suffixed `_locked` assume the lock is held. Slack calls go through `_api_call` (raises `SlackApiError` on `ok: false`). `_render_summary_message` produces the pin text, which always begins with the `SUMMARY_MARKER` constant so an existing pin can be rediscovered via `pins.list`.

3. **`BoardRepository`**: SQLite persistence via `aiosqlite` (WAL mode), spread across four tables (`board_state`, `links`, `link_posters`, `link_statuses`). `save_board` is a full delete-and-reinsert of a channel's rows in one transaction.

### Domain model
- `LinkEntry`: per-URL membership sets — `posters`, `ticket_holders`, `interested`, `ticketswap_wanted` — plus `source_message_ts`.
- `ChannelBoard`: `pin_ts` (pinned summary message ts) + `dict[url, LinkEntry]`.

### Status rules (`_apply_status_reaction`)
Statuses are mutually exclusive and ticket-holder wins: adding `:+1:` clears `interested`/`ticketswap_wanted`; `:question:`/`:pray:` are ignored for users who already hold a ticket. Keep this precedence intact when changing reaction handling.

### Data flows
- **Message with links** → add poster, set earliest `source_message_ts`, re-render pin.
- **Reaction add/remove** → fetch reacted message text, re-extract links, apply status, re-render.
- **Bot joins channel** (`member_joined_channel` for the bot's own user) → scan full history and *merge* into the board.
- **`/concerto rebuild`** (also accepts `rescan`) → scan full history and *replace* the board.

History scans (`_collect_history_link_entries`) paginate `conversations.history` and read `reactions[].users` directly.

## Conventions
- Only public/private channels are supported — guard channel-scoped work with `_is_supported_channel` (ids starting `C`/`G`).
- Slack timestamps are strings; compare with `_ts_key`, not lexically, and use `_set_earliest_source_message_ts` to track the earliest posting.
