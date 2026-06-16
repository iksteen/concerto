# concerto

Basic Slack bot that tracks concert links in any public or private channel where the bot is a member, and stores them with per-user ticket status.

## Behavior
- Monitors any channel the bot is in (public and private)
- Extracts links from channel messages and adds them to a tracked list
- Scrapes each tracked link for concert metadata (band, date, venue) and stores it alongside the link
- On `member_joined_channel`, scans channel history for existing links and `:+1:` / `:question:` / `:pray:` reactions
- `:+1:` (or `:thumbsup:` / `:ticket:`): user has a ticket
- `:question:` (or `:grey_question:`): user is interested, no ticket yet
- `:pray:`: user is trying to get a sold-out ticket via TicketSwap
- Run `/concerto rebuild` in a channel to fully rescan that channel's history
- Tracked data is stored in SQLite only; the bot does not post or pin any messages

The bot connects to Slack over **Socket Mode** (an outbound WebSocket), so no
public callback URL is required. An HTTP server still runs alongside it (serving
a placeholder index and `/healthz`).

## Required environment variables
- `SLACK_BOT_TOKEN` (`xoxb-...`) — Web API calls
- `SLACK_APP_TOKEN` (`xapp-...`) — Socket Mode connection (scope `connections:write`)

Optional:
- `HOST` (default `127.0.0.1`)
- `PORT` (default `8000`)
- `CONCERTO_DB_PATH` (default `./concerto.db`)

## Environment file
- `.env` is loaded automatically at startup via `python-dotenv`.
- Example:
```env
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
CONCERTO_DB_PATH=./concerto.db
HOST=127.0.0.1
PORT=8000
```

## Persistence
- State is persisted in SQLite (links with scraped band/date/venue, ticket holders, and interested users).
- Default database path is `./concerto.db` and can be overridden via `CONCERTO_DB_PATH`.

## Run
```bash
uv run python -m concerto
```

## Slack app setup
- Enable **Socket Mode**
- Create an **app-level token** with the `connections:write` scope (this is `SLACK_APP_TOKEN`)
- Enable **Event Subscriptions** (no Request URL needed in Socket Mode) and subscribe to bot events:
  - `message.channels`
  - `message.groups`
  - `member_joined_channel`
  - `reaction_added`
  - `reaction_removed`
- Add a **Slash Command** `/concerto` (no Request URL needed in Socket Mode), usage hint `rebuild`
- Add OAuth scopes:
  - `channels:history`
  - `groups:history`
  - `reactions:read`
- Install the app to your workspace and invite it to channels you want to track
