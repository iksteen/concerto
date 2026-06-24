# concerto

Bot that tracks concert links in channels and stores them with aggregate
ticket/interest counts. Runs against **Slack** or **Discord** (select with
`CONCERTO_PLATFORM`); both share the same storage and web overview.

## Behavior
- Monitors any channel the bot is in (public and private)
- Extracts links from channel messages and adds them to a tracked list
- Scrapes each tracked link for concert metadata (band, date, venue) and stores it alongside the link
- Marks a link as expired when its page is gone (404/410/401) or redirects to a listing page (the event has been removed and is in the past)
- Serves a web overview of a channel's upcoming events at `GET /board/{channel_id}`, grouped into Date unknown (top), This week, This month, and Upcoming, ordered by date, with expired/past events hidden; each event shows emoji counts of how many have a ticket (🎫), are interested (👀), or are looking for a ticket (🙏)
- On `member_joined_channel`, scans channel history for existing links and `:+1:` / `:question:` / `:pray:` reactions
- `:+1:` (or `:thumbsup:` / `:ticket:`): user has a ticket
- `:question:` (or `:grey_question:` / `:eyes:`): user is interested, no ticket yet
- `:pray:`: user is trying to get a sold-out ticket via TicketSwap
- Run `/concerto rebuild` in a channel to fully rescan that channel's history
- Tracked data is stored in SQLite only; the bot does not post or pin any messages

The bot connects to Slack over **Socket Mode** (an outbound WebSocket), so no
public callback URL is required. An HTTP server still runs alongside it (serving
a placeholder index and `/healthz`).

## Platform selection
- `CONCERTO_PLATFORM` (default `slack`) — `slack` or `discord`. Both platforms'
  dependencies are always installed; no extra is needed.

## Required environment variables

Slack (`CONCERTO_PLATFORM=slack`):
- `SLACK_BOT_TOKEN` (`xoxb-...`) — Web API calls
- `SLACK_APP_TOKEN` (`xapp-...`) — Socket Mode connection (scope `connections:write`)

Discord (`CONCERTO_PLATFORM=discord`):
- `DISCORD_BOT_TOKEN` — bot token from the Discord developer portal

Optional:
- `HOST` (default `127.0.0.1`)
- `PORT` (default `8000`)
- `CONCERTO_DB_PATH` (default `./concerto.db`)
- `CONCERTO_SLASH_COMMAND` (default `/concerto`, Slack) — must match the slash
  command registered in the Slack app; set e.g. `/concerto-dev` for a separate
  dev app (a leading `/` is added if omitted)
- `CONCERTO_DISCORD_COMMAND` (default `!concerto`, Discord) — the text-command
  prefix; send `!concerto rebuild` in a channel to rescan its history
- `LOG_LEVEL` (default `INFO`; set `DEBUG` to log incoming events and scrape results)

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
- State is persisted in SQLite: links with scraped band/date/venue and aggregate
  reaction counts only — the bot never stores who posted or reacted.
- Default database path is `./concerto.db` and can be overridden via `CONCERTO_DB_PATH`.

## Run
```bash
uv run python -m concerto
```

## Docker
```bash
docker compose up -d --build
```
- Reads `SLACK_BOT_TOKEN` and `SLACK_APP_TOKEN` from `.env` (required).
- The SQLite database is stored in the `concerto-data` named volume (mounted at `/data`), so it persists across restarts and image updates.
- The HTTP server is published on port `8000`; `HOST`/`PORT`/`CONCERTO_DB_PATH` are set for the container in `docker-compose.yml`.

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
- You don't need to add OAuth scopes by hand: subscribing to the events above
  and adding the slash command makes Slack add every bot token scope they
  require. For reference, the resulting set is:
  - `channels:history`, `groups:history` — from `message.channels` / `message.groups`
  - `channels:read`, `groups:read` — from `member_joined_channel`, used to scan a
    channel's existing links and reaction emoji when the bot is invited
  - `reactions:read` — from `reaction_added` / `reaction_removed`
  - `commands` — from the `/concerto` slash command
- Install the app to your workspace and invite it to channels you want to track

## Discord app setup
- Create an application + bot in the Discord developer portal; copy the bot
  token into `DISCORD_BOT_TOKEN`
- Under **Privileged Gateway Intents**, enable **Message Content Intent** (the
  bot needs message text to find links)
- Invite the bot with the `bot` scope and the `Read Message History` +
  `Add Reactions` / `Read Messages` permissions
- There is no slash command; send `!concerto rebuild` (or your
  `CONCERTO_DISCORD_COMMAND`) in a channel to rescan its history. Unlike Slack
  there is no automatic backfill when the bot joins — run the command once.
