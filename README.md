# concerto

Bot that tracks concert links in chat channels and stores them with aggregate
ticket/interest counts. A single process hosts **any number of connectors at
once** — multiple Slack workspaces and/or Discord bots — configured in one TOML
file. All connectors share the same storage and web overview, but each
connector's channels are namespaced so they never collide.

## Behavior
- Monitors any channel a connector is in (Slack: public/private; Discord: channels you `!concerto track`)
- Extracts links from channel messages and adds them to a tracked list
- Scrapes each tracked link for concert metadata (band, date, venue) and stores it alongside the link
- Marks a link as expired when its page is gone (404/410/401) or redirects to a listing page (the event has been removed and is in the past)
- Serves a web overview of a channel's upcoming events at `GET /board/{connector}/{channel_id}` (`connector` is the name from the config), grouped into Date unknown (top), This week, This month, and Upcoming, ordered by date, with expired/past events hidden; each event shows emoji counts of how many have a ticket (🎫), are interested (👀), or are looking for a ticket (🙏)
- `:+1:` (or `:thumbsup:` / `:ticket:`): user has a ticket
- `:question:` (or `:grey_question:` / `:eyes:`): user is interested, no ticket yet
- `:pray:`: user is trying to get a sold-out ticket via TicketSwap
- Slack: rescan a channel's history with the `/concerto rebuild` slash command, and the bot backfills automatically when invited to a channel
- Discord: tracking is opt-in — `!concerto track` a channel (backfills in the background), `!concerto untrack` to stop, `!concerto rebuild` to rescan
- Tracked data is stored in SQLite only; the bot never posts messages (on Discord it acknowledges `track`/`untrack`/`rebuild` commands by reacting to them)

Slack connectors connect over **Socket Mode** (an outbound WebSocket), so no
public callback URL is required. An HTTP server runs alongside all connectors
(serving a placeholder index, `/healthz`, and the board overview).

## Configuration

Connectors are defined in a TOML file. Copy the example and fill in tokens:

```bash
cp concerto.toml.example concerto.toml
# edit concerto.toml
```

```toml
[[connector]]
type = "slack"
name = "work"            # used in the board URL (/board/work/<channel>) + storage namespace
bot_token = "xoxb-..."   # Web API
app_token = "xapp-..."   # Socket Mode (connections:write)

[[connector]]
type = "slack"
name = "club"
bot_token = "xoxb-..."
app_token = "xapp-..."

[[connector]]
type = "discord"
name = "main"
token = "..."            # bot token from the developer portal
```

- Each connector needs a unique `name` (no `/`). It appears in the board URL and
  namespaces that connector's channels in storage, so two Slack workspaces with
  the same channel id never collide.
- Optional per-connector `command` overrides the default (`/concerto` for Slack,
  `!concerto` for Discord).
- Optional `[[combined]]` boards merge several channels (across connectors) into
  one page at `/combined/<name>`. Each entry needs a unique `name` (no `/`) and a
  `sources` list of `"connector/channel"` strings. The same event tracked in
  multiple channels is shown once, with interest counts summed.
- An optional `[server]` table sets `db_path`, `host`, `port`, `log_level`.

The process finds its config at `CONCERTO_CONFIG` (default `./concerto.toml`).
These environment variables are used as fallback when the matching `[server]`
key is omitted: `CONCERTO_DB_PATH` (default `./concerto.db`), `HOST` (default
`127.0.0.1`), `PORT` (default `8000`), `LOG_LEVEL` (default `INFO`; `DEBUG` logs
incoming events and scrape results). `.env` is still loaded at startup.

## Persistence
- State is persisted in SQLite: links with scraped band/date/venue and aggregate
  reaction counts only — the bot never stores who posted or reacted.
- Rows are keyed by `connector/channel_id`, so all connectors share one database
  file safely.
- Default database path is `./concerto.db` (override via `[server].db_path` or
  `CONCERTO_DB_PATH`).

## Run
```bash
uv run python -m concerto
```

## Docker
```bash
cp concerto.toml.example concerto.toml   # edit it
docker compose up -d --build
```
- `concerto.toml` is mounted read-only at `/config/concerto.toml` and the
  container sets `CONCERTO_CONFIG` to point at it.
- The SQLite database is stored in the `concerto-data` named volume (mounted at
  `/data`), so it persists across restarts and image updates.
- `HOST`/`PORT`/`CONCERTO_DB_PATH` are set for the container in `docker-compose.yml`.
- The base file does **not** publish a host port (so it won't clash with a
  production reverse proxy / host networking); the server still listens on
  `8000` inside the container. To reach the board overview from your host, add
  the dev override, which maps host `8000` → container `8000`:
  ```bash
  docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
  ```

## Slack app setup

Do this once per Slack workspace connector:
- Enable **Socket Mode**
- Create an **app-level token** with the `connections:write` scope (this is the connector's `app_token`)
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
- Install the app to your workspace and invite it to channels you want to track.
  The `bot_token` is the workspace bot token (`xoxb-...`).

## Discord app setup
- Create an application + bot in the Discord developer portal; copy the bot
  token into the connector's `token`
- Under **Privileged Gateway Intents**, enable **Message Content Intent** (the
  bot needs message text to find links)
- Invite the bot with the `bot` scope and these permissions: **View Channels**,
  **Read Message History** (to read links/reactions and rescan history), and
  **Add Reactions** (the bot acknowledges commands by reacting to them — it
  never posts messages)
- **Tracking is opt-in per channel.** The bot ignores every channel until you
  send `!concerto track` in it; `!concerto untrack` stops tracking. Tracked
  channels are stored in SQLite and survive restarts.
- The `track`/`untrack`/`rebuild` commands are **privileged**: only members with
  **Manage Channels** on that channel may run them (administrators and the guild
  owner have it implicitly). Everyone else gets a 🚫 reaction and the command is
  ignored.
- `track` automatically backfills links posted before the bot joined (a
  background rebuild); `untrack` clears that channel's stored links.
- Commands are acknowledged with a reaction on your command message: ✅ done,
  ❌ refused (e.g. `rebuild` in an untracked channel); `track`/`rebuild` show ⏳
  while scanning, swapped for ✅ when finished.
- `!concerto rebuild` (or `rescan`) re-scans a tracked channel's history on demand.
- Customise the command prefix with the connector's `command` (default
  `!concerto`).
