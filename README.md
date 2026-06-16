# concerto

Basic Slack bot that tracks concert links in any public or private channel where the bot is a member, and stores them with per-user ticket status.

## Behavior
- Monitors any channel the bot is in (public and private)
- Extracts links from channel messages and adds them to a tracked list
- On `member_joined_channel`, scans channel history for existing links and `:+1:` / `:question:` / `:pray:` reactions
- `:+1:` (or `:thumbsup:`): user has a ticket
- `:question:` (or `:grey_question:`): user is interested, no ticket yet
- `:pray:`: user is trying to get a sold-out ticket via TicketSwap
- Run `/concerto rebuild` in a channel to fully rescan that channel's history
- Tracked data is stored in SQLite only; the bot does not post or pin any messages

## Required environment variables
- `SLACK_BOT_TOKEN`
- `SLACK_SIGNING_SECRET`

Optional:
- `HOST` (default `127.0.0.1`)
- `PORT` (default `8000`)
- `CONCERTO_DB_PATH` (default `./concerto.db`)

## Environment file
- `.env` is loaded automatically at startup via `python-dotenv`.
- Example:
```env
SLACK_BOT_TOKEN=xoxb-...
SLACK_SIGNING_SECRET=...
CONCERTO_DB_PATH=./concerto.db
HOST=127.0.0.1
PORT=8000
```

## Persistence
- State is persisted in SQLite (links, ticket holders, and interested users).
- Default database path is `./concerto.db` and can be overridden via `CONCERTO_DB_PATH`.

## Run
```bash
uv run python -m concerto
```

## Slack app setup
- Enable **Event Subscriptions** and set Request URL to `/slack/events`
- Add a **Slash Command**:
  - Command: `/concerto`
  - Request URL: `/slack/commands`
  - Usage hint: `rebuild`
- Subscribe to bot events:
  - `message.channels`
  - `message.groups`
  - `member_joined_channel`
  - `reaction_added`
  - `reaction_removed`
- Add OAuth scopes:
  - `channels:history`
  - `groups:history`
  - `reactions:read`
- Install the app to your workspace and invite it to channels you want to track
