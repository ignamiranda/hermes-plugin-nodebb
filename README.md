# hermes-plugin-nodebb

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) platform
plugin that adds a [NodeBB](https://nodebb.org) forum as a messaging
channel, the way Telegram or Discord work in Hermes. Direct chat comes
first; forum threads, notifications, and the rest of the NodeBB surface
are staged as later milestones (see the issue tracker).

Status: **Milestone 1 (chat)** — login, socket listener, inbound DM
pipeline, outbound replies, and offline catch-up are implemented, with a
fake-NodeBB test suite. Live verification is the proof test in the
tracker.

## Install

```sh
hermes plugins install ignamiranda/hermes-plugin-nodebb --enable
```

Requires Hermes Agent with the `register_platform` plugin API (stable
since v0.12.0). Python dependencies: `httpx` and `python-socketio`
(the plugin's requirement check reports if they are missing).

## Configure

The account Hermes acts as needs **both** a password and a Write API
user token:

- **Username + password** — NodeBB's Socket.IO connection authenticates
  with a *session login*, exactly like a browser. The plugin logs in
  with the account's username and password to get that session.
- **Write API user token** — used for REST calls (reading and sending
  chat messages). Create one in the account's settings on the forum,
  or have an admin generate it.

> A token alone is **not** enough: NodeBB sockets have no token
> authentication path, so the password is required for the live
> connection. This is the first thing to check if the platform installs
> but never connects.

Environment variables:

| Variable | Required | Purpose |
| --- | --- | --- |
| `NODEBB_URL` | yes | Base URL of the forum, e.g. `https://forum.example.com/nodebb` |
| `NODEBB_USERNAME` | yes | The account Hermes acts as |
| `NODEBB_PASSWORD` | yes | That account's password (session login) |
| `NODEBB_TOKEN` | no | Write API user token for REST calls |
| `NODEBB_ALLOWED_USERS` | no | Comma-separated uids allowed to talk to Hermes |
| `NODEBB_HOME_CHANNEL` | no | Chat room id for cron delivery |

Or the equivalent `config.yaml` shape:

```yaml
gateway:
  platforms:
    nodebb:
      enabled: true
      extra:
        base_url: https://forum.example.com/nodebb
        username: hermes
        password: "..."
        token: "..."
        allowed_users: "1"
```

### Who gets answered

Access is **default-deny**: with no allowlist configured, Hermes
answers nobody. Set `NODEBB_ALLOWED_USERS` to the uid(s) that may talk
to it — for a personal deployment, just your own uid. Find a uid in the
forum URL of a profile page, or via `GET /api/config` while logged in.

## How it works

- **Login** — `GET /api/config` for a CSRF token and cookies, then
  `POST /api/v3/utilities/login`. The session is refreshed automatically
  when NodeBB reports it invalid.
- **Listening** — a Socket.IO connection (session cookies, not the
  token) receives `event:chats.receive`. The bot's own messages and
  reconnect replays are filtered out.
- **Inbound** — messages pass the allowlist, are converted from
  NodeBB's HTML storage to plain text, and enter Hermes as one session
  per chat room.
- **Outbound** — replies are sent over REST; messages longer than
  NodeBB's 1,000-character chat cap are split, and sends are paced to
  NodeBB's per-room message delay.
- **Offline catch-up** — per-room watermarks are persisted, so messages
  sent while Hermes was down are replayed, oldest first, on reconnect.
  A room's first run only seeds its watermark — history is never
  flooded in.

## Development

```sh
pip install httpx python-socketio aiohttp pytest
python -m pytest tests/ -q
```

The suite runs against a fake in-process NodeBB (`tests/fake_nodebb.py`)
that speaks the real API shapes — no live forum needed. The adapter
helper tests additionally need a Hermes install importable and skip
elsewhere.

## License

MIT
